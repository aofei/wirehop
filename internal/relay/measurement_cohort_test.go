package relay

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
)

func TestMeasurementCoordinatorConcurrentTargets(t *testing.T) {
	for _, tt := range []struct {
		name       string
		drainDelay time.Duration
	}{
		{name: "OppositePrimariesAfterRTTIncrease"},
		{name: "DelayedOppositePrimaryPrefix", drainDelay: 1200 * time.Millisecond},
		{name: "MultipleFeedbackRoundTrips", drainDelay: 3600 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testMeasurementConcurrentPrimaries(t, tt.drainDelay)
		})
	}

	t.Run("LateTargetCannotExpandTimedCohort", testMeasurementLateTarget)

	for _, tt := range []struct {
		name    string
		targets int
	}{
		{name: "UnqueuedPrimaryHintDoesNotBlockRoot", targets: 1},
		{name: "UnqueuedPrimaryHintAllowsClientEnrollment", targets: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 2)
				server := newMeasurementFixture(t, false, 2)
				server.preferred = 2
				primary := server.lanes[2].registration
				queue := primary.SendMeasurement
				failed := false
				primary.SendMeasurement = func(request MeasurementWrite) bool {
					if !failed {
						failed = true
						return false
					}
					return queue(request)
				}
				server.scheduler.measurements.register(primary)
				for index := range tt.targets {
					client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: protocol.LaneID(index + 1), Generation: 1})
				}
				client.apply(t)
				client.write(t, 1).deliver(t, server, 1)
				server.apply(t)
				server.write(t, 1).deliver(t, client, 1)
				if tt.targets == 2 {
					client.write(t, 1).deliver(t, server, 1)
					server.apply(t)
					server.write(t, 1).deliver(t, client, 1)
				}
				var probes []measurementTransmission
				for range tt.targets {
					probes = append(probes, client.write(t, 100))
				}
				for _, probe := range probes {
					probe.deliver(t, server, 200)
					server.write(t, 200).deliver(t, client, 300)
					client.apply(t)
					result := client.write(t, 400)
					if !result.value.Valid {
						t.Fatal("unqueued primary hint invalidated a carrier's measurement")
					}
					result.sent()
					client.apply(t)
					result.deliver(t, server, 500)
					server.apply(t)
				}
				for _, endpoint := range []*measurementFixture{client, server} {
					for laneID, lane := range endpoint.lanes {
						want := uint64(10_000)
						if int(laneID) <= tt.targets {
							want = 200
						}
						if endpoint.paused() || lane.minimumRTTMicros != want {
							t.Fatal("unqueued primary hint retained ownership or lost an independent carrier's RTT")
						}
					}
				}
			})
		})
	}

	for _, tt := range []struct {
		name          string
		rejectRoot    bool
		rejectedFirst bool
		pendingPrefix bool
	}{
		{name: "ReadyTargetBeforeRejectedMember"},
		{name: "RejectedMemberBeforeReadyTarget", rejectedFirst: true},
		{name: "ReadyTargetBeforeRejectedRoot", rejectRoot: true},
		{name: "RejectedRootBeforeReadyTarget", rejectRoot: true, rejectedFirst: true},
		{name: "RejectedMemberBeforePrefixProof", pendingPrefix: true},
		{name: "RejectedRootBeforePrefixProof", rejectRoot: true, pendingPrefix: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 2)
				other := protocol.LaneGeneration{LaneID: 2, Generation: 1}
				if tt.pendingPrefix {
					client.commit(t, 1)
				}
				client.scheduler.RequestMeasurement(client.target)
				client.scheduler.RequestMeasurement(other)
				client.apply(t)
				deadline := client.scheduler.measurementDeadline
				begin := client.write(t, 1)
				client.write(t, 1)
				ready, rejected := client.target, other
				if tt.rejectRoot {
					ready, rejected = rejected, ready
				}
				accept := func() {
					client.receive(t, ready, protocol.Measurement{Kind: protocol.MeasurementReady, ID: begin.value.ID, Valid: true}, 2)
				}
				decline := func() {
					client.receive(t, rejected, protocol.Measurement{Kind: protocol.MeasurementReady, ID: begin.value.ID}, 2)
				}
				if tt.rejectedFirst {
					decline()
					accept()
				} else {
					accept()
					decline()
				}
				if tt.pendingPrefix {
					if len(client.writes) != 0 {
						t.Fatal("partial rejection bypassed the committed-prefix proof")
					}
					client.acknowledge(t, 1)
				}
				probe := client.write(t, 100)
				if probe.source != ready || probe.value.Kind != protocol.MeasurementProbe || len(client.writes) != 0 {
					t.Fatal("rejected carrier blocked or acquired the remaining target's probe")
				}
				client.receive(t, ready, protocol.Measurement{
					Kind: protocol.MeasurementReply, ID: begin.value.ID, ProbeMicros: 100,
					ReceiveMicros: 200, ReplyMicros: 200, HoldMicros: 1000,
				}, 300)
				client.apply(t)
				result := client.write(t, 400)
				if !result.value.Valid || result.source != ready || client.scheduler.measurementDeadline != deadline ||
					client.lanes[ready.LaneID].minimumRTTMicros != 200 || client.lanes[rejected.LaneID].minimumRTTMicros != 10_000 {
					t.Fatal("partial rejection lost independent timing or changed the fixed deadline")
				}
				result.sent()
				client.apply(t)
				if client.paused() || client.scheduler.measurements.active != nil {
					t.Fatal("partial rejection retained the completed session pause")
				}
			})
		})
	}

	t.Run("PrimaryHintBeforeSessionReady", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, 2)
			server := newMeasurementFixture(t, false, 2)
			client.lanes[1].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
			server.preferred = 2
			client.scheduler.RequestMeasurement(client.target)
			client.apply(t)
			client.write(t, 100).deliver(t, server, 200)
			server.apply(t)
			hint := server.write(t, 250)
			if hint.value.Kind != protocol.MeasurementRequest || len(server.writes) != 0 {
				t.Fatal("session readiness preceded client enrollment of the server primary")
			}
			hint.deliver(t, client, 400)
			begin := client.write(t, 500)
			if begin.value.Kind != protocol.MeasurementBegin || begin.source != hint.source {
				t.Fatal("primary enrollment did not precede its own probe")
			}
			early := protocol.Measurement{Kind: protocol.MeasurementProbe, ID: begin.value.ID, ProbeMicros: 550, HoldMicros: 1000}
			if err := server.scheduler.ObserveMeasurement(begin.source, early, 550); err != ErrUnexpectedFrame {
				t.Fatalf("primary Probe before its Begin returned %v, want protocol error", err)
			}
			begin.deliver(t, server, 600)
			server.apply(t)
			for range 2 {
				server.write(t, 650).deliver(t, client, 800)
			}
			probes := []measurementTransmission{client.write(t, 550), client.write(t, 550)}
			var firstProbe, secondProbe measurementTransmission
			for _, probe := range probes {
				if probe.source == client.target {
					firstProbe = probe
				} else {
					secondProbe = probe
				}
			}
			firstProbe.deliver(t, server, 600)
			server.write(t, 650).deliver(t, client, 800)
			client.apply(t)
			result := client.write(t, 850)
			result.sent()
			client.apply(t)
			result.deliver(t, server, 900)
			server.apply(t)
			if !client.paused() || !server.paused() {
				t.Fatal("first completion released the other enrolled primary")
			}
			secondProbe.deliver(t, server, 1000)
			server.write(t, 1050).deliver(t, client, 1200)
			client.apply(t)
			result = client.write(t, 1250)
			if !result.value.Valid {
				t.Fatal("delayed primary Begin lost the session prefix proof")
			}
			result.sent()
			client.apply(t)
			result.deliver(t, server, 1300)
			server.apply(t)
			if client.paused() || server.paused() {
				t.Fatal("both primary results did not release the session pause")
			}
		})
	})

	t.Run("RootBeginsArriveOutOfOrder", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, 2)
			server := newMeasurementFixture(t, false, 2)
			client.lanes[1].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
			client.preferred = 2
			server.preferred = 2
			client.scheduler.RequestMeasurement(client.target)
			client.apply(t)
			firstBegin := client.write(t, 100)
			secondBegin := client.write(t, 100)
			secondBegin.deliver(t, server, 200)
			server.apply(t)
			server.write(t, 250).deliver(t, client, 400)
			synctest.Wait()
			if len(client.writes) != 0 {
				t.Fatal("timed probes started before the server acknowledged every enrolled carrier")
			}
			firstBegin.deliver(t, server, 500)
			server.apply(t)
			server.write(t, 550).deliver(t, client, 700)
			probes := []measurementTransmission{client.write(t, 800), client.write(t, 800)}
			for _, probe := range probes {
				probe.deliver(t, server, 1000)
				server.write(t, 1050).deliver(t, client, 1250)
				client.apply(t)
				result := client.write(t, 1300)
				result.sent()
				client.apply(t)
				result.deliver(t, server, 1500)
				server.apply(t)
			}
			if client.paused() || server.paused() {
				t.Fatal("reordered initial Begins retained session ownership after both results")
			}
		})
	})

	t.Run("PeerRequestWithStaleClientTiming", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, 1)
			server := newMeasurementFixture(t, false, 1)
			server.lanes[1].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
			server.scheduler.RequestMeasurement(server.target)
			server.write(t, 0).deliver(t, client, 0)
			client.apply(t)
			begin := client.write(t, 0)
			time.Sleep(600 * time.Millisecond)
			begin.deliver(t, server, 600_000)
			server.apply(t)
			ready := server.write(t, 600_000)
			time.Sleep(600 * time.Millisecond)
			ready.deliver(t, client, 1_200_000)
			probe := client.write(t, 1_200_000)
			time.Sleep(600 * time.Millisecond)
			probe.deliver(t, server, 1_800_000)
			reply := server.write(t, 1_800_000)
			time.Sleep(600 * time.Millisecond)
			reply.deliver(t, client, 2_400_000)
			client.apply(t)
			result := client.write(t, 2_400_000)
			if !result.value.Valid {
				t.Fatal("stale client RTT prematurely ended a peer-requested measurement")
			}
			result.sent()
			client.apply(t)
			time.Sleep(600 * time.Millisecond)
			result.deliver(t, server, 3_000_000)
			server.apply(t)
			for _, endpoint := range []*measurementFixture{client, server} {
				if endpoint.paused() || endpoint.lanes[1].minimumRTTMicros != 1_200_000 {
					t.Fatal("peer-requested RTT increase did not complete at both endpoints")
				}
			}
		})
	})

	t.Run("IndependentCarrierSamples", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, 2)
			server := newMeasurementFixture(t, false, 2)
			client.lanes[1].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
			client.preferred = 2
			client.scheduler.lastTransportAt = time.Now().Add(-time.Minute)
			client.scheduler.RequestMeasurement(client.target)
			client.apply(t)
			for range 2 {
				client.write(t, 1).deliver(t, server, 1)
			}
			server.apply(t)
			for range 2 {
				server.write(t, 1).deliver(t, client, 1)
			}
			probes := []measurementTransmission{client.write(t, 100_000), client.write(t, 200_000)}
			for _, probe := range probes {
				rtt := uint64(600_000)
				if probe.source.LaneID == 1 {
					rtt = 1_200_000
				}
				sent := probe.value.ProbeMicros
				probe.deliver(t, server, sent+600_000)
				server.write(t, sent+600_100).deliver(t, client, sent+rtt+100)
				client.apply(t)
				result := client.write(t, sent+rtt+200)
				if !result.value.Valid || result.source != probe.source {
					t.Fatal("carrier result lost its own timed exchange")
				}
				result.sent()
				client.apply(t)
				result.deliver(t, server, sent+rtt+300)
				server.apply(t)
				for _, endpoint := range []*measurementFixture{client, server} {
					if endpoint.lanes[probe.source.LaneID].minimumRTTMicros != rtt {
						t.Fatal("concurrent target inherited another carrier's RTT")
					}
				}
			}
		})
	})

	t.Run("MissingTargetCannotExtendPause", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, 2)
			server := newMeasurementFixture(t, false, 2)
			client.scheduler.RequestMeasurement(client.target)
			client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: 2, Generation: 1})
			client.apply(t)
			deadline := client.scheduler.measurementDeadline
			for range 2 {
				client.write(t, 1).deliver(t, server, 1)
			}
			server.apply(t)
			for range 2 {
				server.write(t, 1).deliver(t, client, 1)
			}
			probes := []measurementTransmission{client.write(t, 100_000), client.write(t, 100_000)}
			probe := probes[0]
			if probe.source != client.target {
				probe = probes[1]
			}
			probe.deliver(t, server, 500_200)
			reply := server.write(t, 500_250)
			reply.deliver(t, client, 100_450)
			client.apply(t)
			result := client.write(t, 100_500)
			if !result.value.Valid {
				t.Fatal("missing second target invalidated the first carrier's proof")
			}
			result.sent()
			client.apply(t)
			result.deliver(t, server, 500_700)
			server.apply(t)
			if !client.paused() || !server.paused() || client.scheduler.measurementDeadline != deadline {
				t.Fatal("completed first target released the remaining target or extended its budget")
			}
			reply.deliver(t, client, 100_450)
			result.deliver(t, server, 500_700)
			time.Sleep(time.Until(deadline))
			client.apply(t)
			if client.paused() || client.scheduler.measurements.active != nil {
				t.Fatal("missing target retained ownership beyond the original client deadline")
			}
			time.Sleep(time.Until(server.scheduler.measurementDeadline))
			server.apply(t)
			if server.paused() || server.scheduler.measurements.active != nil {
				t.Fatal("missing target retained the responder beyond the original operation")
			}
			for _, endpoint := range []*measurementFixture{client, server} {
				if endpoint.lanes[2].minimumRTTMicros != 10_000 {
					t.Fatal("missing target inherited another carrier's RTT")
				}
			}
		})
	})
}

func testMeasurementLateTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 2)
		server := newMeasurementFixture(t, false, 2)
		prepareMeasurement(t, client, server)
		deadline := client.scheduler.measurementDeadline
		probe := client.write(t, 100_000)
		server.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: 2, Generation: 1})
		server.write(t, 100_000).deliver(t, client, 100_100)
		if len(client.writes) != 0 || client.scheduler.measurementDeadline != deadline {
			t.Fatal("late primary request expanded or extended a timed cohort")
		}
		probe.deliver(t, server, 500_200)
		server.write(t, 500_250).deliver(t, client, 100_450)
		client.apply(t)
		result := client.write(t, 100_500)
		if !result.value.Valid {
			t.Fatal("late request invalidated the original carrier's proof")
		}
		result.sent()
		client.apply(t)
		result.deliver(t, server, 500_700)
		server.apply(t)
		for _, endpoint := range []*measurementFixture{client, server} {
			if endpoint.paused() || endpoint.lanes[2].minimumRTTMicros != 10_000 {
				t.Fatal("late request retained ownership or inherited another carrier's RTT")
			}
		}
	})
}

func testMeasurementConcurrentPrimaries(t *testing.T, drainDelay time.Duration) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		nowMicros := func() uint64 { return uint64(time.Since(started) / time.Microsecond) }
		client := newMeasurementFixture(t, true, 2)
		server := newMeasurementFixture(t, false, 2)
		client.lanes[1].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
		server.lanes[2].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
		server.preferred = 2
		server.scheduler.lastTransportAt = time.Now()
		client.commit(t, 1)
		client.scheduler.RequestMeasurement(client.target)
		client.apply(t)
		begin := client.write(t, 0)
		time.Sleep(600 * time.Millisecond)
		server.commit(t, 2)
		begin.deliver(t, server, nowMicros())
		server.apply(t)
		other := protocol.LaneGeneration{LaneID: 2, Generation: 1}
		hint := server.write(t, nowMicros())
		if hint.value.Kind != protocol.MeasurementRequest || hint.source != other {
			t.Fatal("active opposite primary did not join the shared measurement")
		}
		time.Sleep(600 * time.Millisecond)
		client.acknowledge(t, 1)
		hint.deliver(t, client, nowMicros())
		secondBegin := client.write(t, nowMicros())
		if secondBegin.source != other || secondBegin.value.ID != begin.value.ID ||
			client.scheduler.measurements.nextID != 1 {
			t.Fatal("concurrent carrier did not share the original session operation")
		}
		time.Sleep(600 * time.Millisecond)
		secondBegin.deliver(t, server, nowMicros())
		time.Sleep(drainDelay)
		server.acknowledge(t, 2)
		readies := []measurementTransmission{server.write(t, nowMicros()), server.write(t, nowMicros())}
		time.Sleep(600 * time.Millisecond)
		for _, ready := range readies {
			ready.deliver(t, client, nowMicros())
		}
		probes := []measurementTransmission{client.write(t, nowMicros()), client.write(t, nowMicros())}
		time.Sleep(600 * time.Millisecond)
		for _, frame := range probes {
			frame.deliver(t, server, nowMicros())
		}
		replies := []measurementTransmission{server.write(t, nowMicros()), server.write(t, nowMicros())}
		time.Sleep(600 * time.Millisecond)
		for _, frame := range replies {
			frame.deliver(t, client, nowMicros())
		}
		client.apply(t)
		results := []measurementTransmission{client.write(t, nowMicros()), client.write(t, nowMicros())}
		for index, frame := range results {
			if !frame.value.Valid || client.lanes[frame.source.LaneID].minimumRTTMicros != 1_200_000 {
				t.Fatal("concurrent measurement did not validate its actual carrier")
			}
			frame.sent()
			client.apply(t)
			if client.paused() != (index == 0) {
				t.Fatal("session pause did not retain ownership until every target completed")
			}
		}
		time.Sleep(600 * time.Millisecond)
		for index, frame := range results {
			frame.deliver(t, server, nowMicros())
			server.apply(t)
			if server.lanes[frame.source.LaneID].minimumRTTMicros != 1_200_000 ||
				server.paused() != (index == 0) {
				t.Fatal("server lost an independent carrier result or resumed before both completed")
			}
		}
		if len(client.scheduler.measurements.attempts) != 0 || len(server.scheduler.measurements.attempts) != 0 {
			t.Fatal("completed operation retained carrier membership")
		}
		if got := time.Until(server.scheduler.measurements.nextAt); got != (3600*time.Millisecond+drainDelay)*measurementQuietRatio {
			t.Fatalf("shared pause cooldown = %s, want one actual pause charge", got)
		}
	})
}
