package relay

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestMeasurementCoordinatorExchange(t *testing.T) {
	for _, count := range []int{1, 2} {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, count)
			server := newMeasurementFixture(t, false, count)
			client.scheduler.RequestMeasurement(client.target)
			client.apply(t)
			client.write(t, 1).deliver(t, server, 1)
			server.apply(t)
			server.write(t, 1).deliver(t, client, 1)
			client.write(t, 100_000).deliver(t, server, 500_200)
			server.write(t, 500_250).deliver(t, client, 100_450)
			synctest.Wait()
			if !client.paused() || !server.paused() || len(client.writes) != 0 {
				t.Fatal("result preceded controller publication")
			}
			client.apply(t)
			result := client.write(t, 100_500)
			if !result.value.Valid || result.value.ReceiveMicros != 100_450 || client.lanes[1].minimumRTTMicros != 400 {
				t.Fatal("client did not publish the actual target's four-timestamp result")
			}
			result.sent()
			client.apply(t)
			result.deliver(t, server, 500_700)
			synctest.Wait()
			if !server.paused() {
				t.Fatal("server resumed before applying the received result")
			}
			server.apply(t)
			if client.paused() || server.paused() {
				t.Fatal("completed carrier measurement retained Data gates")
			}
			for id, lane := range server.lanes {
				want := uint64(10_000)
				if id == 1 {
					want = 400
				}
				if lane.minimumRTTMicros != want {
					t.Fatal("measurement changed timing outside its actual carrier")
				}
			}
			client.scheduler.RequestMeasurement(client.target)
			client.apply(t)
			if client.scheduler.measurements.active != nil {
				t.Fatal("immediate request bypassed the cost governor")
			}
		})
	}
}

func TestMeasurementCoordinatorParallelPreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 1)
		server := newMeasurementFixture(t, false, 1)
		for _, endpoint := range []*measurementFixture{client, server} {
			endpoint.lanes[1].applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
		}
		client.commit(t, 1)
		client.scheduler.RequestMeasurement(client.target)
		client.apply(t)
		begin := client.write(t, 0)
		if begin.value.Kind != protocol.MeasurementBegin {
			t.Fatal("client waited for its own prefix before starting remote preparation")
		}
		time.Sleep(600 * time.Millisecond)
		server.commit(t, 1)
		begin.deliver(t, server, 600_000)
		server.apply(t)
		if len(server.writes) != 0 || !client.paused() || !server.paused() {
			t.Fatal("server announced readiness before its prefix was parsed")
		}
		time.Sleep(600 * time.Millisecond)
		client.acknowledge(t, 1)
		if len(client.writes) != 0 {
			t.Fatal("client probed before remote readiness")
		}
		time.Sleep(600 * time.Millisecond)
		server.acknowledge(t, 1)
		ready := server.write(t, 1_800_000)
		time.Sleep(600 * time.Millisecond)
		ready.deliver(t, client, 2_400_000)
		probe := client.write(t, 2_400_000)
		time.Sleep(600 * time.Millisecond)
		probe.deliver(t, server, 3_000_000)
		reply := server.write(t, 3_000_000)
		time.Sleep(600 * time.Millisecond)
		reply.deliver(t, client, 3_600_000)
		client.apply(t)
		result := client.write(t, 3_600_000)
		if !result.value.Valid || client.lanes[1].minimumRTTMicros != 1_200_000 {
			t.Fatal("parallel preparation could not measure a 1.2-second path within four seconds")
		}
		result.sent()
		client.apply(t)
		time.Sleep(600 * time.Millisecond)
		result.deliver(t, server, 4_200_000)
		server.apply(t)
		if client.paused() || server.paused() || server.lanes[1].minimumRTTMicros != 1_200_000 {
			t.Fatal("both endpoints did not finish the delayed measurement")
		}
	})
}

func TestMeasurementCoordinatorAbruptRTTIncrease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 1)
		server := newMeasurementFixture(t, false, 1)
		for _, endpoint := range []*measurementFixture{client, server} {
			lane := endpoint.lanes[1]
			lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 600_000}, false)
			if lane.minimumRTTMicros != 10_000 || lane.rttMicros != 83_750 || lane.latestRTTMicros != 600_000 {
				t.Fatal("ordinary RTT increase changed the minimum or bypassed operating RTT smoothing")
			}
		}
		client.scheduler.RequestMeasurement(client.target)
		client.apply(t)
		budget := time.Until(client.scheduler.measurementDeadline)
		if budget != 2500*time.Millisecond {
			t.Fatalf("first-step pause budget = %s, want 2.5 seconds", budget)
		}
		begin := client.write(t, 0)
		time.Sleep(300 * time.Millisecond)
		begin.deliver(t, server, 300_000)
		server.apply(t)
		ready := server.write(t, 300_000)
		time.Sleep(300 * time.Millisecond)
		ready.deliver(t, client, 600_000)
		probe := client.write(t, 600_000)
		time.Sleep(300 * time.Millisecond)
		probe.deliver(t, server, 900_000)
		reply := server.write(t, 900_000)
		time.Sleep(300 * time.Millisecond)
		reply.deliver(t, client, 1_200_000)
		if client.lanes[1].minimumRTTMicros != 10_000 || server.lanes[1].minimumRTTMicros != 10_000 {
			t.Fatal("unpublished measurement raised the minimum RTT")
		}
		client.apply(t)
		result := client.write(t, 1_200_000)
		if !result.value.Valid || client.lanes[1].minimumRTTMicros != 600_000 {
			t.Fatal("first measurement after an abrupt increase expired before validating the actual RTT")
		}
		result.sent()
		client.apply(t)
		time.Sleep(300 * time.Millisecond)
		result.deliver(t, server, 1_500_000)
		server.apply(t)
		if client.paused() || server.paused() || server.lanes[1].minimumRTTMicros != 600_000 {
			t.Fatal("first RTT-step measurement did not complete independently at both endpoints")
		}
		lane := client.lanes[1]
		low := clockmap.Sample{LocalReceiveMicros: 10_000}
		lane.applyTiming(low, false)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		_, proof := client.scheduler.beginMeasurementDrain(ctx, client.target, nil)
		client.apply(t)
		deadline, valid := proof()
		if !valid || time.Until(deadline) >= budget || lane.latestRTTMicros != 10_000 {
			t.Fatal("later low RTT retained the old step's larger pause budget")
		}
		cancel()
		client.apply(t)
		for range 32 {
			lane.applyTiming(low, false)
		}
		ctx, cancel = context.WithCancel(t.Context())
		defer cancel()
		_, proof = client.scheduler.beginMeasurementDrain(ctx, client.target, nil)
		client.apply(t)
		if deadline, valid := proof(); !valid || time.Until(deadline) != 250*time.Millisecond {
			t.Fatal("converged low RTT did not restore the minimum pause budget")
		}
	})
}

func TestMeasurementCoordinatorTimeout(t *testing.T) {
	for stop, phase := range []string{"QueuedBegin", "AwaitReady", "QueuedReady", "ExposedReady", "QueuedProbe", "ExposedProbe", "QueuedReply", "ExposedReply", "QueuedResult", "ExposedResult"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 1)
				server := newMeasurementFixture(t, false, 1)
				client.scheduler.RequestMeasurement(client.target)
				client.apply(t)
				var frame measurementTransmission
				for step := 1; step <= stop; step++ {
					switch step {
					case 1, 5, 9:
						frame = client.write(t, uint64(step*100))
					case 3, 7:
						frame = server.write(t, uint64(step*100))
					case 2, 6:
						frame.deliver(t, server, uint64(step*100))
						server.apply(t)
					case 4, 8:
						frame.deliver(t, client, uint64(step*100))
						client.apply(t)
					}
				}
				time.Sleep(measurementPauseDuration(client.lanes[1].rttMicros))
				for _, endpoint := range []*measurementFixture{client, server} {
					endpoint.apply(t)
					if endpoint.paused() || endpoint.scheduler.measurements.active != nil {
						t.Fatal("missing or ambiguously written phase extended the fixed deadline")
					}
					for len(endpoint.writes) > 0 {
						if value, valid := (<-endpoint.writes).request.Build(1000); valid {
							t.Fatalf("expired queued phase remained writable: %+v", value)
						}
					}
				}
			})
		})
	}
}

func TestMeasurementCoordinatorOwnership(t *testing.T) {
	t.Run("CoalescesConcurrentRequests", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			client := newMeasurementFixture(t, true, 2)
			for range 64 {
				client.scheduler.RequestMeasurement(client.target)
				client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: 2, Generation: 1})
			}
			client.apply(t)
			begin := client.write(t, 1)
			if len(client.writes) != 0 || begin.source != client.target || client.scheduler.measurements.nextID != 1 {
				t.Fatal("requests did not coalesce into one requesting carrier measurement")
			}
		})
	})
	t.Run("HintsDoNotPause", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			server := newMeasurementFixture(t, false, 1)
			for range 64 {
				server.scheduler.RequestMeasurement(server.target)
			}
			server.apply(t)
			hint := server.write(t, 1)
			if hint.value.Kind != protocol.MeasurementRequest || len(server.writes) != 0 || server.paused() {
				t.Fatal("server hint acquired a pause or escaped its throttle")
			}
			client := newMeasurementFixture(t, true, 1)
			hint.deliver(t, client, 1)
			client.apply(t)
			if client.write(t, 1).value.Kind != protocol.MeasurementBegin {
				t.Fatal("client did not own the requested operation")
			}
		})
	})
	t.Run("DuplicateBeginNeverExtendsDeadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			server := newMeasurementFixture(t, false, 1)
			server.lanes[1].rttMicros = 1_000_000
			begin := protocol.Measurement{Kind: protocol.MeasurementBegin, ID: 7}
			server.receive(t, server.target, begin, 1)
			server.apply(t)
			deadline := server.scheduler.measurementDeadline
			time.Sleep(time.Second)
			server.receive(t, server.target, begin, 2)
			server.apply(t)
			if server.scheduler.measurementDeadline != deadline || len(server.writes) != 1 {
				t.Fatal("duplicate preparation restarted or extended the operation")
			}
			time.Sleep(measurementPauseLimit - time.Second)
			server.apply(t)
			server.receive(t, server.target, begin, 3)
			server.apply(t)
			if server.paused() || server.scheduler.measurements.active != nil {
				t.Fatal("expired duplicate reacquired the pause")
			}
		})
	})
	t.Run("RevokedReadyPromiseSurvivesTargetLoss", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			server := newMeasurementFixture(t, false, 1)
			server.receive(t, server.target, protocol.Measurement{Kind: protocol.MeasurementBegin, ID: 1}, 1)
			server.apply(t)
			server.write(t, 100)
			server.scheduler.invalidateMeasurementDrain()
			server.scheduler.measurements.remove(1, 1)
			server.apply(t)
			if !server.paused() {
				t.Fatal("target loss retracted exposed readiness")
			}
			time.Sleep(measurementPauseLimit)
			server.apply(t)
			if server.paused() || server.lanes[1].minimumRTTMicros != 10_000 {
				t.Fatal("retired proof changed RTT or outlived its deadline")
			}
		})
	})
}

func TestMeasurementCoordinatorPublication(t *testing.T) {
	for _, test := range []struct {
		name       string
		invalidate bool
		full       bool
	}{
		{name: "ProofRevokedBeforeApplication", invalidate: true},
		{name: "SchedulerQueueFull", full: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 1)
				server := newMeasurementFixture(t, false, 1)
				prepareMeasurement(t, client, server)
				client.write(t, 100).deliver(t, server, 200)
				reply := server.write(t, 250)
				if test.full {
					for range cap(client.scheduler.events) {
						client.scheduler.events <- schedulerEvent{kind: schedulerTiming}
					}
				}
				reply.deliver(t, client, 400)
				synctest.Wait()
				if test.invalidate {
					client.scheduler.invalidateMeasurementDrain()
				}
				client.apply(t)
				result := client.write(t, 500)
				if result.value.Valid || client.lanes[1].minimumRTTMicros != 10_000 {
					t.Fatal("unapplied or revoked evidence was published as valid")
				}
				result.sent()
				client.apply(t)
				result.deliver(t, server, 600)
				server.apply(t)
				if client.paused() || server.paused() {
					t.Fatal("completed rejection did not release the pause")
				}
			})
		})
	}
}

func TestMeasurementCoordinatorRejectsWrongDirection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, client := range []bool{false, true} {
			fixture := newMeasurementFixture(t, client, 1)
			kind := protocol.MeasurementReady
			if client {
				kind = protocol.MeasurementBegin
			}
			err := fixture.scheduler.ObserveMeasurement(fixture.target, protocol.Measurement{Kind: kind, ID: 1}, 1)
			if !errors.Is(err, ErrUnexpectedFrame) || fixture.paused() {
				t.Fatalf("wrong direction: error %v, paused %t", err, fixture.paused())
			}
		}
	})
}

func TestMeasurementCoordinatorRejectsInvalidEvidence(t *testing.T) {
	for _, test := range []struct {
		name    string
		t4      uint64
		receive uint64
		reply   uint64
		hold    uint64
	}{
		{name: "NegativeSpan", t4: 99, receive: 200, reply: 200, hold: 1_000_000},
		{name: "ProcessingExceedsSpan", t4: 300, receive: 200, reply: 401, hold: 1_000_000},
		{name: "ZeroPeerHold", t4: 300, receive: 200, reply: 200},
		{name: "PeerHoldBoundary", t4: 300, receive: 200, reply: 200, hold: 200},
		{name: "TimestampOverflow", t4: 300, receive: math.MaxUint64, reply: math.MaxUint64, hold: 1_000_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 1)
				server := newMeasurementFixture(t, false, 1)
				prepareMeasurement(t, client, server)
				client.write(t, 100).deliver(t, server, 200)
				reply := server.write(t, 250)
				reply.value.ReceiveMicros = test.receive
				reply.value.ReplyMicros = test.reply
				reply.value.HoldMicros = test.hold
				reply.deliver(t, client, test.t4)
				client.apply(t)
				result := client.write(t, 400)
				if result.value.Valid || client.lanes[1].minimumRTTMicros != 10_000 {
					t.Fatal("invalid evidence changed the propagation minimum")
				}
				result.sent()
				client.apply(t)
				result.deliver(t, server, 500)
				server.apply(t)
				if client.paused() || server.paused() {
					t.Fatal("completed rejection retained the session pause")
				}
			})
		})
	}
}

func prepareMeasurement(t *testing.T, client, server *measurementFixture) {
	t.Helper()
	client.scheduler.RequestMeasurement(client.target)
	client.apply(t)
	client.write(t, 1).deliver(t, server, 1)
	server.apply(t)
	server.write(t, 1).deliver(t, client, 1)
}

type measurementFixture struct {
	scheduler *Scheduler
	lanes     map[protocol.LaneID]*scheduledLane
	target    protocol.LaneGeneration
	writes    chan measurementQueued
}

type measurementQueued struct {
	source  protocol.LaneGeneration
	request MeasurementWrite
}

type measurementTransmission struct {
	value  protocol.Measurement
	source protocol.LaneGeneration
	sent   func()
}

func (frame measurementTransmission) deliver(t *testing.T, destination *measurementFixture, now uint64) {
	t.Helper()
	destination.receive(t, frame.source, frame.value, now)
}

func newMeasurementFixture(t *testing.T, client bool, count int) *measurementFixture {
	t.Helper()
	ingress, scheduler := feedbackScheduler(t)
	f := &measurementFixture{scheduler: scheduler, target: protocol.LaneGeneration{LaneID: 1, Generation: 1},
		lanes: map[protocol.LaneID]*scheduledLane{}, writes: make(chan measurementQueued, 64)}
	for id := byte(1); int(id) <= count; id++ {
		lane := schedulerLane(t, id, id, 10_000, 1_000_000)
		lane.minimumRTTMicros = 10_000
		lane.registration.MeasurementInitiator = client
		lane.registration.SendMeasurement = func(request MeasurementWrite) bool {
			select {
			case f.writes <- measurementQueued{source: protocol.LaneGeneration{LaneID: protocol.LaneID(id), Generation: 1}, request: request}:
				return true
			default:
				return false
			}
		}
		f.lanes[protocol.LaneID(id)] = lane
		scheduler.measurements.register(lane.registration)
	}
	t.Cleanup(func() {
		scheduler.measurements.close()
		scheduler.updateMeasurementDrain(f.lanes)
		if scheduler.measurementTimer != nil {
			scheduler.measurementTimer.Stop()
		}
		for _, lane := range f.lanes {
			releaseTransmissions(lane.registration.Store.drain())
		}
		ingress.Close()
	})
	return f
}

func (f *measurementFixture) apply(t *testing.T) {
	t.Helper()
	for {
		synctest.Wait()
		select {
		case event := <-f.scheduler.events:
			var preferred protocol.LaneID
			f.scheduler.applyEvent(f.lanes, &preferred, event, time.Now())
		default:
			f.scheduler.updateMeasurementDrain(f.lanes)
			synctest.Wait()
			if len(f.scheduler.events) == 0 {
				return
			}
		}
	}
}

func (f *measurementFixture) paused() bool {
	for _, lane := range f.lanes {
		if !lane.registration.Store.measurementDrain.Load() {
			return false
		}
	}
	return true
}

func (f *measurementFixture) write(t *testing.T, now uint64) measurementTransmission {
	t.Helper()
	synctest.Wait()
	select {
	case queued := <-f.writes:
		value, valid := queued.request.Build(now)
		if !valid {
			t.Fatal("queued measurement expired unexpectedly")
		}
		if queued.request.Sent == nil {
			queued.request.Sent = func() {}
		}
		return measurementTransmission{value: value, source: queued.source, sent: queued.request.Sent}
	default:
		t.Fatal("expected phase was not queued")
		return measurementTransmission{}
	}
}

func (f *measurementFixture) receive(t *testing.T, source protocol.LaneGeneration, value protocol.Measurement, now uint64) {
	t.Helper()
	if err := f.scheduler.ObserveMeasurement(source, value, now); err != nil {
		t.Fatal(err)
	}
}

func (f *measurementFixture) commit(t *testing.T, id protocol.LaneID) {
	t.Helper()
	store := f.lanes[id].registration.Store
	if err := store.push(schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
}

func (f *measurementFixture) acknowledge(t *testing.T, id protocol.LaneID) {
	t.Helper()
	if _, _, err := f.lanes[id].registration.Store.acknowledge(1, uint64(time.Now().UnixMicro())); err != nil {
		t.Fatal(err)
	}
	f.apply(t)
}

func TestMeasurementCoordinatorPauseCost(t *testing.T) {
	for _, test := range []struct {
		name   string
		expire bool
	}{
		{name: "UnpausedExpiry", expire: true},
		{name: "UnpausedPreparationExcluded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 1)
				client.lanes[1].rttMicros = 500_000
				wait := time.Second
				if test.expire {
					wait = measurementPauseLimit
				}
				client.lanes[1].probeUntil = client.lanes[1].registration.Store.now().Add(wait)
				client.scheduler.RequestMeasurement(client.target)
				client.apply(t)
				if client.paused() {
					t.Fatal("discovery wait paused Data")
				}
				time.Sleep(wait)
				client.apply(t)
				expected := measurementRetryInterval
				if !test.expire {
					if !client.paused() {
						t.Fatal("completed discovery did not admit the pause")
					}
					time.Sleep(time.Second)
					client.scheduler.measurements.finish(client.scheduler.measurements.active)
					client.apply(t)
					expected = measurementQuietRatio * time.Second
				}
				if client.paused() || client.scheduler.measurements.active != nil {
					t.Fatal("released pause retained session ownership")
				}
				if remaining := time.Until(client.scheduler.measurements.nextAt); remaining != expected {
					t.Fatalf("cooldown = %s, want actual-pause cost %s", remaining, expected)
				}
			})
		})
	}
}
func TestMeasurementCoordinatorServerPreparesFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 1)
		server := newMeasurementFixture(t, false, 1)
		client.commit(t, 1)
		client.scheduler.RequestMeasurement(client.target)
		client.apply(t)
		client.write(t, 1).deliver(t, server, 1)
		server.apply(t)
		ready := server.write(t, 100)
		ready.deliver(t, client, 100)
		if ready.value.Kind != protocol.MeasurementReady || !ready.value.Valid || len(client.writes) != 0 {
			t.Fatal("remote readiness bypassed the client's unparsed prefix")
		}
		client.acknowledge(t, 1)
		client.write(t, 200).deliver(t, server, 300)
		server.write(t, 350).deliver(t, client, 500)
		client.apply(t)
		result := client.write(t, 600)
		result.sent()
		client.apply(t)
		result.deliver(t, server, 700)
		server.apply(t)
		if !result.value.Valid || client.paused() || server.paused() ||
			client.lanes[1].minimumRTTMicros != 250 || server.lanes[1].minimumRTTMicros != 250 {
			t.Fatal("server-first preparation did not publish and finish the same carrier sample")
		}
	})
}

func TestMeasurementCoordinatorIgnoresUnrelatedStandby(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 2)
		server := newMeasurementFixture(t, false, 2)
		for _, endpoint := range []*measurementFixture{client, server} {
			standby := endpoint.lanes[2]
			standby.rttMicros = 12_000_000
			standby.registration.SendMeasurement = func(MeasurementWrite) bool {
				t.Error("unrelated empty standby received a measurement phase")
				return false
			}
			endpoint.scheduler.measurements.register(standby.registration)
		}
		prepareMeasurement(t, client, server)
		client.write(t, 100).deliver(t, server, 200)
		server.write(t, 250).deliver(t, client, 400)
		client.apply(t)
		result := client.write(t, 500)
		if !result.value.Valid {
			t.Fatal("unrelated standby prevented the target measurement")
		}
		result.sent()
		client.apply(t)
		result.deliver(t, server, 600)
		server.apply(t)
		if client.paused() || server.paused() || client.lanes[2].minimumRTTMicros != 10_000 || server.lanes[2].minimumRTTMicros != 10_000 {
			t.Fatal("unrelated standby prolonged the hold or acquired target timing")
		}
	})
}

func TestMeasurementCoordinatorIgnoresWrongCarrierPhases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 2)
		server := newMeasurementFixture(t, false, 2)
		unrelated := protocol.LaneGeneration{LaneID: 2, Generation: 1}
		ignore := func(endpoint *measurementFixture, value protocol.Measurement, now uint64) {
			t.Helper()
			attempt := endpoint.scheduler.measurements.active
			stage := attempt.stage
			writes := len(endpoint.writes)
			endpoint.receive(t, unrelated, value, now)
			endpoint.apply(t)
			if endpoint.scheduler.measurements.active != attempt || !endpoint.paused() || attempt.stage != stage ||
				len(endpoint.writes) != writes || endpoint.lanes[1].minimumRTTMicros != 10_000 {
				t.Fatal("wrong-carrier phase changed the active hold, phase, or timing")
			}
		}
		client.scheduler.RequestMeasurement(client.target)
		client.apply(t)
		client.write(t, 1).deliver(t, server, 1)
		server.apply(t)
		ready := server.write(t, 1)
		ignore(client, protocol.Measurement{Kind: protocol.MeasurementReady, ID: ready.value.ID}, 1)
		ready.deliver(t, client, 1)
		probe := client.write(t, 100)
		ignore(server, probe.value, 200)
		probe.deliver(t, server, 200)
		reply := server.write(t, 250)
		ignore(client, reply.value, 400)
		reply.deliver(t, client, 400)
		client.apply(t)
		result := client.write(t, 500)
		ignore(server, protocol.Measurement{Kind: protocol.MeasurementResult, ID: result.value.ID}, 600)
		result.sent()
		client.apply(t)
		result.deliver(t, server, 600)
		server.apply(t)
		if client.paused() || server.paused() || client.lanes[1].minimumRTTMicros != 250 || server.lanes[1].minimumRTTMicros != 250 {
			t.Fatal("actual target phases failed after unrelated late phases")
		}
	})
}

func TestMeasurementCoordinatorRequiresUnrelatedCommittedPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 2)
		server := newMeasurementFixture(t, false, 2)
		client.lanes[1].rttMicros = 300_000
		server.lanes[1].rttMicros = 300_000
		client.commit(t, 2)
		client.scheduler.RequestMeasurement(client.target)
		client.apply(t)
		client.write(t, 1).deliver(t, server, 1)
		server.apply(t)
		server.write(t, 1).deliver(t, client, 1)
		client.apply(t)
		time.Sleep(time.Second)
		client.apply(t)
		if len(client.writes) != 0 || !client.paused() || !server.paused() {
			t.Fatal("single-target measurement bypassed an unrelated committed prefix")
		}
		client.acknowledge(t, 2)
		client.write(t, 1_000_100).deliver(t, server, 1_000_200)
		server.write(t, 1_000_250).deliver(t, client, 1_000_400)
		client.apply(t)
		result := client.write(t, 1_000_500)
		if !result.value.Valid {
			t.Fatal("parsed unrelated prefix did not permit the target measurement")
		}
		result.sent()
		client.apply(t)
		result.deliver(t, server, 1_000_600)
		server.apply(t)
		if client.paused() || server.paused() {
			t.Fatal("completed target measurement retained the session pause")
		}
	})
}

func TestMeasurementCoordinatorRejectsDuplicateCurrentPhases(t *testing.T) {
	for _, tt := range []struct {
		name string
		kind protocol.MeasurementKind
	}{
		{name: "Ready", kind: protocol.MeasurementReady},
		{name: "Probe", kind: protocol.MeasurementProbe},
		{name: "Reply", kind: protocol.MeasurementReply},
		{name: "Result", kind: protocol.MeasurementResult},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, 1)
				server := newMeasurementFixture(t, false, 1)
				prepareMeasurement(t, client, server)
				frame := measurementTransmission{source: client.target,
					value: protocol.Measurement{Kind: protocol.MeasurementReady, ID: client.scheduler.measurements.active.id, Valid: true}}
				destination := client
				if tt.kind >= protocol.MeasurementProbe {
					frame = client.write(t, 100)
					frame.deliver(t, server, 200)
					destination = server
				}
				if tt.kind >= protocol.MeasurementReply {
					frame = server.write(t, 250)
					frame.deliver(t, client, 400)
					destination = client
				}
				if tt.kind >= protocol.MeasurementResult {
					client.apply(t)
					frame = client.write(t, 500)
					frame.deliver(t, server, 600)
					destination = server
				}
				if err := destination.scheduler.ObserveMeasurement(frame.source, frame.value, 700); !errors.Is(err, ErrUnexpectedFrame) {
					t.Fatalf("duplicate current phase returned %v, want protocol error", err)
				}
			})
		})
	}
}
