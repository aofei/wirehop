package relay

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestMeasurementCoordinatorReplyQueueFull(t *testing.T) {
	for _, tt := range []struct {
		name      string
		targets   int
		failed    protocol.LaneID
		goodFirst bool
	}{
		{name: "OnlyTarget", targets: 1, failed: 1},
		{name: "RejectedRootFirst", targets: 2, failed: 1},
		{name: "RejectedSecondaryFirst", targets: 2, failed: 2},
		{name: "ExposedReplyBeforeRejectedRoot", targets: 2, failed: 1, goodFirst: true},
		{name: "ExposedReplyBeforeRejectedSecondary", targets: 2, failed: 2, goodFirst: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, tt.targets)
				server := newMeasurementFixture(t, false, tt.targets)
				for index := range tt.targets {
					client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: protocol.LaneID(index + 1), Generation: 1})
				}
				client.apply(t)
				for range tt.targets {
					client.write(t, 1).deliver(t, server, 1)
					server.apply(t)
				}
				for range tt.targets {
					server.write(t, 1).deliver(t, client, 1)
				}
				probes := make(map[protocol.LaneID]measurementTransmission)
				for range tt.targets {
					probe := client.write(t, 100)
					probes[probe.source.LaneID] = probe
				}
				attempt := server.scheduler.measurements.attempts[tt.failed]
				deadline := server.scheduler.measurementDeadline
				peer := server.scheduler.measurements.peers[tt.failed]
				refused := 0
				peer.SendMeasurement = func(MeasurementWrite) bool {
					refused++
					return false
				}
				server.scheduler.measurements.register(peer)
				var good protocol.LaneID
				var reply measurementTransmission
				for id, probe := range probes {
					if id == tt.failed {
						continue
					}
					good = id
					if tt.goodFirst {
						probe.deliver(t, server, 200)
						reply = server.write(t, 200)
					}
				}
				probes[tt.failed].deliver(t, server, 200)
				if refused != 1 || !attempt.finished {
					t.Fatal("an unqueued Reply retained a target with no exposed hold promise")
				}
				server.apply(t)
				if tt.targets > 1 {
					if !server.paused() || server.scheduler.measurementDeadline != deadline {
						t.Fatal("refusing one Reply released or extended another target's quiet promise")
					}
					if !tt.goodFirst {
						probes[good].deliver(t, server, 200)
						reply = server.write(t, 200)
					}
					reply.deliver(t, client, 300)
					client.apply(t)
					result := client.write(t, 400)
					if !result.value.Valid {
						t.Fatal("refusing another target's Reply rejected valid independent timing")
					}
					result.sent()
					client.apply(t)
					result.deliver(t, server, 500)
					server.apply(t)
					if server.lanes[good].minimumRTTMicros != 200 {
						t.Fatal("the remaining target did not apply its actual measured RTT")
					}
				}
				if server.paused() || server.scheduler.measurements.active != nil ||
					server.scheduler.pendingMeasurementDrain.Load() || server.scheduler.input.pauseOwner != nil {
					t.Fatal("completed or unqueued replies retained session pause ownership")
				}
				if server.lanes[tt.failed].minimumRTTMicros != 10_000 || !client.paused() {
					t.Fatal("an unqueued server Reply applied timing or retracted the client's independent Probe promise")
				}
				time.Sleep(time.Until(client.scheduler.measurementDeadline))
				client.apply(t)
				if client.paused() {
					t.Fatal("the missing Reply extended the client's fixed pause")
				}
			})
		})
	}
}

func TestMeasurementCoordinatorResultQueueFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 1)
		server := newMeasurementFixture(t, false, 1)
		prepareMeasurement(t, client, server)
		client.write(t, 100).deliver(t, server, 200)
		reply := server.write(t, 200)
		for range cap(client.writes) {
			client.writes <- measurementQueued{}
		}
		reply.deliver(t, client, 300)
		client.apply(t)
		attempt := client.scheduler.measurements.active
		deadline := client.scheduler.measurementDeadline
		if attempt == nil || attempt.finished || attempt.stage != protocol.MeasurementReply || !client.paused() ||
			client.lanes[1].minimumRTTMicros != 200 {
			t.Fatal("an unqueued Result retracted the exposed Probe promise or lost validated local timing")
		}
		time.Sleep(time.Until(deadline) - time.Nanosecond)
		client.apply(t)
		if !client.paused() || client.scheduler.measurementDeadline != deadline {
			t.Fatal("Result queue refusal released or extended the independent Probe promise")
		}
		time.Sleep(time.Nanosecond)
		client.apply(t)
		if client.paused() || client.scheduler.measurements.active != nil || client.scheduler.pendingMeasurementDrain.Load() {
			t.Fatal("an unqueued Result extended the client's fixed pause")
		}
	})
}

func TestMeasurementCoordinatorUnexposedQueueFull(t *testing.T) {
	for _, tt := range []struct {
		name      string
		phase     protocol.MeasurementKind
		targets   int
		failed    protocol.LaneID
		earlyRoot bool
	}{
		{name: "BeginOnlyTarget", phase: protocol.MeasurementBegin, targets: 1, failed: 1},
		{name: "BeginSecondaryTarget", phase: protocol.MeasurementBegin, targets: 2, failed: 2},
		{name: "ReadyOnlyTarget", phase: protocol.MeasurementReady, targets: 1, failed: 1},
		{name: "ReadyRootTarget", phase: protocol.MeasurementReady, targets: 2, failed: 1},
		{name: "ReadyRootBeforeSiblingEnrollment", phase: protocol.MeasurementReady, targets: 2, failed: 1, earlyRoot: true},
		{name: "ReadySecondaryTarget", phase: protocol.MeasurementReady, targets: 2, failed: 2},
		{name: "ProbeOnlyTarget", phase: protocol.MeasurementProbe, targets: 1, failed: 1},
		{name: "ProbeRootTarget", phase: protocol.MeasurementProbe, targets: 2, failed: 1},
		{name: "ProbeSecondaryTarget", phase: protocol.MeasurementProbe, targets: 2, failed: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newMeasurementFixture(t, true, tt.targets)
				server := newMeasurementFixture(t, false, tt.targets)
				rejecting := client
				if tt.phase == protocol.MeasurementReady {
					rejecting = server
				}
				peer := rejecting.lanes[tt.failed].registration
				queue := peer.SendMeasurement
				blocked := tt.phase != protocol.MeasurementProbe
				refused := 0
				peer.SendMeasurement = func(request MeasurementWrite) bool {
					if blocked {
						refused++
						return false
					}
					return queue(request)
				}
				rejecting.scheduler.measurements.register(peer)
				for index := range tt.targets {
					client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: protocol.LaneID(index + 1), Generation: 1})
				}
				client.apply(t)
				clientDeadline := client.scheduler.measurementDeadline
				if tt.phase == protocol.MeasurementBegin {
					blocked = false
				}
				for len(client.writes) > 0 {
					begin := client.write(t, 1)
					if begin.value.Kind != protocol.MeasurementBegin {
						t.Fatal("preparation queued timing before server readiness")
					}
					begin.deliver(t, server, 1)
					if tt.earlyRoot {
						server.apply(t)
					}
				}
				server.apply(t)
				serverDeadline := server.scheduler.measurementDeadline
				if tt.phase == protocol.MeasurementProbe {
					blocked = true
				}
				for len(server.writes) > 0 {
					ready := server.write(t, 1)
					if ready.value.Kind != protocol.MeasurementReady {
						t.Fatal("the server queued a reply before receiving a probe")
					}
					ready.deliver(t, client, 1)
				}
				blocked = false
				client.apply(t)
				server.apply(t)
				if refused != 1 {
					t.Fatalf("refused phase count = %d, want 1", refused)
				}
				completed := 0
				for len(client.writes) > 0 {
					probe := client.write(t, 100)
					if probe.value.Kind != protocol.MeasurementProbe || probe.source.LaneID == tt.failed {
						t.Fatal("an unqueued or unprepared target acquired a timed probe")
					}
					probe.deliver(t, server, 200)
					server.write(t, 200).deliver(t, client, 300)
					client.apply(t)
					result := client.write(t, 400)
					if !result.value.Valid || result.source != probe.source {
						t.Fatal("queue refusal lost an independent carrier's valid timing")
					}
					result.sent()
					client.apply(t)
					result.deliver(t, server, 500)
					server.apply(t)
					completed++
				}
				wantCompleted := 0
				if tt.targets > 1 && tt.phase != protocol.MeasurementReady {
					wantCompleted = 1
				}
				if completed != wantCompleted {
					t.Fatalf("completed measurements = %d, want %d", completed, wantCompleted)
				}
				assertState := func(endpoint *measurementFixture, paused bool) {
					t.Helper()
					if (endpoint.scheduler.measurements.active != nil) != paused ||
						endpoint.scheduler.pendingMeasurementDrain.Load() != paused ||
						(endpoint.scheduler.input.pauseOwner != nil) != paused {
						t.Fatal("queue refusal left inconsistent session ownership")
					}
					for _, lane := range endpoint.lanes {
						store := lane.registration.Store
						if store.measurementDrain.Load() != paused || store.measurementSupplyPaused != paused {
							t.Fatal("queue refusal retained an unexposed pause or retracted another target's promise")
						}
						want := uint64(10_000)
						if wantCompleted > 0 && lane.registration.LaneID != tt.failed {
							want = 200
						}
						if lane.minimumRTTMicros != want {
							t.Fatal("queue refusal applied missing timing or lost independent evidence")
						}
					}
				}
				clientPaused := tt.phase == protocol.MeasurementReady
				serverPaused := tt.phase == protocol.MeasurementProbe ||
					tt.phase == protocol.MeasurementReady && tt.targets > 1 && !tt.earlyRoot
				assertState(client, clientPaused)
				assertState(server, serverPaused)
				if client.scheduler.measurementDeadline != clientDeadline || server.scheduler.measurementDeadline != serverDeadline {
					t.Fatal("a refused queue phase extended the captured deadline")
				}
				if clientPaused {
					time.Sleep(time.Until(clientDeadline) - time.Nanosecond)
					client.apply(t)
					assertState(client, true)
					time.Sleep(time.Nanosecond)
					client.apply(t)
					assertState(client, false)
				}
				if !time.Now().Before(client.scheduler.measurements.nextAt) {
					t.Fatal("the fixture did not check the client's active cooldown")
				}
				client.scheduler.RequestMeasurement(client.target)
				client.apply(t)
				if client.scheduler.measurements.active != nil || len(client.writes) != 0 {
					t.Fatal("queue refusal bypassed the remaining cost governor")
				}
				if tt.phase == protocol.MeasurementProbe {
					pending := server.scheduler.measurements.active
					t.Cleanup(pending.cancel)
					time.Sleep(time.Until(client.scheduler.measurements.nextAt))
					client.scheduler.RequestMeasurement(client.target)
					client.apply(t)
					begin := client.write(t, 1000)
					if begin.value.Kind != protocol.MeasurementBegin || begin.value.ID != 2 {
						t.Fatal("a cooled-down retry did not acquire an independent operation")
					}
					begin.deliver(t, server, 1000)
					server.apply(t)
					declined := server.write(t, 1000)
					if declined.value.Kind != protocol.MeasurementReady || declined.value.Valid || declined.value.ID != 2 {
						t.Fatal("a busy server did not reject the new operation independently")
					}
					declined.deliver(t, client, 1000)
					client.apply(t)
					assertState(client, false)
					assertState(server, true)
					if server.scheduler.measurements.active.id != 1 || server.scheduler.measurementDeadline != serverDeadline {
						t.Fatal("a later retry replaced or extended the server's existing operation")
					}
				}
				if serverPaused {
					time.Sleep(time.Until(serverDeadline) - time.Nanosecond)
					server.apply(t)
					assertState(server, true)
					time.Sleep(time.Nanosecond)
					server.apply(t)
					assertState(server, false)
				}
				if len(client.writes) != 0 || len(server.writes) != 0 {
					t.Fatal("a refused phase left an unexpected write queued")
				}
			})
		})
	}
}
