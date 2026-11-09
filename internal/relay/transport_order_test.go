package relay

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerTransportLaneOrderMembership(t *testing.T) {
	for _, action := range []string{"Register", "Replace", "Remove", "RemoveLast", "StaleRegister", "StaleRemove"} {
		t.Run(action, func(t *testing.T) {
			now := time.UnixMicro(1 << 40)
			ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 8, Bytes: 65536}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer ingress.Close()
			scheduler, err := NewScheduler(&Ingress{queue: ingress})
			if err != nil {
				t.Fatal(err)
			}
			scheduler.lastTransportAt = now
			primary := schedulerLane(t, 1, 1, 10_000, 1_000_000)
			previous := schedulerLane(t, 2, 2, 12_000, 100_000)
			lanes := map[protocol.LaneID]*scheduledLane{1: primary}
			if action != "Register" && action != "RemoveLast" {
				lanes[2] = previous
			}
			for _, lane := range []*scheduledLane{primary, previous} {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			if got := scheduler.selectTransportCandidates(lanes, 1, 1000, math.MaxUint64, now); got.count != 1 || got.lanes[0] != primary {
				t.Fatal("fixture did not preserve the initial primary")
			}
			scheduler.orderedControlLanes(lanes)
			store, err := newTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 65536}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			registration := schedulerRegistration(2, 2, store)
			result := make(chan error, 1)
			event := schedulerEvent{kind: schedulerRegister, registration: registration, result: result}
			var wantErr error
			want := primary
			switch action {
			case "Replace":
				event.registration.Generation = 2
			case "Remove", "StaleRemove":
				event = schedulerEvent{kind: schedulerRemove, laneID: 2, generation: 1, result: result}
				if action == "StaleRemove" {
					event.generation = 2
				}
			case "RemoveLast":
				event = schedulerEvent{kind: schedulerRemove, laneID: 1, generation: 1, result: result}
				previous = primary
				want = nil
			case "StaleRegister":
				wantErr = ErrStaleLane
			}
			preferred := protocol.LaneID(1)
			scheduler.applyEvent(lanes, &preferred, event, now, false)
			if err := <-result; !errors.Is(err, wantErr) {
				t.Fatalf("membership event = %v, want %v", err, wantErr)
			}
			if action == "Register" || action == "Replace" {
				want = lanes[2]
				want.rttMicros = 12_000
				want.deliveryRate = 2_000_000
				want.rateObserved = true
			}
			if action == "Replace" || action == "Remove" || action == "RemoveLast" {
				for _, lane := range scheduler.transportOrder[:cap(scheduler.transportOrder)] {
					if lane == previous {
						t.Fatal("membership change retained the retired generation before any further ingress")
					}
				}
				for _, candidate := range scheduler.controlOrder[:cap(scheduler.controlOrder)] {
					if candidate.lane == previous {
						t.Fatal("control order retained the retired generation before any further control traffic")
					}
				}
			}
			got := scheduler.selectTransportCandidates(lanes, 1, 1000, math.MaxUint64, now)
			if want == nil {
				if got.count != 0 || got.available {
					t.Fatal("last-lane removal left a selectable generation")
				}
			} else if got.count != 1 || got.lanes[0] != want {
				t.Fatalf("selection after %s = %+v, want lane %d generation %d", action, got, want.registration.LaneID, want.registration.Generation)
			}
			members := scheduler.transportLaneOrder(lanes)
			if len(members) != len(lanes) {
				t.Fatalf("member count = %d, want %d", len(members), len(lanes))
			}
			seen := make(map[protocol.LaneID]bool, len(lanes))
			for _, lane := range members {
				id := lane.registration.LaneID
				if seen[id] || lanes[id] != lane {
					t.Fatal("membership contains a duplicate, removed, or superseded generation")
				}
				seen[id] = true
			}
		})
	}
}

func TestSchedulerTransportLaneOrderLiveState(t *testing.T) {
	for _, action := range []string{
		"FasterRate", "RateObserved", "LowerRTT", "EqualRTT", "Degraded", "Recovered",
		"Abandoning", "ClosedCandidate", "QueuedAdmission", "HardCapacity", "CommitReopensQueue", "FeedbackReopensCapacity",
	} {
		t.Run(action, func(t *testing.T) {
			now := time.UnixMicro(1 << 40)
			limits := packetqueue.Limits{Packets: 64, Bytes: 128 * 1024}
			primary := schedulerLaneWithLimits(t, 1, 1, 10_000, 100_000, limits)
			candidate := schedulerLaneWithLimits(t, 2, 2, 12_000, 100_000, limits)
			lanes := map[protocol.LaneID]*scheduledLane{1: primary, 2: candidate}
			for _, lane := range lanes {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			switch action {
			case "RateObserved":
				candidate.deliveryRate *= 2
				candidate.rateObserved = false
			case "Recovered":
				candidate.deliveryRate *= 2
				candidate.degraded = true
			}
			scheduler := Scheduler{lastTransportAt: now}
			if got := scheduler.selectTransportCandidates(lanes, 1, 1000, math.MaxUint64, now); got.count != 1 || got.lanes[0] != primary {
				t.Fatal("fixture did not preserve the initial primary")
			}
			preferred := protocol.LaneID(1)
			want := candidate
			switch action {
			case "FasterRate":
				candidate.deliveryRate *= 2
			case "RateObserved":
				candidate.rateObserved = true
			case "LowerRTT":
				candidate.rttMicros = 5_000
				preferred = 0
			case "EqualRTT":
				candidate.rttMicros = primary.rttMicros
				preferred = 0
				scheduler.transportOrder[0], scheduler.transportOrder[1] = candidate, primary
				want = primary
			case "Degraded":
				primary.degraded = true
			case "Recovered":
				candidate.degraded = false
			case "Abandoning":
				primary.abandoning = true
			case "ClosedCandidate":
				candidate.deliveryRate *= 2
				releaseTransmissions(candidate.registration.Store.drain())
				want = primary
			case "QueuedAdmission", "HardCapacity", "CommitReopensQueue", "FeedbackReopensCapacity":
				candidate.deliveryRate *= 2
				transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
				if action == "QueuedAdmission" || action == "CommitReopensQueue" {
					transmission.packet.Payload = make([]byte, minimumTransportQueueBytes)
					copy(transmission.packet.Payload, relayWireGuardPacket(wgpacket.TransportData))
				} else {
					candidate.registration.Store.limits.Packets = 1
				}
				if err := candidate.registration.Store.pushAt(transmission, now); err != nil {
					t.Fatal(err)
				}
				if got := scheduler.selectTransportCandidates(lanes, 1, 1000, math.MaxUint64, now); got.count != 1 || got.lanes[0] != primary {
					t.Fatal("a newly full candidate bypassed live admission")
				}
				if action == "CommitReopensQueue" || action == "FeedbackReopensCapacity" {
					takeOneTransmission(t, candidate.registration.Store)
					if action == "FeedbackReopensCapacity" {
						if _, _, err := candidate.registration.Store.acknowledge(1, uint64(now.UnixMicro())); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					want = primary
				}
			}
			if got := scheduler.selectTransportCandidates(lanes, preferred, 1000, math.MaxUint64, now); got.count != 1 || got.lanes[0] != want {
				t.Fatalf("selection after live %s change = %+v, want lane %d", action, got, want.registration.LaneID)
			}
		})
	}
}

func TestSchedulerRunReleasesTransportOrder(t *testing.T) {
	for _, action := range []string{"Canceled", "IngressClosed"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 8, Bytes: 65536})
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err := NewScheduler(&Ingress{queue: ingress})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- scheduler.Run(ctx) }()
				defer cancel()
				for id := byte(1); id <= 2; id++ {
					store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 65536})
					if err != nil {
						t.Fatal(err)
					}
					if err := scheduler.Register(ctx, schedulerRegistration(id, id, store)); err != nil {
						t.Fatal(err)
					}
				}
				if err := ingress.Push(ingressProbeItem(wgpacket.TransportData, 64, time.Now().Add(time.Second))); err != nil {
					t.Fatal(err)
				}
				if !scheduler.RouteDeliveryReport(protocol.DeliveryReport{LaneID: 1, Generation: 1}, time.Now(), func(bool) {}) {
					t.Fatal("fixture could not route feedback")
				}
				synctest.Wait()
				if len(scheduler.transportOrder) != 2 || len(scheduler.controlOrder) != 1 {
					t.Fatal("fixture did not populate both lane order buffers")
				}
				wantErr := context.Canceled
				if action == "IngressClosed" {
					ingress.Close()
					wantErr = packetqueue.ErrClosed
				} else {
					cancel()
				}
				if err := <-done; !errors.Is(err, wantErr) {
					t.Fatalf("Run() = %v, want %v", err, wantErr)
				}
				for _, lane := range scheduler.transportOrder[:cap(scheduler.transportOrder)] {
					if lane != nil {
						t.Fatal("stopped scheduler retained a lane generation")
					}
				}
				for _, candidate := range scheduler.controlOrder[:cap(scheduler.controlOrder)] {
					if candidate.lane != nil {
						t.Fatal("stopped scheduler retained a control lane generation")
					}
				}
			})
		})
	}
}
