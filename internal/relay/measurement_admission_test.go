package relay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestIngressMeasurementAdmission(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cancel bool
	}{
		{name: "PauseBeforeRead"},
		{name: "CancellationBeforeRead", cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clock := &testClock{now: 1000}
				queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192}, func() time.Time {
					return monotime.Time(clock.now)
				})
				if err != nil {
					t.Fatal(err)
				}
				defer queue.Close()
				endpoint := newTestEndpoint()
				endpoint.reads <- datagram.Packet{Kind: wgpacket.TransportData, Payload: relayWireGuardPacket(wgpacket.TransportData)}
				input, err := NewIngress(endpoint, queue, clock, DeadlinePolicy{Control: 5 * time.Second, Transport: 5 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				owner, stale := &measurementDrain{}, &measurementDrain{}
				if !input.pauseForMeasurement(owner) || input.pauseForMeasurement(stale) {
					t.Fatal("overlapping admission owner was accepted")
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- input.Run(ctx) }()
				synctest.Wait()
				input.resumeAfterMeasurement(stale)
				synctest.Wait()
				if queue.Len() != 0 || len(endpoint.reads) != 1 {
					t.Fatal("paused ingress consumed an unaccepted datagram")
				}
				select {
				case err := <-done:
					t.Fatalf("batch escaped its admission cut: %v", err)
				default:
				}
				clock.now += 6_000_000
				if tt.cancel {
					cancel()
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Fatalf("Run() = %v", err)
					}
					if queue.Len() != 0 || len(endpoint.reads) != 1 {
						t.Fatal("cancellation consumed or admitted the unaccepted datagram")
					}
					input.resumeAfterMeasurement(owner)
					return
				}
				input.resumeAfterMeasurement(owner)
				<-queue.Ready()
				var item packetqueue.Item[Packet]
				if err := queue.TryPop(&item, queue.Now()); err != nil {
					t.Fatal(err)
				}
				defer item.Release()
				if item.Value.queuedMicros != clock.now || item.Deadline != monotime.Time(clock.now).Add(5*time.Second) {
					t.Fatal("the previously unread datagram lacks its actual read-time lifetime")
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() = %v", err)
				}
			})
		})
	}
}

func TestIngressMeasurementAdmissionCut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &admissionClock{entered: make(chan struct{}), resume: make(chan struct{})}
		queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192}, func() time.Time {
			return monotime.Time(clock.NowMicros())
		})
		if err != nil {
			t.Fatal(err)
		}
		defer queue.Close()
		input, err := NewIngress(newTestEndpoint(), queue, &testClock{now: 1000}, DeadlinePolicy{Control: 5 * time.Second, Transport: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		packets := []datagram.Packet{{Kind: wgpacket.TransportData, Payload: relayWireGuardPacket(wgpacket.TransportData)}}
		items := make([]packetqueue.Item[Packet], len(packets))
		batch := make(chan error, 1)
		go func() { batch <- input.admitBatch(t.Context(), packets, items) }()
		<-clock.entered
		owner := &measurementDrain{}
		// A mutex waiter is not durably blocked in synctest. Release the clock concurrently and require
		// pauseForMeasurement itself to establish that the earlier batch has completed.
		go close(clock.resume)
		if !input.pauseForMeasurement(owner) || queue.Len() != 1 {
			t.Fatal("the complete earlier batch was absent from the admission cut")
		}
		if err := <-batch; err != nil {
			t.Fatal(err)
		}
		input.resumeAfterMeasurement(owner)
	})
}

type admissionClock struct {
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *admissionClock) NowMicros() uint64 {
	c.once.Do(func() { close(c.entered) })
	<-c.resume
	return 1000
}

func TestSchedulerMeasurementDrainsAcceptedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &testClock{now: 1000}
		queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192}, func() time.Time { return monotime.Time(clock.now) })
		if err != nil {
			t.Fatal(err)
		}
		defer queue.Close()
		input, err := NewIngress(newTestEndpoint(), queue, clock, DeadlinePolicy{Control: 5 * time.Second, Transport: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		scheduler, err := NewScheduler(input)
		if err != nil {
			t.Fatal(err)
		}
		lane := schedulerLane(t, 1, 1, 10_000, 1_000_000)
		lane.registration.Store.now = func() time.Time { return monotime.Time(clock.now) }
		defer func() { releaseTransmissions(lane.registration.Store.drain()) }()
		lanes := map[protocol.LaneID]*scheduledLane{1: lane}
		preferred := protocol.LaneID(1)
		packets := []datagram.Packet{{Kind: wgpacket.TransportData, Payload: relayWireGuardPacket(wgpacket.TransportData)}}
		if err := input.admitBatch(t.Context(), packets, make([]packetqueue.Item[Packet], 1)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ready, proof := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, false, nil)
		scheduler.applyEvent(lanes, &preferred, <-scheduler.events, queue.Now(), false)
		if scheduler.measurementQuiet || lane.registration.Store.measurementDrain.Load() {
			t.Fatal("the data gate closed before accepted ingress drained")
		}
		var item packetqueue.Item[Packet]
		if err := queue.TryPop(&item, queue.Now()); err != nil {
			t.Fatal(err)
		}
		deadline := item.Deadline
		scheduler.updateMeasurementDrain(lanes, true)
		if scheduler.measurementQuiet {
			t.Fatal("pending unassigned ingress escaped drain readiness")
		}
		if done, err := scheduler.schedule(lanes, &preferred, &item, queue.Now()); err != nil || !done {
			t.Fatalf("drain schedule: %t, %v", done, err)
		}
		scheduler.updateMeasurementDrain(lanes, false)
		if scheduler.measurementQuiet {
			t.Fatal("queued accepted data escaped drain readiness")
		}
		data := takeOneTransmission(t, lane.registration.Store)
		if data.PacketID != 1 || data.DeadlineMicros != uint64(deadline.UnixMicro()) {
			t.Fatal("draining rewrote an accepted packet identity or lifetime")
		}
		scheduler.updateMeasurementDrain(lanes, false)
		if !scheduler.measurementQuiet || !lane.registration.Store.measurementDrain.Load() {
			t.Fatal("the empty accepted queue did not close its data gate")
		}
		select {
		case <-ready:
			t.Fatal("the committed prefix was not yet reported")
		default:
		}
		if _, stale, err := lane.registration.Store.acknowledge(1, clock.now+1); err != nil || stale {
			t.Fatalf("drain report: %t, %v", stale, err)
		}
		scheduler.updateMeasurementDrain(lanes, false)
		select {
		case <-ready:
		default:
			t.Fatal("the fully drained prefix did not become ready")
		}
		if !measurementDrainValid(proof) {
			t.Fatal("the quiet prefix lacks a bounded proof")
		}
		cancel()
		scheduler.updateMeasurementDrain(lanes, false)
		if input.pauseOwner != nil || lane.registration.Store.measurementDrain.Load() || lane.registration.Store.measurementSupplyPaused {
			t.Fatal("measurement cancellation retained an admission, data gate, or supply restriction")
		}
	})
}

func TestSchedulerMeasurementAdmissionCleanup(t *testing.T) {
	for _, tt := range []struct {
		name        string
		queued      bool
		cancel      bool
		replacement bool
	}{
		{name: "DrainingCancellation", queued: true, cancel: true},
		{name: "DrainingDeadline", queued: true},
		{name: "QuietDeadline"},
		{name: "DrainingReplacement", queued: true, replacement: true},
		{name: "QuietReplacement", replacement: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, scheduler := feedbackScheduler(t)
				lane := schedulerLane(t, 1, 1, 10_000, 1_000_000)
				defer func() { releaseTransmissions(lane.registration.Store.drain()) }()
				lanes := map[protocol.LaneID]*scheduledLane{1: lane}
				if tt.queued {
					if err := queue.Push(ingressProbeItem(wgpacket.TransportData, 64, queue.Now().Add(time.Hour))); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				ready, proof := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, false, nil)
				preferred := protocol.LaneID(1)
				scheduler.applyEvent(lanes, &preferred, <-scheduler.events, queue.Now(), false)
				if scheduler.input.pauseOwner != scheduler.measurementDrain || scheduler.measurementQuiet == tt.queued {
					t.Fatal("fixture did not establish the intended admission cut")
				}
				if tt.replacement {
					replacement := schedulerLane(t, 1, 1, 10_000, 1_000_000)
					replacement.registration.Generation = 2
					defer func() { releaseTransmissions(replacement.registration.Store.drain()) }()
					result := make(chan error, 1)
					scheduler.applyEvent(lanes, &preferred, schedulerEvent{
						kind: schedulerRegister, registration: replacement.registration, result: result,
					}, queue.Now(), false)
					if err := <-result; err != nil {
						t.Fatal(err)
					}
					store := replacement.registration.Store
					if measurementDrainValid(proof) || !store.measurementSupplyPaused || store.measurementDrain.Load() == tt.queued {
						t.Fatal("replacement retained a valid proof or bypassed the active cut")
					}
				}
				if tt.cancel {
					cancel()
				} else {
					time.Sleep(time.Until(scheduler.measurementDeadline))
				}
				scheduler.updateMeasurementDrain(lanes, false)
				select {
				case <-ready:
				default:
					t.Fatal("cancelled or expired preparation did not notify its caller")
				}
				if scheduler.input.pauseOwner != nil || scheduler.input.resumed != nil || scheduler.measurementDrain != nil ||
					scheduler.measurementQuiet || scheduler.pendingMeasurementDrain.Load() || measurementDrainValid(proof) {
					t.Fatal("cancelled or expired operation retained admission ownership or quiet proof")
				}
				for _, active := range lanes {
					store := active.registration.Store
					if store.measurementDrain.Load() || store.measurementSupplyPaused || store.measurementWindow != 0 {
						t.Fatal("cancelled or expired operation retained a writer or temporary credit restriction")
					}
				}
			})
		})
	}
}
