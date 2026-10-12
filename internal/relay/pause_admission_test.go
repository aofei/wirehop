package relay

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerScheduleTimingPause(t *testing.T) {
	for _, tt := range []struct {
		name             string
		kind             wgpacket.Kind
		expired, invalid bool
	}{
		{name: "Transport", kind: wgpacket.TransportData},
		{name: "WireGuardControl", kind: wgpacket.HandshakeInitiation},
		{name: "Expired", kind: wgpacket.TransportData, expired: true},
		{name: "Invalid", kind: wgpacket.TransportData, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			first := schedulerLane(t, 1, 1, 100_000, 100_000)
			second := schedulerLane(t, 2, 1, 10_000, 1_000_000)
			second.rateObserved = false
			lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
			for _, lane := range lanes {
				lane.registration.Store.beginMeasurementPause()
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			scheduler := &Scheduler{measurementStarted: now, packetID: 7}
			preferred := protocol.LaneID(1)
			payload := relayWireGuardPacket(tt.kind)
			item := packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: tt.kind, Payload: payload}), Size: len(payload), Deadline: now.Add(40 * time.Millisecond)}
			if tt.expired {
				item.Deadline = now
			}
			if tt.invalid {
				item.Value.Kind = wgpacket.CookieReply
			}
			done, err := scheduler.schedule(lanes, &preferred, &item, now)
			if err != nil {
				t.Fatal(err)
			}
			if tt.expired || tt.invalid {
				if !done || item.Value.Payload != nil {
					t.Fatal("pause retained an invalid or expired ingress packet")
				}
			} else if !done || item.Value.Payload != nil {
				t.Fatal("active timing pause did not stage valid ingress")
			}
			wantID := uint64(8)
			if tt.expired || tt.invalid {
				wantID = 7
			}
			if scheduler.packetID != wantID || preferred != 1 {
				t.Fatal("active pause changed packet identity or primary")
			}
			for _, lane := range lanes {
				wantPackets := 0
				if !tt.expired && !tt.invalid && (lane == first || tt.kind.Control()) {
					wantPackets = 1
				}
				if packets, _ := lane.registration.Store.backlog(); packets != wantPackets {
					t.Fatalf("paused queued packets = %d, want %d", packets, wantPackets)
				}
				var data [1]protocol.Data
				var ownership [1]datagram.Packet
				if n, err := lane.registration.Store.takeBatch(data[:], ownership[:], 65536); n != 0 || !errors.Is(err, packetqueue.ErrEmpty) {
					t.Fatal("paused staged Data reached carrier commit")
				}
			}
			item.Release()
		})
	}
}

func TestSchedulerRunTimingPauseKeepsControlProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192})
		if err != nil {
			t.Fatal(err)
		}
		defer ingress.Close()
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		store := schedulerStore(t, packetqueue.Limits{Packets: 8, Bytes: 8192})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- scheduler.Run(ctx) }()
		defer func() {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("scheduler exit = %v", err)
			}
		}()
		registration := schedulerRegistration(1, 1, store)
		controls := 0
		registration.SendControl = func(_ protocol.Frame, complete func()) bool {
			controls++
			if complete != nil {
				complete()
			}
			return true
		}
		if err := scheduler.Register(ctx, registration); err != nil {
			t.Fatal(err)
		}
		if err := ingress.Push(ingressProbeItem(wgpacket.TransportData, 100, time.Now().Add(time.Minute))); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if data := takeOneTransmission(t, store); data.PacketID != 1 {
			t.Fatal("initial useful packet did not establish the primary")
		}
		leaseContext, release := context.WithCancel(ctx)
		defer release()
		ready, _ := scheduler.beginMeasurementDrain(leaseContext, nil)
		synctest.Wait()
		for _, kind := range []wgpacket.Kind{wgpacket.TransportData, wgpacket.HandshakeInitiation} {
			payload := relayWireGuardPacket(kind)
			priority := packetqueue.PriorityNormal
			if kind.Control() {
				priority = packetqueue.PriorityControl
			}
			if err := ingress.Push(packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: kind, Payload: payload}), Size: len(payload), Priority: priority, Deadline: time.Now().Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if packets, _ := store.backlog(); packets != 3 {
			t.Fatal("pause did not retain pending ingress behind the committed prefix")
		}
		if err := scheduler.ObserveDeliveryReport(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 1}, uint64(time.Now().UnixMicro())+1, make(chan error, 1)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-ready:
		default:
			t.Fatal("paused scheduler did not apply committed-prefix feedback")
		}
		closeFrame, err := protocol.MarshalSessionClose(protocol.CloseClientShutdown)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		scheduler.events <- schedulerEvent{kind: schedulerCloseSession, frame: closeFrame, result: result}
		if err := <-result; err != nil || controls != 1 {
			t.Fatalf("paused protocol control = %v, writes %d", err, controls)
		}
		synctest.Wait()
		if packets, _ := store.backlog(); packets != 2 || !store.measurementDrain.Load() {
			t.Fatal("protocol progress changed staged data or ended the pause")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		release()
		synctest.Wait()
		if packets, _ := store.backlog(); packets != 0 {
			t.Fatal("expired pending ingress was admitted after resume")
		}
		payload := relayWireGuardPacket(wgpacket.TransportData)
		deadline := time.Now().Add(time.Second)
		if err := ingress.Push(packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}), Size: len(payload), Deadline: deadline}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		resumed := takeOneTransmission(t, store)
		if resumed.PacketID != 4 || resumed.DeadlineMicros != uint64(deadline.UnixMicro()) {
			t.Fatal("quiet period blocked fresh ingress or changed its identity or deadline")
		}
	})
}
