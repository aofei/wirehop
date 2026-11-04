package relay

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerRunBatchBoundaries(t *testing.T) {
	for _, action := range []string{"Cancel", "Expire", "Control", "CloseSession", "BeforeElapsedBudget", "ElapsedBudget", "Readiness", "MeasurementChanged"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const packets = 4 * maximumScheduleBatchPackets
				now := time.Unix(123, 0)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var scheduler *Scheduler
				var ingress *packetqueue.Queue[Packet]
				var store *TransmissionStore
				armed, triggered := false, false
				observed := uint64(0)
				clock := func() time.Time {
					if !armed {
						return now
					}
					previouslyTriggered := triggered
					if !triggered && scheduler.packetID >= 3 {
						triggered = true
						switch action {
						case "Cancel":
							cancel()
						case "Expire":
							now = now.Add(2 * time.Second)
						case "Control":
							item := ingressProbeItem(wgpacket.HandshakeInitiation, 0, now.Add(time.Second))
							if err := ingress.Push(item); err != nil {
								t.Fatal(err)
							}
						case "Readiness":
							scheduler.transportReady <- struct{}{}
						case "MeasurementChanged":
							scheduler.notifyMeasurementChanged()
						case "CloseSession", "BeforeElapsedBudget", "ElapsedBudget":
							if action == "BeforeElapsedBudget" {
								now = now.Add(maximumScheduleBatchDuration - time.Microsecond)
							} else if action == "ElapsedBudget" {
								now = now.Add(maximumScheduleBatchDuration)
							}
							frame, err := protocol.MarshalSessionClose(protocol.CloseClientShutdown)
							if err != nil {
								t.Fatal(err)
							}
							scheduler.events <- schedulerEvent{kind: schedulerCloseSession, frame: frame, result: make(chan error, 1)}
						}
					}
					if action == "Control" && observed == 0 {
						if control, ok := store.control.peek(); ok {
							observed = control.packetID
						}
					}
					if triggered && (action == "Readiness" && len(scheduler.transportReady) == 0 ||
						action == "MeasurementChanged" && len(scheduler.measurementChanged) == 0) {
						observed = scheduler.packetID
						cancel()
					}
					if scheduler.packetID >= packets || previouslyTriggered && action == "Expire" {
						cancel()
					}
					return now
				}
				budget, err := retention.NewBudget(retention.Limits{Packets: packets + 4, Bytes: 65536})
				if err != nil {
					t.Fatal(err)
				}
				ingress, err = packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: packets, Bytes: 32768}, budget, clock)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err = NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				store, err = NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: packets + 1, Bytes: 32768}, budget, func() time.Time { return now })
				if err != nil {
					t.Fatal(err)
				}
				registration := schedulerRegistration(1, 1, store)
				registration.SendControl = func(protocol.Frame, func()) bool {
					observed = scheduler.packetID
					cancel()
					return true
				}
				for range packets {
					if err := ingress.Push(ingressProbeItem(wgpacket.TransportData, 64, now.Add(time.Second))); err != nil {
						t.Fatal(err)
					}
				}
				scheduler.events <- schedulerEvent{kind: schedulerRegister, registration: registration, result: make(chan error, 1)}
				armed = true
				if err := scheduler.Run(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() = %v, want cancellation", err)
				}
				if !triggered {
					t.Fatal("batch did not reach the injected transition")
				}
				switch action {
				case "Cancel", "Expire":
					if scheduler.packetID != 3 {
						t.Fatalf("assigned %d packet IDs after cancellation or expiry, want 3", scheduler.packetID)
					}
				case "Control":
					if observed != 4 {
						t.Fatalf("control packet ID = %d, want 4", observed)
					}
					// The control is admitted before the next transport at the same packet boundary.
					if scheduler.packetID != packets || ingress.Len() != 1 {
						t.Fatalf("assigned IDs = %d, queued = %d", scheduler.packetID, ingress.Len())
					}
				case "CloseSession", "BeforeElapsedBudget", "MeasurementChanged":
					if observed != 4 {
						t.Fatalf("state change observed after packet %d, want one further assignment", observed)
					}
				case "ElapsedBudget":
					if observed != 3 {
						t.Fatalf("expired work budget admitted packet %d before the waiting event", observed)
					}
				case "Readiness":
					if observed != 1+maximumScheduleBatchPackets {
						t.Fatalf("readiness observed after packet %d, want the first complete batch boundary", observed)
					}
				}
				if action == "Expire" && ingress.Len() != 0 {
					t.Fatal("expired ingress remained queued after the clock advanced")
				}
				ingress.Close()
				if usage := budget.Usage(); usage != (retention.Usage{}) {
					t.Fatalf("shutdown retained aggregate ownership: %+v", usage)
				}
			})
		})
	}
}

func TestSchedulerRunBatchFailure(t *testing.T) {
	for _, action := range []string{"IngressClosed", "HeldIngressClosed", "CounterExhausted"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const packets = 4 * maximumScheduleBatchPackets
				now := time.Unix(123, 0)
				var scheduler *Scheduler
				var ingress *packetqueue.Queue[Packet]
				closed := false
				clock := func() time.Time {
					if !closed && scheduler != nil && scheduler.packetID == 3 && (action == "IngressClosed" ||
						action == "HeldIngressClosed" && ingress.Len() == packets-4) {
						closed = true
						ingress.Close()
					}
					return now
				}
				budget, err := retention.NewBudget(retention.Limits{Packets: packets, Bytes: 65536})
				if err != nil {
					t.Fatal(err)
				}
				ingress, err = packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: packets, Bytes: 32768}, budget, clock)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err = NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				storePackets := packets
				if action == "HeldIngressClosed" {
					storePackets = 3
				}
				store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: storePackets, Bytes: 32768}, budget, func() time.Time { return now })
				if err != nil {
					t.Fatal(err)
				}
				deadline := now.Add(time.Second)
				for index := range packets {
					item := ingressProbeItem(wgpacket.TransportData, 64, deadline)
					item.Value.Payload[4] = byte(index)
					if err := ingress.Push(item); err != nil {
						t.Fatal(err)
					}
				}
				wantErr := packetqueue.ErrClosed
				wantPacketID := uint64(3)
				wantQueued := 0
				if action == "CounterExhausted" {
					scheduler.packetID = math.MaxUint64 - 3
					wantErr = ErrCounterExhausted
					wantPacketID = math.MaxUint64
					wantQueued = packets - 3
				}
				scheduler.events <- schedulerEvent{kind: schedulerRegister,
					registration: schedulerRegistration(1, 1, store), result: make(chan error, 1)}
				if err := scheduler.Run(t.Context()); !errors.Is(err, wantErr) {
					t.Fatalf("Run() = %v, want %v", err, wantErr)
				}
				if scheduler.packetID != wantPacketID || ingress.Len() != wantQueued {
					t.Fatalf("packet ID = %d, queued = %d, want %d and %d", scheduler.packetID, ingress.Len(), wantPacketID, wantQueued)
				}
				if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
					t.Fatalf("failed batch retained store ownership: %d packets, %d bytes", packets, bytes)
				}
				if usage := budget.Usage(); usage != (retention.Usage{Packets: wantQueued, Bytes: wantQueued * 64}) {
					t.Fatalf("failed batch retained aggregate ownership: %+v", usage)
				}
				var seen [packets]bool
				for range wantQueued {
					var item packetqueue.Item[Packet]
					if err := ingress.TryPop(&item, now); err != nil {
						t.Fatal(err)
					}
					index := int(item.Value.Payload[4])
					valid := index >= 3 && index < packets && !seen[index] && item.Deadline.Equal(deadline)
					item.Release()
					if !valid {
						t.Fatalf("failed batch restored invalid, duplicate, or changed item %d", index)
					}
					seen[index] = true
				}
				if usage := budget.Usage(); usage != (retention.Usage{}) {
					t.Fatalf("drained failure retained aggregate ownership: %+v", usage)
				}
			})
		})
	}
}

func TestSchedulerRunBatchIdleWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const packets = 2 * maximumScheduleBatchPackets
		var clockReads atomic.Uint64
		now := func() time.Time {
			clockReads.Add(1)
			return time.Now()
		}
		budget, err := retention.NewBudget(retention.Limits{Packets: packets, Bytes: 65536})
		if err != nil {
			t.Fatal(err)
		}
		ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: packets, Bytes: 32768}, budget, now)
		if err != nil {
			t.Fatal(err)
		}
		defer ingress.Close()
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: packets, Bytes: 32768}, budget, now)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- scheduler.Run(ctx) }()
		defer func() {
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Errorf("Run() = %v, want cancellation", err)
			}
		}()
		if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
			t.Fatal(err)
		}
		var packetID uint64
		for _, size := range []int{1, maximumScheduleBatchPackets - 1, maximumScheduleBatchPackets, maximumScheduleBatchPackets + 1, packets} {
			started := time.Now()
			deadline := started.Add(time.Second)
			items := make([]packetqueue.Item[Packet], size)
			for index := range items {
				items[index] = ingressProbeItem(wgpacket.TransportData, 64, deadline)
				items[index].Value.Payload[4] = byte(index)
			}
			if count, err := ingress.PushBatch(items); err != nil || count != size {
				t.Fatalf("PushBatch() = %d, %v, want %d", count, err, size)
			}
			synctest.Wait()
			if !time.Now().Equal(started) || ingress.Len() != 0 {
				t.Fatal("ready batch waited for a timer or remained queued")
			}
			received := 0
			for received < size {
				var data [datagram.MaximumBatchSize]protocol.Data
				var ownership [datagram.MaximumBatchSize]datagram.Packet
				count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes, 0)
				releaseBatchOwnership(ownership[:count])
				if err != nil || count == 0 {
					t.Fatalf("takeBatch() = %d, %v", count, err)
				}
				for _, item := range data[:count] {
					packetID++
					if item.PacketID != packetID || item.Payload[4] != byte(received) || item.DeadlineMicros != uint64(deadline.UnixMicro()) {
						t.Fatalf("batch item %d changed identity, payload, order, or deadline", packetID)
					}
					received++
				}
				source := protocol.LaneGeneration{LaneID: 1, Generation: 1}
				report := protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: packetID}
				if err := scheduler.ObserveDeliveryReport(ctx, source, report, uint64(now().UnixMicro()), make(chan error, 1)); err != nil {
					t.Fatal(err)
				}
			}
			synctest.Wait()
			if usage := budget.Usage(); usage != (retention.Usage{}) {
				t.Fatalf("idle batch retained aggregate ownership: %+v", usage)
			}
			before := clockReads.Load()
			synctest.Sleep(2 * abandonmentCheckInterval)
			synctest.Wait()
			if clockReads.Load() != before {
				t.Fatal("drained scheduler continued polling before the next ingress wake")
			}
		}
	})
}

func TestSchedulerRunBatchTransitions(t *testing.T) {
	for _, action := range []string{"Detach", "DetachPreempted", "Replace", "PauseShutdown"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const packets = 2 * maximumScheduleBatchPackets
				const admitted = maximumScheduleBatchPackets + 1
				var clockReads atomic.Uint64
				now := func() time.Time {
					clockReads.Add(1)
					return time.Now()
				}
				budget, err := retention.NewBudget(retention.Limits{Packets: packets + 1, Bytes: 65536})
				if err != nil {
					t.Fatal(err)
				}
				ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: packets + 1, Bytes: 32768}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err := NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: admitted, Bytes: 32768}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				deadline := now().Add(8 * abandonmentCheckInterval)
				for index := range packets {
					item := ingressProbeItem(wgpacket.TransportData, 64, deadline)
					item.Value.Payload[4] = byte(index)
					if err := ingress.Push(item); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var runErr error
				done := make(chan struct{})
				go func() {
					runErr = scheduler.Run(ctx)
					close(done)
				}()
				defer func() {
					cancel()
					<-done
					if !errors.Is(runErr, context.Canceled) {
						t.Errorf("Run() = %v, want cancellation", runErr)
					}
				}()
				abandoned := false
				registration := schedulerRegistration(1, 1, store)
				registration.Abandon = func() { abandoned = true }
				registration.MeasurementInitiator = action == "PauseShutdown"
				if err := scheduler.Register(ctx, registration); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if count, _ := store.backlog(); count != admitted || scheduler.packetID != admitted || ingress.Len() != packets-admitted-1 {
					t.Fatal("fixture did not fill a complete batch and retain the next blocked item")
				}
				if action == "PauseShutdown" {
					scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: 1, Generation: 1})
					synctest.Wait()
					attempt := scheduler.measurements.active
					if attempt == nil || !measurementDrainValid(attempt.proof) || !store.measurementDrain.Load() || scheduler.measurementTimer == nil {
						t.Fatal("measurement did not pause the full store and arm its fixed deadline")
					}
					cancel()
					<-done
					synctest.Wait()
					if attempt.ctx.Err() == nil || measurementDrainValid(attempt.proof) || store.measurementDrain.Load() {
						t.Fatal("shutdown retained a measurement operation, prefix proof, or writer pause")
					}
					if count, _ := store.backlog(); count != 0 || ingress.Len() != packets-admitted {
						t.Fatal("shutdown lost held ingress or retained the closed generation")
					}
					var seen [packets]bool
					for range packets - admitted {
						var item packetqueue.Item[Packet]
						if err := ingress.TryPop(&item, now()); err != nil {
							t.Fatal(err)
						}
						index := int(item.Value.Payload[4])
						valid := index >= admitted && index < packets && !seen[index] && item.Deadline.Equal(deadline)
						item.Release()
						if !valid {
							t.Fatal("paused shutdown lost or duplicated a held or queued item")
						}
						seen[index] = true
					}
					if usage := budget.Usage(); usage != (retention.Usage{}) {
						t.Fatalf("paused shutdown retained aggregate ownership: %+v", usage)
					}
					for len(scheduler.measurementChanged) > 0 {
						<-scheduler.measurementChanged
					}
					before := clockReads.Load()
					synctest.Sleep(measurementPauseLimit + abandonmentCheckInterval)
					synctest.Wait()
					if len(scheduler.measurementChanged) != 0 || clockReads.Load() != before {
						t.Fatal("stopped scheduler retained a live measurement timer or maintenance poll")
					}
					return
				}
				if action == "DetachPreempted" {
					if err := ingress.Push(ingressProbeItem(wgpacket.HandshakeInitiation, 0, deadline.Add(8*abandonmentCheckInterval))); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					if ingress.Len() != packets-admitted-1 {
						t.Fatal("full generation did not hold the preempting control beside the normal item")
					}
				}
				if action != "Replace" {
					if err := scheduler.Remove(ctx, 1, 1); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					synctest.Sleep(time.Until(deadline) - time.Nanosecond)
					synctest.Wait()
					wantRetained := packets - admitted
					if action == "DetachPreempted" {
						wantRetained++
					}
					if usage := budget.Usage(); usage.Packets != wantRetained {
						t.Fatalf("detached fresh ownership = %+v, want %d packets", usage, wantRetained)
					}
					synctest.Sleep(time.Nanosecond)
					synctest.Wait()
					if action == "DetachPreempted" {
						want := retention.Usage{Packets: 1, Bytes: len(relayWireGuardPacket(wgpacket.HandshakeInitiation))}
						if usage := budget.Usage(); usage != want {
							t.Fatalf("transport expiry retained ownership %+v, want only the later control", usage)
						}
						synctest.Sleep(8*abandonmentCheckInterval - time.Nanosecond)
						synctest.Wait()
						if usage := budget.Usage(); usage != want {
							t.Fatal("transport expiry discarded the still-fresh preempting control")
						}
						synctest.Sleep(time.Nanosecond)
						synctest.Wait()
					}
					if usage := budget.Usage(); usage != (retention.Usage{}) || ingress.Len() != 0 || scheduler.packetID != admitted {
						t.Fatal("last-lane removal retained expired pending, preempted, or queued ownership")
					}
					before := clockReads.Load()
					synctest.Sleep(2 * abandonmentCheckInterval)
					synctest.Wait()
					if clockReads.Load() != before {
						t.Fatal("fully expired detached scheduler continued polling")
					}
				}
				replacement, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: packets + 1, Bytes: 32768}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				next := schedulerRegistration(1, 1, replacement)
				next.Generation = 2
				if err := scheduler.Register(ctx, next); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if !store.closed || action == "Replace" && !abandoned {
					t.Fatal("generation transition retained the old store or failed to abandon its writer")
				}
				if err := scheduler.Register(ctx, registration); !errors.Is(err, ErrStaleLane) {
					t.Fatalf("stale Register() = %v, want stale lane", err)
				}
				if err := scheduler.Remove(ctx, 1, 1); err != nil {
					t.Fatal(err)
				}
				freshDeadline := now().Add(time.Second)
				fresh := ingressProbeItem(wgpacket.TransportData, 64, freshDeadline)
				fresh.Value.Payload[4] = packets
				if err := ingress.Push(fresh); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				wantQueued := 1
				packetID := uint64(admitted)
				if action == "Replace" {
					wantQueued = packets + 1
					packetID = 0
				}
				if count, _ := replacement.backlog(); count != wantQueued || ingress.Len() != 0 {
					t.Fatalf("replacement queued %d packets, want %d with no waiting ingress", count, wantQueued)
				}
				for index := range wantQueued {
					data := takeOneTransmission(t, replacement)
					packetID++
					wantMarker, wantDeadline := byte(index), deadline
					if index == wantQueued-1 {
						wantMarker, wantDeadline = packets, freshDeadline
					}
					if data.PacketID != packetID || data.Payload[4] != wantMarker || data.DeadlineMicros != uint64(wantDeadline.UnixMicro()) {
						t.Fatalf("generation transition changed packet %d identity, order, payload, or deadline", index)
					}
				}
				source := protocol.LaneGeneration{LaneID: 1, Generation: 2}
				report := protocol.DeliveryReport{LaneID: 1, Generation: 2, DataPackets: uint64(wantQueued)}
				if err := scheduler.ObserveDeliveryReport(ctx, source, report, uint64(now().UnixMicro()), make(chan error, 1)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if usage := budget.Usage(); usage != (retention.Usage{}) {
					t.Fatalf("generation transition retained aggregate ownership: %+v", usage)
				}
			})
		})
	}
}

func TestSchedulerRunPreemptedExpiryActiveLane(t *testing.T) {
	for _, tt := range []struct {
		name      string
		size      int
		committed bool
	}{
		{name: "Queued", size: 64},
		{name: "MaximumQueued", size: protocol.MaxPacketSize},
		{name: "Committed", size: 64, committed: true},
		{name: "MaximumCommitted", size: protocol.MaxPacketSize, committed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var clockReads atomic.Uint64
				now := func() time.Time {
					clockReads.Add(1)
					return time.Now()
				}
				budget, err := retention.NewBudget(retention.Limits{Packets: 3, Bytes: 2 * protocol.MaxEncodedFrameSize})
				if err != nil {
					t.Fatal(err)
				}
				ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{
					Packets: 3, Bytes: 2 * protocol.MaxEncodedFrameSize, ControlPreemption: true,
				}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err := NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: protocol.MaxEncodedFrameSize}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- scheduler.Run(ctx) }()
				defer func() {
					cancel()
					if err := <-result; !errors.Is(err, context.Canceled) {
						t.Errorf("Run() = %v, want cancellation", err)
					}
				}()
				abandoned := false
				registration := schedulerRegistration(1, 1, store)
				registration.Abandon = func() { abandoned = true }
				if err := scheduler.Register(ctx, registration); err != nil {
					t.Fatal(err)
				}
				prefixDeadline := now().Add(time.Second)
				prefix := ingressProbeItem(wgpacket.TransportData, 64, prefixDeadline)
				prefix.Value.Payload[4] = 7
				if err := ingress.Push(prefix); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if count, _ := store.backlog(); count != 1 || scheduler.packetID != 1 {
					t.Fatal("initial packet did not fill the live generation")
				}
				var sent protocol.Data
				if tt.committed {
					sent = takeOneTransmission(t, store)
				}
				synctest.Wait()
				normalDeadline := now().Add(4 * abandonmentCheckInterval)
				if err := ingress.Push(ingressProbeItem(wgpacket.TransportData, tt.size, normalDeadline)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if ingress.Len() != 0 || scheduler.packetID != 1 {
					t.Fatal("normal packet did not remain held behind the full generation")
				}
				controlDeadline := normalDeadline.Add(4 * abandonmentCheckInterval)
				control := ingressProbeItem(wgpacket.HandshakeInitiation, 0, controlDeadline)
				controlSize := control.Size
				if err := ingress.Push(control); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				_, retainedBytes := store.backlog()
				want := retention.Usage{Packets: 3, Bytes: int(retainedBytes) + tt.size + controlSize}
				if usage := budget.Usage(); usage != want || ingress.Len() != 0 {
					t.Fatalf("preempted active ownership = %+v, want %+v with no queued ingress", usage, want)
				}
				synctest.Sleep(time.Until(normalDeadline) - time.Nanosecond)
				synctest.Wait()
				if usage := budget.Usage(); usage != want {
					t.Fatal("active generation released a fresh preempted packet before its deadline")
				}
				synctest.Sleep(time.Nanosecond)
				synctest.Wait()
				want = retention.Usage{Packets: 2, Bytes: int(retainedBytes) + controlSize}
				if usage := budget.Usage(); usage != want || scheduler.packetID != 1 || store.closed || abandoned {
					t.Fatalf("active transport expiry retained %+v, want only prefix and control %+v on the same generation", usage, want)
				}
				if !tt.committed {
					sent = takeOneTransmission(t, store)
				}
				if sent.PacketID != 1 || sent.Payload[4] != 7 || sent.DeadlineMicros != uint64(prefixDeadline.UnixMicro()) {
					t.Fatal("preempted expiry changed the live prefix payload, identity, or deadline")
				}
				synctest.Wait()
				if scheduler.packetID != 1 || budget.Usage() != want {
					t.Fatal("local commitment released hard retained capacity before parsing feedback")
				}
				started := time.Now()
				source := protocol.LaneGeneration{LaneID: 1, Generation: 1}
				report := protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 1}
				if err := scheduler.ObserveDeliveryReport(ctx, source, report, uint64(now().UnixMicro()), make(chan error, 1)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if !time.Now().Equal(started) || scheduler.packetID != 2 {
					t.Fatal("feedback did not immediately admit the still-fresh preempting control")
				}
				data := takeOneTransmission(t, store)
				if data.PacketID != 2 || wgpacket.Classify(data.Payload) != wgpacket.HandshakeInitiation ||
					data.DeadlineMicros != uint64(controlDeadline.UnixMicro()) {
					t.Fatal("feedback resurrected expired transport or changed the pending control")
				}
				if usage := budget.Usage(); usage.Packets != 1 || ingress.Len() != 0 {
					t.Fatalf("control admission retained unexpected ownership: %+v", usage)
				}
				report.DataPackets = 2
				if err := scheduler.ObserveDeliveryReport(ctx, source, report, uint64(now().UnixMicro()), make(chan error, 1)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if usage := budget.Usage(); usage != (retention.Usage{}) || abandoned {
					t.Fatalf("active expiry and feedback retained ownership: %+v", usage)
				}
				before := clockReads.Load()
				synctest.Sleep(2 * abandonmentCheckInterval)
				synctest.Wait()
				if clockReads.Load() != before {
					t.Fatal("recovered generation continued polling after its final feedback")
				}
			})
		})
	}
}

func TestSchedulerRunRestoresUnderPressure(t *testing.T) {
	for _, action := range []string{"Empty", "FullNormal", "FullControl", "FullBytes", "SharedBudget", "Closed", "Expired"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var offset atomic.Int64
				now := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
				budget, err := retention.NewBudget(retention.Limits{Packets: 5, Bytes: 4096})
				if err != nil {
					t.Fatal(err)
				}
				ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{
					Packets: 2, Bytes: 512, ControlPreemption: true,
				}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				other, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: 2, Bytes: 512}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				scheduler, err := NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: 512}, budget, now)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var runErr error
				done := make(chan struct{})
				go func() {
					runErr = scheduler.Run(ctx)
					close(done)
				}()
				defer func() {
					cancel()
					<-done
					if !errors.Is(runErr, context.Canceled) {
						t.Errorf("Run() = %v, want cancellation", runErr)
					}
				}()
				if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
					t.Fatal(err)
				}
				deadline := now().Add(time.Second)
				for index, kind := range []wgpacket.Kind{wgpacket.TransportData, wgpacket.TransportData, wgpacket.HandshakeInitiation} {
					item := ingressProbeItem(kind, 64, deadline)
					item.Value.Payload[4] = byte(index)
					if err := ingress.Push(item); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
				}
				if usage := budget.Usage(); usage.Packets != 3 || ingress.Len() != 0 || scheduler.packetID != 1 {
					t.Fatal("fixture did not hold normal and control work beside the assigned prefix")
				}
				want := []byte{2, 1}
				waiting := ingress
				fillCount, fillSize := 0, 64
				fillKind := wgpacket.TransportData
				switch action {
				case "FullNormal":
					fillCount = 2
					want = []byte{2, 4}
				case "FullControl":
					fillCount = 2
					fillKind = wgpacket.HandshakeInitiation
					want = []byte{3, 4}
				case "FullBytes":
					fillCount, fillSize = 1, 512
					want = []byte{2}
				case "SharedBudget":
					waiting = other
					fillCount = 2
				case "Closed", "Expired":
					want = nil
				}
				for index := range fillCount {
					item := ingressProbeItem(fillKind, fillSize, deadline)
					item.Value.Payload[4] = byte(index + 3)
					if err := waiting.Push(item); err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				if usage := budget.Usage(); usage.Packets != 3+fillCount || scheduler.packetID != 1 {
					t.Fatal("pressure setup lost owned work or admitted a packet through the full lane")
				}
				if action == "Expired" {
					offset.Add(int64(2 * time.Second))
				}
				cancel()
				if action == "Closed" {
					ingress.Close()
				}
				<-done
				synctest.Wait()
				if !store.closed || ingress.Len() != len(want) {
					t.Fatalf("shutdown retained %d ingress items, want %d with a closed old store", ingress.Len(), len(want))
				}
				wantBytes := 0
				for _, marker := range want {
					if marker == 2 || action == "FullControl" {
						wantBytes += len(relayWireGuardPacket(wgpacket.HandshakeInitiation))
					} else {
						wantBytes += 64
					}
				}
				usage := retention.Usage{Packets: len(want) + other.Len(), Bytes: wantBytes + other.Bytes()}
				if actual := budget.Usage(); actual != usage {
					t.Fatalf("restored aggregate ownership = %+v, want %+v", actual, usage)
				}
				if len(want) > 0 {
					next, err := NewScheduler(ingress)
					if err != nil {
						t.Fatal(err)
					}
					nextStore, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 4, Bytes: 4096}, budget, now)
					if err != nil {
						t.Fatal(err)
					}
					nextContext, nextCancel := context.WithCancel(t.Context())
					defer nextCancel()
					nextResult := make(chan error, 1)
					go func() { nextResult <- next.Run(nextContext) }()
					defer func() {
						nextCancel()
						if err := <-nextResult; !errors.Is(err, context.Canceled) {
							t.Errorf("replacement Run() = %v, want cancellation", err)
						}
					}()
					if err := next.Register(nextContext, schedulerRegistration(1, 1, nextStore)); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					if count, _ := nextStore.backlog(); count != len(want) || ingress.Len() != 0 {
						t.Fatal("replacement scheduler did not consume the restored ingress")
					}
					for index, marker := range want {
						data := takeOneTransmission(t, nextStore)
						kind, size := wgpacket.TransportData, 64
						if marker == 2 || action == "FullControl" {
							kind = wgpacket.HandshakeInitiation
							size = len(relayWireGuardPacket(kind))
						}
						if data.PacketID != uint64(index+1) || data.Payload[4] != marker || len(data.Payload) != size ||
							wgpacket.Classify(data.Payload) != kind || data.DeadlineMicros != uint64(deadline.UnixMicro()) {
							t.Fatalf("restored item %d changed priority, payload, deadline, or the new packet namespace", index)
						}
					}
					if err := next.ObserveDeliveryReport(nextContext, protocol.LaneGeneration{LaneID: 1, Generation: 1},
						protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: uint64(len(want))},
						uint64(now().UnixMicro()), make(chan error, 1)); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
				}
				if actual := budget.Usage(); actual != (retention.Usage{Packets: other.Len(), Bytes: other.Bytes()}) {
					t.Fatalf("restored feedback retained unexpected ownership: %+v", actual)
				}
				other.Close()
				ingress.Close()
				if actual := budget.Usage(); actual != (retention.Usage{}) {
					t.Fatalf("restoration shutdown leaked aggregate ownership: %+v", actual)
				}
			})
		})
	}
}

func TestSchedulerRunFeedbackGenerationOrder(t *testing.T) {
	for _, tt := range []struct {
		name        string
		reportFirst bool
		staleWake   bool
	}{
		{name: "ReportThenReplace", reportFirst: true},
		{name: "ReportThenReplaceWithStaleWake", reportFirst: true, staleWake: true},
		{name: "ReplaceThenReport"},
		{name: "ReplaceThenReportWithStaleWake", staleWake: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const packets = 2 * maximumScheduleBatchPackets
				const prefix = maximumScheduleBatchPackets + 1
				now := time.Unix(123, 0)
				deadline := now.Add(time.Second)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				var scheduler *Scheduler
				var oldStore, nextStore *TransmissionStore
				var next LaneRegistration
				armed, triggered, collected, completed := false, false, false, false
				abandoned := false
				oldValidations, nextValidations := 0, 0
				reportResult := make(chan error, 1)
				registerResult := make(chan error, 1)
				currentReportResult := make(chan error, 1)
				clock := func() time.Time {
					if !armed || completed {
						return now
					}
					if collected {
						if len(currentReportResult) == 0 {
							return now
						}
						if err := <-currentReportResult; err != nil || nextStore.reportedPackets != 1 || nextValidations != 1 {
							t.Fatalf("replacement report = %v, reported packets %d, validations %d", err, nextStore.reportedPackets, nextValidations)
						}
						want := packets - 1
						if tt.reportFirst {
							want--
						}
						if count, _ := nextStore.backlog(); count != want {
							t.Fatal("replacement feedback did not release exactly its own committed prefix")
						}
						completed = true
						cancel()
						return now
					}
					if !triggered && scheduler.packetID == prefix {
						triggered = true
						for packetID := uint64(1); packetID <= prefix; packetID++ {
							data := takeOneTransmission(t, oldStore)
							if data.PacketID != packetID || data.Payload[4] != byte(packetID) {
								t.Fatal("initial committed prefix changed identity or order")
							}
						}
						report := schedulerEvent{
							kind: schedulerReport, laneID: 1, generation: 1, result: reportResult,
							report:        protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 1},
							receiveMicros: uint64(now.UnixMicro()) + 1,
						}
						registration := schedulerEvent{kind: schedulerRegister, registration: next, result: registerResult}
						if tt.reportFirst {
							scheduler.events <- report
							scheduler.events <- registration
						} else {
							scheduler.events <- registration
							scheduler.events <- report
						}
					}
					if scheduler.packetID != packets || len(reportResult) == 0 || len(registerResult) == 0 {
						return now
					}
					if err := <-registerResult; err != nil {
						t.Fatal(err)
					}
					if err := <-reportResult; err != nil {
						t.Fatalf("ordered report = %v, want accepted progress or ignored stale generation", err)
					}
					first := uint64(1)
					wantOldReports := uint64(0)
					if tt.reportFirst {
						first = 2
						wantOldReports = 1
					}
					if !abandoned || !oldStore.closed || oldStore.reportedPackets != wantOldReports || nextStore.reportedPackets != 0 ||
						oldValidations != int(wantOldReports) || nextValidations != 0 {
						t.Fatal("generation ordering changed validation, credit, or old-generation cleanup")
					}
					if count, _ := nextStore.backlog(); count != packets-int(first)+1 {
						t.Fatalf("replacement retained %d packets, want only the unreported migration and new suffix", count)
					}
					for packetID := first; packetID <= packets; packetID++ {
						data := takeOneTransmission(t, nextStore)
						if data.PacketID != packetID || data.Payload[4] != byte(packetID) ||
							data.DeadlineMicros != uint64(deadline.UnixMicro()) {
							t.Fatalf("generation transition changed packet %d identity, payload, order, or deadline", packetID)
						}
					}
					collected = true
					scheduler.events <- schedulerEvent{
						kind: schedulerReport, laneID: 1, generation: 2, result: currentReportResult,
						report:        protocol.DeliveryReport{LaneID: 1, Generation: 2, DataPackets: 1},
						receiveMicros: uint64(now.UnixMicro()) + 2,
					}
					return now
				}
				budget, err := retention.NewBudget(retention.Limits{Packets: packets, Bytes: 65536})
				if err != nil {
					t.Fatal(err)
				}
				ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: packets, Bytes: 32768}, budget, clock)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err = NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				oldStore, err = NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: prefix, Bytes: 32768}, budget, func() time.Time { return now })
				if err != nil {
					t.Fatal(err)
				}
				nextStore, err = NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: packets + 1, Bytes: 32768}, budget, func() time.Time { return now })
				if err != nil {
					t.Fatal(err)
				}
				registration := schedulerRegistration(1, 1, oldStore)
				registration.ValidatePingProgress = func(uint64) bool {
					oldValidations++
					return true
				}
				registration.Abandon = func() {
					abandoned = true
					if tt.staleWake {
						select {
						case scheduler.transportReady <- struct{}{}:
						default:
						}
					}
				}
				next = schedulerRegistration(1, 1, nextStore)
				next.Generation = 2
				next.ValidatePingProgress = func(uint64) bool {
					nextValidations++
					return true
				}
				for index := range packets {
					item := ingressProbeItem(wgpacket.TransportData, 64, deadline)
					item.Value.Payload[4] = byte(index + 1)
					if err := ingress.Push(item); err != nil {
						t.Fatal(err)
					}
				}
				scheduler.events <- schedulerEvent{kind: schedulerRegister, registration: registration, result: make(chan error, 1)}
				armed = true
				if err := scheduler.Run(ctx); !errors.Is(err, context.Canceled) || !triggered || !collected || !completed {
					t.Fatalf("Run() = %v, transition triggered %t, data collected %t, feedback completed %t", err, triggered, collected, completed)
				}
				if !nextStore.closed || ingress.Len() != 0 || budget.Usage() != (retention.Usage{}) {
					t.Fatal("generation/report ordering retained ingress, generation state, or aggregate ownership")
				}
			})
		})
	}
}
