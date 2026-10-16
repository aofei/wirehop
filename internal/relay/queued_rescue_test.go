package relay

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerRescuesQueuedTransportWithoutIngress(t *testing.T) {
	rescuePreferred := protocol.LaneID(1)
	now := time.Unix(100, 0)
	source := schedulerLane(t, 1, 1, 5_000, 200_000_000)
	target := schedulerLane(t, 2, 1, 300_000, 1_000_000)
	target.rateObserved = false
	target.registration.Store.transportReported.Store(0)
	abandoned := false
	source.registration.Abandon = func() { abandoned = true }
	for _, lane := range []*scheduledLane{source, target} {
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	source.lastProgressAt = now
	if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source.registration.Store)
	queued := schedulerTransmission(2, wgpacket.TransportData, now.Add(5*time.Second))
	if err := source.registration.Store.push(queued); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: target}
	var scheduler Scheduler
	scheduler.checkAbandonment(lanes, &rescuePreferred, now)
	if packets, _ := target.registration.Store.backlog(); packets != 1 {
		t.Fatalf("unsent transport remained on stalled source without new ingress: target backlog = %d, want 1", packets)
	}
	if rescuePreferred != 2 || !scheduler.lastTransportAt.Equal(now) || !scheduler.transportHoldUntil.After(now) {
		t.Fatal("rescue did not promote the current primary through transport assignment")
	}
	if abandoned || source.abandoning || source.registration.Store.closed {
		t.Fatal("rescuing unsent work closed the committed source generation")
	}
	if source.registration.Store.sentPackets != 1 || source.registration.Store.reportedPackets != 0 || source.registration.Store.sent.len() != 1 {
		t.Fatal("unsent rescue modified the source committed prefix")
	}
	got := takeOneTransmission(t, target.registration.Store)
	if got.PacketID != queued.packetID || got.DeadlineMicros != queued.deadlineMicros {
		t.Fatalf("rescued identity = %+v, want original ID and deadline", got)
	}
}

func TestSchedulerQueuedRescueEligibility(t *testing.T) {
	for _, mode := range []string{
		"FreshAtRiskPrefix", "HealthySource", "AbandoningSource", "SourcePaused", "TargetPaused", "SessionPaused",
		"StalledTarget", "FullTarget", "TooLateTarget", "AlreadyMigrated", "SourceDiscovery", "TargetDiscovery", "OtherPrimary",
	} {
		t.Run(mode, func(t *testing.T) {
			now := time.Unix(100, 0)
			source := schedulerLane(t, 1, 1, 5_000, 200_000_000)
			target := schedulerLane(t, 2, 1, 300_000, 1_000_000)
			target.rateObserved = false
			target.registration.Store.transportReported.Store(0)
			for _, lane := range []*scheduledLane{source, target} {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			source.registration.Abandon = func() { t.Error("unsent rescue abandoned the source") }
			source.lastProgressAt = now
			deadline := now.Add(time.Second)
			if mode == "FreshAtRiskPrefix" {
				deadline = now.Add(2*time.Second + time.Millisecond)
			}
			if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, deadline)); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			queued := schedulerTransmission(2, wgpacket.TransportData, now.Add(5*time.Second))
			queued.migrated = mode == "AlreadyMigrated"
			if err := source.registration.Store.push(queued); err != nil {
				t.Fatal(err)
			}
			var scheduler Scheduler
			preferred := protocol.LaneID(1)
			lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: target}
			want := mode == "FreshAtRiskPrefix" || mode == "SourceDiscovery" || mode == "TargetDiscovery" || mode == "OtherPrimary"
			switch mode {
			case "HealthySource":
				source.lastProgressAt = now.Add(2 * time.Second)
			case "AbandoningSource":
				source.abandoning = true
			case "SourcePaused":
				source.registration.Store.beginMeasurementPause()
			case "TargetPaused":
				target.registration.Store.beginMeasurementPause()
			case "SessionPaused":
				scheduler.measurementStarted = now
			case "StalledTarget":
				target.lastProgressAt = now
				if err := target.registration.Store.push(schedulerTransmission(3, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, target.registration.Store)
			case "FullTarget":
				target.registration.Store.limits.Packets = 1
				if err := target.registration.Store.push(schedulerTransmission(3, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
					t.Fatal(err)
				}
			case "TooLateTarget":
				target.rttMicros = 6_000_000
			case "SourceDiscovery":
				source.probeUntil = now.Add(5 * time.Second)
			case "TargetDiscovery":
				target.probeUntil = now.Add(5 * time.Second)
			case "OtherPrimary":
				primary := schedulerLane(t, 3, 1, 1_000_000, 1_000_000)
				primary.rateObserved = false
				t.Cleanup(func() { releaseTransmissions(primary.registration.Store.drain()) })
				lanes[3] = primary
				preferred = 3
			}
			before, _ := target.registration.Store.backlog()
			now = now.Add(2 * time.Second)
			scheduler.checkAbandonment(lanes, &preferred, now)
			after, _ := target.registration.Store.backlog()
			if (after == before+1) != want {
				t.Fatalf("rescued = %t, want %t (backlog %d -> %d)", after == before+1, want, before, after)
			}
			if mode == "OtherPrimary" && (preferred != 3 || !scheduler.transportHoldUntil.IsZero()) {
				t.Fatal("rescuing a non-primary source replaced the healthy primary")
			}
		})
	}
}

func TestSchedulerQueuedRescueRetriesPartialCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	source := schedulerLane(t, 1, 1, 5_000, 200_000_000)
	target := schedulerLaneWithLimits(t, 2, 1, 300_000, 1_000_000, packetqueue.Limits{Packets: 1, Bytes: 4096})
	target.rateObserved = false
	target.registration.Store.transportReported.Store(0)
	for _, lane := range []*scheduledLane{source, target} {
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	source.lastProgressAt = now
	if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source.registration.Store)
	for _, id := range []uint64{2, 3} {
		if err := source.registration.Store.push(schedulerTransmission(id, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Second)
	var scheduler Scheduler
	preferred := protocol.LaneID(1)
	lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: target}
	for _, id := range []uint64{2, 3} {
		scheduler.checkAbandonment(lanes, &preferred, now)
		if got := takeOneTransmission(t, target.registration.Store); got.PacketID != id {
			t.Fatalf("rescued packet ID = %d, want %d", got.PacketID, id)
		}
		if _, _, err := target.registration.Store.acknowledge(id-1, uint64(now.UnixMicro())); err != nil {
			t.Fatal(err)
		}
	}
	if packets, _ := source.registration.Store.backlog(); packets != 1 || preferred != 2 {
		t.Fatalf("partial rescue retained %d source packets with preferred %d", packets, preferred)
	}
}

func TestTransmissionStoreTransferQueuedTo(t *testing.T) {
	for _, limit := range []string{"Packets", "Bytes", "Deadline"} {
		t.Run(limit, func(t *testing.T) {
			now := time.Unix(100, 0)
			source, target, budget := queuedRescueStores(t, &now)
			for _, id := range []uint64{1, 2, 3} {
				if err := source.push(schedulerTransmission(id, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
					t.Fatal(err)
				}
			}
			takeOneTransmission(t, source)
			if err := target.push(schedulerTransmission(4, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
				t.Fatal(err)
			}
			before := budget.Usage()
			first, _ := source.normal.peek()
			payload := bytes.Clone(first.packet.Payload)
			originalDelivery := source.sent.items[0].delivery
			originalLimited := source.sent.items[0].applicationLimited
			delay := func(_, _, backlog uint64) uint64 {
				if limit == "Deadline" && backlog > uint64(first.size) {
					return 5_000_000
				}
				return 0
			}
			if limit == "Packets" {
				target.limits.Packets = 2
			}
			if limit == "Bytes" {
				target.limits.Bytes = target.bytes + int(first.size)
			}
			if moved := source.transferQueuedTo(target, now, delay); moved != 1 {
				t.Fatalf("transfer moved %d packets, want 1", moved)
			}
			if budget.Usage() != before || source.sentPackets != 1 || source.reportedPackets != 0 || target.sentPackets != 0 ||
				source.sent.items[0].delivery != originalDelivery || source.sent.items[0].applicationLimited != originalLimited {
				t.Fatal("transfer changed aggregate ownership or committed delivery provenance")
			}
			remaining, _ := source.normal.peek()
			if remaining.packetID != 3 || remaining.migrated {
				t.Fatal("capacity rejection changed source FIFO or marked an unmoved packet")
			}
			if got := takeOneTransmission(t, target); got.PacketID != 4 {
				t.Fatal("transfer overtook existing destination work")
			}
			got := takeOneTransmission(t, target)
			if got.PacketID != 2 || got.DeadlineMicros != first.deadlineMicros || !bytes.Equal(got.Payload, payload) ||
				!target.sent.items[len(target.sent.items)-1].migrated {
				t.Fatal("transfer changed identity, deadline, payload, or one-migration bound")
			}
			assertQueuedRescueAccounting(t, source, target)
		})
	}
}

func TestTransmissionStoreTransferQueuedToWakesExposedPadding(t *testing.T) {
	now := time.Unix(100, 0)
	source, target, _ := queuedRescueStores(t, &now)
	if err := source.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source)
	large := schedulerTransmission(2, wgpacket.TransportData, now.Add(5*time.Second))
	large.packet.Payload = make([]byte, 8192)
	large.packet.Payload[0] = 4
	if err := source.push(large); err != nil {
		t.Fatal(err)
	}
	padding := retainedTransmission{deadlineMicros: uint64(now.Add(5 * time.Second).UnixMicro()), packet: datagram.Packet{Payload: make([]byte, protocol.ProbePayloadSize)}}
	if err := source.push(padding); err != nil {
		t.Fatal(err)
	}
	source.setDeliveryWindows(source.sentBytes+uint64(dataFrameSize(padding.data())), source.sentBytes+uint64(dataFrameSize(padding.data())))
	<-source.Ready()
	var frames [1]protocol.Data
	var ownership [1]datagram.Packet
	count, err := source.takeBatch(frames[:], ownership[:], 8192, 0)
	if count != 0 || !errors.Is(err, packetqueue.ErrEmpty) {
		t.Fatalf("large head did not exhaust credit: %d, %v", count, err)
	}
	if moved := source.transferQueuedTo(target, now, func(_, _, _ uint64) uint64 { return 0 }); moved != 1 {
		t.Fatalf("moved %d packets, want only the real transport head", moved)
	}
	for name, store := range map[string]*TransmissionStore{"Source": source, "Target": target} {
		select {
		case <-store.Ready():
		default:
			t.Fatalf("%s writer was not notified", name)
		}
	}
	if source.sentPackets != 1 {
		t.Fatal("transfer changed source commitments before the next writer turn")
	}
	if got := takeOneTransmission(t, source); got.PacketID != 0 || len(got.Payload) != protocol.ProbePayloadSize {
		t.Fatalf("source did not retain and send exposed padding: %+v", got)
	}
}

func TestTransmissionStoreTransferQueuedToExclusions(t *testing.T) {
	for _, mode := range []string{"SourceClosed", "TargetClosed", "SourcePaused", "TargetPaused", "Control", "Migrated", "Expired"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Unix(100, 0)
			source, target, _ := queuedRescueStores(t, &now)
			packet := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
			if mode == "Control" {
				packet = schedulerTransmission(1, wgpacket.HandshakeInitiation, now.Add(time.Second))
			}
			packet.migrated = mode == "Migrated"
			if err := source.push(packet); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "SourceClosed":
				takeOneTransmission(t, source)
				releaseTransmissions(source.drain())
			case "TargetClosed":
				if err := target.push(schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second))); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, target)
				releaseTransmissions(target.drain())
			case "SourcePaused":
				source.beginMeasurementPause()
			case "TargetPaused":
				target.beginMeasurementPause()
			case "Expired":
				now = now.Add(time.Second)
			}
			if moved := source.transferQueuedTo(target, now, func(_, _, _ uint64) uint64 { return 0 }); moved != 0 {
				t.Fatalf("excluded transfer moved %d packets", moved)
			}
		})
	}
}

func TestTransmissionStoreTransferQueuedToConcurrentCommit(t *testing.T) {
	now := time.Unix(100, 0)
	source, target, _ := queuedRescueStores(t, &now)
	const packets = 64
	for id := uint64(1); id <= packets; id++ {
		if err := source.push(schedulerTransmission(id, wgpacket.TransportData, now.Add(time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	written := make(chan []uint64, 1)
	writeError := make(chan error, 1)
	go func() {
		<-start
		var ids []uint64
		for {
			var frames [1]protocol.Data
			var ownership [1]datagram.Packet
			count, err := source.takeBatch(frames[:], ownership[:], 4096, 0)
			releaseBatchOwnership(ownership[:count])
			if errors.Is(err, packetqueue.ErrEmpty) {
				written <- ids
				return
			}
			if err != nil {
				writeError <- err
				written <- ids
				return
			}
			ids = append(ids, frames[0].PacketID)
		}
	}()
	close(start)
	source.transferQueuedTo(target, now, func(_, _, _ uint64) uint64 { return 0 })
	ids := <-written
	select {
	case err := <-writeError:
		t.Fatal(err)
	default:
	}
	for target.normal.len() > 0 {
		ids = append(ids, takeOneTransmission(t, target).PacketID)
	}
	seen := make(map[uint64]bool)
	for _, id := range ids {
		if id == 0 || id > packets || seen[id] {
			t.Fatalf("concurrent transfer duplicated or changed packet %d", id)
		}
		seen[id] = true
	}
	if len(seen) != packets {
		t.Fatalf("concurrent transfer delivered %d identities, want %d", len(seen), packets)
	}
	assertQueuedRescueAccounting(t, source, target)
}

func queuedRescueStores(t *testing.T, now *time.Time) (*TransmissionStore, *TransmissionStore, *retention.Budget) {
	t.Helper()
	budget, err := retention.NewBudget(retention.Limits{Packets: 256, Bytes: 2 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	stores := [2]*TransmissionStore{}
	for index := range stores {
		store, err := newTransmissionStoreWithBudget(packetqueue.Limits{Packets: 128, Bytes: 1024 * 1024}, func() time.Time { return *now }, budget)
		if err != nil {
			t.Fatal(err)
		}
		stores[index] = store
	}
	t.Cleanup(func() {
		for _, store := range stores {
			releaseTransmissions(store.drain())
		}
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Errorf("transfer leaked aggregate retention: %+v", got)
		}
	})
	return stores[0], stores[1], budget
}

func TestSchedulerQueuedRescueSkipsUntimelyHead(t *testing.T) {
	now := time.Unix(100, 0)
	source := schedulerLane(t, 1, 1, 5_000, 200_000_000)
	target := schedulerLane(t, 2, 1, 4_000_000, 1_000_000)
	target.rateObserved = false
	target.registration.Store.transportReported.Store(0)
	for _, lane := range []*scheduledLane{source, target} {
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	source.lastProgressAt = now
	if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source.registration.Store)
	for index, lifetime := range []time.Duration{3 * time.Second, 5 * time.Second} {
		if err := source.registration.Store.push(schedulerTransmission(uint64(index+2), wgpacket.TransportData, now.Add(lifetime))); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Second)
	var scheduler Scheduler
	preferred := protocol.LaneID(1)
	scheduler.checkAbandonment(map[protocol.LaneID]*scheduledLane{1: source, 2: target}, &preferred, now)
	if packets, _ := target.registration.Store.backlog(); packets != 1 {
		t.Fatalf("later useful packet was blocked by the one-second head: %d transferred", packets)
	}
	if got := takeOneTransmission(t, target.registration.Store); got.PacketID != 3 {
		t.Fatalf("rescued ID = %d, want later three-second packet 3", got.PacketID)
	}
	if head, _ := source.registration.Store.normal.peek(); head.packetID != 2 || head.migrated {
		t.Fatal("untimely source head was lost or marked migrated")
	}
}

func TestTransmissionStoreTransferQueuedToSkipsOversizedHead(t *testing.T) {
	now := time.Unix(100, 0)
	source, target, _ := queuedRescueStores(t, &now)
	source.setDeliveryWindows(uint64(source.limits.Bytes), uint64(source.limits.Bytes))
	for id := uint64(1); id <= 4; id++ {
		packet := schedulerTransmission(id, wgpacket.TransportData, now.Add(time.Second))
		if id%2 != 0 {
			packet.packet.Payload = make([]byte, 8192)
			packet.packet.Payload[0] = 4
		}
		if err := source.push(packet); err != nil {
			t.Fatal(err)
		}
	}
	target.limits.Bytes = 4096
	if count := source.transferQueuedTo(target, now, func(_, _, _ uint64) uint64 { return 0 }); count != 2 {
		t.Fatalf("mixed-size transfer moved %d packets, want 2", count)
	}
	for _, id := range []uint64{2, 4} {
		if got := takeOneTransmission(t, target); got.PacketID != id {
			t.Fatalf("destination subsequence = %d, want %d", got.PacketID, id)
		}
	}
	for _, id := range []uint64{1, 3} {
		if got := takeOneTransmission(t, source); got.PacketID != id {
			t.Fatalf("source subsequence = %d, want %d", got.PacketID, id)
		}
	}
	assertQueuedRescueAccounting(t, source, target)
}

func assertQueuedRescueAccounting(t *testing.T, stores ...*TransmissionStore) {
	t.Helper()
	var total retention.Usage
	for _, store := range stores {
		packets, bytes := 0, 0
		visit := func(transmission retainedTransmission) bool {
			packets++
			bytes += int(transmission.size)
			if transmission.budget != store.budget {
				t.Error("transferred descriptor lost aggregate ownership")
			}
			return true
		}
		store.control.each(visit)
		store.normal.each(visit)
		store.sent.each(visit)
		if gotPackets, gotBytes := store.backlog(); gotPackets != packets || gotBytes != uint64(bytes) {
			t.Errorf("store backlog = %d/%d, want %d/%d", gotPackets, gotBytes, packets, bytes)
		}
		total.Packets += packets
		total.Bytes += bytes
	}
	if got := stores[0].budget.Usage(); got != total {
		t.Errorf("aggregate usage = %+v, want %+v", got, total)
	}
}

func TestSchedulerQueuedRescueUsesOptimisticBounds(t *testing.T) {
	for _, skipFirst := range []bool{false, true} {
		name := "EarlierSmallFrame"
		if skipFirst {
			name = "FirstTargetMovesNothing"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			source := schedulerLane(t, 1, 1, 5_000, 200_000_000)
			first := schedulerLaneWithLimits(t, 2, 1, 4_000_000, 1_000_000, packetqueue.Limits{Packets: 8, Bytes: 4096})
			second := schedulerLane(t, 3, 1, 6_000_000, 1_000_000)
			first.rateObserved = false
			second.rateObserved = false
			for _, lane := range []*scheduledLane{source, first, second} {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			source.lastProgressAt = now
			if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			lifetime := 5 * time.Second
			if skipFirst {
				lifetime = 3 * time.Second
			}
			if err := source.registration.Store.push(schedulerTransmission(2, wgpacket.TransportData, now.Add(lifetime))); err != nil {
				t.Fatal(err)
			}
			large := schedulerTransmission(3, wgpacket.TransportData, now.Add(6*time.Second))
			large.packet.Payload = make([]byte, 8192)
			large.packet.Payload[0] = 4
			if err := source.registration.Store.push(large); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Second)
			var scheduler Scheduler
			preferred := protocol.LaneID(1)
			scheduler.checkAbandonment(map[protocol.LaneID]*scheduledLane{1: source, 2: first, 3: second}, &preferred, now)
			want := protocol.LaneID(2)
			packetID := uint64(2)
			target, unused := first, second
			if skipFirst {
				want, packetID = 3, 3
				target, unused = second, first
			}
			if preferred != want {
				t.Fatalf("rescue preferred lane = %d, want %d", preferred, want)
			}
			if got := takeOneTransmission(t, target.registration.Store); got.PacketID != packetID {
				t.Fatalf("rescued packet = %d, want %d", got.PacketID, packetID)
			}
			if packets, _ := unused.registration.Store.backlog(); packets != 0 {
				t.Fatal("one rescue pass distributed work over multiple targets")
			}
		})
	}
}
