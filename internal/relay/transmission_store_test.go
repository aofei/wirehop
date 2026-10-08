package relay

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestTransmissionStoreValidation(t *testing.T) {
	for _, limits := range []packetqueue.Limits{
		{},
		{Packets: 1, Bytes: 1024, ControlPreemption: true},
	} {
		if _, err := NewTransmissionStore(limits); !errors.Is(err, ErrInvalidTransmissionStore) {
			t.Fatalf("NewTransmissionStore(%+v) error = %v", limits, err)
		}
	}

	now := time.Now()
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 2, Bytes: 4096}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
	migratedControl := schedulerTransmission(2, wgpacket.HandshakeInitiation, now.Add(time.Second))
	migratedControl.migrated = true
	for _, transmission := range []retainedTransmission{
		{},
		{
			packetID: valid.packetID, wireDeadline: valid.wireDeadline, deadline: valid.deadline,
			packet: datagram.Packet{Kind: wgpacket.HandshakeInitiation, Payload: valid.packet.Payload},
		},
		migratedControl,
	} {
		if err := store.push(transmission); !errors.Is(err, ErrInvalidTransmission) {
			t.Fatalf("push(%+v) error = %v, want %v", transmission, err, ErrInvalidTransmission)
		}
	}
	expired := valid
	expired.deadline = now
	if err := store.push(expired); !errors.Is(err, packetqueue.ErrExpired) {
		t.Fatalf("expired push error = %v, want %v", err, packetqueue.ErrExpired)
	}
}

func TestTransmissionStorePreservesWriteView(t *testing.T) {
	for _, action := range []string{
		"Drain", "Acknowledgment", "Expiry", "ExpiryThenAcknowledgment",
		"ConcurrentExpiryAndAcknowledgment", "ConcurrentExpiryAndDrain",
	} {
		t.Run(action, func(t *testing.T) {
			testTransmissionStorePreservesWriteView(t, action)
		})
	}
}

func testTransmissionStorePreservesWriteView(t *testing.T, action string) {
	t.Helper()
	now := time.Unix(100, 0)
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 1, Bytes: 4096}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
	local, err := datagram.ListenLocal(netip.MustParseAddrPort("127.0.0.1:0"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	peer, err := net.ListenUDP("udp4", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.WriteToUDPAddrPort(transmission.packet.Payload, local.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	packet, err := local.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transmission.packet = packet
	wantPayload := string(transmission.packet.Payload)
	if err := store.push(transmission); err != nil {
		packet.Release()
		t.Fatal(err)
	}
	var batch [1]protocol.Data
	var ownership [1]datagram.Packet
	count, err := store.takeBatch(batch[:], ownership[:], 4096)
	if err != nil || count != 1 {
		t.Fatalf("takeBatch() = %d, %v", count, err)
	}
	defer ownership[0].Release()
	switch action {
	case "Acknowledgment":
		if _, _, err := store.acknowledge(store.sentPackets, uint64(store.now().UnixMicro())); err != nil {
			t.Fatal(err)
		}
	case "Expiry", "ExpiryThenAcknowledgment":
		now = transmission.deadline
		store.expire(now)
		if store.sent.items[store.sent.head].packet.Payload != nil {
			t.Fatal("expired sent payload remains retained")
		}
		if action == "ExpiryThenAcknowledgment" {
			if _, stale, err := store.acknowledge(1, uint64(now.UnixMicro())); err != nil || stale {
				t.Fatalf("late acknowledge() = stale %t, %v", stale, err)
			}
			if store.transportReported.Load() != uint64(len(wantPayload)) {
				t.Fatal("late acknowledgment lost expired transport proof")
			}
		}
		releaseTransmissions(store.drain())
	case "Drain":
		releaseTransmissions(store.drain())
	case "ConcurrentExpiryAndAcknowledgment", "ConcurrentExpiryAndDrain":
		now = transmission.deadline
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Go(func() {
			<-start
			for range 64 {
				store.expire(now)
			}
		})
		workers.Go(func() {
			<-start
			for range 64 {
				if action == "ConcurrentExpiryAndDrain" {
					releaseTransmissions(store.drain())
				} else if _, stale, err := store.acknowledge(1, uint64(now.UnixMicro())); err != nil || stale {
					t.Errorf("concurrent acknowledge() = stale %t, %v", stale, err)
				}
			}
		})
		workers.Go(func() {
			<-start
			for range 64 {
				retained := ownership[0].Retain()
				if string(retained.Payload) != wantPayload {
					t.Error("concurrent writer payload changed")
				}
				retained.Release()
			}
		})
		close(start)
		workers.Wait()
		wantProof := uint64(len(wantPayload))
		if action == "ConcurrentExpiryAndDrain" {
			wantProof = 0
		}
		if store.transportReported.Load() != wantProof {
			t.Fatal("concurrent acknowledgment lost transport proof")
		}
		if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
			t.Fatalf("concurrent acknowledgment retained %d packets and %d bytes", packets, bytes)
		}
	}
	if wgpacket.Classify(ownership[0].Payload) != ownership[0].Kind {
		t.Fatal("writer ownership changed its packet classification")
	}
	if got := string(batch[0].Payload); got != wantPayload {
		t.Fatalf("payload after release = %q, want %q", got, wantPayload)
	}
	retained := ownership[0].Retain()
	retained.Release()
}

func testTransmissionStoreSentPayloadExpiryAfterCompaction(t *testing.T) {
	t.Helper()
	const total = maximumRetainedDequeCapacity * 4
	const remaining = maximumRetainedDequeCapacity / 2
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: total, Bytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	store, err := newTransmissionStoreWithBudget(packetqueue.Limits{Packets: total, Bytes: 1024 * 1024},
		func() time.Time { return now }, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	for packetID := uint64(1); packetID <= total; packetID++ {
		deadline := now.Add(time.Second)
		if packetID%2 == 0 {
			deadline = now.Add(10 * time.Second)
		}
		if err := store.push(schedulerTransmission(packetID, wgpacket.TransportData, deadline)); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, store)
	}
	if cap(store.sent.items) <= maximumRetainedDequeCapacity {
		t.Fatal("test did not grow an exceptional sent prefix")
	}
	acknowledged := uint64(total - remaining)
	if _, stale, err := store.acknowledge(acknowledged, uint64(now.UnixMicro())); err != nil || stale {
		t.Fatalf("acknowledge() = stale %t, %v", stale, err)
	}
	if cap(store.sent.items) != maximumRetainedDequeCapacity || store.sent.head != 0 {
		t.Fatal("acknowledgment did not compact and shrink the sent prefix")
	}
	acknowledged += 5
	if _, stale, err := store.acknowledge(acknowledged, uint64(now.UnixMicro())); err != nil || stale {
		t.Fatalf("partial acknowledge() = stale %t, %v", stale, err)
	}
	if store.sent.head != 5 {
		t.Fatal("test did not retain a consumed prefix")
	}
	charged := budget.Usage()
	now = now.Add(time.Second)
	store.expire(now)
	assertTransmissionStoreInvariants(t, store)
	store.sent.each(func(transmission retainedTransmission) bool {
		if (transmission.packet.Payload == nil) != (transmission.packetID%2 != 0) {
			t.Fatal("compacted sent payload did not respect its own deadline")
		}
		return true
	})
	now = now.Add(9 * time.Second)
	store.expire(now)
	assertTransmissionStoreInvariants(t, store)
	if budget.Usage() != charged || !store.sent.earliestDeadline.IsZero() {
		t.Fatal("full payload expiry changed accounting or retained an expiry bound")
	}
	if err := store.push(schedulerTransmission(total+1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if !store.sent.earliestDeadline.Equal(now.Add(time.Second)) {
		t.Fatal("compacted prefix did not rearm payload expiry")
	}
	if _, stale, err := store.acknowledge(total+1, uint64(now.UnixMicro())); err != nil || stale {
		t.Fatalf("final acknowledge() = stale %t, %v", stale, err)
	}
	assertTransmissionStoreInvariants(t, store)
	if budget.Usage() != (retention.Usage{}) || store.transportReported.Load() != minimumRateSampleBytes {
		t.Fatal("final acknowledgment lost accounting or saturated transport proof")
	}
}

func TestTransmissionStoreSentPayloadExpiry(t *testing.T) {
	t.Run("AfterCompaction", testTransmissionStoreSentPayloadExpiryAfterCompaction)
	t.Run("LateFeedbackAtCounterLimits", testTransmissionStoreSentPayloadExpiryLateFeedbackAtCounterLimits)
	for _, acknowledge := range []bool{false, true} {
		name := "Drain"
		if acknowledge {
			name = "Acknowledgment"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			budget, err := retention.NewBudget(retention.Limits{Packets: 4, Bytes: 32 * 1024})
			if err != nil {
				t.Fatal(err)
			}
			store, err := newTransmissionStoreWithBudget(packetqueue.Limits{Packets: 4, Bytes: 32 * 1024},
				func() time.Time { return now }, budget)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			for index, kind := range []wgpacket.Kind{wgpacket.TransportData, wgpacket.HandshakeInitiation, wgpacket.NonWireGuard} {
				deadline := now.Add(time.Duration(3-index) * time.Second)
				var transmission retainedTransmission
				if kind == wgpacket.NonWireGuard {
					transmission = retainedTransmission{deadline: deadline, packet: datagram.Packet{Payload: probePadding[:]}}
				} else {
					transmission = schedulerTransmission(uint64(index+1), kind, deadline)
				}
				if err := store.push(transmission); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, store)
			}
			charged := budget.Usage()
			start := now
			for elapsed := 0; elapsed <= 3; elapsed++ {
				now = start.Add(time.Duration(elapsed) * time.Second)
				store.expire(now)
				assertTransmissionStoreInvariants(t, store)
				if budget.Usage() != charged || store.sentPackets != 3 || store.reportedPackets != 0 {
					t.Fatal("payload expiry changed cumulative progress or retained accounting")
				}
				for index, transmission := range store.sent.items {
					if (transmission.packet.Payload == nil) != (elapsed >= 3-index) {
						t.Fatalf("payload %d expiry at %d seconds is incorrect", index, elapsed)
					}
				}
			}
			if !store.sent.earliestDeadline.IsZero() {
				t.Fatal("fully released prefix retained a payload-expiry bound")
			}
			// Reuse the bound with an expired descriptor still at the head.
			if err := store.push(schedulerTransmission(4, wgpacket.TransportData, now.Add(time.Second))); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, store)
			if !store.sent.earliestDeadline.Equal(now.Add(time.Second)) {
				t.Fatal("new sent payload did not rearm expiry")
			}
			if acknowledge {
				for count := uint64(1); count <= 4; count++ {
					if _, stale, err := store.acknowledge(count, uint64(now.UnixMicro())); err != nil || stale {
						t.Fatalf("acknowledge(%d) = stale %t, %v", count, stale, err)
					}
				}
				if store.transportReported.Load() != 2*wgpacket.TransportKeepaliveLength {
					t.Fatal("late feedback lost transport proof or counted control and probe bytes")
				}
			}
			releaseTransmissions(store.drain())
			if got := budget.Usage(); got != (retention.Usage{}) {
				t.Fatalf("completed generation retained ownership: %+v", got)
			}
		})
	}
}

func testTransmissionStoreSentPayloadExpiryLateFeedbackAtCounterLimits(t *testing.T) {
	now := time.Unix(100, 0)
	start := now
	limits := packetqueue.Limits{Packets: 3, Bytes: 96 * 1024}
	budget, err := retention.NewBudget(retention.Limits{Packets: limits.Packets, Bytes: limits.Bytes})
	if err != nil {
		t.Fatal(err)
	}
	store, err := newTransmissionStoreWithBudget(limits, func() time.Time { return now }, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	var sizes [3]int
	for index, kind := range []wgpacket.Kind{wgpacket.TransportData, wgpacket.TransportData, wgpacket.HandshakeInitiation} {
		transmission := schedulerTransmission(math.MaxUint64-uint64(2-index), kind, now.Add(time.Second))
		transmission.wireDeadline = math.MaxUint64 - math.MaxUint64%protocol.DeadlineResolutionMicros
		if index == 1 {
			transmission.packet.Payload = make([]byte, protocol.MaxPacketSize)
			transmission.packet.Payload[0] = 4
		}
		sizes[index] = dataFrameSize(transmission.data())
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
	}
	charged := budget.Usage()
	store.sentPackets = math.MaxUint64 - 3
	store.reportedPackets = store.sentPackets
	store.sentBytes = math.MaxUint64 - uint64(charged.Bytes)
	store.reportedBytes = store.sentBytes
	var batch [3]protocol.Data
	var ownership [3]datagram.Packet
	count, err := store.takeBatch(batch[:], ownership[:], limits.Bytes)
	defer releaseBatchOwnership(ownership[:count])
	if err != nil || count != len(batch) || store.sentPackets != math.MaxUint64 || store.sentBytes != math.MaxUint64 {
		t.Fatalf("takeBatch() = %d, %v, want complete prefix at both cumulative limits", count, err)
	}
	// Carrier priority sends the control first, followed by both transport packets.
	if batch[0].PacketID != math.MaxUint64 || len(batch[2].Payload) != protocol.MaxPacketSize {
		t.Fatal("boundary batch changed control priority or maximum payload length")
	}
	now = now.Add(time.Second)
	assessment := store.assessDeadlines(now, func(uint64) uint64 { return 0 })
	if !assessment.retained || !assessment.atRisk || !assessment.usefulDeadline.IsZero() || assessment.usefulBytes != 0 {
		t.Fatalf("expired prefix assessment = %+v", assessment)
	}
	assertTransmissionStoreInvariants(t, store)
	if budget.Usage() != charged {
		t.Fatal("expiry changed boundary-prefix retention charges")
	}
	for _, packets := range []uint64{store.reportedPackets, store.reportedPackets - 1} {
		sample, stale, err := store.acknowledge(packets, uint64(now.UnixMicro()))
		if err != nil || stale != (packets < store.reportedPackets) || sample != (deliverySample{}) || budget.Usage() != charged {
			t.Fatalf("non-progress report at counter limit = %+v, stale %t, error %v", sample, stale, err)
		}
	}
	var prefixBytes uint64
	for index, size := range []int{sizes[2], sizes[0], sizes[1]} {
		prefixBytes += uint64(size)
		receiveMicros := uint64(start.Add(2*time.Second).UnixMicro()) + uint64(index+1)
		sample, stale, err := store.acknowledge(math.MaxUint64-uint64(2-index), receiveMicros)
		want := deliverySample{bytes: prefixBytes, intervalMicros: 2_000_000 + uint64(index+1)}
		if err != nil || stale || sample != want {
			t.Fatalf("late boundary report %d = %+v, stale %t, error %v, want %+v", index, sample, stale, err, want)
		}
		wantProof := []uint64{0, wgpacket.TransportKeepaliveLength, minimumRateSampleBytes}[index]
		if store.transportReported.Load() != wantProof {
			t.Fatalf("late boundary report %d transport proof = %d, want %d", index, store.transportReported.Load(), wantProof)
		}
		assertTransmissionStoreInvariants(t, store)
	}
	if budget.Usage() != (retention.Usage{}) || store.reportedPackets != math.MaxUint64 || store.reportedBytes != math.MaxUint64 {
		t.Fatal("late boundary reports lost cumulative progress or retained charges")
	}
}

func TestTransmissionStoreAggregateBudget(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 2, Bytes: 4096}, budget, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 2, Bytes: 4096}, budget, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	transmission := schedulerTransmission(1, wgpacket.TransportData, deadline)
	if err := first.push(transmission); err != nil {
		t.Fatal(err)
	}
	transmission.packetID++
	if err := second.push(transmission); !errors.Is(err, packetqueue.ErrFull) {
		t.Fatalf("push() error = %v, want %v", err, packetqueue.ErrFull)
	}
	var batch [1]protocol.Data
	var ownership [1]datagram.Packet
	count, err := first.takeBatch(batch[:], ownership[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
	releaseBatchOwnership(ownership[:count])
	if _, _, err := first.acknowledge(1, uint64(first.now().UnixMicro())); err != nil {
		t.Fatal(err)
	}
	if err := second.push(transmission); err != nil {
		t.Fatal(err)
	}
	releaseTransmissions(second.drain())
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after drain release = %+v", got)
	}
}

func TestTransmissionStoreReclaimsExpiredAggregateCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	expired := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Millisecond))
	live := schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second))
	budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: expired.size + live.size})
	if err != nil {
		t.Fatal(err)
	}
	store, err := newTransmissionStoreWithBudget(packetqueue.Limits{
		Packets: 4, Bytes: 4 * expired.size,
	}, func() time.Time { return now }, budget)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.push(expired); err != nil {
		t.Fatal(err)
	}
	if err := store.push(live); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Millisecond)
	fresh := schedulerTransmission(3, wgpacket.TransportData, now.Add(time.Second))
	if err := store.push(fresh); err != nil {
		t.Fatalf("push() after aggregate expiry error = %v", err)
	}
	if packets, bytes := store.backlog(); packets != 2 || bytes != uint64(live.size+fresh.size) {
		t.Fatalf("backlog = %d packets, %d bytes", packets, bytes)
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 2, Bytes: live.size + fresh.size}) {
		t.Fatalf("budget usage = %+v", got)
	}
	releaseTransmissions(store.drain())
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after drain = %+v", got)
	}
}

func TestTransmissionStoreCarrierOrderAndAcknowledgement(t *testing.T) {
	store := schedulerStore(t, packetqueue.Limits{Packets: 4, Bytes: 4096})
	deadline := time.Now().Add(time.Second)
	normalFirst := schedulerTransmission(1, wgpacket.TransportData, deadline)
	control := schedulerTransmission(2, wgpacket.HandshakeInitiation, deadline)
	normalSecond := schedulerTransmission(3, wgpacket.TransportData, deadline)
	for _, transmission := range []retainedTransmission{normalFirst, control, normalSecond} {
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
	}

	var batch [3]protocol.Data
	var ownership [3]datagram.Packet
	count, err := store.takeBatch(batch[:], ownership[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBatchOwnership(ownership[:count])
	if count != 3 || batch[0].PacketID != 2 || batch[1].PacketID != 1 || batch[2].PacketID != 3 {
		t.Fatalf("carrier PacketIDs = %d, %d, %d", batch[0].PacketID, batch[1].PacketID, batch[2].PacketID)
	}
	if packets, bytes := store.backlog(); packets != 3 ||
		bytes != uint64(control.size+normalFirst.size+normalSecond.size) {
		t.Fatalf("retained backlog = %d packets, %d bytes", packets, bytes)
	}

	if _, _, err := store.acknowledge(4, uint64(store.now().UnixMicro())); !errors.Is(
		err, ErrInvalidDeliveryReport,
	) {
		t.Fatalf("mismatched acknowledgement error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	if packets, _ := store.backlog(); packets != 3 {
		t.Fatalf("invalid acknowledgement released backlog, packets = %d", packets)
	}
	_, stale, err := store.acknowledge(2, uint64(store.now().UnixMicro()))
	if err != nil || stale {
		t.Fatalf("acknowledge() = stale %t, error %v", stale, err)
	}
	if packets, bytes := store.backlog(); packets != 1 || bytes != uint64(normalSecond.size) {
		t.Fatalf("remaining backlog = %d packets, %d bytes", packets, bytes)
	}
	if _, stale, err := store.acknowledge(1, uint64(store.now().UnixMicro())); err != nil || !stale {
		t.Fatalf("stale acknowledge = %t, %v", stale, err)
	}

}

func TestTransmissionStoreCapacityIncludesSentPrefix(t *testing.T) {
	store := schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096})
	transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(transmission); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if store.canAccept(uint64(transmission.size)) {
		t.Fatal("sent but unreported prefix released capacity")
	}
	if err := store.push(schedulerTransmission(
		2, wgpacket.TransportData, time.Now().Add(time.Second),
	)); !errors.Is(err, packetqueue.ErrFull) {
		t.Fatalf("push() error = %v, want %v", err, packetqueue.ErrFull)
	}
	if _, _, err := store.acknowledge(1, uint64(store.now().UnixMicro())); err != nil {
		t.Fatal(err)
	}
	if !store.canAccept(uint64(transmission.size)) {
		t.Fatal("reported prefix did not release capacity")
	}
}

func TestTransmissionStoreExpiryDistinguishesQueuedAndSent(t *testing.T) {
	now := time.Now()
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 3, Bytes: 4096}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	sent := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
	queued := schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second))
	if err := store.push(sent); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if err := store.push(queued); err != nil {
		t.Fatal(err)
	}

	now = now.Add(2 * time.Second)
	assessment := store.assessDeadlines(now, func(uint64) uint64 { return 0 })
	if !assessment.retained || !assessment.atRisk || !assessment.usefulDeadline.IsZero() || assessment.usefulBytes != 0 {
		t.Fatalf("deadline assessment = %+v", assessment)
	}
	if packets, backlogBytes := store.backlog(); packets != 1 || backlogBytes != uint64(sent.size) {
		t.Fatalf("expired queued reclaim = %d packets, %d bytes", packets, backlogBytes)
	}
	drained := store.drain()
	if len(drained) != 1 || drained[0].packetID != sent.packetID {
		t.Fatalf("drain() = %+v", drained)
	}
}

func TestTransmissionStoreReclaimsUnorderedDeadlines(t *testing.T) {
	now := time.Unix(100, 0)
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 3, Bytes: 4096}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	start := now
	for _, transmission := range []retainedTransmission{
		schedulerTransmission(1, wgpacket.TransportData, start.Add(3*time.Second)),
		schedulerTransmission(2, wgpacket.TransportData, start.Add(time.Second)),
		schedulerTransmission(3, wgpacket.TransportData, start.Add(2*time.Second)),
	} {
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
	}
	now = start.Add(time.Second)
	if err := store.push(schedulerTransmission(4, wgpacket.TransportData, start.Add(4*time.Second))); err != nil {
		t.Fatal(err)
	}
	now = start.Add(2 * time.Second)
	store.expire(now)
	if got := takeOneTransmission(t, store).PacketID; got != 1 {
		t.Fatalf("oldest live PacketID = %d, want 1", got)
	}
	if err := store.push(schedulerTransmission(5, wgpacket.TransportData, start.Add(2500*time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	now = start.Add(2500 * time.Millisecond)
	if err := store.push(schedulerTransmission(6, wgpacket.TransportData, start.Add(5*time.Second))); err != nil {
		t.Fatal(err)
	}
	if got := takeOneTransmission(t, store).PacketID; got != 4 {
		t.Fatalf("queued FIFO PacketID = %d, want 4", got)
	}
	now = start.Add(10 * time.Second)
	store.expire(now)
	retained := store.drain()
	defer releaseTransmissions(retained)
	if len(retained) != 2 || retained[0].packetID != 1 || retained[1].packetID != 4 {
		t.Fatalf("expired sent prefix = %+v, want PacketIDs 1 and 4", retained)
	}
}

func TestTransmissionStoreTakeBatchSkipsExpiredWork(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 4, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	store, err := newTransmissionStoreWithBudget(
		packetqueue.Limits{Packets: 4, Bytes: 4096}, func() time.Time { return now }, budget,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, transmission := range []retainedTransmission{
		schedulerTransmission(1, wgpacket.HandshakeInitiation, now.Add(time.Millisecond)),
		schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second)),
		schedulerTransmission(3, wgpacket.TransportData, now.Add(time.Millisecond)),
		schedulerTransmission(4, wgpacket.TransportData, now.Add(time.Second)),
	} {
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Millisecond)
	var batch [4]protocol.Data
	var ownership [4]datagram.Packet
	count, err := store.takeBatch(batch[:], ownership[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBatchOwnership(ownership[:count])
	if count != 2 || batch[0].PacketID != 2 || batch[1].PacketID != 4 {
		t.Fatalf("takeBatch() returned %d packets with IDs %d and %d", count, batch[0].PacketID, batch[1].PacketID)
	}
	if packets, bytes := store.backlog(); packets != 2 || bytes != uint64(store.sentBytes) {
		t.Fatalf("backlog = %d packets, %d bytes", packets, bytes)
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 2, Bytes: int(store.sentBytes)}) {
		t.Fatalf("budget usage = %+v", got)
	}
	releaseTransmissions(store.drain())
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after drain = %+v", got)
	}
}

func TestTransmissionStoreReclaimsExpiredLocalCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	expired := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Millisecond))
	live := schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second))
	store, err := newTransmissionStore(packetqueue.Limits{
		Packets: 2, Bytes: expired.size + live.size,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.push(expired); err != nil {
		t.Fatal(err)
	}
	if err := store.push(live); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Millisecond)
	fresh := schedulerTransmission(3, wgpacket.TransportData, now.Add(time.Second))
	if err := store.push(fresh); err != nil {
		t.Fatalf("push() after local expiry error = %v", err)
	}
	if packets, bytes := store.backlog(); packets != 2 || bytes != uint64(live.size+fresh.size) {
		t.Fatalf("backlog = %d packets, %d bytes", packets, bytes)
	}
}

func TestTransmissionStoreAtRiskUsesCarrierPrefix(t *testing.T) {
	now := time.Now()
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 3, Bytes: 4096}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	first := schedulerTransmission(1, wgpacket.TransportData, now.Add(100*time.Millisecond))
	second := schedulerTransmission(2, wgpacket.TransportData, now.Add(50*time.Millisecond))
	if err := store.push(first); err != nil {
		t.Fatal(err)
	}
	if err := store.push(second); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if store.atRisk(now, func(uint64) uint64 { return 40_000 }) {
		t.Fatal("40ms prefix delay incorrectly missed either deadline")
	}
	if !store.atRisk(now, func(bytes uint64) uint64 {
		if bytes > uint64(first.size) {
			return 60_000
		}
		return 40_000
	}) {
		t.Fatal("60ms second-prefix delay did not detect the 50ms deadline")
	}
}

func TestTransmissionStoreTakeBatchCounterBoundaries(t *testing.T) {
	for _, test := range []struct {
		name        string
		packetLimit bool
		capacity    int
		byteSlack   int
		wantCount   int
	}{
		{name: "PacketCounterFull", packetLimit: true},
		{name: "PacketCounterFitsOne", packetLimit: true, capacity: 1, wantCount: 1},
		{name: "PacketCounterFitsTwo", packetLimit: true, capacity: 2, wantCount: 2},
		{name: "ByteCounterFull"},
		{name: "ByteCounterWouldOverflow", capacity: 1, byteSlack: -1},
		{name: "ByteCounterFitsOne", capacity: 1, wantCount: 1},
		{name: "ByteCounterFitsTwo", capacity: 2, wantCount: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			budget, err := retention.NewBudget(retention.Limits{Packets: 3, Bytes: 32 * 1024})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if got := budget.Usage(); got != (retention.Usage{}) {
					t.Fatalf("completed generation retained aggregate ownership: %+v", got)
				}
			})
			store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 3, Bytes: 32 * 1024},
				budget, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			var prefixBytes [4]int
			for index, size := range []int{32, 1452, 8000} {
				transmission := schedulerTransmission(uint64(index+1), wgpacket.TransportData, now.Add(time.Second))
				transmission.packet.Payload = make([]byte, size)
				transmission.packet.Payload[0] = 4
				transmission.packet.Payload[4] = byte(index + 1)
				prefixBytes[index+1] = prefixBytes[index] + dataFrameSize(transmission.data())
				if err := store.push(transmission); err != nil {
					t.Fatal(err)
				}
			}
			store.sentPackets = 100
			store.sentBytes = 1000
			if test.packetLimit {
				store.sentPackets = math.MaxUint64 - uint64(test.capacity)
			} else {
				store.sentBytes = math.MaxUint64 - uint64(prefixBytes[test.capacity]+test.byteSlack)
			}
			store.reportedPackets = store.sentPackets
			store.reportedBytes = store.sentBytes
			initialPackets, initialBytes := store.sentPackets, store.sentBytes
			var batch [3]protocol.Data
			var ownership [3]datagram.Packet
			count, err := store.takeBatch(batch[:], ownership[:], 32*1024)
			defer releaseBatchOwnership(ownership[:count])
			if count != test.wantCount || count == 0 && !errors.Is(err, ErrCounterExhausted) || count > 0 && err != nil {
				t.Fatalf("takeBatch() = %d, %v, want %d packets within cumulative counter limits", count, err, test.wantCount)
			}
			if store.sentPackets != initialPackets+uint64(count) || store.sentBytes != initialBytes+uint64(prefixBytes[count]) {
				t.Fatal("successful prefix changed cumulative counters incorrectly")
			}
			for index, data := range batch[:count] {
				if data.PacketID != uint64(index+1) || data.Payload[4] != byte(index+1) {
					t.Fatalf("sent prefix entry %d changed packet identity", index)
				}
			}
			var next [1]protocol.Data
			var nextOwnership [1]datagram.Packet
			for attempt := range 2 {
				if taken, err := store.takeBatch(next[:], nextOwnership[:], 32*1024); taken != 0 || !errors.Is(err, ErrCounterExhausted) {
					t.Fatalf("exhausted takeBatch() = %d, %v, want an unchanged queued suffix", taken, err)
				}
				if attempt == 0 {
					if _, stale, err := store.acknowledge(store.sentPackets, 2000); stale || err != nil {
						t.Fatalf("successful prefix acknowledgement = stale %t, %v", stale, err)
					}
				}
			}
			remaining := retention.Usage{Packets: len(batch) - count, Bytes: prefixBytes[len(batch)] - prefixBytes[count]}
			if packets, bytes := store.backlog(); packets != remaining.Packets || bytes != uint64(remaining.Bytes) {
				t.Fatalf("retained suffix = %d packets, %d bytes, want %+v", packets, bytes, remaining)
			}
			if got := budget.Usage(); got != remaining {
				t.Fatalf("retained aggregate ownership = %+v, want %+v", got, remaining)
			}
			retained := store.drain()
			defer releaseTransmissions(retained)
			if len(retained) != remaining.Packets {
				t.Fatalf("drained %d packets, want %d", len(retained), remaining.Packets)
			}
			if got := budget.Usage(); got != remaining {
				t.Fatalf("drain lost the returned suffix's aggregate ownership: %+v", got)
			}
			for index, transmission := range retained {
				wantID := uint64(count + index + 1)
				if transmission.packetID != wantID || transmission.packet.Payload[4] != byte(wantID) {
					t.Fatalf("drained suffix entry %d changed packet identity", index)
				}
			}
			for index := range ownership[:count] {
				if wgpacket.Classify(ownership[index].Payload) != ownership[index].Kind || batch[index].Payload[4] != byte(index+1) {
					t.Fatalf("writer ownership %d changed during acknowledgement and drain", index)
				}
			}
		})
	}
}

func TestTransmissionStoreDeliveryConstrained(t *testing.T) {
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 4, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	first := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(first); err != nil {
		t.Fatal(err)
	}
	if !store.deliveryConstrained(uint64(store.limits.Bytes)) {
		t.Fatal("queued work did not constrain delivery")
	}
	takeOneTransmission(t, store)
	if store.deliveryConstrained(uint64(store.limits.Bytes)) {
		t.Fatal("small sent prefix constrained an application-limited sample")
	}
	second := schedulerTransmission(2, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(second); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if !store.deliveryConstrained(uint64(store.limits.Bytes)) {
		t.Fatal("half-full retained window did not constrain delivery")
	}
}

func TestTransmissionDequeDiscardPrefix(t *testing.T) {
	for _, tt := range []struct {
		name     string
		total    int
		count    int
		wantHead int
	}{
		{name: "Uncompacted", total: 100, count: 20, wantHead: 20},
		{name: "Compacted", total: 128, count: 64},
		{name: "Emptied", total: 128, count: 128},
	} {
		t.Run(tt.name, func(t *testing.T) {
			backing := make([]retainedTransmission, tt.total)
			for index := range backing {
				backing[index] = retainedTransmission{
					size: index + 1, packet: datagram.Packet{Payload: []byte{byte(index)}},
				}
			}
			deque := transmissionDeque{items: backing}
			deque.discardPrefix(tt.count)
			if deque.len() != tt.total-tt.count || deque.head != tt.wantHead {
				t.Fatalf("deque length = %d, head = %d, want %d and %d", deque.len(), deque.head, tt.total-tt.count, tt.wantHead)
			}
			for index, transmission := range backing {
				if index >= deque.head && index < len(deque.items) {
					original := tt.count + index - deque.head
					if transmission.size != original+1 || len(transmission.packet.Payload) != 1 || transmission.packet.Payload[0] != byte(original) {
						t.Fatalf("retained entry %d does not match original entry %d", index, original)
					}
				} else if transmission.size != 0 || transmission.packet.Payload != nil {
					t.Fatalf("consumed entry %d retained state", index)
				}
			}
		})
	}
}

func TestTransmissionDequeCapacityRetention(t *testing.T) {
	for _, test := range []struct {
		name   string
		empty  func(*testing.T, *transmissionDeque)
		retain func(*testing.T, *transmissionDeque, int)
	}{
		{
			name: "Pop",
			empty: func(_ *testing.T, deque *transmissionDeque) {
				for range deque.items {
					deque.pop()
				}
			},
			retain: func(_ *testing.T, deque *transmissionDeque, count int) {
				for deque.len() > count {
					deque.pop()
				}
			},
		},
		{
			name: "DiscardPrefix",
			empty: func(_ *testing.T, deque *transmissionDeque) {
				deque.discardPrefix(deque.len())
			},
			retain: func(_ *testing.T, deque *transmissionDeque, count int) {
				deque.discardPrefix(deque.len() - count)
			},
		},
		{
			name: "Expiry",
			empty: func(_ *testing.T, deque *transmissionDeque) {
				for index := range deque.items {
					deque.items[index].deadline = time.Unix(100, 0)
				}
				deque.earliestDeadline = time.Unix(100, 0)
				deque.removeExpired(time.Unix(101, 0))
			},
			retain: func(_ *testing.T, deque *transmissionDeque, count int) {
				boundary := len(deque.items) - count
				for index := range deque.items {
					deque.items[index].deadline = time.Unix(102, 0)
					if index < boundary {
						deque.items[index].deadline = time.Unix(100, 0)
					}
				}
				deque.earliestDeadline = time.Unix(100, 0)
				deque.removeExpired(time.Unix(101, 0))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("OrdinaryBatchReused", func(t *testing.T) {
				deque := transmissionDeque{items: make([]retainedTransmission, maximumRetainedDequeCapacity)}
				test.empty(t, &deque)
				if cap(deque.items) != maximumRetainedDequeCapacity || deque.head != 0 {
					t.Fatalf("empty deque capacity = %d, head = %d", cap(deque.items), deque.head)
				}
			})

			t.Run("ExceptionalBatchCapped", func(t *testing.T) {
				deque := transmissionDeque{items: make([]retainedTransmission, maximumRetainedDequeCapacity+1)}
				test.empty(t, &deque)
				if len(deque.items) != 0 || cap(deque.items) != maximumRetainedDequeCapacity || deque.head != 0 {
					t.Fatalf("empty deque capacity = %d, head = %d", cap(deque.items), deque.head)
				}
				for index := range maximumRetainedDequeCapacity {
					deque.push(retainedTransmission{size: index})
				}
				test.empty(t, &deque)
				if len(deque.items) != 0 || cap(deque.items) != maximumRetainedDequeCapacity || deque.head != 0 {
					t.Fatalf("reused deque capacity = %d, head = %d", cap(deque.items), deque.head)
				}
			})

			t.Run("ExceptionalBatchShrinksWhileNonempty", func(t *testing.T) {
				const retained = maximumRetainedDequeCapacity / 2
				const total = maximumRetainedDequeCapacity * 8
				deque := transmissionDeque{items: make([]retainedTransmission, total)}
				for index := range deque.items {
					deque.items[index].size = index
				}
				test.retain(t, &deque, retained)
				if deque.len() != retained || cap(deque.items) != maximumRetainedDequeCapacity {
					t.Fatalf("retained deque length = %d, capacity = %d, head = %d",
						deque.len(), cap(deque.items), deque.head)
				}
				if got := deque.items[deque.head].size; got != total-retained {
					t.Fatalf("first retained size = %d, want %d", got, total-retained)
				}
			})
		})
	}
}

func TestTransmissionDequeFeedbackWindowReusesStorage(t *testing.T) {
	var deque transmissionDeque
	for range 128 {
		deque.push(retainedTransmission{})
	}
	allocations := testing.AllocsPerRun(100, func() {
		for range reportPacketThreshold {
			deque.push(retainedTransmission{})
		}
		deque.discardPrefix(reportPacketThreshold)
	})
	if allocations != 0 {
		t.Fatalf("steady feedback window allocated %.1f times per report", allocations)
	}
}

func TestTransmissionStoreStateMachine(t *testing.T) {
	for _, tt := range []struct {
		name              string
		bytes             int
		aggregate, probes bool
	}{
		{name: "Local", bytes: 4096},
		{name: "Aggregate", bytes: 4096, aggregate: true},
		{name: "LocalWithProbes", bytes: 16 * 1024, probes: true},
		{name: "AggregateWithProbes", bytes: 16 * 1024, aggregate: true, probes: true},
		{name: "LocalSingleProbeCapacity", bytes: 4101, probes: true},
		{name: "AggregateSingleProbeCapacity", bytes: 4101, aggregate: true, probes: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			limits := packetqueue.Limits{Packets: 32, Bytes: tt.bytes}
			var budget *retention.Budget
			if tt.aggregate {
				var err error
				budget, err = retention.NewBudget(retention.Limits{Packets: limits.Packets, Bytes: limits.Bytes})
				if err != nil {
					t.Fatal(err)
				}
			}
			for seed := range uint64(16) {
				t.Run(fmt.Sprintf("Seed%d", seed), func(t *testing.T) {
					testTransmissionStoreStateMachine(t, limits, budget, tt.probes, 0x4d595df4d0f33173+seed)
				})
			}
		})
	}
}

func testTransmissionStoreStateMachine(t *testing.T, limits packetqueue.Limits, budget *retention.Budget, probes bool, seed uint64) {
	t.Helper()
	now := time.Unix(0, 0)
	store, err := newTransmissionStoreWithBudget(limits, func() time.Time { return now }, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	padding := make([]byte, protocol.ProbePayloadSize)
	padding[0] = 4
	type writtenFrame struct {
		transportBytes uint64
		probe          bool
	}
	var unreported []writtenFrame
	var reported, transportProof uint64
	var acceptedProbes, sentProbes, reportedProbes int
	next := func() uint64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return seed
	}
	packetID := uint64(0)
	for range 10_000 {
		switch next() % 7 {
		case 0, 1:
			packetID++
			kind := wgpacket.TransportData
			if next()%4 == 0 {
				kind = wgpacket.HandshakeInitiation
			}
			deadline := now.Add(time.Duration(next()%20+1) * time.Millisecond)
			transmission := schedulerTransmission(packetID, kind, deadline)
			if probes && next()%3 == 0 {
				transmission = retainedTransmission{deadline: deadline, packet: datagram.Packet{Payload: padding}}
			}
			err := store.push(transmission)
			if err != nil && !errors.Is(err, packetqueue.ErrFull) {
				t.Fatalf("push() error = %v", err)
			}
			if err == nil && transmission.packetID == 0 {
				acceptedProbes++
			}
		case 2:
			var batch [8]protocol.Data
			var ownership [8]datagram.Packet
			count, err := store.takeBatch(batch[:], ownership[:], int(next()%1024+1))
			if err != nil && !errors.Is(err, packetqueue.ErrEmpty) {
				t.Fatalf("takeBatch() error = %v", err)
			}
			for _, frame := range batch[:count] {
				written := writtenFrame{probe: frame.PacketID == 0}
				if written.probe {
					sentProbes++
				} else if frame.Payload[0] == 4 {
					written.transportBytes = uint64(len(frame.Payload))
				}
				unreported = append(unreported, written)
			}
			releaseBatchOwnership(ownership[:count])
		case 3:
			count := int(next() % uint64(len(unreported)+1))
			if _, stale, err := store.acknowledge(
				reported+uint64(count), uint64(store.now().UnixMicro())); err != nil || stale {
				t.Fatalf("acknowledge() = stale %t, error %v", stale, err)
			}
			for _, written := range unreported[:count] {
				transportProof = min(minimumRateSampleBytes, transportProof+written.transportBytes)
				if written.probe {
					reportedProbes++
				}
			}
			unreported = unreported[count:]
			reported += uint64(count)
		case 4:
			now = now.Add(time.Duration(next()%5+1) * time.Millisecond)
			store.assessDeadlines(now, func(uint64) uint64 { return 0 })
			store.sent.each(func(transmission retainedTransmission) bool {
				if !now.Before(transmission.deadline) && transmission.packet.Payload != nil {
					t.Fatal("maintenance retained an expired sent payload")
				}
				return true
			})
		case 5:
			if reported > 0 {
				if _, stale, err := store.acknowledge(0, uint64(store.now().UnixMicro())); err != nil || !stale {
					t.Fatalf("stale acknowledge() = stale %t, error %v", stale, err)
				}
			}
		case 6:
			if _, _, err := store.acknowledge(
				reported+uint64(len(unreported))+1, uint64(store.now().UnixMicro())); !errors.Is(err, ErrInvalidDeliveryReport) {
				t.Fatalf("partial acknowledge() error = %v, want %v", err, ErrInvalidDeliveryReport)
			}
		}
		assertTransmissionStoreInvariants(t, store)
		if store.transportReported.Load() != transportProof || store.reportedPackets != reported ||
			store.sentPackets != reported+uint64(len(unreported)) {
			t.Fatal("cumulative feedback or recovery proof differs from the observed writer prefix")
		}
	}
	if probes && (acceptedProbes == 0 || sentProbes == 0 || reportedProbes == 0) {
		t.Fatal("mixed state-machine run did not admit, write, and acknowledge capacity probes")
	}

	if packets, _ := store.backlog(); packets == 0 {
		if err := store.push(schedulerTransmission(
			packetID+1, wgpacket.TransportData, now.Add(time.Second),
		)); err != nil {
			t.Fatal(err)
		}
	}
	drained := store.drain()
	if len(drained) == 0 {
		t.Fatal("state-machine run ended without retained work")
	}
	if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
		t.Fatalf("drained backlog = %d packets and %d bytes", packets, bytes)
	}
	if store.sent.len() != 0 || store.control.len() != 0 || store.normal.len() != 0 {
		t.Fatal("drain retained deque entries")
	}
	releaseTransmissions(drained)
	if budget != nil {
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Fatalf("budget usage after drain release = %+v", got)
		}
	}
}

func assertTransmissionStoreInvariants(t *testing.T, store *TransmissionStore) {
	t.Helper()
	queuedSnapshot, retainedSnapshot := store.deliveryBacklog()
	store.mu.Lock()
	defer store.mu.Unlock()

	packets := store.sent.len() + store.control.len() + store.normal.len()
	bytes := 0
	sentBytes := uint64(0)
	store.sent.each(func(transmission retainedTransmission) bool {
		if transmission.packet.Payload != nil {
			if store.sent.earliestDeadline.IsZero() || transmission.deadline.Before(store.sent.earliestDeadline) {
				t.Fatal("sent-payload expiry bound is missing or later than a retained deadline")
			}
		}
		return true
	})
	for _, deque := range []*transmissionDeque{&store.control, &store.normal} {
		if (deque.len() == 0) != deque.earliestDeadline.IsZero() {
			t.Fatal("expiry bound does not match queued occupancy")
		}
		deque.each(func(transmission retainedTransmission) bool {
			if transmission.packet.Kind.Control() != (deque == &store.control) {
				t.Fatal("queued packet classification does not match carrier priority")
			}
			if transmission.deadline.Before(deque.earliestDeadline) {
				t.Fatal("expiry bound is later than a queued deadline")
			}
			return true
		})
	}
	validate := func(transmission retainedTransmission, sent bool) bool {
		data := transmission.data()
		if data.Payload == nil {
			if !sent || store.now().Before(transmission.deadline) {
				t.Fatal("fresh or queued payload was released")
			}
			data.Payload = make([]byte, transmission.payloadBytes)
		} else if len(data.Payload) != int(transmission.payloadBytes) {
			t.Fatal("retained payload size differs from its descriptor")
		}
		size, err := protocol.DataFrameSize(data)
		if err != nil || size != transmission.size || transmission.budget != store.budget {
			t.Fatalf("invalid retained transmission: %+v", transmission)
		}
		if transmission.packetID == 0 {
			if transmission.packet.Kind != wgpacket.NonWireGuard {
				t.Fatal("capacity probe retained a real packet classification")
			}
		} else if !transmission.packet.Kind.Accepted() || transmission.packet.Payload != nil &&
			wgpacket.Classify(transmission.packet.Payload) != transmission.packet.Kind {
			t.Fatal("retained packet classification differs from its payload")
		}
		bytes += transmission.size
		if sent {
			sentBytes += uint64(transmission.size)
		}
		return true
	}
	store.sent.each(func(transmission retainedTransmission) bool { return validate(transmission, true) })
	store.control.each(func(transmission retainedTransmission) bool { return validate(transmission, false) })
	store.normal.each(func(transmission retainedTransmission) bool { return validate(transmission, false) })
	if retainedSnapshot != uint64(bytes) || queuedSnapshot != uint64(bytes)-sentBytes {
		t.Fatalf("delivery backlog = %d queued and %d retained bytes, want %d and %d",
			queuedSnapshot, retainedSnapshot, uint64(bytes)-sentBytes, bytes)
	}

	if packets != store.packets || bytes != store.bytes {
		t.Fatalf("exact backlog = %d packets and %d bytes, fields = %d and %d",
			packets, bytes, store.packets, store.bytes)
	}
	if int(store.backlogPackets.Load()) != packets || store.backlogBytes.Load() != uint64(bytes) {
		t.Fatalf("atomic backlog = %d packets and %d bytes, want %d and %d",
			store.backlogPackets.Load(), store.backlogBytes.Load(), packets, bytes)
	}
	if packets > store.limits.Packets || bytes > store.limits.Bytes {
		t.Fatalf("backlog exceeds limits: %d packets and %d bytes", packets, bytes)
	}
	if store.budget != nil {
		if got := store.budget.Usage(); got != (retention.Usage{Packets: packets, Bytes: bytes}) {
			t.Fatalf("aggregate backlog = %+v, want %d packets and %d bytes", got, packets, bytes)
		}
	}
	if store.sentPackets < store.reportedPackets ||
		store.sentPackets-store.reportedPackets != uint64(store.sent.len()) {
		t.Fatalf("sent packet counters = %d sent and %d reported for %d retained",
			store.sentPackets, store.reportedPackets, store.sent.len())
	}
	if store.sentBytes < store.reportedBytes || store.sentBytes-store.reportedBytes != sentBytes {
		t.Fatalf("sent byte counters = %d sent and %d reported for %d retained",
			store.sentBytes, store.reportedBytes, sentBytes)
	}
}

func releaseBatchOwnership(packets []datagram.Packet) {
	for index := range packets {
		packets[index].Release()
	}
}
