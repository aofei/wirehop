package relay

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestTransmissionStoreFlightCredit(t *testing.T) {
	t.Run("InitialWindows", func(t *testing.T) {
		for _, hard := range []int{4096, 65536} {
			now := time.UnixMicro(1000)
			store := newFlightCreditStore(t, &now, hard)
			want := min(uint64(hard), initialDeliveryWindow)
			if store.deliveryWindow != want || store.probeWindow != want {
				t.Fatal("startup doubled padding credit or exceeded the hard byte limit")
			}
		}
	})
	t.Run("QueuedOwnershipAndACKWake", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 65536)
		var size uint64
		for id := uint64(1); id <= 6; id++ {
			size = pushFlightCreditPacket(t, store, id, 32, now.Add(time.Second))
		}
		store.setDeliveryWindows(2*size, 2*size)
		consumeFlightCreditReady(store)
		assertFlightCreditTake(t, store, 1, 2)
		assertTransmissionStoreInvariants(t, store)
		if got := store.budget.Usage(); got.Packets != 6 || got.Bytes != 6*int(size) {
			t.Fatalf("committing a prefix changed retained ownership: %+v", got)
		}
		consumeFlightCreditReady(store)
		for range 3 {
			assertFlightCreditTake(t, store)
			assertFlightCreditSilent(t, store)
		}
		acknowledgeFlightCredit(t, store, 1, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 3)
		if got := store.budget.Usage(); got.Packets != 5 || got.Bytes != 5*int(size) {
			t.Fatalf("ACK did not release exactly one retained packet: %+v", got)
		}
		consumeFlightCreditReady(store)
		acknowledgeFlightCredit(t, store, 1, now.Add(time.Millisecond))
		assertFlightCreditSilent(t, store)
		assertTransmissionStoreInvariants(t, store)
	})
	t.Run("ShrinkAndGrow", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 65536)
		var size uint64
		for id := uint64(1); id <= 5; id++ {
			size = pushFlightCreditPacket(t, store, id, 32, now.Add(time.Second))
		}
		store.setDeliveryWindows(3*size, 3*size)
		assertFlightCreditTake(t, store, 1, 2, 3)
		store.setDeliveryWindows(size, size)
		consumeFlightCreditReady(store)
		assertFlightCreditTake(t, store)
		acknowledgeFlightCredit(t, store, 2, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store)
		assertFlightCreditSilent(t, store)
		store.setDeliveryWindows(2*size, 2*size)
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 4)
		consumeFlightCreditReady(store)
		acknowledgeFlightCredit(t, store, 4, now.Add(2*time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 5)
		assertTransmissionStoreInvariants(t, store)
	})
	t.Run("SharedProbePrefix", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 65536)
		normal := pushFlightCreditPacket(t, store, 1, 32, now.Add(time.Second))
		probe := pushFlightCreditPacket(t, store, 0, protocol.ProbePayloadSize, now.Add(time.Second))
		pushFlightCreditPacket(t, store, 2, 32, now.Add(time.Second))
		store.setDeliveryWindows(normal, normal+probe)
		assertFlightCreditTake(t, store, 1, 0)
		consumeFlightCreditReady(store)
		assertFlightCreditTake(t, store)
		acknowledgeFlightCredit(t, store, 1, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store)
		acknowledgeFlightCredit(t, store, 2, now.Add(2*time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 2)
		assertTransmissionStoreInvariants(t, store)
	})
	t.Run("TLSRecordPrefixWithSharedProbeCredit", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 65536)
		var normalBytes uint64
		for id := uint64(1); id <= 10; id++ {
			normalBytes = pushFlightCreditPacket(t, store, id, 1616, now.Add(time.Second))
		}
		var probeBytes uint64
		for range 4 {
			probeBytes = pushFlightCreditPacket(t, store, 0, protocol.ProbePayloadSize, now.Add(time.Second))
		}
		pushFlightCreditPacket(t, store, 11, 1616, now.Add(time.Second))
		control := schedulerTransmission(100, wgpacket.HandshakeInitiation, now.Add(time.Second))
		if err := store.push(control); err != nil {
			t.Fatal(err)
		}
		// The control and ten transport frames fit one record. Four probes exceed the normal window by 20 bytes.
		prefixBytes := uint64(control.size) + 10*normalBytes
		store.setDeliveryWindows(16*1024, prefixBytes+4*probeBytes)
		take := func(ids ...uint64) {
			t.Helper()
			var data [16]protocol.Data
			var ownership [16]datagram.Packet
			count, err := store.takeBatch(data[:], ownership[:], 64*1024, 16*1024)
			defer releaseBatchOwnership(ownership[:count])
			if len(ids) == 0 {
				if count != 0 || !errors.Is(err, packetqueue.ErrEmpty) {
					t.Fatalf("blocked TLS batch = %d, %v", count, err)
				}
			} else if count != len(ids) || err != nil {
				t.Fatalf("TLS batch = %d, %v, want IDs %v", count, err, ids)
			}
			for index, id := range ids {
				if data[index].PacketID != id {
					t.Fatalf("TLS batch packet %d ID = %d, want %d", index, data[index].PacketID, id)
				}
			}
			assertTransmissionStoreInvariants(t, store)
		}
		take(100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		take(0, 0, 0)
		take(0)
		consumeFlightCreditReady(store)
		take()
		assertFlightCreditSilent(t, store)
		acknowledgeFlightCredit(t, store, 11, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		if got := store.budget.Usage(); got != (retention.Usage{Packets: 5, Bytes: int(normalBytes + 4*probeBytes)}) {
			t.Fatalf("TLS prefix acknowledgement changed remaining normal/probe ownership: %+v", got)
		}
		take()
		assertFlightCreditSilent(t, store)
		acknowledgeFlightCredit(t, store, 12, now.Add(2*time.Millisecond))
		assertFlightCreditReady(t, store)
		take(11)
		acknowledgeFlightCredit(t, store, 16, now.Add(3*time.Millisecond))
		if got := store.budget.Usage(); got != (retention.Usage{}) || !store.sentEmpty() {
			t.Fatalf("completed TLS flight retained ownership: %+v", got)
		}
	})

	t.Run("WireGuardControlUsesNormalCredit", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 65536)
		size := pushFlightCreditPacket(t, store, 1, 32, now.Add(time.Second))
		store.setDeliveryWindows(size, 65536)
		assertFlightCreditTake(t, store, 1)
		control := schedulerTransmission(2, wgpacket.HandshakeInitiation, now.Add(time.Second))
		if err := store.push(control); err != nil {
			t.Fatal(err)
		}
		consumeFlightCreditReady(store)
		assertFlightCreditTake(t, store)
		assertFlightCreditSilent(t, store)
		acknowledgeFlightCredit(t, store, 1, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 2)
	})
	t.Run("OneOversizedFrameAtEmptyPrefix", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 3*protocol.MaxPacketSize)
		pushFlightCreditPacket(t, store, 1, protocol.MaxPacketSize, now.Add(time.Second))
		pushFlightCreditPacket(t, store, 2, protocol.MaxPacketSize, now.Add(time.Second))
		store.setDeliveryWindows(1, 1)
		assertFlightCreditTake(t, store, 1)
		consumeFlightCreditReady(store)
		assertFlightCreditTake(t, store)
		assertFlightCreditSilent(t, store)
		acknowledgeFlightCredit(t, store, 1, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 2)
		assertTransmissionStoreInvariants(t, store)
	})
	t.Run("PauseRetainsCreditAndProvenance", func(t *testing.T) {
		now := time.UnixMicro(1000)
		store := newFlightCreditStore(t, &now, 65536)
		size := pushFlightCreditPacket(t, store, 1, 32, now.Add(time.Second))
		pushFlightCreditPacket(t, store, 2, 32, now.Add(time.Second))
		pushFlightCreditPacket(t, store, 3, 32, now.Add(time.Second))
		store.setDeliveryWindows(size, size)
		assertFlightCreditTake(t, store, 1)
		store.beginMeasurementPause()
		acknowledgeFlightCredit(t, store, 1, now.Add(time.Millisecond))
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store)
		if store.sentPackets != 1 {
			t.Fatal("ACK reopened the timing pause")
		}
		store.endMeasurementPause()
		assertFlightCreditReady(t, store)
		assertFlightCreditTake(t, store, 2)
		if !store.sent.items[store.sent.head].applicationLimited {
			t.Fatal("first resumed descriptor lost limited provenance")
		}
		acknowledgeFlightCredit(t, store, 2, now.Add(2*time.Millisecond))
		assertFlightCreditTake(t, store, 3)
		if store.sent.items[store.sent.head].applicationLimited {
			t.Fatal("resumed ACK did not restore constrained sampling")
		}
	})
	t.Run("ExpiredHeadRevealsSendableFrame", func(t *testing.T) {
		for _, sweep := range []bool{false, true} {
			now := time.UnixMicro(1000)
			store := newFlightCreditStore(t, &now, 65536)
			size := pushFlightCreditPacket(t, store, 1, 32, now.Add(time.Second))
			store.setDeliveryWindows(2*size, 2*size)
			assertFlightCreditTake(t, store, 1)
			pushFlightCreditPacket(t, store, 2, 200, now.Add(time.Millisecond))
			pushFlightCreditPacket(t, store, 3, 32, now.Add(time.Second))
			consumeFlightCreditReady(store)
			assertFlightCreditTake(t, store)
			assertFlightCreditSilent(t, store)
			now = now.Add(time.Millisecond)
			if sweep {
				store.expire(now)
				assertFlightCreditReady(t, store)
			}
			assertFlightCreditTake(t, store, 3)
			if got := store.budget.Usage(); got.Packets != 2 || got.Bytes != 2*int(size) {
				t.Fatalf("expired head changed unrelated retained ownership: %+v", got)
			}
			assertTransmissionStoreInvariants(t, store)
		}
	})
}

func newFlightCreditStore(t *testing.T, now *time.Time, hardBytes int) *TransmissionStore {
	t.Helper()
	budget, err := retention.NewBudget(retention.Limits{Packets: 16, Bytes: hardBytes})
	if err != nil {
		t.Fatal(err)
	}
	store, err := newTransmissionStoreWithBudget(packetqueue.Limits{Packets: 16, Bytes: hardBytes}, func() time.Time {
		return *now
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseTransmissions(store.drain())
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Errorf("store cleanup retained aggregate ownership: %+v", got)
		}
	})
	return store
}

func pushFlightCreditPacket(t *testing.T, store *TransmissionStore, id uint64, payloadBytes int, deadline time.Time) uint64 {
	t.Helper()
	transmission := retainedTransmission{
		packetID: id, deadlineMicros: uint64(deadline.UnixMicro()),
		packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: make([]byte, payloadBytes)},
	}
	transmission.packet.Payload[0] = 4
	if id == 0 {
		transmission.packet.Kind = wgpacket.NonWireGuard
	}
	if err := store.push(transmission); err != nil {
		t.Fatal(err)
	}
	return uint64(dataFrameSize(transmission.data()))
}

func assertFlightCreditTake(t *testing.T, store *TransmissionStore, ids ...uint64) {
	t.Helper()
	var data [16]protocol.Data
	var ownership [16]datagram.Packet
	count, err := store.takeBatch(data[:], ownership[:], math.MaxInt, 0)
	defer releaseBatchOwnership(ownership[:count])
	if len(ids) == 0 {
		if count != 0 || !errors.Is(err, packetqueue.ErrEmpty) {
			t.Fatalf("blocked takeBatch = %d, %v", count, err)
		}
		return
	}
	if count != len(ids) || err != nil {
		t.Fatalf("takeBatch = %d, %v, want IDs %v", count, err, ids)
	}
	for index, id := range ids {
		if data[index].PacketID != id {
			t.Fatalf("batch packet %d ID = %d, want %d", index, data[index].PacketID, id)
		}
	}
}

func acknowledgeFlightCredit(t *testing.T, store *TransmissionStore, packets uint64, now time.Time) {
	t.Helper()
	if _, stale, err := store.acknowledge(packets, uint64(now.UnixMicro())); err != nil || stale {
		t.Fatalf("acknowledge %d = stale %t, %v", packets, stale, err)
	}
}

func consumeFlightCreditReady(store *TransmissionStore) {
	select {
	case <-store.Ready():
	default:
	}
}

func assertFlightCreditReady(t *testing.T, store *TransmissionStore) {
	t.Helper()
	select {
	case <-store.Ready():
	default:
		t.Fatal("queued writer was not notified")
	}
}

func assertFlightCreditSilent(t *testing.T, store *TransmissionStore) {
	t.Helper()
	select {
	case <-store.Ready():
		t.Fatal("blocked writer republished its own readiness")
	default:
	}
}
