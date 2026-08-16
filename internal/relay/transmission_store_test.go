package relay

import (
	"errors"
	"math"
	"testing"
	"time"

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
			data: valid.data, kind: wgpacket.HandshakeInitiation, priority: packetqueue.PriorityControl,
			deadline: valid.deadline,
		},
		{
			data: valid.data, kind: valid.kind, priority: packetqueue.PriorityControl,
			deadline: valid.deadline,
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

func TestTransmissionStoreAggregateBudget(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 2, Bytes: 4096}, budget)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 2, Bytes: 4096}, budget)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	transmission := schedulerTransmission(1, wgpacket.TransportData, deadline)
	if err := first.push(transmission); err != nil {
		t.Fatal(err)
	}
	transmission.data.PacketID++
	if err := second.push(transmission); !errors.Is(err, packetqueue.ErrFull) {
		t.Fatalf("push() error = %v, want %v", err, packetqueue.ErrFull)
	}
	var batch [1]protocol.Data
	if _, err := first.takeBatch(batch[:], 4096); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.acknowledge(1, uint64(first.sentBytes)); err != nil {
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
	count, err := store.takeBatch(batch[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || batch[0].PacketID != 2 || batch[1].PacketID != 1 || batch[2].PacketID != 3 {
		t.Fatalf("carrier PacketIDs = %d, %d, %d", batch[0].PacketID, batch[1].PacketID, batch[2].PacketID)
	}
	if packets, bytes := store.backlog(); packets != 3 ||
		bytes != uint64(control.size+normalFirst.size+normalSecond.size) {
		t.Fatalf("retained backlog = %d packets, %d bytes", packets, bytes)
	}

	if _, _, err := store.acknowledge(2, uint64(control.size+normalFirst.size+1)); !errors.Is(
		err, ErrInvalidDeliveryReport,
	) {
		t.Fatalf("mismatched acknowledgement error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	if packets, _ := store.backlog(); packets != 3 {
		t.Fatalf("invalid acknowledgement released backlog, packets = %d", packets)
	}
	released, stale, err := store.acknowledge(2, uint64(control.size+normalFirst.size))
	if err != nil || stale || released != uint64(control.size+normalFirst.size) {
		t.Fatalf("acknowledge() = %d, %t, %v", released, stale, err)
	}
	if packets, bytes := store.backlog(); packets != 1 || bytes != uint64(normalSecond.size) {
		t.Fatalf("remaining backlog = %d packets, %d bytes", packets, bytes)
	}
	if _, stale, err := store.acknowledge(1, uint64(control.size)); err != nil || !stale {
		t.Fatalf("stale acknowledge = %t, %v", stale, err)
	}
	if _, _, err := store.acknowledge(1, uint64(control.size+normalFirst.size)); !errors.Is(
		err, ErrInvalidDeliveryReport,
	) {
		t.Fatalf("partially changed acknowledgement error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	if _, _, err := store.acknowledge(1, uint64(control.size+normalFirst.size+normalSecond.size)); !errors.Is(
		err, ErrInvalidDeliveryReport,
	) {
		t.Fatalf("mixed acknowledgement error = %v, want %v", err, ErrInvalidDeliveryReport)
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
	if _, _, err := store.acknowledge(1, uint64(transmission.size)); err != nil {
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
	if !assessment.retained || !assessment.deadline.Equal(sent.deadline) ||
		assessment.frameBytes != uint64(sent.size) {
		t.Fatalf("deadline assessment = %+v", assessment)
	}
	if packets, backlogBytes := store.backlog(); packets != 1 || backlogBytes != uint64(sent.size) {
		t.Fatalf("expired queued reclaim = %d packets, %d bytes", packets, backlogBytes)
	}
	drained := store.drain()
	if len(drained) != 1 || drained[0].data.PacketID != sent.data.PacketID {
		t.Fatalf("drain() = %+v", drained)
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
	count, err := store.takeBatch(batch[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
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

func TestTransmissionStoreCounterExhaustionDoesNotConsumeQueue(t *testing.T) {
	store := schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096})
	transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(transmission); err != nil {
		t.Fatal(err)
	}
	store.sentPackets = math.MaxUint64
	var batch [1]protocol.Data
	if _, err := store.takeBatch(batch[:], 4096); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("takeBatch() error = %v, want %v", err, ErrCounterExhausted)
	}
	if packets, bytes := store.backlog(); packets != 1 || bytes != uint64(transmission.size) {
		t.Fatalf("counter exhaustion changed backlog to %d packets, %d bytes", packets, bytes)
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
	if !store.deliveryConstrained() {
		t.Fatal("queued work did not constrain delivery")
	}
	takeOneTransmission(t, store)
	if store.deliveryConstrained() {
		t.Fatal("small sent prefix constrained an application-limited sample")
	}
	second := schedulerTransmission(2, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(second); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if !store.deliveryConstrained() {
		t.Fatal("half-full retained window did not constrain delivery")
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
			name: "Expiry",
			empty: func(_ *testing.T, deque *transmissionDeque) {
				for index := range deque.items {
					deque.items[index].deadline = time.Unix(100, 0)
				}
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

func TestTransmissionStoreStateMachine(t *testing.T) {
	t.Run("Local", func(t *testing.T) {
		testTransmissionStoreStateMachine(t, nil)
	})
	t.Run("Aggregate", func(t *testing.T) {
		budget, err := retention.NewBudget(retention.Limits{Packets: 32, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		testTransmissionStoreStateMachine(t, budget)
	})
}

func testTransmissionStoreStateMachine(t *testing.T, budget *retention.Budget) {
	t.Helper()
	now := time.Unix(0, 0)
	store, err := newTransmissionStoreWithBudget(
		packetqueue.Limits{Packets: 32, Bytes: 4096}, func() time.Time { return now }, budget,
	)
	if err != nil {
		t.Fatal(err)
	}
	seed := uint64(0x4d595df4d0f33173)
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
			err := store.push(schedulerTransmission(packetID, kind, deadline))
			if err != nil && !errors.Is(err, packetqueue.ErrFull) {
				t.Fatalf("push() error = %v", err)
			}
		case 2:
			var batch [8]protocol.Data
			_, err := store.takeBatch(batch[:], int(next()%1024+1))
			if err != nil && !errors.Is(err, packetqueue.ErrEmpty) {
				t.Fatalf("takeBatch() error = %v", err)
			}
		case 3:
			count := uint64(0)
			if store.sent.len() > 0 {
				count = next() % (uint64(store.sent.len()) + 1)
			}
			size, ok := store.sent.prefixSize(count)
			if !ok {
				t.Fatal("valid sent prefix was rejected")
			}
			if _, stale, err := store.acknowledge(
				store.reportedPackets+count, store.reportedBytes+size,
			); err != nil || stale {
				t.Fatalf("acknowledge() = stale %t, error %v", stale, err)
			}
		case 4:
			now = now.Add(time.Duration(next()%5+1) * time.Millisecond)
			store.assessDeadlines(now, func(uint64) uint64 { return 0 })
		case 5:
			if store.reportedPackets > 0 {
				if _, stale, err := store.acknowledge(0, 0); err != nil || !stale {
					t.Fatalf("stale acknowledge() = stale %t, error %v", stale, err)
				}
			}
		case 6:
			if _, _, err := store.acknowledge(
				store.reportedPackets, store.reportedBytes+1,
			); !errors.Is(err, ErrInvalidDeliveryReport) {
				t.Fatalf("partial acknowledge() error = %v, want %v", err, ErrInvalidDeliveryReport)
			}
		}
		assertTransmissionStoreInvariants(t, store)
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
	if budget != nil {
		releaseTransmissions(drained)
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Fatalf("budget usage after drain release = %+v", got)
		}
	}
}

func assertTransmissionStoreInvariants(t *testing.T, store *TransmissionStore) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()

	packets := store.sent.len() + store.control.len() + store.normal.len()
	bytes := 0
	sentBytes := uint64(0)
	validate := func(transmission retainedTransmission, sent bool) bool {
		size, err := protocol.DataFrameSize(transmission.data)
		if err != nil || size != transmission.size || !transmission.kind.Accepted() ||
			wgpacket.Classify(transmission.data.Payload) != transmission.kind ||
			(transmission.priority == packetqueue.PriorityControl) != transmission.kind.Control() ||
			transmission.budget != store.budget {
			t.Fatalf("invalid retained transmission: %+v", transmission)
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
