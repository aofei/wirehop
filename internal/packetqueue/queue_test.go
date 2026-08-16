package packetqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

func TestQueue(t *testing.T) {
	now := time.Unix(100, 0)
	queue, err := NewWithClock[string](Limits{Packets: 4, Bytes: 10}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []Item[string]{
		{Value: "normal-1", Size: 2, Priority: PriorityNormal, Deadline: now.Add(time.Second)},
		{Value: "expired", Size: 2, Priority: PriorityControl, Deadline: now.Add(time.Millisecond)},
		{Value: "control", Size: 3, Priority: PriorityControl, Deadline: now.Add(time.Second)},
		{Value: "normal-2", Size: 1, Priority: PriorityNormal, Deadline: now.Add(time.Second)},
	} {
		if err := queue.Push(item); err != nil {
			t.Fatal(err)
		}
	}
	if queue.Len() != 4 || queue.Bytes() != 8 {
		t.Fatalf("queue accounting = %d packets, %d bytes", queue.Len(), queue.Bytes())
	}
	now = now.Add(2 * time.Millisecond)
	for _, want := range []string{"control", "normal-1", "normal-2"} {
		item, err := queue.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if item.Value != want {
			t.Fatalf("Pop() = %q, want %q", item.Value, want)
		}
	}
	if queue.Len() != 0 || queue.Bytes() != 0 {
		t.Fatalf("empty queue accounting = %d packets, %d bytes", queue.Len(), queue.Bytes())
	}
	if _, err := queue.TryPop(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("TryPop() error = %v, want %v", err, ErrEmpty)
	}
}

func TestQueueAggregateBudget(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewWithBudget[int](Limits{Packets: 2, Bytes: 10}, budget)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWithBudget[int](Limits{Packets: 2, Bytes: 10}, budget)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	if err := first.Push(Item[int]{Value: 1, Size: 6, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	if err := second.Push(Item[int]{Value: 2, Size: 5, Deadline: deadline}); !errors.Is(err, ErrFull) {
		t.Fatalf("Push() error = %v, want %v", err, ErrFull)
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 6}) {
		t.Fatalf("budget usage = %+v", got)
	}
	item, err := first.TryPop()
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 6}) {
		t.Fatalf("budget usage after pop = %+v", got)
	}
	item.ReleaseRetention()
	if err := second.Push(Item[int]{Value: 2, Size: 5, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	second.Close()
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after close = %+v", got)
	}
}

func TestPushReclaimsExpiredAggregateCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 4, Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := newQueue[string](Limits{Packets: 4, Bytes: 20}, func() time.Time { return now }, budget)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []Item[string]{
		{Value: "expired", Size: 6, Deadline: now.Add(time.Millisecond)},
		{Value: "live", Size: 2, Deadline: now.Add(time.Second)},
	} {
		if err := queue.Push(item); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Millisecond)
	if err := queue.Push(Item[string]{Value: "fresh", Size: 4, Deadline: now.Add(time.Second)}); err != nil {
		t.Fatalf("Push() after aggregate expiry error = %v", err)
	}
	if queue.Len() != 2 || queue.Bytes() != 6 {
		t.Fatalf("queue accounting = %d packets, %d bytes", queue.Len(), queue.Bytes())
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 2, Bytes: 6}) {
		t.Fatalf("budget usage = %+v", got)
	}
	queue.Close()
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after close = %+v", got)
	}
}

func TestQueueTransfersAggregateRetention(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewWithBudget[int](Limits{Packets: 1, Bytes: 10}, budget)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWithBudget[int](Limits{Packets: 1, Bytes: 10}, budget)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	if err := first.Push(Item[int]{Value: 1, Size: 6, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	item, err := first.TryPop()
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Push(item); err != nil {
		t.Fatal(err)
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 6}) {
		t.Fatalf("budget usage after transfer = %+v", got)
	}
	second.Close()
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after close = %+v", got)
	}
}

func TestTryPopPriority(t *testing.T) {
	now := time.Unix(100, 0)
	queue, err := NewWithClock[string](Limits{Packets: 3, Bytes: 6}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []Item[string]{
		{Value: "normal", Size: 2, Priority: PriorityNormal, Deadline: now.Add(time.Second)},
		{Value: "expired", Size: 2, Priority: PriorityControl, Deadline: now.Add(time.Millisecond)},
		{Value: "control", Size: 2, Priority: PriorityControl, Deadline: now.Add(time.Second)},
	} {
		if err := queue.Push(item); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Millisecond)
	item, err := queue.TryPopPriority(PriorityControl)
	if err != nil || item.Value != "control" {
		t.Fatalf("TryPopPriority(Control) = %q, %v", item.Value, err)
	}
	if _, err := queue.TryPopPriority(PriorityControl); !errors.Is(err, ErrEmpty) {
		t.Fatalf("empty control dequeue error = %v, want %v", err, ErrEmpty)
	}
	item, err = queue.TryPopPriority(PriorityNormal)
	if err != nil || item.Value != "normal" {
		t.Fatalf("TryPopPriority(Normal) = %q, %v", item.Value, err)
	}
	if _, err := queue.TryPopPriority(Priority(2)); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("invalid priority error = %v, want %v", err, ErrInvalidItem)
	}
}

func TestQueueLimits(t *testing.T) {
	now := time.Unix(100, 0)
	queue, err := NewWithClock[int](Limits{Packets: 1, Bytes: 2}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	valid := Item[int]{Value: 1, Size: 2, Priority: PriorityNormal, Deadline: now.Add(time.Second)}
	if err := queue.Push(valid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-queue.Ready():
	default:
		t.Fatal("Ready() did not report queued work")
	}
	if err := queue.Push(valid); !errors.Is(err, ErrFull) {
		t.Fatalf("full queue error = %v", err)
	}
	if _, err := queue.Pop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(Item[int]{Size: 3, Priority: PriorityNormal, Deadline: now.Add(time.Second)}); !errors.Is(err, ErrFull) {
		t.Fatalf("byte limit error = %v", err)
	}
	if err := queue.Push(Item[int]{Size: 1, Priority: PriorityNormal, Deadline: now}); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired item error = %v", err)
	}
	if err := queue.Push(Item[int]{}); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("invalid item error = %v", err)
	}
}

func TestPushReclaimsExpiredCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	queue, err := NewWithClock[string](Limits{Packets: 2, Bytes: 4}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []Item[string]{
		{Value: "expired", Size: 2, Priority: PriorityNormal, Deadline: now.Add(time.Millisecond)},
		{Value: "control", Size: 2, Priority: PriorityControl, Deadline: now.Add(time.Second)},
	} {
		if err := queue.Push(item); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Millisecond)
	if err := queue.Push(Item[string]{
		Value: "fresh", Size: 2, Priority: PriorityNormal, Deadline: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("Push() after expiry error = %v", err)
	}
	if queue.Len() != 2 || queue.Bytes() != 4 {
		t.Fatalf("queue accounting = %d packets, %d bytes", queue.Len(), queue.Bytes())
	}
	for _, want := range []string{"control", "fresh"} {
		item, err := queue.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if item.Value != want {
			t.Fatalf("Pop() = %q, want %q", item.Value, want)
		}
	}
}

func TestControlAdmissionEvictsNormal(t *testing.T) {
	queue, err := New[string](Limits{Packets: 2, Bytes: 4, ControlPreemption: true})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for _, value := range []string{"old", "new"} {
		if err := queue.Push(Item[string]{
			Value: value, Size: 2, Priority: PriorityNormal, Deadline: deadline,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := queue.Push(Item[string]{
		Value: "control", Size: 2, Priority: PriorityControl, Deadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"control", "new"} {
		item, err := queue.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if item.Value != want {
			t.Fatalf("Pop() = %q, want %q", item.Value, want)
		}
	}
}

func TestControlAdmissionEvictsNormalForAggregateCapacity(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 10, Bytes: 12})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewWithBudget[string](Limits{Packets: 4, Bytes: 12, ControlPreemption: true}, budget)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewWithBudget[string](Limits{Packets: 1, Bytes: 4}, budget)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	if err := other.Push(Item[string]{Value: "other", Size: 4, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"normal-1", "normal-2", "normal-3"} {
		if err := queue.Push(Item[string]{Value: value, Size: 2, Deadline: deadline}); err != nil {
			t.Fatal(err)
		}
	}
	if err := queue.Push(Item[string]{
		Value: "control", Size: 5, Priority: PriorityControl, Deadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"control", "normal-3"} {
		item, err := queue.TryPop()
		if err != nil {
			t.Fatal(err)
		}
		if item.Value != want {
			t.Fatalf("TryPop() = %q, want %q", item.Value, want)
		}
		item.ReleaseRetention()
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 4}) {
		t.Fatalf("budget usage = %+v", got)
	}
	other.Close()
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after close = %+v", got)
	}
}

func TestQueueClose(t *testing.T) {
	queue, err := New[int](Limits{Packets: 2, Bytes: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(Item[int]{
		Value: 1, Size: 1, Priority: PriorityNormal, Deadline: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(Item[int]{
		Value: 2, Size: 1, Priority: PriorityControl, Deadline: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	queue.Close()
	if queue.Len() != 0 || queue.Bytes() != 0 {
		t.Fatalf("closed queue accounting = %d packets, %d bytes", queue.Len(), queue.Bytes())
	}
	if _, err := queue.TryPop(); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryPop() error = %v, want %v", err, ErrClosed)
	}

	queue, err = New[int](Limits{Packets: 1, Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := queue.Pop(context.Background())
		done <- err
	}()
	queue.Close()
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop() error = %v", err)
	}
	if err := queue.Push(Item[int]{Size: 1, Priority: PriorityNormal, Deadline: time.Now().Add(time.Second)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push() error = %v", err)
	}
	if _, err := queue.TryPop(); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryPop() error = %v, want %v", err, ErrClosed)
	}
}

func TestNewQueue(t *testing.T) {
	for _, limits := range []Limits{{}, {Packets: 1}, {Bytes: 1}} {
		if _, err := New[int](limits); !errors.Is(err, ErrInvalidLimits) {
			t.Fatalf("New(%#v) error = %v", limits, err)
		}
	}
}

func TestDequeCapacityRetention(t *testing.T) {
	for _, test := range []struct {
		name   string
		empty  func(*testing.T, *deque[int])
		retain func(*testing.T, *deque[int], int)
	}{
		{
			name: "Pop",
			empty: func(t *testing.T, deque *deque[int]) {
				for range deque.items {
					if _, ok := deque.pop(); !ok {
						t.Fatal("pop() reported an empty deque")
					}
				}
			},
			retain: func(t *testing.T, deque *deque[int], count int) {
				for len(deque.items)-deque.head > count {
					if _, ok := deque.pop(); !ok {
						t.Fatal("pop() reported an empty deque")
					}
				}
			},
		},
		{
			name: "Expiry",
			empty: func(_ *testing.T, deque *deque[int]) {
				for index := range deque.items {
					deque.items[index].Deadline = time.Unix(100, 0)
				}
				deque.removeExpired(time.Unix(101, 0))
			},
			retain: func(_ *testing.T, deque *deque[int], count int) {
				boundary := len(deque.items) - count
				for index := range deque.items {
					deque.items[index].Deadline = time.Unix(102, 0)
					if index < boundary {
						deque.items[index].Deadline = time.Unix(100, 0)
					}
				}
				deque.removeExpired(time.Unix(101, 0))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("OrdinaryBurstReused", func(t *testing.T) {
				deque := deque[int]{items: make([]Item[int], maximumRetainedDequeCapacity)}
				test.empty(t, &deque)
				if cap(deque.items) != maximumRetainedDequeCapacity || deque.head != 0 {
					t.Fatalf("empty deque capacity = %d, head = %d", cap(deque.items), deque.head)
				}
			})

			t.Run("ExceptionalBurstCapped", func(t *testing.T) {
				deque := deque[int]{items: make([]Item[int], maximumRetainedDequeCapacity+1)}
				test.empty(t, &deque)
				if len(deque.items) != 0 || cap(deque.items) != maximumRetainedDequeCapacity || deque.head != 0 {
					t.Fatalf("empty deque capacity = %d, head = %d", cap(deque.items), deque.head)
				}
				for index := range maximumRetainedDequeCapacity {
					deque.push(Item[int]{Value: index})
				}
				test.empty(t, &deque)
				if len(deque.items) != 0 || cap(deque.items) != maximumRetainedDequeCapacity || deque.head != 0 {
					t.Fatalf("reused deque capacity = %d, head = %d", cap(deque.items), deque.head)
				}
			})

			t.Run("ExceptionalBurstShrinksWhileNonempty", func(t *testing.T) {
				const retained = maximumRetainedDequeCapacity / 2
				const total = maximumRetainedDequeCapacity * 8
				deque := deque[int]{items: make([]Item[int], total)}
				for index := range deque.items {
					deque.items[index].Value = index
				}
				test.retain(t, &deque, retained)
				if len(deque.items)-deque.head != retained || cap(deque.items) != maximumRetainedDequeCapacity {
					t.Fatalf("retained deque length = %d, capacity = %d, head = %d",
						len(deque.items)-deque.head, cap(deque.items), deque.head)
				}
				if got := deque.items[deque.head].Value; got != total-retained {
					t.Fatalf("first retained value = %d, want %d", got, total-retained)
				}
			})
		})
	}
}

func TestQueueRetentionStateMachine(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 16, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	queues := make([]*Queue[int], 2)
	for index := range queues {
		queues[index], err = newQueue[int](Limits{
			Packets: 8, Bytes: 512, ControlPreemption: true,
		}, func() time.Time { return now }, budget)
		if err != nil {
			t.Fatal(err)
		}
	}
	seed := uint64(0x4d595df4d0f33173)
	next := func() uint64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return seed
	}
	held := make([]Item[int], 0, 16)
	for step := range 10_000 {
		queue := queues[next()%uint64(len(queues))]
		switch next() % 6 {
		case 0:
			size := int(next()%64 + 1)
			priority := Priority(next() % 2)
			item := Item[int]{
				Value: step, Size: size, Priority: priority,
				Deadline: now.Add(time.Duration(next()%20+1) * time.Millisecond),
			}
			if err := queue.Push(item); err != nil && !errors.Is(err, ErrFull) {
				t.Fatalf("Push() error = %v", err)
			}
		case 1:
			item, err := queue.TryPop()
			if err == nil {
				held = append(held, item)
			} else if !errors.Is(err, ErrEmpty) {
				t.Fatalf("TryPop() error = %v", err)
			}
		case 2:
			priority := Priority(next() % 2)
			item, err := queue.TryPopPriority(priority)
			if err == nil {
				held = append(held, item)
			} else if !errors.Is(err, ErrEmpty) {
				t.Fatalf("TryPopPriority() error = %v", err)
			}
		case 3:
			if len(held) > 0 {
				index := int(next() % uint64(len(held)))
				err := queue.Push(held[index])
				if err == nil {
					held[index] = held[len(held)-1]
					held = held[:len(held)-1]
				} else if !errors.Is(err, ErrFull) && !errors.Is(err, ErrExpired) {
					t.Fatalf("transferred Push() error = %v", err)
				}
			}
		case 4:
			if len(held) > 0 {
				index := int(next() % uint64(len(held)))
				held[index].ReleaseRetention()
				held[index] = held[len(held)-1]
				held = held[:len(held)-1]
			}
		case 5:
			now = now.Add(time.Duration(next()%5+1) * time.Millisecond)
		}
		assertQueueRetentionInvariants(t, queues, held, budget)
	}
	for index := range held {
		held[index].ReleaseRetention()
	}
	for _, queue := range queues {
		queue.Close()
	}
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after cleanup = %+v", got)
	}
}

func assertQueueRetentionInvariants(t *testing.T, queues []*Queue[int], held []Item[int],
	budget *retention.Budget) {
	t.Helper()
	want := retention.Usage{}
	for _, queue := range queues {
		queue.mu.Lock()
		packets := 0
		bytes := 0
		visit := func(items []Item[int]) {
			for _, item := range items {
				if item.retention != budget || item.retentionBytes != item.Size {
					t.Fatalf("invalid queued retention: %+v", item)
				}
				packets++
				bytes += item.Size
			}
		}
		visit(queue.normal.items[queue.normal.head:])
		visit(queue.control.items[queue.control.head:])
		if packets != queue.packets || bytes != queue.bytes {
			t.Fatalf("queue accounting = %d packets and %d bytes, want %d and %d",
				queue.packets, queue.bytes, packets, bytes)
		}
		want.Packets += queue.packets
		want.Bytes += queue.bytes
		queue.mu.Unlock()
	}
	for _, item := range held {
		if item.retention != budget || item.retentionBytes != item.Size {
			t.Fatalf("invalid held retention: %+v", item)
		}
		want.Packets++
		want.Bytes += item.Size
	}
	if got := budget.Usage(); got != want {
		t.Fatalf("budget usage = %+v, want %+v", got, want)
	}
}
