package packetqueue

import (
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

func TestQueueNextDeadline(t *testing.T) {
	t.Run("NonFIFOExpiry", testQueueNextDeadlineNonFIFOExpiry)
	t.Run("FailedControlPreemption", testQueueNextDeadlineFailedControlPreemption)
	t.Run("PriorityBounds", testQueueNextDeadlinePriorityBounds)
}

func testQueueNextDeadlineNonFIFOExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	queue, err := NewWithClock[int](Limits{Packets: 4, Bytes: 4}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	first := now.Add(time.Second)
	last := now.Add(3 * time.Second)
	for _, item := range []Item[int]{
		{Value: 1, Size: 1, Priority: PriorityControl, Deadline: first},
		{Value: 2, Size: 1, Priority: PriorityNormal, Deadline: last},
		{Value: 3, Size: 1, Priority: PriorityNormal, Deadline: now.Add(2 * time.Second)},
	} {
		if err := queue.Push(item); err != nil {
			t.Fatal(err)
		}
	}
	var item Item[int]
	if err := queue.TryPop(&item, queue.Now()); err != nil || item.Value != 1 {
		t.Fatalf("priority pop = %+v, %v", item, err)
	}
	if deadline := queue.NextDeadline(queue.Now()); deadline.After(now.Add(2*time.Second)) || deadline.Before(first) {
		t.Fatalf("conservative wake-up after priority pop = %v", deadline)
	}
	now = now.Add(2 * time.Second)
	if deadline := queue.NextDeadline(queue.Now()); !deadline.Equal(last) || queue.Len() != 1 {
		t.Fatalf("non-FIFO expiry retained deadline %v and %d packets", deadline, queue.Len())
	}
	if err := queue.TryPop(&item, queue.Now()); err != nil || item.Value != 2 || !queue.NextDeadline(queue.Now()).IsZero() {
		t.Fatalf("last pop retained expiry state: item %+v, error %v", item, err)
	}
	queue.Close()
	queue.Expire()
	if !queue.NextDeadline(queue.Now()).IsZero() {
		t.Fatal("closed queue retained expiry state")
	}
}

func testQueueNextDeadlineFailedControlPreemption(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: 2})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewWithBudget[int](Limits{Packets: 2, Bytes: 4, ControlPreemption: true}, budget,
		func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	deadline := now.Add(time.Second)
	if err := queue.Push(Item[int]{Value: 1, Size: 1, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	if !budget.Reserve(1, 1) {
		t.Fatal("failed to reserve the other owner's capacity")
	}
	defer budget.Release(1, 1)
	if err := queue.Push(Item[int]{Value: 2, Size: 2, Priority: PriorityControl, Deadline: deadline}); !errors.Is(err, ErrFull) {
		t.Fatalf("control admission = %v, want aggregate capacity rejection", err)
	}
	if queue.Len() != 0 || !queue.NextDeadline(queue.Now()).IsZero() {
		t.Fatal("empty preempted queue retained an expiry deadline")
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 1}) {
		t.Fatalf("failed admission retained %+v, want only the other owner's capacity", got)
	}
}

func testQueueNextDeadlinePriorityBounds(t *testing.T) {
	start := time.Unix(100, 0)
	now := start
	budget, err := retention.NewBudget(retention.Limits{Packets: 6, Bytes: 6})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewWithBudget[ownedQueueValue](Limits{Packets: 6, Bytes: 6}, budget,
		func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	var releases [6]int
	push := func(index int, priority Priority, lifetime time.Duration) {
		t.Helper()
		if err := queue.Push(Item[ownedQueueValue]{
			Value: ownedQueueValue{releases: &releases[index]}, Size: 1,
			Priority: priority, Deadline: start.Add(lifetime),
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertDeadline := func(lifetime time.Duration) {
		t.Helper()
		if deadline := queue.NextDeadline(queue.Now()); !deadline.Equal(start.Add(lifetime)) {
			t.Fatalf("next deadline = %v, want %v", deadline, start.Add(lifetime))
		}
	}
	pop := func(index int) {
		t.Helper()
		var item Item[ownedQueueValue]
		if err := queue.TryPop(&item, queue.Now()); err != nil {
			t.Fatal(err)
		}
		if item.Value.releases != &releases[index] {
			t.Fatalf("priority FIFO value does not match index %d", index)
		}
		item.Release()
	}
	push(0, PriorityNormal, 3*time.Second)
	push(1, PriorityNormal, time.Second)
	push(2, PriorityNormal, 4*time.Second)
	push(3, PriorityControl, 2*time.Second)
	push(4, PriorityControl, 5*time.Second)
	assertDeadline(time.Second)
	pop(3)
	now = start.Add(time.Second)
	// The normal expiry exposes the consumed control deadline as a safe early wakeup.
	assertDeadline(2 * time.Second)
	now = start.Add(2 * time.Second)
	assertDeadline(3 * time.Second)
	// Migrated work lowers the bound even while other priority entries are retained.
	push(5, PriorityNormal, 2500*time.Millisecond)
	assertDeadline(2500 * time.Millisecond)
	now = start.Add(2500 * time.Millisecond)
	queue.Expire()
	assertDeadline(3 * time.Second)
	pop(4)
	pop(0)
	assertDeadline(3 * time.Second)
	now = start.Add(3 * time.Second)
	assertDeadline(4 * time.Second)
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 1}) {
		t.Fatalf("retained usage before final expiry = %+v", got)
	}
	now = start.Add(4 * time.Second)
	queue.Expire()
	if !queue.NextDeadline(queue.Now()).IsZero() || releases != ([6]int{1, 1, 1, 1, 1, 1}) ||
		budget.Usage() != (retention.Usage{}) {
		t.Fatalf("final releases = %v, retained usage = %+v", releases, budget.Usage())
	}
}
