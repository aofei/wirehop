package packetqueue

import (
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

func TestQueueNextDeadline(t *testing.T) {
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
	if err := queue.TryPop(&item); err != nil || item.Value != 1 {
		t.Fatalf("priority pop = %+v, %v", item, err)
	}
	if deadline := queue.NextDeadline(); deadline.After(now.Add(2*time.Second)) || deadline.Before(first) {
		t.Fatalf("conservative wake-up after priority pop = %v", deadline)
	}
	now = now.Add(2 * time.Second)
	if deadline := queue.NextDeadline(); !deadline.Equal(last) || queue.Len() != 1 {
		t.Fatalf("non-FIFO expiry retained deadline %v and %d packets", deadline, queue.Len())
	}
	if err := queue.TryPop(&item); err != nil || item.Value != 2 || !queue.NextDeadline().IsZero() {
		t.Fatalf("last pop retained expiry state: item %+v, error %v", item, err)
	}
	queue.Close()
	queue.Expire()
	if !queue.NextDeadline().IsZero() {
		t.Fatal("closed queue retained expiry state")
	}
}

func TestQueueNextDeadlineAfterFailedControlPreemption(t *testing.T) {
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
	if queue.Len() != 0 || !queue.NextDeadline().IsZero() {
		t.Fatal("empty preempted queue retained an expiry deadline")
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 1}) {
		t.Fatalf("failed admission retained %+v, want only the other owner's capacity", got)
	}
}
