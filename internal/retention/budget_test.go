package retention

import (
	"errors"
	"sync"
	"testing"
)

func TestNewBudget(t *testing.T) {
	for _, limits := range []Limits{
		{},
		{Packets: 1},
		{Bytes: 1},
		{Packets: -1, Bytes: 1},
		{Packets: 1, Bytes: -1},
	} {
		if _, err := NewBudget(limits); !errors.Is(err, ErrInvalidLimits) {
			t.Fatalf("NewBudget(%+v) error = %v, want %v", limits, err, ErrInvalidLimits)
		}
	}
}

func TestBudgetReserve(t *testing.T) {
	budget, err := NewBudget(Limits{Packets: 2, Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !budget.Reserve(1, 6) {
		t.Fatal("Reserve(1, 6) = false")
	}
	if budget.Reserve(1, 5) {
		t.Fatal("Reserve(1, 5) = true after byte limit")
	}
	if got := budget.Usage(); got != (Usage{Packets: 1, Bytes: 6}) {
		t.Fatalf("Usage() = %+v, want 1 packet and 6 bytes", got)
	}
	if !budget.Reserve(1, 4) {
		t.Fatal("Reserve(1, 4) = false")
	}
	if budget.Reserve(1, 1) {
		t.Fatal("Reserve(1, 1) = true after packet limit")
	}
	budget.Release(2, 10)
	if got := budget.Usage(); got != (Usage{}) {
		t.Fatalf("Usage() after release = %+v", got)
	}
}

func TestBudgetConcurrentReserve(t *testing.T) {
	const limit = 100
	budget, err := NewBudget(Limits{Packets: limit, Bytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan bool, limit*2)
	var workers sync.WaitGroup
	for range limit * 2 {
		workers.Go(func() {
			<-start
			results <- budget.Reserve(1, 1)
		})
	}
	close(start)
	workers.Wait()
	close(results)
	accepted := 0
	for result := range results {
		if result {
			accepted++
		}
	}
	if accepted != limit {
		t.Fatalf("accepted reservations = %d, want %d", accepted, limit)
	}
	if got := budget.Usage(); got != (Usage{Packets: limit, Bytes: limit}) {
		t.Fatalf("Usage() = %+v, want limits", got)
	}
}

func TestBudgetResizeBytes(t *testing.T) {
	budget, err := NewBudget(Limits{Packets: 1, Bytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !budget.Reserve(1, 6) || !budget.ResizeBytes(6, 10) {
		t.Fatal("reservation did not grow to the byte limit")
	}
	if budget.ResizeBytes(10, 11) {
		t.Fatal("reservation grew above the byte limit")
	}
	if !budget.ResizeBytes(10, 4) {
		t.Fatal("reservation did not shrink")
	}
	if got := budget.Usage(); got != (Usage{Packets: 1, Bytes: 4}) {
		t.Fatalf("Usage() = %+v, want 1 packet and 4 bytes", got)
	}
	budget.Release(1, 4)
}
