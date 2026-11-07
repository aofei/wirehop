package packetqueue

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

func TestQueuePushBatchBudget(t *testing.T) {
	t.Run("IndividualOwnership", func(t *testing.T) {
		for _, count := range []int{1, 2, 3, 16, 17, 18, 33, 40} {
			t.Run(fmt.Sprintf("Packets%d", count), func(t *testing.T) {
				now := time.Unix(100, 0)
				budget := newBatchBudget(t, 64, 4096)
				calls := 0
				queue := newBatchQueue[ownedQueueValue](t, Limits{Packets: 64, Bytes: 4096}, budget, func() time.Time {
					calls++
					return now
				})
				releases := make([]int, count)
				items := batchOwnedItems(releases, 3, now.Add(time.Second))
				items[count-1].Deadline = now.Add(time.Millisecond)
				written, err := queue.PushBatch(items)
				if written != count || err != nil || calls != 1 {
					t.Fatalf("PushBatch() = %d, %v, clock calls %d", written, err, calls)
				}
				if got := budget.Usage(); got != (retention.Usage{Packets: count, Bytes: count * 3}) {
					t.Fatalf("reserved usage = %+v", got)
				}
				if deadline := queue.NextDeadline(now); !deadline.Equal(items[count-1].Deadline) {
					t.Fatalf("batch expiry bound = %v", deadline)
				}
				for index := range items {
					if items[index].retention != nil || items[index].Value.releases != &releases[index] {
						t.Fatal("batch admission changed caller metadata")
					}
					var item Item[ownedQueueValue]
					if err := queue.TryPop(&item, now); err != nil {
						t.Fatal(err)
					}
					if item.Value.releases != &releases[index] || item.retention != budget || item.retentionBytes != 3 {
						t.Fatalf("item %d lost FIFO ownership or its individual reservation", index)
					}
					if got := budget.Usage(); got.Packets != count-index {
						t.Fatal("pop released ownership still held by the caller")
					}
					item.Release()
					if releases[index] != 1 {
						t.Fatalf("item %d was not released once", index)
					}
				}
				if queue.Len() != 0 || queue.Bytes() != 0 || budget.Usage() != (retention.Usage{}) {
					t.Fatal("batch did not drain")
				}
			})
		}
	})

	t.Run("HeldOwnershipDuringCleanup", func(t *testing.T) {
		for _, cleanup := range []string{"Close", "Expire"} {
			t.Run(cleanup, func(t *testing.T) {
				now := time.Unix(100, 0)
				budget := newBatchBudget(t, 4, 8)
				queue := newBatchQueue[ownedQueueValue](t, Limits{Packets: 4, Bytes: 8}, budget, func() time.Time { return now })
				var releases [4]int
				items := batchOwnedItems(releases[:], 2, now.Add(time.Second))
				items[0].Priority = PriorityControl
				if count, err := queue.PushBatch(items); count != len(items) || err != nil {
					t.Fatalf("PushBatch() = %d, %v", count, err)
				}
				var held Item[ownedQueueValue]
				if err := queue.TryPopPriority(PriorityNormal, &held, now); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(held.Release)
				if held.Value.releases != &releases[1] {
					t.Fatal("held ownership did not come from the grouped normal prefix")
				}
				if reservation, ok := held.TakeRetention(3); reservation != nil || ok || held.retention != budget ||
					held.retentionBytes != 2 || budget.Usage() != (retention.Usage{Packets: 4, Bytes: 8}) {
					t.Fatal("failed resize changed the held reservation")
				}
				if cleanup == "Close" {
					queue.Close()
				} else {
					now = now.Add(time.Second)
					queue.Expire()
				}
				if queue.Len() != 0 || queue.Bytes() != 0 || releases != ([4]int{1, 0, 1, 1}) ||
					budget.Usage() != (retention.Usage{Packets: 1, Bytes: 2}) {
					t.Fatal("queue cleanup released held ownership or retained queued ownership")
				}
				reservation, ok := held.TakeRetention(8)
				if !ok || reservation != budget || budget.Usage() != (retention.Usage{Packets: 1, Bytes: 8}) {
					t.Fatal("held ownership could not resize after queue cleanup")
				}
				held.RestoreRetention(reservation, 8)
				held.Release()
				if releases != ([4]int{1, 1, 1, 1}) || budget.Usage() != (retention.Usage{}) {
					t.Fatal("held ownership did not release exactly once")
				}
			})
		}
	})

	t.Run("PartialPrefix", func(t *testing.T) {
		for _, tt := range []struct {
			name   string
			queue  Limits
			budget retention.Limits
			want   int
		}{
			{name: "AggregatePackets", queue: Limits{Packets: 8, Bytes: 32}, budget: retention.Limits{Packets: 4, Bytes: 32}, want: 4},
			{name: "AggregateBytes", queue: Limits{Packets: 8, Bytes: 32}, budget: retention.Limits{Packets: 8, Bytes: 10}, want: 3},
			{name: "QueuePackets", queue: Limits{Packets: 4, Bytes: 32}, budget: retention.Limits{Packets: 8, Bytes: 32}, want: 4},
			{name: "QueueBytes", queue: Limits{Packets: 8, Bytes: 10}, budget: retention.Limits{Packets: 8, Bytes: 32}, want: 3},
		} {
			t.Run(tt.name, func(t *testing.T) {
				now := time.Unix(100, 0)
				budget := newBatchBudget(t, tt.budget.Packets, tt.budget.Bytes)
				queue := newBatchQueue[ownedQueueValue](t, tt.queue, budget, func() time.Time { return now })
				var releases [6]int
				items := batchOwnedItems(releases[:], 3, now.Add(time.Second))
				written, err := queue.PushBatch(items)
				if written != tt.want || !errors.Is(err, ErrFull) {
					t.Fatalf("PushBatch() = %d, %v, want %d, %v", written, err, tt.want, ErrFull)
				}
				if got := budget.Usage(); got != (retention.Usage{Packets: tt.want, Bytes: tt.want * 3}) {
					t.Fatalf("partial reservation = %+v", got)
				}
				select {
				case <-queue.Ready():
				default:
					t.Fatal("accepted prefix did not notify consumers")
				}
				queue.Close()
				for index := range releases {
					want := 0
					if index < tt.want {
						want = 1
					}
					if releases[index] != want {
						t.Fatalf("item %d releases = %d, want %d", index, releases[index], want)
					}
				}
				for index := tt.want; index < len(items); index++ {
					items[index].Release()
				}
				if budget.Usage() != (retention.Usage{}) {
					t.Fatal("partial admission leaked reservations")
				}
			})
		}
	})

	t.Run("ReclaimsExpiredAggregateCapacity", func(t *testing.T) {
		now := time.Unix(100, 0)
		budget := newBatchBudget(t, 8, 12)
		queue := newBatchQueue[ownedQueueValue](t, Limits{Packets: 8, Bytes: 16}, budget, func() time.Time { return now })
		expiredReleases := 0
		if err := queue.Push(Item[ownedQueueValue]{Value: ownedQueueValue{releases: &expiredReleases}, Size: 5,
			Deadline: now.Add(time.Millisecond)}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Millisecond)
		var releases [5]int
		items := batchOwnedItems(releases[:], 2, now.Add(time.Second))
		if count, err := queue.PushBatch(items); count != len(items) || err != nil {
			t.Fatalf("PushBatch() after reclamation = %d, %v", count, err)
		}
		if expiredReleases != 1 || queue.Len() != 5 || queue.Bytes() != 10 ||
			budget.Usage() != (retention.Usage{Packets: 5, Bytes: 10}) || !queue.NextDeadline(now).Equal(items[0].Deadline) {
			t.Fatal("group failure bypassed scalar expiry reclamation")
		}
		queue.Close()
		if budget.Usage() != (retention.Usage{}) || releases != ([5]int{1, 1, 1, 1, 1}) {
			t.Fatal("reclaimed batch retained ownership")
		}
	})

	t.Run("ControlPreemption", func(t *testing.T) {
		now := time.Unix(100, 0)
		budget := newBatchBudget(t, 4, 16)
		queue := newBatchQueue[ownedQueueValue](t, Limits{Packets: 4, Bytes: 16, ControlPreemption: true}, budget,
			func() time.Time { return now })
		var releases [5]int
		items := batchOwnedItems(releases[:], 2, now.Add(time.Second))
		items[4].Priority = PriorityControl
		if count, err := queue.PushBatch(items); count != len(items) || err != nil {
			t.Fatalf("mixed priority batch = %d, %v", count, err)
		}
		if releases[0] != 1 || budget.Usage() != (retention.Usage{Packets: 4, Bytes: 8}) {
			t.Fatal("control did not reclaim the oldest normal reservation")
		}
		for _, index := range []int{4, 1, 2, 3} {
			var item Item[ownedQueueValue]
			if err := queue.TryPop(&item, now); err != nil || item.Value.releases != &releases[index] {
				t.Fatalf("dequeue after preemption: item %+v, error %v", item, err)
			}
			item.Release()
		}
		if releases != ([5]int{1, 1, 1, 1, 1}) || budget.Usage() != (retention.Usage{}) {
			t.Fatal("mixed priority batch leaked ownership")
		}
	})

	t.Run("InvalidOrExpiredTail", func(t *testing.T) {
		for _, tt := range []struct {
			name   string
			change func(*Item[ownedQueueValue])
			want   error
		}{
			{name: "InvalidSize", change: func(item *Item[ownedQueueValue]) { item.Size = 0 }, want: ErrInvalidItem},
			{name: "InvalidPriority", change: func(item *Item[ownedQueueValue]) { item.Priority = 2 }, want: ErrInvalidItem},
			{name: "MissingDeadline", change: func(item *Item[ownedQueueValue]) { item.Deadline = time.Time{} }, want: ErrInvalidItem},
			{name: "Expired", change: func(item *Item[ownedQueueValue]) { item.Deadline = time.Unix(100, 0) }, want: ErrExpired},
		} {
			t.Run(tt.name, func(t *testing.T) {
				now := time.Unix(100, 0)
				budget := newBatchBudget(t, 8, 32)
				queue := newBatchQueue[ownedQueueValue](t, Limits{Packets: 8, Bytes: 32}, budget, func() time.Time { return now })
				var releases [5]int
				items := batchOwnedItems(releases[:], 2, now.Add(time.Second))
				tt.change(&items[3])
				if count, err := queue.PushBatch(items); count != 3 || !errors.Is(err, tt.want) {
					t.Fatalf("PushBatch() = %d, %v, want 3, %v", count, err, tt.want)
				}
				queue.Close()
				if releases != ([5]int{1, 1, 1, 0, 0}) || budget.Usage() != (retention.Usage{}) {
					t.Fatal("rejected tail changed ownership")
				}
				items[3].Release()
				items[4].Release()
			})
		}
	})

	t.Run("TransferredAndForeignReservations", func(t *testing.T) {
		now := time.Unix(100, 0)
		limits := Limits{Packets: 8, Bytes: 32}
		budget := newBatchBudget(t, 8, 32)
		foreignBudget := newBatchBudget(t, 8, 32)
		source := newBatchQueue[ownedQueueValue](t, limits, budget, func() time.Time { return now })
		foreignSource := newBatchQueue[ownedQueueValue](t, limits, foreignBudget, func() time.Time { return now })
		target := newBatchQueue[ownedQueueValue](t, limits, budget, func() time.Time { return now })
		var releases [7]int
		items := batchOwnedItems(releases[:], 2, now.Add(time.Second))
		for _, transfer := range []struct {
			queue *Queue[ownedQueueValue]
			index int
		}{
			{queue: source, index: 3},
			{queue: foreignSource, index: 6},
		} {
			if err := transfer.queue.Push(items[transfer.index]); err != nil {
				t.Fatal(err)
			}
			if err := transfer.queue.TryPop(&items[transfer.index], now); err != nil {
				t.Fatal(err)
			}
		}
		if count, err := target.PushBatch(items); count != 6 || !errors.Is(err, ErrInvalidItem) {
			t.Fatalf("transferred batch = %d, %v, want 6, %v", count, err, ErrInvalidItem)
		}
		if budget.Usage() != (retention.Usage{Packets: 6, Bytes: 12}) ||
			foreignBudget.Usage() != (retention.Usage{Packets: 1, Bytes: 2}) {
			t.Fatal("batch recharged an owned item or accepted a foreign reservation")
		}
		var item Item[ownedQueueValue]
		if err := target.TryPop(&item, now); err != nil {
			t.Fatal(err)
		}
		reservation, ok := item.TakeRetention(5)
		if !ok || reservation != budget || budget.Usage().Bytes != 15 {
			t.Fatal("individual batch reservation could not be resized")
		}
		item.RestoreRetention(reservation, 5)
		if budget.Usage().Bytes != 12 {
			t.Fatal("failed transfer could not restore its original charge")
		}
		item.Release()
		target.Close()
		items[6].Release()
		if releases != ([7]int{1, 1, 1, 1, 1, 1, 1}) || budget.Usage() != (retention.Usage{}) ||
			foreignBudget.Usage() != (retention.Usage{}) {
			t.Fatal("transferred batch leaked or duplicated ownership")
		}
	})

	t.Run("ClosedQueue", func(t *testing.T) {
		now := time.Unix(100, 0)
		budget := newBatchBudget(t, 8, 32)
		queue := newBatchQueue[ownedQueueValue](t, Limits{Packets: 8, Bytes: 32}, budget, func() time.Time { return now })
		queue.Close()
		var releases [3]int
		items := batchOwnedItems(releases[:], 2, now.Add(time.Second))
		if count, err := queue.PushBatch(nil); count != 0 || err != nil {
			t.Fatalf("empty closed batch = %d, %v", count, err)
		}
		if count, err := queue.PushBatch(items); count != 0 || !errors.Is(err, ErrClosed) {
			t.Fatalf("closed batch = %d, %v", count, err)
		}
		items[0].Size = 0
		if count, err := queue.PushBatch(items); count != 0 || !errors.Is(err, ErrInvalidItem) {
			t.Fatalf("invalid closed batch = %d, %v", count, err)
		}
		if releases != ([3]int{}) || budget.Usage() != (retention.Usage{}) {
			t.Fatal("closed admission took caller ownership")
		}
		for index := range items {
			items[index].Release()
		}
	})

	t.Run("MaximumByteCharge", func(t *testing.T) {
		now := time.Unix(100, 0)
		budget := newBatchBudget(t, 8, math.MaxInt)
		queue := newBatchQueue[int](t, Limits{Packets: 8, Bytes: math.MaxInt}, budget, func() time.Time { return now })
		var items []Item[int]
		for _, size := range []int{1, math.MaxInt - 3, 1, 1, 1} {
			items = append(items, Item[int]{Size: size, Deadline: now.Add(time.Second)})
		}
		if count, err := queue.PushBatch(items); count != 4 || !errors.Is(err, ErrFull) {
			t.Fatalf("maximum charge batch = %d, %v", count, err)
		}
		if queue.Bytes() != math.MaxInt || budget.Usage() != (retention.Usage{Packets: 4, Bytes: math.MaxInt}) {
			t.Fatal("batch byte sum overflowed")
		}
		queue.Close()
		if budget.Usage() != (retention.Usage{}) {
			t.Fatal("maximum charge did not release")
		}
	})
}

func newBatchBudget(t *testing.T, packets, bytes int) *retention.Budget {
	t.Helper()
	budget, err := retention.NewBudget(retention.Limits{Packets: packets, Bytes: bytes})
	if err != nil {
		t.Fatal(err)
	}
	return budget
}

func newBatchQueue[T any](t *testing.T, limits Limits, budget *retention.Budget, now func() time.Time) *Queue[T] {
	t.Helper()
	queue, err := NewWithBudget[T](limits, budget, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	return queue
}

func batchOwnedItems(releases []int, size int, deadline time.Time) []Item[ownedQueueValue] {
	items := make([]Item[ownedQueueValue], len(releases))
	for index := range items {
		items[index] = Item[ownedQueueValue]{Value: ownedQueueValue{releases: &releases[index]}, Size: size, Deadline: deadline}
	}
	return items
}
