package packetqueue

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

func BenchmarkQueuePushBatch(b *testing.B) {
	for _, shared := range []bool{false, true} {
		name := "Local"
		if shared {
			name = "SharedBudget"
		}
		b.Run(name, func(b *testing.B) {
			for _, packets := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("Packets%d", packets), func(b *testing.B) {
					var budget *retention.Budget
					if shared {
						var err error
						budget, err = retention.NewBudget(retention.Limits{Packets: 1 << 20, Bytes: 1 << 30})
						if err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						now := time.Unix(100, 0)
						queue, err := newQueue[int](Limits{Packets: 16, Bytes: 16 * 1452}, func() time.Time { return now }, budget)
						if err != nil {
							b.Error(err)
							return
						}
						defer queue.Close()
						var items [16]Item[int]
						for index := range items {
							items[index] = Item[int]{Value: index, Size: 1452, Deadline: now.Add(time.Second)}
						}
						for pb.Next() {
							if count, err := queue.PushBatch(items[:packets]); count != packets || err != nil {
								b.Errorf("PushBatch() = %d, %v", count, err)
								return
							}
							for range packets {
								var item Item[int]
								if err := queue.TryPop(&item, now); err != nil {
									b.Error(err)
									return
								}
								item.Release()
							}
						}
					})
					if budget != nil && budget.Usage() != (retention.Usage{}) {
						b.Fatal("shared reservations did not drain")
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(packets)), "ns/packet")
				})
			}
		})
	}
}

func BenchmarkQueuePushBatchExhaustedBudget(b *testing.B) {
	for _, bytes := range []bool{false, true} {
		name := "Packets"
		limits := retention.Limits{Packets: 16, Bytes: 1 << 20}
		reservedPackets := 16
		if bytes {
			name = "Bytes"
			limits = retention.Limits{Packets: 1024, Bytes: 1024}
			reservedPackets = 1
		}
		b.Run(name, func(b *testing.B) {
			budget, err := retention.NewBudget(limits)
			if err != nil || !budget.Reserve(reservedPackets, 1024) {
				b.Fatal("prepare exhausted budget", err)
			}
			defer budget.Release(reservedPackets, 1024)
			now := time.Unix(100, 0)
			queue, err := NewWithBudget[int](Limits{Packets: 16, Bytes: 1 << 20}, budget, func() time.Time { return now })
			if err != nil {
				b.Fatal(err)
			}
			defer queue.Close()
			var items [16]Item[int]
			for index := range items {
				items[index] = Item[int]{Size: 64, Deadline: now.Add(time.Second)}
			}
			b.ReportAllocs()
			for b.Loop() {
				if count, err := queue.PushBatch(items[:]); count != 0 || !errors.Is(err, ErrFull) {
					b.Fatalf("exhausted admission = %d, %v", count, err)
				}
			}
			if budget.Usage() != (retention.Usage{Packets: reservedPackets, Bytes: 1024}) {
				b.Fatal("rejected batch changed retained capacity")
			}
		})
	}
}

func BenchmarkQueuePushBatchPartialBudget(b *testing.B) {
	for _, available := range []int{1, 4, 15} {
		for _, byteLimit := range []bool{false, true} {
			kind := "Packets"
			limits := retention.Limits{Packets: 16, Bytes: 1 << 20}
			heldPackets, heldBytes := 16-available, 1024
			if byteLimit {
				kind = "Bytes"
				limits = retention.Limits{Packets: 1024, Bytes: 1024}
				heldPackets, heldBytes = 1, (16-available)*64
			}
			b.Run(fmt.Sprintf("%s/Available%d", kind, available), func(b *testing.B) {
				budget, err := retention.NewBudget(limits)
				if err != nil || !budget.Reserve(heldPackets, heldBytes) {
					b.Fatal("prepare partial budget", err)
				}
				defer budget.Release(heldPackets, heldBytes)
				now := time.Unix(100, 0)
				queue, err := NewWithBudget[int](Limits{Packets: 16, Bytes: 1 << 20}, budget, func() time.Time { return now })
				if err != nil {
					b.Fatal(err)
				}
				defer queue.Close()
				var items [16]Item[int]
				for index := range items {
					items[index] = Item[int]{Size: 64, Deadline: now.Add(time.Second)}
				}
				b.ReportAllocs()
				for b.Loop() {
					count, err := queue.PushBatch(items[:])
					if count != available || !errors.Is(err, ErrFull) {
						b.Fatalf("partial admission = %d, %v, want %d, %v", count, err, available, ErrFull)
					}
					for range count {
						var item Item[int]
						if err := queue.TryPop(&item, now); err != nil {
							b.Fatal(err)
						}
						item.Release()
					}
				}
				if budget.Usage() != (retention.Usage{Packets: heldPackets, Bytes: heldBytes}) {
					b.Fatal("partial batch changed the other owner's capacity")
				}
			})
		}
	}
}
