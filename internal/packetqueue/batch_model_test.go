package packetqueue

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

func TestQueuePushBatchEquivalence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		budget  bool
		preempt bool
	}{
		{name: "Local"},
		{name: "LocalPreemption", preempt: true},
		{name: "SharedBudget", budget: true},
		{name: "SharedBudgetPreemption", budget: true, preempt: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 16; seed++ {
				random := rand.New(rand.NewPCG(seed, 0xd1342543de82ef95))
				now := time.Unix(100, 0)
				var queues [2][2]*Queue[int]
				var budgets [2]*retention.Budget
				for model := range queues {
					if tt.budget {
						budgets[model] = newBatchBudget(t, 24, 2048)
					}
				}
				createQueue := func(model int) *Queue[int] {
					queue, err := newQueue[int](Limits{Packets: 20, Bytes: 1536, ControlPreemption: tt.preempt},
						func() time.Time { return now }, budgets[model])
					if err != nil {
						t.Fatal(err)
					}
					return queue
				}
				for model := range queues {
					for index := range queues[model] {
						queues[model][index] = createQueue(model)
					}
				}
				push := func(index int, items [2][]Item[int]) (int, error) {
					t.Helper()
					wantCount, wantErr := pushBatchIndividually(queues[0][index], items[0])
					gotCount, gotErr := queues[1][index].PushBatch(items[1])
					if gotCount != wantCount || !errors.Is(gotErr, wantErr) {
						t.Fatalf("seed %d: PushBatch() = %d, %v, want %d, %v", seed, gotCount, gotErr, wantCount, wantErr)
					}
					return gotCount, gotErr
				}
				for step := range 2000 {
					index := random.IntN(2)
					switch random.IntN(7) {
					case 0, 1:
						items := make([]Item[int], random.IntN(40))
						for position := range items {
							items[position] = Item[int]{Value: step*40 + position, Size: random.IntN(160) + 1,
								Deadline: now.Add(time.Duration(random.IntN(8)-1) * 20 * time.Millisecond)}
							if random.IntN(6) == 0 {
								items[position].Priority = PriorityControl
							}
							switch random.IntN(80) {
							case 0:
								items[position].Size = 0
							case 1:
								items[position].Priority = 2
							case 2:
								items[position].Deadline = time.Time{}
							}
						}
						push(index, [2][]Item[int]{items, items})
					case 2, 3:
						var items [2]Item[int]
						var errs [2]error
						for model := range queues {
							errs[model] = queues[model][index].TryPop(&items[model], now)
						}
						if !errors.Is(errs[0], errs[1]) || items[0].Value != items[1].Value ||
							items[0].Size != items[1].Size || items[0].retentionBytes != items[1].retentionBytes {
							t.Fatalf("seed %d step %d: dequeued ownership differs", seed, step)
						}
						if errs[0] != nil {
							break
						}
						if random.IntN(2) == 0 {
							var batches [2][]Item[int]
							for model := range batches {
								for position := range 7 {
									batches[model] = append(batches[model], Item[int]{Value: step*40 + position,
										Size: 16, Deadline: now.Add(100 * time.Millisecond)})
								}
								batches[model][3] = items[model]
							}
							if count, _ := push(1-index, batches); count > 3 {
								break
							}
						}
						for model := range items {
							items[model].Release()
						}
					case 4:
						now = now.Add(time.Duration(random.IntN(5)+1) * time.Millisecond)
						for model := range queues {
							queues[model][index].Expire()
						}
					case 5:
						for model := range queues {
							if queues[model][index].closed {
								queues[model][index] = createQueue(model)
							} else {
								queues[model][index].Close()
							}
						}
					case 6:
						for model := range queues {
							select {
							case <-queues[model][index].Ready():
							default:
							}
						}
					}
					for index := range queues[0] {
						if !reflect.DeepEqual(batchQueueView(t, queues[0][index]), batchQueueView(t, queues[1][index])) {
							t.Fatalf("seed %d step %d: queue %d state differs", seed, step, index)
						}
					}
					if tt.budget {
						for model := range queues {
							assertQueueRetentionInvariants(t, queues[model][:], nil, budgets[model])
						}
						if budgets[0].Usage() != budgets[1].Usage() {
							t.Fatalf("seed %d step %d: aggregate usage differs", seed, step)
						}
					}
				}
				for model := range queues {
					for _, queue := range queues[model] {
						queue.Close()
					}
					if tt.budget && budgets[model].Usage() != (retention.Usage{}) {
						t.Fatal("model retained aggregate capacity after close")
					}
				}
			}
		})
	}
}

func TestQueuePushBatchConcurrentBudget(t *testing.T) {
	for _, tt := range []struct {
		name   string
		limits retention.Limits
	}{
		{name: "Packets", limits: retention.Limits{Packets: 64, Bytes: 1 << 20}},
		{name: "Bytes", limits: retention.Limits{Packets: 1024, Bytes: 4096}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			budget := newBatchBudget(t, tt.limits.Packets, tt.limits.Bytes)
			var workers sync.WaitGroup
			for worker := range 8 {
				workers.Go(func() {
					now := time.Unix(100, 0)
					queue, err := NewWithBudget[int](Limits{Packets: 16, Bytes: 4096}, budget, func() time.Time { return now })
					if err != nil {
						t.Error(err)
						return
					}
					defer queue.Close()
					var items [16]Item[int]
					for index := range items {
						items[index] = Item[int]{Value: worker*16 + index, Size: 32 + index*8, Deadline: now.Add(time.Second)}
					}
					for range 1000 {
						count, err := queue.PushBatch(items[:])
						if err != nil && !errors.Is(err, ErrFull) {
							t.Error(err)
							return
						}
						usage := budget.Usage()
						if usage.Packets < 0 || usage.Packets > tt.limits.Packets || usage.Bytes < 0 || usage.Bytes > tt.limits.Bytes {
							t.Errorf("aggregate limits exceeded: %+v", usage)
							return
						}
						for range count {
							var item Item[int]
							if err := queue.TryPop(&item, now); err != nil {
								t.Error(err)
								return
							}
							item.Release()
						}
					}
				})
			}
			workers.Wait()
			if budget.Usage() != (retention.Usage{}) {
				t.Fatal("concurrent queues leaked reservations")
			}
		})
	}
}

func pushBatchIndividually[T any](queue *Queue[T], items []Item[T]) (int, error) {
	for index, item := range items {
		if err := queue.Push(item); err != nil {
			return index, err
		}
	}
	return len(items), nil
}

func batchQueueView(t *testing.T, queue *Queue[int]) []string {
	t.Helper()
	view := []string{fmt.Sprintf("packets=%d bytes=%d closed=%t ready=%d", queue.packets, queue.bytes, queue.closed, len(queue.notify))}
	for _, deque := range []*deque[int]{&queue.control, &queue.normal} {
		view = append(view, fmt.Sprintf("deadline=%v", deque.earliestDeadline))
		for _, item := range deque.items[deque.head:] {
			if item.retention != queue.budget {
				t.Fatal("queued reservation belongs to another budget")
			}
			view = append(view, fmt.Sprintf("value=%d size=%d priority=%d deadline=%v charge=%d",
				item.Value, item.Size, item.Priority, item.Deadline, item.retentionBytes))
		}
	}
	return view
}
