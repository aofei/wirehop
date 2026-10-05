package relay

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func BenchmarkSaturatedIngress(b *testing.B) {
	for _, packets := range []int{64, 1024, 16384} {
		b.Run(strconv.Itoa(packets), func(b *testing.B) {
			now := time.Unix(100, 0)
			queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{
				Packets: packets, Bytes: packets * 1452,
			}, func() time.Time { return now })
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(queue.Close)
			payload := make([]byte, 1452)
			payload[0] = 4
			item := packetqueue.Item[Packet]{
				Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 1_000_000},
				Size:  1452, Deadline: now.Add(time.Second),
			}
			for range packets {
				if err := queue.Push(item); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := queue.Push(item); !errors.Is(err, packetqueue.ErrFull) {
					b.Fatalf("Push() error = %v, want %v", err, packetqueue.ErrFull)
				}
			}
		})
	}
}

func BenchmarkIngressExpiryReclaim(b *testing.B) {
	const packets = 1024
	now := time.Unix(100, 0)
	queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{
		Packets: packets, Bytes: packets * 1452,
	}, func() time.Time { return now })
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(queue.Close)
	payload := make([]byte, 1452)
	payload[0] = 4
	live := packetqueue.Item[Packet]{
		Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 1_000_000},
		Size:  1452, Deadline: now.Add(time.Hour),
	}
	for range packets - 1 {
		if err := queue.Push(live); err != nil {
			b.Fatal(err)
		}
	}
	expiring := live
	expiring.Deadline = now.Add(time.Nanosecond)
	if err := queue.Push(expiring); err != nil {
		b.Fatal(err)
	}
	var popped packetqueue.Item[Packet]
	b.ReportAllocs()
	for b.Loop() {
		now = now.Add(time.Nanosecond)
		if err := queue.Push(live); err != nil {
			b.Fatal(err)
		}
		if err := queue.TryPop(&popped); err != nil {
			b.Fatal(err)
		}
		popped.Release()
		expiring.Deadline = now.Add(time.Nanosecond)
		if err := queue.Push(expiring); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchedulerMaintenance(b *testing.B) {
	for _, packets := range []int{64, 1024, 16384} {
		b.Run(strconv.Itoa(packets), func(b *testing.B) {
			for _, stalled := range []bool{false, true} {
				name := "Progressing"
				if stalled {
					name = "Stalled"
				}
				b.Run(name, func(b *testing.B) {
					now := time.Unix(100, 0)
					store, err := newTransmissionStore(packetqueue.Limits{
						Packets: packets, Bytes: packets * 4096,
					}, func() time.Time { return now })
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { releaseTransmissions(store.drain()) })
					for index := range packets {
						if err := store.push(schedulerTransmission(
							uint64(index+1), wgpacket.TransportData, now.Add(time.Hour),
						)); err != nil {
							b.Fatal(err)
						}
					}
					var data [1]protocol.Data
					var ownership [1]Packet
					count, err := store.takeBatch(data[:], ownership[:], 4096)
					releaseBatchOwnership(ownership[:count])
					if err != nil || count != 1 {
						b.Fatalf("takeBatch() = %d, %v, want one sent packet", count, err)
					}
					laneID := protocol.LaneID{1}
					lane := &scheduledLane{
						registration: LaneRegistration{LaneID: laneID, Store: store},
						deliveryRate: 10_000_000, lastProgressAt: now,
					}
					if stalled {
						now = now.Add(time.Second)
					}
					lanes := map[protocol.LaneID]*scheduledLane{laneID: lane}
					scheduler := new(Scheduler)
					b.ReportAllocs()
					for b.Loop() {
						scheduler.checkAbandonment(lanes, now)
					}
				})
			}
		})
	}
}
