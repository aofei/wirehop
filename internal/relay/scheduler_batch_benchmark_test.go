package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func BenchmarkSchedulerRun(b *testing.B) {
	for _, tt := range []struct {
		name  string
		lanes byte
	}{
		{name: "Single", lanes: 1},
		{name: "Two", lanes: 2},
		{name: "Sixteen", lanes: 16},
	} {
		b.Run(tt.name, func(b *testing.B) {
			const packets = 128
			clock := monotime.New()
			now := func() time.Time { return time.UnixMicro(int64(clock.NowMicros())) }
			queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: packets, Bytes: packets * 1452}, now)
			if err != nil {
				b.Fatal(err)
			}
			defer queue.Close()
			scheduler, err := NewScheduler(&Ingress{queue: queue})
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()
			scheduled := make(chan error, 1)
			go func() { scheduled <- scheduler.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-scheduled; !errors.Is(err, context.Canceled) {
					b.Errorf("Run() = %v", err)
				}
			}()
			var primary *TransmissionStore
			for id := byte(1); id <= tt.lanes; id++ {
				store, err := newTransmissionStore(packetqueue.Limits{Packets: 256, Bytes: 1024 * 1024}, now)
				if err != nil {
					b.Fatal(err)
				}
				if id == 1 {
					primary = store
				}
				if err := scheduler.Register(ctx, schedulerRegistration(id, 1, store)); err != nil {
					b.Fatal(err)
				}
			}
			completed := make(chan struct{})
			written := make(chan error, 1)
			go func() {
				defer cancel()
				var data [maximumDataBatchFrames]protocol.Data
				var ownership [maximumDataBatchFrames]datagram.Packet
				received := 0
				for {
					count, err := primary.takeBatch(data[:], ownership[:], targetDataBatchBytes, 0)
					if err == nil {
						last := data[count-1].PacketID
						releaseBatchOwnership(ownership[:count])
						if _, _, err := primary.acknowledge(last, uint64(now().UnixMicro())); err != nil {
							written <- err
							return
						}
						received += count
						if received == packets {
							received = 0
							select {
							case <-ctx.Done():
								written <- nil
								return
							case completed <- struct{}{}:
							}
						}
						continue
					}
					if ctx.Err() != nil {
						written <- nil
						return
					}
					if !errors.Is(err, packetqueue.ErrEmpty) {
						written <- err
						return
					}
					select {
					case <-ctx.Done():
						written <- nil
						return
					case <-primary.Ready():
					}
				}
			}()
			defer func() {
				cancel()
				if err := <-written; err != nil {
					b.Error(err)
				}
			}()
			var items [packets]packetqueue.Item[Packet]
			for index := range items {
				items[index] = ingressProbeItem(wgpacket.TransportData, 1452, now().Add(time.Hour))
			}
			b.ReportAllocs()
			b.SetBytes(packets * 1452)
			for b.Loop() {
				if count, err := queue.PushBatch(items[:]); err != nil || count != packets {
					b.Fatalf("PushBatch() = %d, %v", count, err)
				}
				select {
				case <-ctx.Done():
					b.Fatal(ctx.Err())
				case <-completed:
				}
			}
			b.ReportMetric(float64(b.N)*packets/b.Elapsed().Seconds(), "packets/s")
		})
	}
}
