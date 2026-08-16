package relay

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var benchmarkCandidateSink uint64

type benchmarkEndpoint struct{}

func (benchmarkEndpoint) Read(context.Context) (datagram.Packet, error) {
	panic("unexpected benchmark read")
}

func (benchmarkEndpoint) Write(context.Context, []byte, time.Time) error {
	return nil
}

func (benchmarkEndpoint) Close() error {
	return nil
}

func BenchmarkSelectCandidates(b *testing.B) {
	lanes := make(map[protocol.LaneID]*scheduledLane, 16)
	for index := range 16 {
		laneID := protocol.LaneID{byte(index + 1)}
		store, err := NewTransmissionStore(packetqueue.Limits{Packets: 1024, Bytes: 16 * 1024 * 1024})
		if err != nil {
			b.Fatal(err)
		}
		store.backlogBytes.Store(uint64(index * 1500))
		lanes[laneID] = &scheduledLane{
			registration: LaneRegistration{
				LaneID: laneID, PathGroupID: protocol.PathGroupID{byte(index/2 + 1)}, Store: store,
			},
			rttMicros: uint64(10_000 + index*250), deliveryRate: 10_000_000,
		}
	}
	b.Run("Transport", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			selected := selectCandidates(lanes, protocol.LaneID{2}, false, 1500, math.MaxUint64)
			benchmarkCandidateSink = selected.lanes[0].score(1500)
		}
	})
	b.Run("Control", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			selected := selectCandidates(lanes, protocol.LaneID{}, true, 1500, math.MaxUint64)
			benchmarkCandidateSink = selected.lanes[selected.count-1].score(1500)
		}
	})
}

func BenchmarkTransmissionStoreCycle(b *testing.B) {
	b.Run("Local", func(b *testing.B) { benchmarkTransmissionStoreCycle(b, nil) })
	budget, err := retention.NewBudget(retention.Limits{Packets: 1024, Bytes: 16 * 1024 * 1024})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Aggregate", func(b *testing.B) { benchmarkTransmissionStoreCycle(b, budget) })
}

func benchmarkTransmissionStoreCycle(b *testing.B, budget *retention.Budget) {
	now := time.Now()
	store, err := newTransmissionStoreWithBudget(
		packetqueue.Limits{Packets: 256, Bytes: 2 * 1024 * 1024}, func() time.Time { return now }, budget,
	)
	if err != nil {
		b.Fatal(err)
	}
	transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
	var batch [1]protocol.Data
	var packets uint64
	var bytes uint64
	b.ReportAllocs()
	b.SetBytes(int64(len(transmission.data.Payload)))
	for b.Loop() {
		packets++
		transmission.data.PacketID = packets
		if err := store.push(transmission); err != nil {
			b.Fatal(err)
		}
		if count, err := store.takeBatch(batch[:], transmission.size); err != nil || count != 1 {
			b.Fatalf("takeBatch() = %d, %v", count, err)
		}
		bytes += uint64(transmission.size)
		if _, _, err := store.acknowledge(packets, bytes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransmissionStoreBacklogCycle(b *testing.B) {
	const backlogPackets = 1024
	now := time.Now()
	store, err := newTransmissionStore(
		packetqueue.Limits{Packets: backlogPackets, Bytes: 16 * 1024 * 1024}, func() time.Time { return now },
	)
	if err != nil {
		b.Fatal(err)
	}
	transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Hour))
	packetID := uint64(0)
	for range backlogPackets {
		packetID++
		transmission.data.PacketID = packetID
		if err := store.push(transmission); err != nil {
			b.Fatal(err)
		}
	}
	var batch [maximumDataBatchFrames]protocol.Data
	var sentPackets uint64
	var sentBytes uint64
	b.ReportAllocs()
	b.SetBytes(int64(len(transmission.data.Payload) * len(batch)))
	for b.Loop() {
		count, err := store.takeBatch(batch[:], targetDataBatchBytes)
		if err != nil || count != len(batch) {
			b.Fatalf("takeBatch() = %d, %v", count, err)
		}
		sentPackets += uint64(count)
		sentBytes += uint64(count * transmission.size)
		if _, _, err := store.acknowledge(sentPackets, sentBytes); err != nil {
			b.Fatal(err)
		}
		for range count {
			packetID++
			transmission.data.PacketID = packetID
			if err := store.push(transmission); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkReceiverDeliver(b *testing.B) {
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: benchmarkEndpoint{}, Clock: &testClock{now: 1}, DeduplicationSize: 1_048_576,
	})
	if err != nil {
		b.Fatal(err)
	}
	data := protocol.Data{
		DeadlineMicros: protocol.MaxPacketLifetimeMicros,
		Payload:        relayWireGuardPacket(wgpacket.TransportData),
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data.Payload)))
	for b.Loop() {
		data.PacketID++
		if err := receiver.Deliver(context.Background(), data); err != nil {
			b.Fatal(err)
		}
	}
}
