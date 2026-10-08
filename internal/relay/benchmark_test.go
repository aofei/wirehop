package relay

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var benchmarkCandidateSink uint64

func BenchmarkSchedulerApplyEvent(b *testing.B) {
	for _, kind := range []schedulerEventKind{schedulerTiming, schedulerReport} {
		name := "Timing"
		if kind == schedulerReport {
			name = "DeliveryReport"
		}
		b.Run(name, func(b *testing.B) {
			now := time.Unix(100, 0)
			store, err := newTransmissionStore(packetqueue.Limits{Packets: 256, Bytes: 2 * 1024 * 1024},
				func() time.Time { return now })
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { releaseTransmissions(store.drain()) })
			laneID := protocol.LaneID(1)
			lanes := map[protocol.LaneID]*scheduledLane{
				laneID: {registration: LaneRegistration{
					LaneID: laneID, Generation: 1, Store: store, ValidatePingProgress: func(uint64) bool { return true },
				}, deliveryRate: defaultInitialRateBytesPerSecond},
			}
			event := schedulerEvent{
				kind: kind, laneID: laneID, generation: 1, result: make(chan error, 1),
				report: protocol.DeliveryReport{LaneID: laneID, Generation: 1},
				timing: clockmap.Sample{LocalSendMicros: 1, RemoteReceiveMicros: 2,
					RemoteSendMicros: 2, LocalReceiveMicros: 3},
			}
			scheduler := new(Scheduler)
			preferred := laneID
			transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Hour))
			var data [1]protocol.Data
			var ownership [1]datagram.Packet
			b.ReportAllocs()
			for b.Loop() {
				if kind == schedulerReport {
					event.report.DataPackets++
					transmission.packetID = event.report.DataPackets
					if err := store.push(transmission); err != nil {
						b.Fatal(err)
					}
					count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes)
					releaseBatchOwnership(ownership[:count])
					if err != nil || count != 1 {
						b.Fatalf("takeBatch() = %d, %v", count, err)
					}
					now = now.Add(time.Microsecond)
					event.receiveMicros = uint64(now.UnixMicro())
				}
				scheduler.applyEvent(lanes, &preferred, event, now)
				if kind == schedulerReport {
					if err := <-event.result; err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

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
	deadline := time.Now().Add(time.Hour)
	for index := range 16 {
		laneID := protocol.LaneID(byte(index + 1))
		store, err := NewTransmissionStore(packetqueue.Limits{Packets: 1024, Bytes: 16 * 1024 * 1024})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { releaseTransmissions(store.drain()) })
		for packetIndex := range index {
			transmission := schedulerTransmission(uint64(packetIndex+1), wgpacket.TransportData, deadline)
			payload := make([]byte, 1452)
			copy(payload, transmission.packet.Payload)
			transmission.packet.Payload = payload
			if err := store.push(transmission); err != nil {
				b.Fatal(err)
			}
		}
		if committed := index / 2; committed > 0 {
			var batch [16]protocol.Data
			var ownership [16]datagram.Packet
			count, err := store.takeBatch(batch[:committed], ownership[:], committed*1500)
			releaseBatchOwnership(ownership[:count])
			if err != nil || count != committed {
				b.Fatalf("takeBatch() = %d, %v, want %d committed packets", count, err, committed)
			}
		}
		lanes[laneID] = &scheduledLane{
			registration: LaneRegistration{
				LaneID: laneID, PathGroupID: protocol.PathGroupID(byte(index/2 + 1)), Store: store,
			},
			rttMicros: uint64(10_000 + index*250), deliveryRate: 10_000_000,
		}
	}
	b.Run("Transport", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			selected := selectCandidates(lanes, protocol.LaneID(2), 1500, math.MaxUint64)
			benchmarkCandidateSink = selected.lanes[0].score(1500)
		}
	})
	b.Run("Control", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			selected := selectControlCandidates(lanes, 1500, math.MaxUint64)
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
	var ownership [1]datagram.Packet
	var packets uint64
	b.ReportAllocs()
	b.SetBytes(int64(len(transmission.packet.Payload)))
	for b.Loop() {
		packets++
		transmission.packetID = packets
		if err := store.push(transmission); err != nil {
			b.Fatal(err)
		}
		count, err := store.takeBatch(batch[:], ownership[:], targetDataBatchBytes)
		if err != nil || count != 1 {
			b.Fatalf("takeBatch() = %d, %v", count, err)
		}
		releaseBatchOwnership(ownership[:count])
		if _, _, err := store.acknowledge(packets, uint64(store.now().UnixMicro())); err != nil {
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
		transmission.packetID = packetID
		if err := store.push(transmission); err != nil {
			b.Fatal(err)
		}
	}
	var batch [maximumDataBatchFrames]protocol.Data
	var ownership [maximumDataBatchFrames]datagram.Packet
	b.ReportAllocs()
	b.SetBytes(int64(len(transmission.packet.Payload) * len(batch)))
	for b.Loop() {
		count, err := store.takeBatch(batch[:], ownership[:], targetDataBatchBytes)
		if err != nil || count != len(batch) {
			b.Fatalf("takeBatch() = %d, %v", count, err)
		}
		releaseBatchOwnership(ownership[:count])
		if _, _, err := store.acknowledge(store.sentPackets, uint64(store.now().UnixMicro())); err != nil {
			b.Fatal(err)
		}
		for range count {
			packetID++
			transmission.packetID = packetID
			if err := store.push(transmission); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTransmissionDequeFeedbackWindow(b *testing.B) {
	var deque transmissionDeque
	for range 128 {
		deque.push(retainedTransmission{})
	}
	b.ReportAllocs()
	for b.Loop() {
		for range reportPacketThreshold {
			deque.push(retainedTransmission{})
		}
		deque.discardPrefix(reportPacketThreshold)
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

type benchmarkStreamSink struct{}

func (benchmarkStreamSink) Read([]byte) (int, error)         { panic("unexpected read") }
func (benchmarkStreamSink) Write(value []byte) (int, error)  { return len(value), nil }
func (benchmarkStreamSink) Close() error                     { return nil }
func (benchmarkStreamSink) LocalAddr() net.Addr              { return nil }
func (benchmarkStreamSink) RemoteAddr() net.Addr             { return nil }
func (benchmarkStreamSink) SetDeadline(time.Time) error      { return nil }
func (benchmarkStreamSink) SetReadDeadline(time.Time) error  { return nil }
func (benchmarkStreamSink) SetWriteDeadline(time.Time) error { return nil }

func BenchmarkLaneWriteControlBatch(b *testing.B) {
	for _, tt := range []struct {
		name  string
		count int
	}{{name: "Single", count: 1}, {name: "Eight", count: 8}} {
		b.Run(tt.name, func(b *testing.B) {
			lane := &Lane{carrier: carrier.NewStreamConn(benchmarkStreamSink{}), clock: &testClock{now: 1000},
				control: make(chan controlWrite, maximumConsecutiveControlFrames), writeTimeout: time.Second}
			frame := protocol.Frame{Type: protocol.FrameDeliveryReport, Payload: make([]byte, 1200)}
			request := controlWrite{build: func(uint64) (protocol.Frame, error) { return frame, nil }}
			ctx := context.Background()
			if _, err := lane.writeControlBatch(ctx, request, maximumConsecutiveControlFrames); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				for range tt.count - 1 {
					lane.control <- request
				}
				count, err := lane.writeControlBatch(ctx, request, maximumConsecutiveControlFrames)
				if err != nil || count != tt.count {
					b.Fatalf("count %d, error %v", count, err)
				}
			}
		})
	}
}
