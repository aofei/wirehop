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

func BenchmarkSchedulerRouteReport(b *testing.B) {
	for _, tt := range []struct {
		name       string
		lanes      int
		rejectBest bool
		rejectAll  bool
	}{
		{name: "Unroutable"},
		{name: "Single", lanes: 1},
		{name: "Duplicate", lanes: 2},
		{name: "SixteenLanes", lanes: 16},
		{name: "SixtyFourLanes", lanes: 64},
		{name: "BestAlternateFull", lanes: 16, rejectBest: true},
		{name: "AllQueuesFull", lanes: 16, rejectAll: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			var callbacks [2]func()
			callbackCount := 0
			lanes := make(map[protocol.LaneID]*scheduledLane, tt.lanes)
			for index := range tt.lanes {
				store, err := NewTransmissionStore(packetqueue.Limits{Packets: 256, Bytes: 2 * 1024 * 1024})
				if err != nil {
					b.Fatal(err)
				}
				laneID := protocol.LaneID(index + 1)
				lanes[laneID] = &scheduledLane{registration: LaneRegistration{
					LaneID: laneID, Generation: 1, Store: store,
					SendDeliveryReport: func(_ protocol.DeliveryReport, _ time.Time, onSent func()) bool {
						if tt.rejectAll || tt.rejectBest && int(laneID) == tt.lanes {
							return false
						}
						callbacks[callbackCount] = onSent
						callbackCount++
						return true
					},
				}, rttMicros: uint64(tt.lanes-index) * 2000, deliveryRate: defaultInitialRateBytesPerSecond}
			}
			scheduler := new(Scheduler)
			report := protocol.DeliveryReport{LaneID: 1, Generation: 1}
			parsedAt := time.Now()
			completed := 0
			complete := func(sent bool) {
				if sent != (tt.lanes > 0 && !tt.rejectAll) {
					b.Fatalf("completion sent = %t, lanes = %d", sent, tt.lanes)
				}
				completed++
			}
			b.ReportAllocs()
			for b.Loop() {
				callbackCount = 0
				completed = 0
				scheduler.routeReport(lanes, report, parsedAt, complete)
				for index := range callbackCount {
					callbacks[index]()
					callbacks[index] = nil
				}
				if completed != 1 {
					b.Fatalf("completion calls = %d, want 1", completed)
				}
			}
		})
	}
}

func BenchmarkSchedulerFillTransportProbe(b *testing.B) {
	for _, tt := range []struct {
		name   string
		lanes  int
		groups int
		fresh  bool
	}{
		{name: "TwoDistinct", lanes: 2, groups: 2},
		{name: "SixteenMixed", lanes: 16, groups: 2},
		{name: "SixteenDistinct", lanes: 16, groups: 16},
		{name: "FreshTwoDistinct", lanes: 2, groups: 2, fresh: true},
		{name: "FreshSixteenMixed", lanes: 16, groups: 2, fresh: true},
		{name: "FreshSixteenDistinct", lanes: 16, groups: 16, fresh: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			now := time.Unix(100, 0)
			lanes := make(map[protocol.LaneID]*scheduledLane, tt.lanes)
			for index := range tt.lanes {
				store, err := newTransmissionStore(packetqueue.Limits{Packets: 64, Bytes: 128 * 1024},
					func() time.Time { return now })
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { releaseTransmissions(store.drain()) })
				if !tt.fresh {
					if err := store.pushAt(retainedTransmission{
						deadlineMicros: uint64(now.Add(time.Hour).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]},
					}, now); err != nil {
						b.Fatal(err)
					}
				}
				laneID := protocol.LaneID(index + 1)
				lanes[laneID] = &scheduledLane{
					registration: LaneRegistration{
						LaneID: laneID, Generation: 1, PathGroupID: protocol.PathGroupID(index%tt.groups + 1), Store: store,
					},
					rttMicros: uint64(10_000 + index*1000), deliveryRate: defaultInitialRateBytesPerSecond,
				}
			}
			var scheduler Scheduler
			var data [1]protocol.Data
			var ownership [1]datagram.Packet
			b.ReportAllocs()
			for b.Loop() {
				admitted := scheduler.fillTransportProbe(lanes, 1, now, 0)
				if admitted != tt.fresh {
					b.Fatalf("probe admitted = %t, want %t", admitted, tt.fresh)
				}
				if !tt.fresh {
					continue
				}
				lane := lanes[scheduler.lastProbeLane]
				store := lane.registration.Store
				count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes)
				releaseBatchOwnership(ownership[:count])
				if err != nil || count != 1 {
					b.Fatalf("takeBatch() = %d, %v", count, err)
				}
				now = now.Add(time.Microsecond)
				if _, stale, err := store.acknowledge(store.sentPackets, uint64(now.UnixMicro())); err != nil || stale {
					b.Fatalf("acknowledge() = stale %t, %v", stale, err)
				}
				lane.probeUntil = time.Time{}
				lane.nextProbeAt = time.Time{}
				lane.probeBytes = 0
			}
		})
	}
}

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

func BenchmarkSchedulerSelectTransportCandidates(b *testing.B) {
	for _, tt := range []struct {
		name          string
		lanes         int
		slower        bool
		faster        bool
		queuedPrimary bool
		fullPrimary   bool
		maximumScore  uint64
		wantCandidate protocol.LaneID
	}{
		{name: "Single", lanes: 1, wantCandidate: 1},
		{name: "EqualPair", lanes: 2, wantCandidate: 1},
		{name: "SlowerPair", lanes: 2, slower: true, wantCandidate: 1},
		{name: "EqualSixteen", lanes: 16, wantCandidate: 1},
		{name: "SlowerSixteen", lanes: 16, slower: true, wantCandidate: 1},
		{name: "FasterHighDelayAlternative", lanes: 16, faster: true, wantCandidate: 1},
		{name: "FasterAlternativeUnderLoad", lanes: 16, faster: true, queuedPrimary: true, wantCandidate: 16},
		{name: "DeadlineRejectedAlternative", lanes: 16, faster: true, maximumScore: 1000, wantCandidate: 1},
		{name: "FullPrimary", lanes: 16, fullPrimary: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			now := time.UnixMicro(1 << 40)
			lanes := benchmarkTransportLanes(b, tt.lanes, now)
			for laneID, lane := range lanes {
				if tt.slower && laneID != 1 {
					lane.deliveryRate /= 2
				}
			}
			if tt.faster {
				lanes[protocol.LaneID(tt.lanes)].deliveryRate *= 2
			}
			if tt.queuedPrimary {
				store := lanes[1].registration.Store
				for index := range 20 {
					transmission := schedulerTransmission(uint64(index+1), wgpacket.TransportData, now.Add(time.Second))
					transmission.packet.Payload = make([]byte, 4096)
					copy(transmission.packet.Payload, relayWireGuardPacket(wgpacket.TransportData))
					if err := store.pushAt(transmission, now); err != nil {
						b.Fatal(err)
					}
				}
			}
			if tt.fullPrimary {
				store := lanes[1].registration.Store
				store.limits.Packets = 1
				if err := store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
					b.Fatal(err)
				}
			}
			maximumScore := tt.maximumScore
			if maximumScore == 0 {
				maximumScore = math.MaxUint64
			}
			var scheduler Scheduler
			b.ReportAllocs()
			for b.Loop() {
				selected := scheduler.selectTransportCandidates(lanes, 1, 1500, maximumScore, now)
				if tt.wantCandidate == 0 {
					if selected.count != 0 || selected.available {
						b.Fatalf("selection = %+v, want no available candidate", selected)
					}
					continue
				}
				if selected.count != 1 || selected.lanes[0].registration.LaneID != tt.wantCandidate {
					b.Fatalf("selection = %+v, want lane %d", selected, tt.wantCandidate)
				}
				benchmarkCandidateSink = uint64(selected.lanes[0].registration.LaneID)
			}
		})
	}
}

func BenchmarkSchedulerScheduleCycle(b *testing.B) {
	for _, tt := range []struct {
		name    string
		lanes   int
		payload int
	}{
		{name: "SingleSmall", lanes: 1, payload: 32},
		{name: "SingleMTU", lanes: 1, payload: 1452},
		{name: "SixteenSmall", lanes: 16, payload: 32},
		{name: "SixteenMTU", lanes: 16, payload: 1452},
	} {
		b.Run(tt.name, func(b *testing.B) {
			now := time.UnixMicro(1 << 40)
			lanes := benchmarkTransportLanes(b, tt.lanes, now)
			store := lanes[1].registration.Store
			payload := make([]byte, tt.payload)
			copy(payload, relayWireGuardPacket(wgpacket.TransportData))
			deadline := now.Add(time.Second)
			preferred := protocol.LaneID(1)
			scheduler := Scheduler{packetID: 1 << 28}
			var batch [1]protocol.Data
			var ownership [1]datagram.Packet
			var item packetqueue.Item[Packet]
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				item = packetqueue.Item[Packet]{
					Value: Packet{Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}},
					Size:  len(payload), Deadline: deadline,
				}
				if scheduled, err := scheduler.schedule(lanes, &preferred, &item, now); err != nil || !scheduled {
					b.Fatalf("schedule() = %t, %v", scheduled, err)
				}
				if preferred != 1 {
					b.Fatalf("preferred lane = %d, want 1", preferred)
				}
				count, err := store.takeBatch(batch[:], ownership[:], targetDataBatchBytes)
				if err != nil || count != 1 || batch[0].PacketID != scheduler.packetID {
					b.Fatalf("takeBatch() = %d, %v, packet ID %d, want %d", count, err, batch[0].PacketID, scheduler.packetID)
				}
				releaseBatchOwnership(ownership[:count])
				if _, stale, err := store.acknowledge(store.sentPackets, uint64(now.UnixMicro())); err != nil || stale {
					b.Fatalf("acknowledge() = stale %t, %v", stale, err)
				}
			}
		})
	}
}

func benchmarkTransportLanes(b *testing.B, count int, now time.Time) map[protocol.LaneID]*scheduledLane {
	b.Helper()
	lanes := make(map[protocol.LaneID]*scheduledLane, count)
	for index := range count {
		store, err := newTransmissionStore(packetqueue.Limits{Packets: 256, Bytes: 2 * 1024 * 1024},
			func() time.Time { return now })
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { releaseTransmissions(store.drain()) })
		laneID := protocol.LaneID(index + 1)
		lanes[laneID] = &scheduledLane{
			registration: LaneRegistration{LaneID: laneID, Generation: 1, PathGroupID: protocol.PathGroupID(index + 1), Store: store},
			rttMicros:    uint64(10_000 + index*1000), deliveryRate: 10_000_000, rateObserved: true,
		}
	}
	return lanes
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
	now := time.UnixMicro(1000)
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
	now := time.UnixMicro(1000)
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

func BenchmarkSchedulerObserveDeliveryReport(b *testing.B) {
	scheduler := &Scheduler{events: make(chan schedulerEvent, schedulerEventCapacity)}
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for event := range scheduler.events {
			event.result <- nil
		}
	}()
	defer func() {
		close(scheduler.events)
		<-workerDone
	}()
	result := make(chan error, 1)
	source := protocol.LaneGeneration{LaneID: 1, Generation: 1}
	report := protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 1}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := scheduler.ObserveDeliveryReport(ctx, source, report, 123, result); err != nil {
			b.Fatal(err)
		}
	}
}
