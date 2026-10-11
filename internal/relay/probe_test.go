package relay

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerPrioritizesRealTrafficOverProbeBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		payload := make([]byte, 5008)
		copy(payload, relayWireGuardPacket(wgpacket.TransportData))
		budget, err := retention.NewBudget(retention.Limits{Packets: 4, Bytes: len(payload) + 4101})
		if err != nil {
			t.Fatal(err)
		}
		ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: 1, Bytes: len(payload)}, budget, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		defer ingress.Close()
		if err := ingress.Push(packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: wgpacket.TransportData,
			Payload: payload}), Size: len(payload), Deadline: time.Now().Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		scheduler.lastTransportAt = time.Now()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- scheduler.Run(ctx) }()
		var target *TransmissionStore
		for id := byte(1); id <= 2; id++ {
			store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 4, Bytes: int(id) * 4101}, budget, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			if err := scheduler.Register(ctx, schedulerRegistration(id, id, store)); err != nil {
				t.Fatal(err)
			}
			target = store
		}
		synctest.Wait()
		if packets, _ := target.backlog(); packets != 1 {
			t.Fatalf("real packet admission retained %d transmissions", packets)
		}
		if data := takeOneTransmission(t, target); data.PacketID != 1 {
			t.Fatal("capacity padding consumed aggregate framing headroom before pending real traffic")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Fatalf("scheduler shutdown retained aggregate capacity: %+v", got)
		}
	})
}

func TestSchedulerRunRetriesSharedBudgetAfterRelease(t *testing.T) {
	for _, tt := range []struct {
		name      string
		laneBytes int
		release   string
	}{
		{name: "SmallLaneAcknowledgment", laneBytes: 2048, release: "Acknowledgment"},
		{name: "SmallLaneExpiry", laneBytes: 2048, release: "Expiry"},
		{name: "SmallLaneDrain", laneBytes: 2048, release: "Drain"},
		{name: "BelowProbeAcknowledgment", laneBytes: 4100, release: "Acknowledgment"},
		{name: "BelowProbeExpiry", laneBytes: 4100, release: "Expiry"},
		{name: "BelowProbeDrain", laneBytes: 4100, release: "Drain"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				payload := make([]byte, 1452)
				copy(payload, relayWireGuardPacket(wgpacket.TransportData))
				probeBytes, err := protocol.DataFrameSize(protocol.Data{Payload: probePadding[:]})
				if err != nil {
					t.Fatal(err)
				}
				budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: probeBytes + len(payload)})
				if err != nil {
					t.Fatal(err)
				}
				external, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: probeBytes}, budget, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { releaseTransmissions(external.drain()) }()
				if err := external.push(retainedTransmission{deadlineMicros: uint64(time.Now().Add(time.Millisecond).UnixMicro()),
					packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
					t.Fatal(err)
				}
				if tt.release == "Acknowledgment" {
					takeOneTransmission(t, external)
				}
				ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: 1, Bytes: len(payload)}, budget, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				deadline := time.Now().Add(time.Second)
				if err := ingress.Push(packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{
					Kind: wgpacket.TransportData, Payload: payload,
				}), Size: len(payload), Deadline: deadline}); err != nil {
					t.Fatal(err)
				}
				scheduler, err := NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- scheduler.Run(ctx) }()
				defer func() {
					cancel()
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Errorf("scheduler exit = %v", err)
					}
				}()
				var stores [2]*TransmissionStore
				for index := range stores {
					store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: tt.laneBytes}, budget, time.Now)
					if err != nil {
						t.Fatal(err)
					}
					stores[index] = store
					if err := scheduler.Register(ctx, schedulerRegistration(byte(index+1), byte(index+1), store)); err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				for _, store := range stores {
					if packets, _ := store.backlog(); packets != 0 {
						t.Fatal("full shared budget admitted a transmission")
					}
				}
				if got := budget.Usage(); got != (retention.Usage{Packets: 2, Bytes: probeBytes + len(payload)}) {
					t.Fatalf("blocked packet lost its aggregate ownership: %+v", got)
				}
				time.Sleep(time.Millisecond)
				switch tt.release {
				case "Acknowledgment":
					if _, stale, err := external.acknowledge(1, uint64(time.Now().UnixMicro())); err != nil || stale {
						t.Fatalf("external acknowledgment = stale %t, %v", stale, err)
					}
				case "Expiry":
					external.expire(time.Now())
				case "Drain":
					releaseTransmissions(external.drain())
				}
				if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: len(payload)}) {
					t.Fatalf("external release retained capacity: %+v", got)
				}
				time.Sleep(abandonmentCheckInterval)
				synctest.Wait()
				data := takeOneTransmission(t, stores[0])
				if data.PacketID != 1 || data.DeadlineMicros != uint64(deadline.UnixMicro()) || !slices.Equal(data.Payload, payload) {
					t.Fatal("shared capacity release did not resume the pending real packet")
				}
				if packets, _ := stores[1].backlog(); packets != 0 {
					t.Fatal("small lane admitted padding or duplicated the real packet")
				}
				realBytes, err := protocol.DataFrameSize(data)
				if err != nil {
					t.Fatal(err)
				}
				if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: realBytes}) {
					t.Fatalf("resumed packet retention = %+v", got)
				}
				if err := scheduler.ObserveDeliveryReport(ctx, protocol.LaneGeneration{LaneID: 2, Generation: 1},
					protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 1}, uint64(time.Now().UnixMicro()), make(chan error, 1)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if got := budget.Usage(); got != (retention.Usage{}) {
					t.Fatalf("cross-lane feedback retained capacity: %+v", got)
				}
			})
		})
	}
}

func TestLaneReadDataBatchCapacityProbe(t *testing.T) {
	for _, tt := range []struct {
		name, position string
		realSize       int
	}{
		{name: "First", position: "First", realSize: 32},
		{name: "Middle", position: "Middle", realSize: 32},
		{name: "Last", position: "Last", realSize: 32},
		{name: "Only", position: "Only", realSize: 32},
		{name: "RealPaddingSizeBefore", position: "First", realSize: protocol.ProbePayloadSize},
		{name: "RealPaddingSizeMiddle", position: "Middle", realSize: protocol.ProbePayloadSize},
		{name: "RealPaddingSizeAfter", position: "Last", realSize: protocol.ProbePayloadSize},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := newTestEndpoint()
			lane := newTestLane(t, newTestCarrier(), endpoint)
			padding := make([]byte, protocol.ProbePayloadSize)
			padding[0] = 4
			probe, err := protocol.MarshalData(protocol.Data{Payload: padding})
			if err != nil {
				t.Fatal(err)
			}
			var frames []protocol.Frame
			for id := uint64(1); id <= 2; id++ {
				payload := make([]byte, tt.realSize)
				copy(payload, relayWireGuardPacket(wgpacket.TransportData))
				frame, err := protocol.MarshalData(protocol.Data{PacketID: id, DeadlineMicros: 1_000_000,
					Payload: payload})
				if err != nil {
					t.Fatal(err)
				}
				frames = append(frames, frame)
			}
			switch tt.position {
			case "First":
				frames = append([]protocol.Frame{probe}, frames...)
			case "Middle":
				frames = []protocol.Frame{frames[0], probe, frames[1]}
			case "Last":
				frames = append(frames, probe)
			case "Only":
				frames = []protocol.Frame{probe}
			}
			if err := lane.readDataBatch(t.Context(), frames); err != nil {
				t.Fatal(err)
			}
			want := 2
			if tt.position == "Only" {
				want = 0
			}
			if len(endpoint.writes) != want || lane.progress.dataPackets != uint64(len(frames)) {
				t.Fatalf("probe delivery = %d UDP writes, %d parsed frames", len(endpoint.writes), lane.progress.dataPackets)
			}
			for range want {
				if payload := <-endpoint.writes; len(payload) != tt.realSize {
					t.Fatalf("real UDP payload size = %d, want %d", len(payload), tt.realSize)
				}
			}
			var bytes uint64
			for _, frame := range frames {
				bytes += uint64(protocol.FrameSize(len(frame.Payload)))
			}
			if lane.progress.dataBytes != bytes {
				t.Fatalf("parsed bytes = %d, want %d", lane.progress.dataBytes, bytes)
			}
			probe.Payload = probe.Payload[:len(probe.Payload)-1]
			if err := lane.readDataBatch(t.Context(), []protocol.Frame{probe}); !errors.Is(err, protocol.ErrInvalidDataFrame) {
				t.Fatalf("malformed probe = %v", err)
			}
			if lane.progress.dataPackets != uint64(len(frames)) {
				t.Fatal("malformed probe advanced parsing feedback")
			}
		})
	}
	t.Run("BlockedUDP", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			endpoint := &blockingWriteEndpoint{testEndpoint: newTestEndpoint(), entered: make(chan byte, 1), release: make(chan struct{})}
			first := newTestLane(t, newTestCarrier(), endpoint.testEndpoint)
			first.receiver.endpoint = endpoint
			second := newTestLane(t, newTestCarrier(), endpoint.testEndpoint)
			second.receiver = first.receiver
			frame, err := protocol.MarshalData(protocol.Data{PacketID: 1, DeadlineMicros: 1_000_000,
				Payload: relayWireGuardPacket(wgpacket.TransportData)})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- first.readDataBatch(ctx, []protocol.Frame{frame}) }()
			<-endpoint.entered
			probe, err := protocol.MarshalData(protocol.Data{Payload: probePadding[:]})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			if err := second.readDataBatch(t.Context(), []protocol.Frame{probe}); err != nil {
				t.Fatal(err)
			}
			if time.Now() != started || second.progress.dataPackets != 1 || first.progress.dataPackets != 1 {
				t.Fatal("padding acknowledgement waited for another lane's UDP write")
			}
			close(endpoint.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	})
}

func FuzzLaneReadDataBatch(f *testing.F) {
	for _, input := range [][]byte{
		{0, 0},
		{1, 0, 0, 0, 1, 0, 1, 1},
		{2, 0, 1, 0, 0, 0, 1, 1},
		{0, 0, 1, 0, 3, 1},
		{4, 0, 0, 0, 1, 1},
		{0, 0, 35, 0},
		{1, 0, 0, 0, 1, 1},
		make([]byte, 2*datagram.MaximumBatchSize),
	} {
		for _, vector := range []bool{false, true} {
			for overflow := uint8(0); overflow < 3; overflow++ {
				f.Add(input, vector, overflow)
			}
		}
	}
	f.Fuzz(func(t *testing.T, input []byte, vector bool, overflow uint8) {
		input = input[:min(len(input), 2*datagram.MaximumBatchSize)]
		if len(input) < 2 {
			return
		}
		endpoint := newTestEndpoint()
		endpoint.writes = make(chan []byte, datagram.MaximumBatchSize)
		lane := newTestLane(t, newTestCarrier(), endpoint)
		if vector {
			lane.receiver.endpoint = &recordingBatchEndpoint{testEndpoint: endpoint}
		}
		var frames []protocol.Frame
		var expected []byte
		seen := make(map[uint64]bool)
		var highest, encodedBytes uint64
		var parseError error
		invalidDeadline := false
		for offset := 0; offset+1 < len(input); offset += 2 {
			selector := input[offset]
			packet := protocol.Data{
				PacketID: uint64(input[offset+1]%16) + 1, DeadlineMicros: 1_000_000,
				Payload: relayWireGuardPacket(wgpacket.TransportData),
			}
			packet.Payload[4] = byte(offset + 1)
			switch selector % 5 {
			case 0:
				padding := make([]byte, protocol.ProbePayloadSize)
				copy(padding, packet.Payload)
				packet = protocol.Data{Payload: padding}
			case 2:
				packet.DeadlineMicros = 1000
			case 3:
				packet.Payload[0] = 0
				if parseError == nil {
					parseError = ErrInvalidWireGuardPacket
				}
			case 4:
				packet.DeadlineMicros = protocol.MaxPacketLifetimeMicros + 2_000_000
				invalidDeadline = true
			}
			frame, err := protocol.MarshalData(packet)
			if err != nil {
				t.Fatal(err)
			}
			if packet.PacketID == 0 && selector&32 != 0 {
				frame.Payload = frame.Payload[:len(frame.Payload)-1]
				if parseError == nil {
					parseError = protocol.ErrInvalidDataFrame
				}
			}
			frames = append(frames, frame)
			encodedBytes += uint64(protocol.FrameSize(len(frame.Payload)))
			if packet.PacketID > 0 && selector%5 == 1 && !seen[packet.PacketID] {
				seen[packet.PacketID] = true
				expected = append(expected, packet.Payload[4])
				highest = max(highest, packet.PacketID)
			}
		}
		wantError := parseError
		if wantError == nil && invalidDeadline {
			wantError = ErrInvalidPacketDeadline
		}
		switch overflow % 3 {
		case 1:
			lane.progress.dataPackets = math.MaxUint64 - uint64(len(frames)) + 1
		case 2:
			lane.progress.dataBytes = math.MaxUint64 - encodedBytes + 1
		}
		if wantError == nil && overflow%3 != 0 {
			wantError = ErrCounterExhausted
		}
		beforePackets, beforeBytes := lane.progress.dataPackets, lane.progress.dataBytes
		if err := lane.readDataBatch(t.Context(), frames); !errors.Is(err, wantError) {
			t.Fatalf("mixed batch error = %v, want %v", err, wantError)
		}
		if wantError != nil {
			if lane.progress.dataPackets != beforePackets || lane.progress.dataBytes != beforeBytes ||
				lane.progress.revision != 0 || len(lane.progress.notify) != 0 || len(endpoint.writes) != 0 ||
				lane.receiver.deduplication.Highest() != 0 {
				t.Fatal("rejected mixed batch changed progress, UDP delivery, or deduplication")
			}
			return
		}
		var delivered []byte
		for len(endpoint.writes) > 0 {
			delivered = append(delivered, (<-endpoint.writes)[4])
		}
		if !slices.Equal(delivered, expected) || lane.receiver.deduplication.Highest() != highest {
			t.Fatalf("mixed batch delivery = %v, want %v", delivered, expected)
		}
		if err := lane.readDataBatch(t.Context(), frames); err != nil {
			t.Fatal(err)
		}
		if len(endpoint.writes) != 0 || lane.progress.dataPackets != 2*uint64(len(frames)) ||
			lane.progress.dataBytes != 2*encodedBytes {
			t.Fatal("repeated mixed batch changed exactly-once delivery or cumulative frame accounting")
		}
	})
}

func TestSchedulerFillTransportProbe(t *testing.T) {
	now := time.Unix(100, 0)
	ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	first := schedulerLaneWithLimits(t, 1, 1, 10_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
	second := schedulerLaneWithLimits(t, 2, 2, 300_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
	for _, lane := range []*scheduledLane{first, second} {
		lane.rateObserved = false
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	if !scheduler.fillTransportProbe(lanes, 0, now, 0) || !scheduler.fillTransportProbe(lanes, 0, now, 0) {
		t.Fatal("admitted paths were not both sampled before transport traffic")
	}
	if scheduler.fillTransportProbe(lanes, 0, now, 0) {
		t.Fatal("unknown capacity admitted a second unreported probe")
	}
	for _, lane := range []*scheduledLane{first, second} {
		packets, bytes := lane.registration.Store.backlog()
		if packets != 1 || bytes != 4101 || bytes > initialDeliveryWindow {
			t.Fatalf("startup probe backlog = %d packets, %d bytes", packets, bytes)
		}
		var data [16]protocol.Data
		var ownership [16]datagram.Packet
		count, err := lane.registration.Store.takeBatch(data[:], ownership[:], targetDataBatchBytes)
		if err != nil || count != packets {
			t.Fatalf("take probes = %d, %v", count, err)
		}
		for _, packet := range data[:count] {
			if packet.PacketID != 0 || packet.DeadlineMicros != 0 || len(packet.Payload) != protocol.ProbePayloadSize {
				t.Fatal("probe consumed a real packet ID or deadline")
			}
		}
		releaseBatchOwnership(ownership[:count])
		if _, stale, err := lane.registration.Store.acknowledge(uint64(count), uint64(now.UnixMicro())+300_000); err != nil || stale {
			t.Fatalf("probe feedback = stale %t, %v", stale, err)
		}
	}
	if scheduler.packetID != 0 {
		t.Fatal("discovery exhausted the real packet ID sequence")
	}
	now = now.Add(maximumTransportProbeDuration + transportProbeInterval)
	if scheduler.fillTransportProbe(lanes, 0, now, 0) {
		t.Fatal("idle session restarted capacity training")
	}
	for _, lane := range lanes {
		lane.nextProbeAt = time.Time{}
		lane.probeUntil = time.Time{}
		lane.registration.PathGroupID = 1
	}
	if scheduler.fillTransportProbe(lanes, 0, now, 0) {
		t.Fatal("same-group lanes consumed redundant capacity training")
	}
}

func TestScheduledLaneProbeWindow(t *testing.T) {
	lane := schedulerLaneWithLimits(t, 1, 1, 200_000, 1000,
		packetqueue.Limits{Packets: 64, Bytes: 256 * 1024})
	lane.rateObserved = false
	if got := lane.probeWindowBytes(0); got != initialDeliveryWindow {
		t.Fatalf("unmeasured probe window = %d, want the startup byte bound", got)
	}
	lane.rateObserved = true
	if got := lane.probeWindowBytes(0); got != lane.deliveryWindowBytes() {
		t.Fatalf("low-rate probe window = %d, want useful-traffic headroom", got)
	}
	lane.deliveryRate = 100_000
	if got, want := lane.probeWindowBytes(0), uint64(184_096); got != want {
		t.Fatalf("measured probe window = %d, want %d", got, want)
	}
	if got, want := lane.probeWindowBytes(1500), lane.deliveryWindowBytes()-1500; got != want {
		t.Fatalf("reserved probe window = %d, want %d", got, want)
	}
	lane.registration.Store.limits.Bytes = 128 * 1024
	if got := lane.probeWindowBytes(0); got != 128*1024 {
		t.Fatalf("hard-limited probe window = %d", got)
	}
	lane.rttMicros = 975_000
	for _, tt := range []struct{ rate, want uint64 }{
		{rate: 2050, want: 12_296},
		{rate: 2051, want: 20_504},
	} {
		lane.deliveryRate = tt.rate
		if got := lane.probeWindowBytes(0); got != tt.want {
			t.Fatalf("half-frame boundary at %d bytes/s = %d, want %d", tt.rate, got, tt.want)
		}
	}
}

func TestSchedulerSlowProbeStaysSingleFrameAndRefreshes(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLane(t, 1, 1, 200_000, 1000)
	second := schedulerLane(t, 2, 2, 200_000, 1000)
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	for _, lane := range lanes {
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	scheduler := Scheduler{lastTransportAt: now}
	if !scheduler.fillTransportProbe(lanes, 1, now, 0) {
		t.Fatal("slow alternative did not receive its initial refresh sample")
	}
	until, next := second.probeUntil, second.nextProbeAt
	takeOneTransmission(t, second.registration.Store)
	if _, _, err := second.registration.Store.acknowledge(1, uint64(now.UnixMicro())+5_000_000); err != nil {
		t.Fatal(err)
	}
	if !scheduler.fillTransportProbe(lanes, 1, now, 0) || second.probeBytes != 2*4101 ||
		second.probeUntil != until || second.nextProbeAt != next {
		t.Fatal("single-frame discovery stopped early or changed its original bounds")
	}
	if scheduler.fillTransportProbe(lanes, 1, now, 0) {
		t.Fatal("slow-path discovery admitted more than one unreported frame")
	}
	takeOneTransmission(t, second.registration.Store)
	if _, _, err := second.registration.Store.acknowledge(2, uint64(now.UnixMicro())+5_000_000); err != nil {
		t.Fatal(err)
	}
	now = next
	scheduler.lastTransportAt = now
	if !scheduler.fillTransportProbe(lanes, 1, now, 0) || second.probeBytes != 4101 {
		t.Fatal("slow alternative could not refresh capacity after its cooldown")
	}
}

func TestSchedulerSelectTransportCandidatesCapacity(t *testing.T) {
	first := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	second := schedulerLane(t, 2, 2, 300_000, 10_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
	t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	got := scheduler.selectTransportCandidates(lanes, 1, 1500, math.MaxUint64, time.Now())
	if got.count != 1 || got.lanes[0] != second {
		t.Fatalf("capacity selection = %+v", got)
	}
	item := packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: wgpacket.TransportData,
		Payload: append(relayWireGuardPacket(wgpacket.TransportData), make([]byte, 16)...)}), Deadline: time.Now().Add(time.Second), Size: 32}
	preferred := protocol.LaneID(1)
	if ok, err := scheduler.schedule(lanes, &preferred, &item, ingress.Now()); err != nil || !ok || preferred != 2 {
		t.Fatalf("capacity promotion = %t, %v, lane %d", ok, err, preferred)
	}
	if first.registration.Store.backlogBytes.Load() != 0 {
		t.Fatal("transport promotion duplicated the application packet")
	}
}

func TestScheduledLanePingKeepsStartupWriteBudget(t *testing.T) {
	lane := schedulerLane(t, 1, 1, 10_000, defaultInitialRateBytesPerSecond)
	lane.rateObserved = false
	t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	if progressed, err := lane.applyReport(protocol.DeliveryReport{PingID: 1}, 100_000, time.Unix(1, 0)); err != nil || !progressed {
		t.Fatalf("ping feedback = %t, %v", progressed, err)
	}
	if lane.rateObserved || lane.registration.Store.writeBudget.Load() != 1024 {
		t.Fatal("ping-only feedback increased the unmeasured writer quantum")
	}
}

func TestTransmissionStoreProbeDeadlineAssessment(t *testing.T) {
	now := time.Unix(100, 0)
	store := schedulerStore(t, packetqueue.Limits{Packets: 4, Bytes: 32 * 1024})
	store.now = func() time.Time { return now }
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	if err := store.push(retainedTransmission{deadlineMicros: uint64(now.Add(time.Second).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	if assessment := store.assessDeadlines(now.Add(2*time.Second), func(uint64) uint64 { return math.MaxUint64 }); assessment.retained || assessment.atRisk || !assessment.usefulDeadline.IsZero() {
		t.Fatalf("expired padding justified recovery: %+v", assessment)
	}
	transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(3*time.Second))
	if err := store.push(transmission); err != nil {
		t.Fatal(err)
	}
	var prefix uint64
	assessment := store.assessDeadlines(now, func(bytes uint64) uint64 { prefix = bytes; return 0 })
	if !assessment.retained || assessment.atRisk || uint64(assessment.usefulDeadline.UnixMicro()) != transmission.deadlineMicros ||
		prefix != uint64(4101+transmission.size) {
		t.Fatalf("padding did not preserve useful carrier order: %+v, prefix %d", assessment, prefix)
	}
}

func TestTransmissionStoreAcknowledgeMixedProbePrefix(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 8, Bytes: 128 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 8, Bytes: 128 * 1024},
		budget, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	first := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
	first.packet.Payload = make([]byte, 2048)
	first.packet.Payload[0] = 4
	migrated := first
	migrated.packetID = 3
	migrated.migrated = true
	large := first
	large.packetID = 4
	large.packet.Payload = make([]byte, protocol.MaxPacketSize)
	large.packet.Payload[0] = 4
	for _, transmission := range []retainedTransmission{
		first,
		{deadlineMicros: uint64(now.Add(time.Second).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]}},
		migrated,
		schedulerTransmission(2, wgpacket.HandshakeResponse, now.Add(time.Second)),
		large,
	} {
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
	}
	var data [maximumDataBatchFrames]protocol.Data
	var ownership [maximumDataBatchFrames]datagram.Packet
	count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes)
	if err != nil || count != 5 {
		t.Fatalf("mixed writer batch = %d, %v", count, err)
	}
	defer releaseBatchOwnership(ownership[:count])
	var sizes [5]int
	for index, id := range []uint64{2, 1, 0, 3, 4} {
		if data[index].PacketID != id {
			t.Fatalf("carrier position %d has ID %d, want %d", index, data[index].PacketID, id)
		}
		encoded, err := protocol.MarshalDataFrame(data[index])
		if err != nil {
			t.Fatal(err)
		}
		sizes[index] = len(encoded)
	}
	now = now.Add(2 * time.Second)
	store.expire(now)
	for _, tt := range []struct {
		name             string
		packets, proof   uint64
		remaining        int
		stale, malformed bool
	}{
		{name: "ControlAndReal", packets: 2, proof: 2048, remaining: 3},
		{name: "BeyondSentPrefix", packets: 6, proof: 2048, remaining: 3, malformed: true},
		{name: "PaddingOnly", packets: 3, proof: 2048, remaining: 2},
		{name: "StalePrefix", packets: 2, proof: 2048, remaining: 2, stale: true},
		{name: "MigratedReal", packets: 4, proof: minimumRateSampleBytes, remaining: 1},
		{name: "OversizedRealSaturates", packets: 5, proof: minimumRateSampleBytes},
		{name: "RepeatedPrefix", packets: 5, proof: minimumRateSampleBytes},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, stale, err := store.acknowledge(tt.packets, uint64(now.UnixMicro()))
			if stale != tt.stale || tt.malformed && !errors.Is(err, ErrInvalidDeliveryReport) || !tt.malformed && err != nil {
				t.Fatalf("mixed prefix feedback = stale %t, %v", stale, err)
			}
			var bytes int
			for _, size := range sizes[len(sizes)-tt.remaining:] {
				bytes += size
			}
			packets, retained := store.backlog()
			if packets != tt.remaining || retained != uint64(bytes) ||
				budget.Usage() != (retention.Usage{Packets: tt.remaining, Bytes: bytes}) ||
				store.transportReported.Load() != tt.proof {
				t.Fatal("mixed prefix changed exact capacity accounting or real-transport proof")
			}
		})
	}
}

func TestSchedulerInitialPrimaryWaitsForLowDelayPath(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLaneWithLimits(t, 1, 1, 10_000, 43_000, packetqueue.Limits{Packets: 1, Bytes: 16 * 1024})
	second := schedulerLane(t, 2, 2, 300_000, defaultInitialRateBytesPerSecond)
	t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
	t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
	first.registration.Store.now = func() time.Time { return now }
	second.rateObserved = false
	if err := first.registration.Store.push(retainedTransmission{deadlineMicros: uint64(now.Add(time.Second).UnixMicro()),
		packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
		t.Fatal(err)
	}
	var scheduler Scheduler
	got := scheduler.selectTransportCandidates(map[protocol.LaneID]*scheduledLane{1: first, 2: second},
		0, 1500, math.MaxUint64, now)
	if got.count != 0 {
		t.Fatal("startup padding displaced the low-delay primary onto an unmeasured path")
	}
}

func TestScheduledLaneRateFilterStartupAndCapacityDrop(t *testing.T) {
	lane := &scheduledLane{deliveryRate: defaultInitialRateBytesPerSecond}
	lane.updateDeliveryRate(deliverySample{bytes: 5000, intervalMicros: 25_000}, 0)
	lane.updateDeliveryRate(deliverySample{bytes: 50_000, intervalMicros: 25_000}, 0)
	if lane.deliveryRate != 2_000_000 {
		t.Fatalf("startup growth delayed: %d", lane.deliveryRate)
	}
	for range len(lane.rateHistory) {
		lane.updateDeliveryRate(deliverySample{bytes: 5000, intervalMicros: 25_000}, 0)
	}
	if lane.deliveryRate >= 2_000_000 {
		t.Fatal("expired peak prevented a constrained capacity decrease")
	}
	before := *lane
	lane.updateDeliveryRate(deliverySample{bytes: 100_000, intervalMicros: 999}, 0)
	if lane.deliveryRate != before.deliveryRate || lane.rateHistory != before.rateHistory ||
		lane.rateHistoryCount != before.rateHistoryCount || lane.rateHistoryNext != before.rateHistoryNext {
		t.Fatal("compressed feedback changed the capacity filter")
	}
}

func TestSchedulerTransportSelectionBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name                                    string
		holding, failed, full, deadlineFallback bool
		want                                    protocol.LaneID
	}{
		{name: "MeasuredPromotion", want: 2},
		{name: "InitialHold", holding: true, want: 1},
		{name: "FailedPrimaryBypassesHold", holding: true, failed: true, want: 2},
		{name: "FullPrimaryWaits", holding: true, full: true},
		{name: "FullLatePrimaryPermitsFallback", holding: true, full: true, deadlineFallback: true, want: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			first := schedulerLaneWithLimits(t, 1, 1, 10_000, 100_000, packetqueue.Limits{Packets: 1, Bytes: 16 * 1024})
			second := schedulerLane(t, 2, 2, 20_000, 10_000_000)
			first.registration.Store.now = func() time.Time { return now }
			t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
			t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
			first.degraded = tt.failed
			if tt.full {
				if err := first.registration.Store.push(retainedTransmission{deadlineMicros: uint64(now.Add(time.Second).UnixMicro()),
					packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
					t.Fatal(err)
				}
			}
			var scheduler Scheduler
			if tt.holding {
				scheduler.transportHoldUntil = now.Add(time.Second)
			}
			maximumScore := uint64(math.MaxUint64)
			if tt.deadlineFallback {
				maximumScore = 20_000
			}
			got := scheduler.selectTransportCandidates(map[protocol.LaneID]*scheduledLane{1: first, 2: second},
				1, 1500, maximumScore, now)
			if tt.want == 0 {
				if got.count != 0 || got.available {
					t.Fatalf("full primary did not wait: %+v", got)
				}
			} else if got.count != 1 || got.lanes[0].registration.LaneID != tt.want {
				t.Fatalf("selection = %+v, want lane %d", got, tt.want)
			}
		})
	}
}

func TestSchedulerProbeBudgetAndCooldown(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	second := schedulerLaneWithLimits(t, 2, 2, 300_000, 1_000_000,
		packetqueue.Limits{Packets: 64, Bytes: 4 * 1024 * 1024})
	budget, err := retention.NewBudget(retention.Limits{Packets: 64, Bytes: 4 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	second.registration.Store.budget = budget
	second.registration.Store.now = func() time.Time { return now }
	t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
	t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	scheduler := Scheduler{lastTransportAt: now}
	var acknowledged uint64
	for scheduler.fillTransportProbe(lanes, 1, now, 0) {
		packets, bytes := second.registration.Store.backlog()
		if got := budget.Usage(); got != (retention.Usage{Packets: packets, Bytes: int(bytes)}) {
			t.Fatalf("probe accounting = %+v, backlog %d/%d", got, packets, bytes)
		}
		for range packets {
			takeOneTransmission(t, second.registration.Store)
		}
		acknowledged += uint64(packets)
		if _, _, err := second.registration.Store.acknowledge(acknowledged, uint64(now.UnixMicro())+1); err != nil {
			t.Fatal(err)
		}
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Fatalf("probe feedback retained capacity: %+v", got)
		}
	}
	if second.probeBytes > maximumTransportProbeBytes || maximumTransportProbeBytes-second.probeBytes >= 4101 {
		t.Fatalf("probe byte cap = %d", second.probeBytes)
	}
	now = second.nextProbeAt.Add(-time.Microsecond)
	scheduler.lastTransportAt = now
	if scheduler.fillTransportProbe(lanes, 1, now, 0) {
		t.Fatal("discovery restarted before its cooldown")
	}
	now = second.nextProbeAt
	scheduler.lastTransportAt = now
	if !scheduler.fillTransportProbe(lanes, 1, now, 0) || second.probeBytes >= maximumTransportProbeBytes {
		t.Fatal("active traffic did not restart a fresh bounded period")
	}
	releaseTransmissions(second.registration.Store.drain())
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("probe cancellation retained capacity: %+v", got)
	}
}

func TestSchedulerDiscoveryPreservesQueuedControl(t *testing.T) {
	now := time.Now()
	first := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	second := schedulerLane(t, 2, 2, 300_000, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	for _, lane := range lanes {
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	payload := relayWireGuardPacket(wgpacket.HandshakeInitiation)
	item := packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: wgpacket.HandshakeInitiation,
		Payload: payload}), Deadline: now.Add(time.Second), Size: len(payload)}
	var preferred protocol.LaneID
	if ok, err := scheduler.schedule(lanes, &preferred, &item, now); err != nil || !ok {
		t.Fatalf("initial handshake = %t, %v", ok, err)
	}
	if !scheduler.fillTransportProbe(lanes, preferred, now, 0) || !scheduler.lastTransportAt.IsZero() {
		t.Fatal("discovery failed or control traffic changed real-transport activity")
	}
	for _, lane := range lanes {
		data := takeOneTransmission(t, lane.registration.Store)
		if data.PacketID != 1 || wgpacket.Classify(data.Payload) != wgpacket.HandshakeInitiation {
			t.Fatal("capacity padding displaced the queued handshake copy")
		}
	}
}

func TestSchedulerInitialKeepaliveStartsDiscoveryWithoutPrimary(t *testing.T) {
	now := time.Now()
	first := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	second := schedulerLane(t, 2, 2, 300_000, 10_000_000)
	first.rateObserved = false
	second.rateObserved = false
	t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
	t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	item := packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: wgpacket.TransportData,
		Payload: relayWireGuardPacket(wgpacket.TransportData)}), Deadline: now.Add(time.Second), Size: 32}
	var preferred protocol.LaneID
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	if ok, err := scheduler.schedule(lanes, &preferred, &item, now); err != nil || !ok || preferred != 0 || scheduler.packetID != 1 {
		t.Fatalf("initial keepalive = %t, %v, preferred %d, packet ID %d", ok, err, preferred, scheduler.packetID)
	}
	if !scheduler.fillTransportProbe(lanes, preferred, now, 0) || !scheduler.fillTransportProbe(lanes, preferred, now, 0) || first.probeUntil.IsZero() || second.probeUntil.IsZero() || !scheduler.lastTransportAt.IsZero() {
		t.Fatal("initial keepalive did not start discovery on both paths")
	}
	if packets, _ := first.registration.Store.backlog(); packets != 2 {
		t.Fatal("initial keepalive did not retain headroom for the bounded discovery flight")
	}
	if data := takeOneTransmission(t, first.registration.Store); data.PacketID != 1 || len(data.Payload) != 32 {
		t.Fatal("initial keepalive did not use the low-delay path before padding")
	}
}

func TestSchedulerProbeReservesPendingPacket(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	second := schedulerLaneWithLimits(t, 2, 2, 300_000, 1_000_000, packetqueue.Limits{Packets: 2, Bytes: 16 * 1024})
	t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
	t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
	second.registration.Store.now = func() time.Time { return now }
	scheduler := Scheduler{lastTransportAt: now}
	if !scheduler.fillTransportProbe(map[protocol.LaneID]*scheduledLane{1: first, 2: second}, 1, now, 1500) {
		t.Fatal("pending packet prevented all useful discovery")
	}
	packets, bytes := second.registration.Store.backlog()
	if packets != 1 || bytes+1500 > second.deliveryWindowBytes() {
		t.Fatalf("discovery consumed pending capacity: %d packets, %d bytes", packets, bytes)
	}
	second.registration.Store.limits.Packets = 1
	if scheduler.fillTransportProbe(map[protocol.LaneID]*scheduledLane{1: first, 2: second}, 1, now, 1500) {
		t.Fatal("discovery consumed the last packet slot")
	}
}

func TestScheduledLaneDeliveryRateSamplingInterval(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		rtt, interval, bytes uint64
		observed             bool
	}{
		{name: "FastLAN", rtt: 200, interval: 1000, bytes: 4096, observed: true},
		{name: "CompressedLAN", rtt: 200, interval: 999, bytes: 4096},
		{name: "HighDelay", rtt: 200_000, interval: 100_000, bytes: 4096, observed: true},
		{name: "CompressedHighDelay", rtt: 200_000, interval: 999, bytes: 4096},
		{name: "CrossCarrierFeedback", rtt: 400_000, interval: 150_000, bytes: 4096, observed: true},
		{name: "AsymmetricCrossCarrierFeedback", rtt: 200_000, interval: 1000, bytes: 4096, observed: true},
		{name: "SmallSample", rtt: 200, interval: 1000, bytes: 4095},
		{name: "Overflow", interval: 1000, bytes: math.MaxUint64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := &scheduledLane{deliveryRate: defaultInitialRateBytesPerSecond, minimumRTTMicros: tt.rtt}
			lane.updateDeliveryRate(deliverySample{bytes: tt.bytes, intervalMicros: tt.interval}, 0)
			if lane.rateObserved != tt.observed {
				t.Fatalf("rate observed = %t, want %t", lane.rateObserved, tt.observed)
			}
		})
	}
}

func TestScheduledLaneDeliveryRateAcceptsFasterFeedbackCarrier(t *testing.T) {
	now := time.UnixMicro(1000)
	lane := schedulerLaneWithLimits(t, 1, 1, 400_000, defaultInitialRateBytesPerSecond,
		packetqueue.Limits{Packets: 8, Bytes: 128 * 1024})
	lane.minimumRTTMicros = 400_000
	lane.rateObserved = false
	lane.registration.Store.now = func() time.Time { return now }
	t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
	payload := make([]byte, 64_000)
	copy(payload, transmission.packet.Payload)
	transmission.packet.Payload = payload
	if err := lane.registration.Store.push(transmission); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, lane.registration.Store)
	now = now.Add(150 * time.Millisecond)
	if progressed, err := lane.applyReport(protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 1}, uint64(now.UnixMicro()), now); err != nil || !progressed {
		t.Fatalf("cross-carrier report = %t, %v", progressed, err)
	}
	if !lane.rateObserved || lane.deliveryRate == 0 || lane.deliveryRate >= defaultInitialRateBytesPerSecond {
		t.Fatalf("valid faster-carrier sample was rejected: observed %t, rate %d", lane.rateObserved, lane.deliveryRate)
	}
}

func TestSchedulerStalledProbePermitsInitialFailover(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLane(t, 1, 1, 10_000, defaultInitialRateBytesPerSecond)
	second := schedulerLane(t, 2, 2, 100_000, 100_000)
	first.rateObserved = false
	first.lastProgressAt = now
	first.registration.Store.now = func() time.Time { return now }
	var abandoned bool
	first.registration.Abandon = func() { abandoned = true }
	t.Cleanup(func() { releaseTransmissions(first.registration.Store.drain()) })
	t.Cleanup(func() { releaseTransmissions(second.registration.Store.drain()) })
	if err := first.registration.Store.push(retainedTransmission{deadlineMicros: uint64(now.Add(4 * time.Second).UnixMicro()),
		packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, first.registration.Store)
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	var scheduler Scheduler
	now = now.Add(time.Second)
	scheduler.checkAbandonment(lanes, now)
	if !first.degraded || first.abandoning || abandoned {
		t.Fatal("stalled padding did not divert initial traffic without generation abandonment")
	}
	got := scheduler.selectTransportCandidates(lanes, 0, 1500, 5_000_000, now)
	if got.count != 1 || got.lanes[0] != second {
		t.Fatal("unmeasured stalled path blocked a measured lower-rate alternative")
	}
	if progressed, err := first.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(now.UnixMicro()), now); err != nil || !progressed || first.degraded {
		t.Fatalf("resumed parsing did not restore probe path: %t, %v", progressed, err)
	}
}

func TestLaneWriteDataBatchBudget(t *testing.T) {
	for _, tt := range []struct {
		name   string
		budget uint64
		want   int
	}{
		{name: "Startup", want: 1},
		{name: "SlowPath", budget: 1024, want: 1},
		{name: "MeasuredPath", budget: 4096, want: 3},
		{name: "HighCapacity", budget: 64 * 1024, want: maximumDataBatchFrames},
	} {
		t.Run(tt.name, func(t *testing.T) {
			connection := newTestCarrier()
			lane := newTestLane(t, connection, newTestEndpoint())
			lane.store = schedulerStore(t, packetqueue.Limits{Packets: 32, Bytes: 128 * 1024})
			lane.store.writeBudget.Store(tt.budget)
			t.Cleanup(func() { releaseTransmissions(lane.store.drain()) })
			for id := uint64(1); id <= maximumDataBatchFrames; id++ {
				transmission := schedulerTransmission(id, wgpacket.TransportData, time.Now().Add(time.Second))
				payload := make([]byte, 1452)
				copy(payload, transmission.packet.Payload)
				transmission.packet.Payload = payload
				if err := lane.store.push(transmission); err != nil {
					t.Fatal(err)
				}
			}
			if err := lane.writeDataBatch(t.Context()); err != nil {
				t.Fatal(err)
			}
			frames := <-connection.writes
			if len(frames) != tt.want {
				t.Fatalf("writer committed %d frames, want %d", len(frames), tt.want)
			}
			for index, frame := range frames {
				data, err := protocol.ParseData(frame)
				if err != nil || data.PacketID != uint64(index+1) || len(data.Payload) != 1452 {
					t.Fatalf("writer changed frame identity or payload: %+v, %v", data, err)
				}
			}
		})
	}
}

func TestSchedulerMigrateDiscardsCapacityProbe(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 8, Bytes: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	makeLane := func(id byte) *scheduledLane {
		store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 8, Bytes: 16 * 1024},
			budget, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { releaseTransmissions(store.drain()) })
		return &scheduledLane{registration: schedulerRegistration(id, id, store), rttMicros: 1000,
			deliveryRate: 1_000_000, rateObserved: true}
	}
	source, destination, third := makeLane(1), makeLane(2), makeLane(3)
	for _, transmission := range []retainedTransmission{
		{deadlineMicros: uint64(now.Add(time.Second).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]}},
		schedulerTransmission(17, wgpacket.TransportData, now.Add(time.Second)),
		schedulerTransmission(18, wgpacket.HandshakeInitiation, now.Add(time.Second)),
	} {
		if err := source.registration.Store.push(transmission); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, source.registration.Store)
	}
	ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.migrateTransmissions(map[protocol.LaneID]*scheduledLane{2: destination}, source)
	if packets, _ := destination.registration.Store.backlog(); packets != 1 {
		t.Fatalf("migration kept %d packets, want only real transport", packets)
	}
	data := takeOneTransmission(t, destination.registration.Store)
	if data.PacketID != 17 {
		t.Fatalf("migrated packet ID = %d", data.PacketID)
	}
	scheduler.migrateTransmissions(map[protocol.LaneID]*scheduledLane{3: third}, destination)
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("second migration retained capacity: %+v", got)
	}
}

func TestSchedulerReplacementResetsTransportHold(t *testing.T) {
	now := time.Unix(100, 0)
	lane := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	lanes := map[protocol.LaneID]*scheduledLane{1: lane}
	preferred := protocol.LaneID(1)
	ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.transportHoldUntil = now.Add(time.Second)
	registration := lane.registration
	registration.Generation++
	registration.Store = schedulerStore(t, packetqueue.Limits{Packets: 8, Bytes: 16 * 1024})
	t.Cleanup(func() { releaseTransmissions(registration.Store.drain()) })
	result := make(chan error, 1)
	scheduler.applyEvent(lanes, &preferred, schedulerEvent{kind: schedulerRegister, registration: registration, result: result}, now)
	if err := <-result; err != nil || preferred != 0 {
		t.Fatalf("replacement = %v, preferred %d", err, preferred)
	}
	scheduler.recordTransportAssignment(lanes, preferred, lanes[1], now)
	if !scheduler.transportHoldUntil.IsZero() {
		t.Fatal("new generation inherited the removed primary's hold")
	}
}

func TestSchedulerReplacementDiscardsProbeState(t *testing.T) {
	now := time.Unix(100, 0)
	budget, err := retention.NewBudget(retention.Limits{Packets: 8, Bytes: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	limits := packetqueue.Limits{Packets: 8, Bytes: 16 * 1024}
	oldStore, err := NewTransmissionStoreWithBudget(limits, budget, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	freshStore, err := NewTransmissionStoreWithBudget(limits, budget, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []*TransmissionStore{oldStore, freshStore} {
		t.Cleanup(func() { releaseTransmissions(store.drain()) })
	}
	old := &scheduledLane{registration: schedulerRegistration(1, 1, oldStore), rttMicros: 10_000,
		deliveryRate: defaultInitialRateBytesPerSecond, lastProgressAt: now}
	transmission := schedulerTransmission(7, wgpacket.TransportData, now.Add(time.Second))
	transmission.packet.Payload = make([]byte, minimumRateSampleBytes)
	transmission.packet.Payload[0] = 4
	if err := oldStore.push(transmission); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, oldStore)
	old.probeUntil = now.Add(minimumTransportProbeDuration)
	old.nextProbeAt = old.probeUntil.Add(transportProbeInterval)
	old.probeBytes = 4101
	if err := oldStore.push(retainedTransmission{deadlineMicros: uint64(old.probeUntil.UnixMicro()),
		packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, oldStore)
	now = now.Add(10 * time.Millisecond)
	if progressed, err := old.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(now.UnixMicro()), now); err != nil || !progressed {
		t.Fatalf("old generation's real feedback = %t, %v", progressed, err)
	}
	if !old.rateObserved || oldStore.transportReported.Load() != minimumRateSampleBytes {
		t.Fatal("old generation did not establish capacity and real-transport proof")
	}
	abandonments := 0
	old.registration.Abandon = func() { abandonments++ }
	lanes := map[protocol.LaneID]*scheduledLane{1: old}
	preferred := protocol.LaneID(1)
	ingress, err := packetqueue.NewWithClock[Packet](limits, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	registration := schedulerRegistration(1, 1, freshStore)
	registration.Generation = 2
	registration.Abandon = func() { abandonments++ }
	result := make(chan error, 1)
	scheduler.applyEvent(lanes, &preferred, schedulerEvent{kind: schedulerRegister, registration: registration, result: result}, now)
	if err := <-result; err != nil || preferred != 0 || abandonments != 1 {
		t.Fatalf("replacement = %v, preferred %d, abandonments %d", err, preferred, abandonments)
	}
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("old generation retained capacity padding: %+v", got)
	}
	select {
	case <-oldStore.Done():
	default:
		t.Fatal("replacement did not close the old store")
	}
	fresh := lanes[1]
	if !fresh.probeUntil.IsZero() || !fresh.nextProbeAt.IsZero() || fresh.probeBytes != 0 ||
		fresh.rateHistoryCount != 0 || fresh.rateHistory != ([5]rateObservation{}) || fresh.rateObserved {
		t.Fatal("replacement inherited old capacity-discovery state")
	}
	if err := freshStore.push(transmission); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, freshStore)
	usage := budget.Usage()
	for _, tt := range []struct {
		name  string
		event schedulerEvent
	}{
		{name: "ProbeReport", event: schedulerEvent{kind: schedulerReport,
			report: protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 2, DelayMicros: 1000}}},
		{name: "ImpossibleOldReport", event: schedulerEvent{kind: schedulerReport,
			report: protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: 99}}},
		{name: "Timing", event: schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 1,
			timing: clockmap.Sample{LocalSendMicros: 1000, RemoteReceiveMicros: 2000, RemoteSendMicros: 3000, LocalReceiveMicros: 9000}}},
		{name: "Remove", event: schedulerEvent{kind: schedulerRemove, laneID: 1, generation: 1}},
		{name: "PeerAbandon", event: schedulerEvent{kind: schedulerPeerAbandon, laneID: 1, generation: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.event.result = result
			scheduler.applyEvent(lanes, &preferred, tt.event, now.Add(time.Second))
			if tt.event.kind != schedulerTiming {
				if err := <-result; err != nil {
					t.Fatalf("old generation's late event = %v", err)
				}
			}
			if lanes[1] != fresh || abandonments != 1 || fresh.abandoning || fresh.degraded || fresh.rateObserved || fresh.rttObserved ||
				fresh.rttMicros != defaultInitialRTTMicros || fresh.minimumRTTMicros != 0 ||
				fresh.deliveryRate != defaultInitialRateBytesPerSecond || fresh.feedbackDelayMicros != 0 ||
				fresh.lastDataPackets != 0 || fresh.lastPingID != 0 || fresh.lastProgressAt != now ||
				freshStore.transportReported.Load() != 0 || freshStore.reportedPackets != 0 || budget.Usage() != usage {
				t.Fatal("old generation's late event changed fresh capacity, liveness, or retained ownership")
			}
		})
	}
	now = now.Add(time.Millisecond)
	scheduler.applyEvent(lanes, &preferred, schedulerEvent{kind: schedulerReport, laneID: 1, generation: 2,
		report:        protocol.DeliveryReport{LaneID: 1, Generation: 2, DataPackets: 1},
		receiveMicros: uint64(now.UnixMicro()), result: result}, now)
	if err := <-result; err != nil {
		t.Fatalf("fresh generation's real feedback = %v", err)
	}
	if got := budget.Usage(); got != (retention.Usage{}) || !fresh.rateObserved || freshStore.transportReported.Load() != minimumRateSampleBytes {
		t.Fatal("fresh real feedback did not independently establish capacity, proof, and exact release")
	}
}

func TestSchedulerPromotionPreservesActiveProbe(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLane(t, 1, 1, 10_000, 1_000_000)
	second := schedulerLaneWithLimits(t, 2, 2, 300_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	until := now.Add(minimumTransportProbeDuration)
	for _, lane := range lanes {
		lane.probeUntil = until
		lane.nextProbeAt = until.Add(transportProbeInterval)
		lane.probeBytes = 4101
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	var scheduler Scheduler
	scheduler.recordTransportAssignment(lanes, 1, second, now)
	for _, lane := range lanes {
		if lane.probeUntil != until || lane.nextProbeAt != until.Add(transportProbeInterval) || lane.probeBytes != 4101 {
			t.Fatal("promotion reset the active discovery deadline, cooldown, or byte cap")
		}
	}
	scheduler.lastProbeLane = 1
	if !scheduler.fillTransportProbe(lanes, 2, now, 0) || second.probeBytes <= 4101 {
		t.Fatal("primary selection stopped an incomplete capacity measurement")
	}
	if second.probeUntil != until || second.probeBytes > maximumTransportProbeBytes {
		t.Fatal("primary discovery exceeded its original period or byte cap")
	}
	now = until
	if scheduler.fillTransportProbe(lanes, 2, now, 0) {
		t.Fatal("primary discovery continued after its existing period")
	}
}
