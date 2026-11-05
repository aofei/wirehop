package relay

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"testing"
	"testing/synctest"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestReceiverDeliverBatchWhileWaiting(t *testing.T) {
	for _, tt := range []struct {
		name     string
		deadline uint64
		mapping  clockmap.Mapping
		cancel   bool
		writes   int
		err      error
	}{
		{name: "ClockUpdate", deadline: 100_900, mapping: clockmap.Mapping{OffsetMicros: -100_000}},
		{name: "ClockUpdateRevives", deadline: 900, mapping: clockmap.Mapping{OffsetMicros: 1000}, writes: 1},
		{name: "UncertaintyUpdateRevives", deadline: 900, mapping: clockmap.Mapping{UncertaintyMicros: 200}, writes: 1},
		{name: "ClockUpdateOverflows", deadline: 900, mapping: clockmap.Mapping{OffsetMicros: -1000}, err: ErrInvalidPacketDeadline},
		{name: "Cancellation", deadline: 100_900, cancel: true, err: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				endpoint := &blockingWriteEndpoint{
					testEndpoint: newTestEndpoint(), entered: make(chan byte, 2), release: make(chan struct{}),
				}
				receiver, err := NewReceiver(ReceiverConfig{
					Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64,
				})
				if err != nil {
					t.Fatal(err)
				}
				data := []protocol.Data{{
					PacketID: 1, DeadlineMicros: 100_900, Payload: relayWireGuardPacket(wgpacket.TransportData),
				}}
				data[0].Payload[4] = 1
				first := make(chan error, 1)
				go func() { first <- receiver.deliverBatch(t.Context(), data) }()
				if packetID := <-endpoint.entered; packetID != 1 {
					t.Fatal("first delivery changed packet order")
				}
				waiting := append([]protocol.Data(nil), data...)
				waiting[0].PacketID = 2
				waiting[0].DeadlineMicros = tt.deadline
				waiting[0].Payload = bytes.Clone(waiting[0].Payload)
				waiting[0].Payload[4] = 2
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				second := make(chan error, 1)
				go func() { second <- receiver.Deliver(ctx, waiting[0]) }()
				synctest.Wait()
				if tt.cancel {
					cancel()
				} else {
					receiver.UpdateClock(tt.mapping)
				}
				close(endpoint.release)
				if err := <-first; err != nil {
					t.Fatal(err)
				}
				if err := <-second; !errors.Is(err, tt.err) {
					t.Fatalf("waiting delivery = %v, want %v", err, tt.err)
				}
				if len(endpoint.entered) != tt.writes ||
					len(receiver.writeSlot) != 0 || receiver.deduplication.Highest() != uint64(1+tt.writes) {
					t.Fatalf("waiting delivery = %d writes, highest %d, slots %d, want %d writes, highest %d, slots 0",
						len(endpoint.entered), receiver.deduplication.Highest(), len(receiver.writeSlot), tt.writes, 1+tt.writes)
				}
				for range tt.writes {
					if packetID := <-endpoint.entered; packetID != 2 {
						t.Fatal("mapping update changed serialized packet order")
					}
				}
				retry := newTestEndpoint()
				receiver.endpoint = retry
				waiting[0].DeadlineMicros = 200_900
				for range 2 {
					if err := receiver.deliverBatch(t.Context(), waiting); err != nil {
						t.Fatal(err)
					}
				}
				if len(retry.writes) != 1-tt.writes || len(receiver.writeSlot) != 0 {
					t.Fatal("waiting delivery changed later exactly-once delivery")
				}
			})
		})
	}
}

func TestReceiverDeliverBatchDeadlineBounds(t *testing.T) {
	for _, tt := range []struct {
		name        string
		now         uint64
		deadline    uint64
		uncertainty uint64
		writes      int
	}{
		{name: "Live", now: 5000, deadline: 6000, writes: 2},
		{name: "Expired", now: 5000, deadline: 5000, writes: 1},
		{name: "UncertaintyGrace", now: 5000, deadline: 5000, uncertainty: 1000, writes: 2},
		{name: "UncertaintyBoundary", now: 5000, deadline: 4000, uncertainty: 1000, writes: 1},
		{name: "ZeroClockLive", deadline: 1000, writes: 2},
		{name: "ZeroClockExpired", writes: 1},
		{name: "SaturatedLatestLive", now: math.MaxUint64 - 500, deadline: math.MaxUint64 - 1, uncertainty: 1000, writes: 2},
		{name: "NearMaximumExpired", now: math.MaxUint64 - 500, deadline: math.MaxUint64 - 1500, uncertainty: 1000, writes: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &recordingBatchEndpoint{testEndpoint: newTestEndpoint()}
			receiver, err := NewReceiver(ReceiverConfig{
				Endpoint: endpoint, Clock: &testClock{now: tt.now}, DeduplicationSize: 64,
				ClockMapping: clockmap.Mapping{UncertaintyMicros: tt.uncertainty},
			})
			if err != nil {
				t.Fatal(err)
			}
			liveDeadline := tt.now + min(uint64(10_000), math.MaxUint64-tt.now)
			data := []protocol.Data{
				{PacketID: 2, DeadlineMicros: liveDeadline, Payload: relayWireGuardPacket(wgpacket.TransportData)},
				{PacketID: 1, DeadlineMicros: tt.deadline, Payload: relayWireGuardPacket(wgpacket.TransportData)},
			}
			for index := range data {
				data[index].Payload[4] = byte(data[index].PacketID)
			}
			if err := receiver.ValidateDeadlines([]uint64{liveDeadline, tt.deadline}); err != nil {
				t.Fatal(err)
			}
			if err := receiver.deliverBatch(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			if len(endpoint.writes) != tt.writes || len(endpoint.calls) != tt.writes ||
				receiver.deduplication.Highest() != 2 || len(receiver.writeSlot) != 0 {
				t.Fatal("deadline bounds changed ordered delivery, deduplication, or write slot ownership")
			}
			for index := range tt.writes {
				if payload := <-endpoint.writes; payload[4] != byte(2-index) {
					t.Fatal("deadline filtering changed delivery order")
				}
			}
			data[1].DeadlineMicros = liveDeadline
			if err := receiver.Deliver(t.Context(), data[1]); err != nil {
				t.Fatal(err)
			}
			if len(endpoint.writes) != 2-tt.writes || len(receiver.writeSlot) != 0 {
				t.Fatal("expired packet was recorded as delivered or a live packet was delivered twice")
			}
		})
	}
}

func TestReceiverDeliverDeadlineBeforeDeduplication(t *testing.T) {
	for _, tt := range []struct {
		name     string
		deadline uint64
		mapping  clockmap.Mapping
		err      error
	}{
		{name: "ValidDuplicate", deadline: 2000},
		{name: "ExpiredDuplicate", deadline: 900},
		{name: "FutureDuplicate", deadline: 1000 + protocol.MaxPacketLifetimeMicros + protocol.DeadlineResolutionMicros,
			err: ErrInvalidPacketDeadline},
		{name: "OverflowingDuplicate", deadline: 9, mapping: clockmap.Mapping{OffsetMicros: -10},
			err: ErrInvalidPacketDeadline},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := newTestEndpoint()
			receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64})
			if err != nil {
				t.Fatal(err)
			}
			data := protocol.Data{PacketID: 1, DeadlineMicros: 2000, Payload: relayWireGuardPacket(wgpacket.TransportData)}
			if err := receiver.Deliver(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			receiver.UpdateClock(tt.mapping)
			data.DeadlineMicros = tt.deadline
			if err := receiver.Deliver(t.Context(), data); !errors.Is(err, tt.err) {
				t.Fatalf("duplicate deadline %d: error = %v, want %v", tt.deadline, err, tt.err)
			}
			if len(endpoint.writes) != 1 || len(receiver.writeSlot) != 0 {
				t.Fatal("duplicate changed delivery or retained a write slot")
			}
			receiver.UpdateClock(clockmap.Mapping{})
			data.PacketID = 2
			data.DeadlineMicros = 2000
			if err := receiver.Deliver(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			if len(endpoint.writes) != 2 || receiver.deduplication.Highest() != 2 {
				t.Fatal("deadline rejection changed subsequent delivery")
			}
		})
	}
}

func FuzzReceiverDeliverBatch(f *testing.F) {
	var encoded []byte
	for _, id := range []uint16{1, 63, 64, 1, 65, 127, 128, 129, 191, 192, 193, 255, 256, 257, 0x8000 | 258, 65,
		257, 256, 64, 63, 1} {
		encoded = binary.LittleEndian.AppendUint16(encoded, id)
	}
	for _, capacity := range []uint16{1, 2, 63, 64, 65, 127, 128, 129, 257} {
		for _, base := range []uint64{0, math.MaxUint64 - 128} {
			for _, fault := range []uint8{0, 1, 2, 3, 9, 10, 11, 73, 128, 193} {
				for timing := range uint16(7) {
					f.Add((capacity-1)*7+timing, base, fault, encoded)
				}
			}
		}
	}
	var descending []byte
	for id := uint16(datagram.MaximumBatchSize); id > 0; id-- {
		descending = binary.LittleEndian.AppendUint16(descending, 0x8000|id)
	}
	f.Add(uint16(63*7), uint64(0), uint8(65), descending)
	f.Fuzz(func(t *testing.T, selector uint16, base uint64, fault uint8, encoded []byte) {
		capacity := uint64(selector/7%257) + 1
		// The model uses relative times while sender and receiver clocks use independent origins.
		timing := [...]struct{ senderBase, receiverBase, uncertainty uint64 }{
			{},
			{10_000, 100_000, 0},
			{100_000, 10_000, 0},
			{1, math.MaxInt64 - 100_000, 999},
			{math.MaxInt64 - 100_000, 1, 100},
			{math.MaxInt64, math.MaxUint64 - 200_000, 0},
			{math.MaxInt64, math.MaxUint64 - 200_000, 5_000_000},
		}[selector%7]
		encoded = encoded[:min(len(encoded), 128)]
		var packets []protocol.Data
		for len(encoded) >= 2 {
			value := binary.LittleEndian.Uint16(encoded)
			encoded = encoded[2:]
			id := base + uint64(value&0x7fff)
			if id == 0 {
				id = 1
			}
			deadline := uint64(100_900)
			if value&0x8000 != 0 {
				deadline = 1900
			}
			payload := relayWireGuardPacket(wgpacket.TransportData)
			binary.LittleEndian.PutUint64(payload[4:12], id)
			packets = append(packets, protocol.Data{PacketID: id, DeadlineMicros: deadline, Payload: payload})
		}
		for _, vector := range []bool{false, true} {
			clock := &timingClock{}
			clock.now.Store(timing.receiverBase + 1000)
			endpoint := &partialWriteEndpoint{testEndpoint: newTestEndpoint()}
			endpoint.writes = make(chan []byte, datagram.MaximumBatchSize)
			var output datagram.Endpoint = endpoint
			if vector {
				output = &partialBatchWriteEndpoint{partialWriteEndpoint: endpoint}
			}
			receiver, err := NewReceiver(ReceiverConfig{
				Endpoint: output, Clock: clock, DeduplicationSize: int(capacity),
				ClockMapping: clockmap.Mapping{
					OffsetMicros: int64(timing.receiverBase - timing.senderBase), UncertaintyMicros: timing.uncertainty,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			observed := make(map[uint64]bool)
			var highest uint64
			now := uint64(1000)
			for pass := range 3 {
				for offset := 0; offset < len(packets); offset += datagram.MaximumBatchSize {
					var batch []protocol.Data
					for index := offset; index < min(offset+datagram.MaximumBatchSize, len(packets)); index++ {
						packet := packets[index]
						if pass == 2 {
							packet = packets[len(packets)-1-index]
							packet.DeadlineMicros = 100_900
						}
						batch = append(batch, packet)
					}
					endpoint.calls = 0
					endpoint.failureAt = 0
					if pass == 0 {
						endpoint.failure = []error{nil, datagram.ErrDatagramDropped, datagram.ErrNoLocalPeer, net.ErrClosed}[fault%4]
						if endpoint.failure != nil {
							endpoint.failureAt = 1 + int(fault>>2&15)
						}
						if fault&0x40 != 0 {
							endpoint.afterFailure = func() { clock.now.Store(timing.receiverBase + 2000) }
						}
					}
					var ctx context.Context = t.Context()
					if fault&0x80 != 0 {
						ctx = deadlineHookContext{
							Context: ctx, beforeDeadline: func() { clock.now.Store(timing.receiverBase + 2000) },
						}
						// Preparing the first new run queries the context before validating its current expiry.
						for _, packet := range batch {
							if packet.PacketID > highest || highest-packet.PacketID < capacity && !observed[packet.PacketID] {
								now = 2000
								break
							}
						}
					}
					var expected [][]byte
					var wantError error
					attempts := 0
					// This scalar set model commits only successful writes and does not use the production bitmap or batching.
					for _, packet := range batch {
						if packet.DeadlineMicros+timing.uncertainty <= now || packet.PacketID <= highest &&
							(highest-packet.PacketID >= capacity || observed[packet.PacketID]) {
							continue
						}
						attempts++
						if attempts == endpoint.failureAt {
							if fault&0x40 != 0 {
								now = 2000
							}
							if errors.Is(endpoint.failure, datagram.ErrDatagramDropped) {
								continue
							}
							if !errors.Is(endpoint.failure, datagram.ErrNoLocalPeer) {
								wantError = endpoint.failure
							}
							break
						}
						observed[packet.PacketID] = true
						highest = max(highest, packet.PacketID)
						expected = append(expected, packet.Payload)
					}
					mapped := make([]protocol.Data, len(batch))
					for index, packet := range batch {
						packet.DeadlineMicros += timing.senderBase
						mapped[index] = packet
					}
					err := receiver.deliverBatch(ctx, mapped)
					if !errors.Is(err, wantError) || wantError != nil && !errors.Is(err, ErrEndpointFailure) {
						t.Fatalf("vector %t pass %d offset %d: error = %v, want %v", vector, pass, offset, err, wantError)
					}
					if endpoint.calls != attempts || len(endpoint.writes) != len(expected) {
						t.Fatalf("vector %t pass %d offset %d: attempts/writes = %d/%d, want %d/%d", vector, pass, offset,
							endpoint.calls, len(endpoint.writes), attempts, len(expected))
					}
					for index, payload := range expected {
						if !bytes.Equal(<-endpoint.writes, payload) {
							t.Fatalf("vector %t pass %d offset %d: payload %d differs", vector, pass, offset, index)
						}
					}
					if receiver.deduplication.Highest() != highest || clock.now.Load() != timing.receiverBase+now || len(receiver.writeSlot) != 0 {
						t.Fatal("delivery retained a write slot or diverged from committed sequence and clock state")
					}
					for index := range receiver.payloads {
						if receiver.payloads[index] != nil || receiver.packetIDs[index] != 0 || receiver.deadlines[index] != 0 {
							t.Fatalf("delivery retained temporary buffer state at index %d", index)
						}
					}
				}
			}
		}
	})
}
