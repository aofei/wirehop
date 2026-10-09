package relay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"testing"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func FuzzReceiverDeliverBatch(f *testing.F) {
	var encoded []byte
	for _, id := range []uint16{1, 63, 64, 1, 65, 127, 128, 129, 191, 192, 193, 255, 256, 257, 0x8000 | 258, 65,
		257, 256, 64, 63, 1} {
		encoded = binary.LittleEndian.AppendUint16(encoded, id)
	}
	for _, capacity := range []uint16{1, 2, 63, 64, 65, 127, 128, 129, 257} {
		for _, base := range []uint64{0, math.MaxUint64 - 128} {
			for _, fault := range []uint8{0, 1, 2, 3, 9, 10, 11, 73} {
				for timing := range uint16(7) {
					f.Add((capacity-1)*7+timing, base, fault, encoded)
				}
			}
		}
	}
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
			clock := &testClock{now: timing.receiverBase + 1000}
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
							endpoint.afterFailure = func() { clock.now = timing.receiverBase + 2000 }
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
					err := receiver.deliverBatch(t.Context(), mapped)
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
					if receiver.deduplication.Highest() != highest || clock.now != timing.receiverBase+now || len(receiver.writeSlot) != 0 {
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
