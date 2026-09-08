package datagram

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/wgpacket"
)

type reservedTestEndpoint struct {
	packets  []Packet
	writes   [][]byte
	writeErr error
}

func (e *reservedTestEndpoint) Read(context.Context) (Packet, error) {
	packet := e.packets[0]
	e.packets = e.packets[1:]
	return packet, nil
}

func (e *reservedTestEndpoint) ReadBatch(_ context.Context, packets []Packet) (int, error) {
	count := min(len(packets), len(e.packets))
	copy(packets, e.packets[:count])
	e.packets = e.packets[count:]
	return count, nil
}

func (e *reservedTestEndpoint) Write(_ context.Context, payload []byte, _ time.Time) error {
	e.writes = append(e.writes, append([]byte(nil), payload...))
	return e.writeErr
}

func (e *reservedTestEndpoint) WriteBatch(_ context.Context, payloads [][]byte, _ time.Time) (int, error) {
	for _, payload := range payloads {
		e.writes = append(e.writes, append([]byte(nil), payload...))
	}
	return len(payloads), e.writeErr
}

func (*reservedTestEndpoint) Close() error {
	return nil
}

func TestReservedEndpointRead(t *testing.T) {
	reserved := wgpacket.Reserved{1, 2, 3}
	payload := make([]byte, 32)
	payload[0] = 4
	inner := &reservedTestEndpoint{packets: []Packet{{Kind: wgpacket.TransportData, Payload: payload}}}
	packet, err := WithReservedTranslation(inner, reserved).Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := wgpacket.Reserved(packet.Payload[1:4]); got != reserved {
		t.Fatalf("reserved = %v, want %v", got, reserved)
	}
	packet.Release()
}

func TestReservedEndpointBatch(t *testing.T) {
	reserved := wgpacket.Reserved{1, 2, 3}
	packets := []Packet{
		{Kind: wgpacket.TransportData, Payload: make([]byte, 32)},
		{Kind: wgpacket.TransportData, Payload: make([]byte, 32)},
	}
	for index := range packets {
		packets[index].Payload[0] = 4
	}
	inner := &reservedTestEndpoint{packets: packets}
	endpoint := WithReservedTranslation(inner, reserved)
	var read [2]Packet
	count, err := ReadBatch(context.Background(), endpoint, read[:])
	if err != nil || count != len(read) {
		t.Fatalf("ReadBatch() = %d, %v, want %d, nil", count, err, len(read))
	}
	for index := range count {
		if got := wgpacket.Reserved(read[index].Payload[1:4]); got != reserved {
			t.Fatalf("packet %d reserved = %v, want %v", index, got, reserved)
		}
	}
	payloads := [][]byte{read[0].Payload, read[1].Payload}
	written, err := WriteBatch(context.Background(), endpoint, payloads, time.Time{})
	if err != nil || written != len(payloads) {
		t.Fatalf("WriteBatch() = %d, %v, want %d, nil", written, err, len(payloads))
	}
	for index, payload := range inner.writes {
		if got := wgpacket.Reserved(payload[1:4]); got != (wgpacket.Reserved{}) {
			t.Fatalf("local packet %d reserved = %v, want zero", index, got)
		}
		if got := wgpacket.Reserved(payloads[index][1:4]); got != reserved {
			t.Fatalf("restored packet %d reserved = %v, want %v", index, got, reserved)
		}
		read[index].Release()
	}
}

func TestReservedEndpointWrite(t *testing.T) {
	reserved := wgpacket.Reserved{1, 2, 3}
	payload := make([]byte, 32)
	payload[0] = 4
	copy(payload[1:4], reserved[:])
	inner := &reservedTestEndpoint{}
	if err := WithReservedTranslation(inner, reserved).Write(context.Background(), payload, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if len(inner.writes) != 1 || wgpacket.Reserved(inner.writes[0][1:4]) != (wgpacket.Reserved{}) {
		t.Fatalf("local writes = %v, want one zero-reserved packet", inner.writes)
	}
	if got := wgpacket.Reserved(payload[1:4]); got != reserved {
		t.Fatalf("restored reserved = %v, want %v", got, reserved)
	}
}

func TestReservedEndpointWriteDropsMismatch(t *testing.T) {
	payload := make([]byte, 32)
	payload[0] = 4
	copy(payload[1:4], []byte{1, 2, 3})
	inner := &reservedTestEndpoint{}
	err := WithReservedTranslation(inner, wgpacket.Reserved{4, 5, 6}).Write(context.Background(), payload, time.Time{})
	if !errors.Is(err, ErrDatagramDropped) || len(inner.writes) != 0 {
		t.Fatalf("Write() = %v with %d local writes, want dropped without write", err, len(inner.writes))
	}
	if got, want := wgpacket.Reserved(payload[1:4]), (wgpacket.Reserved{1, 2, 3}); got != want {
		t.Fatalf("reserved after drop = %v, want %v", got, want)
	}
}

func TestReservedEndpointWriteRestoresAfterFailure(t *testing.T) {
	writeErr := errors.New("test write failure")
	reserved := wgpacket.Reserved{1, 2, 3}
	payload := make([]byte, 32)
	payload[0] = 4
	copy(payload[1:4], reserved[:])
	inner := &reservedTestEndpoint{writeErr: writeErr}
	err := WithReservedTranslation(inner, reserved).Write(context.Background(), payload, time.Time{})
	if err != writeErr {
		t.Fatalf("Write() error = %v, want %v", err, writeErr)
	}
	if got := wgpacket.Reserved(payload[1:4]); got != reserved {
		t.Fatalf("restored reserved = %v, want %v", got, reserved)
	}
}

func TestWithReservedTranslationDisabled(t *testing.T) {
	inner := &reservedTestEndpoint{}
	if got := WithReservedTranslation(inner, wgpacket.Reserved{}); got != inner {
		t.Fatalf("WithReservedTranslation() = %T, want original endpoint", got)
	}
}
