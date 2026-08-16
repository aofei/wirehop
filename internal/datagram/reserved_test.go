package datagram

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/wgpacket"
)

type reservedTestEndpoint struct {
	packet   Packet
	writes   [][]byte
	writeErr error
}

func (e *reservedTestEndpoint) Read(context.Context) (Packet, error) {
	return e.packet, nil
}

func (e *reservedTestEndpoint) Write(_ context.Context, payload []byte, _ time.Time) error {
	e.writes = append(e.writes, append([]byte(nil), payload...))
	return e.writeErr
}

func (*reservedTestEndpoint) Close() error {
	return nil
}

func TestReservedEndpointRead(t *testing.T) {
	reserved := wgpacket.Reserved{1, 2, 3}
	payload := make([]byte, 32)
	payload[0] = 4
	inner := &reservedTestEndpoint{packet: Packet{Kind: wgpacket.TransportData, Payload: payload}}
	packet, err := WithReservedTranslation(inner, reserved).Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := wgpacket.Reserved(packet.Payload[1:4]); got != reserved {
		t.Fatalf("reserved = %v, want %v", got, reserved)
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
