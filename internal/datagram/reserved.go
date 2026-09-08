package datagram

import (
	"context"
	"time"

	"github.com/aofei/wirehop/internal/wgpacket"
)

// reservedEndpoint adapts a zero-reserved local WireGuard endpoint to one fixed nonzero on-wire value.
type reservedEndpoint struct {
	Endpoint
	reserved wgpacket.Reserved
}

// WithReservedTranslation wraps endpoint when reserved translation is enabled.
func WithReservedTranslation(endpoint Endpoint, reserved wgpacket.Reserved) Endpoint {
	if !reserved.Enabled() {
		return endpoint
	}
	return &reservedEndpoint{Endpoint: endpoint, reserved: reserved}
}

// Read injects the configured reserved value into one locally produced WireGuard packet.
func (e *reservedEndpoint) Read(ctx context.Context) (Packet, error) {
	packet, err := e.Endpoint.Read(ctx)
	if err != nil {
		return Packet{}, err
	}
	copy(packet.Payload[1:4], e.reserved[:])
	return packet, nil
}

// ReadBatch injects the configured reserved value into an accepted packet vector.
func (e *reservedEndpoint) ReadBatch(ctx context.Context, packets []Packet) (int, error) {
	count, err := ReadBatch(ctx, e.Endpoint, packets)
	for index := range count {
		copy(packets[index].Payload[1:4], e.reserved[:])
	}
	return count, err
}

// Write validates and clears the configured reserved value before local UDP delivery.
func (e *reservedEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	if wgpacket.Reserved(payload[1:4]) != e.reserved {
		return ErrDatagramDropped
	}
	clear(payload[1:4])
	err := e.Endpoint.Write(ctx, payload, deadline)
	copy(payload[1:4], e.reserved[:])
	return err
}

// WriteBatch validates, clears, and restores reserved bytes around one ordered vector write.
func (e *reservedEndpoint) WriteBatch(ctx context.Context, payloads [][]byte, deadline time.Time) (int, error) {
	valid := 0
	for valid < len(payloads) && wgpacket.Reserved(payloads[valid][1:4]) == e.reserved {
		clear(payloads[valid][1:4])
		valid++
	}
	written, err := WriteBatch(ctx, e.Endpoint, payloads[:valid], deadline)
	for index := range valid {
		copy(payloads[index][1:4], e.reserved[:])
	}
	if err != nil {
		return written, err
	}
	if valid < len(payloads) {
		return valid, ErrDatagramDropped
	}
	return valid, nil
}
