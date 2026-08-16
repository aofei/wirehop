// Package relay implements the shared WireGuard packet forwarding data plane.
package relay

import (
	"errors"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var (
	// ErrInvalidDeadlinePolicy indicates a zero or protocol-invalid packet lifetime.
	ErrInvalidDeadlinePolicy = errors.New("invalid packet deadline policy")
	// ErrCounterExhausted indicates that a direction-local identifier space is exhausted.
	ErrCounterExhausted = errors.New("relay counter exhausted")
	// ErrInvalidPacket indicates inconsistent relay packet metadata.
	ErrInvalidPacket = errors.New("invalid relay packet")
)

// Clock supplies process-local monotonic timestamps in protocol microseconds.
type Clock interface {
	NowMicros() uint64
}

// DeadlinePolicy assigns bounded lifetimes to WireGuard packet classes.
type DeadlinePolicy struct {
	Control   time.Duration
	Transport time.Duration
}

// Validate verifies that both lifetimes fit the wire protocol.
func (p DeadlinePolicy) Validate() error {
	maximum := time.Duration(protocol.MaxPacketLifetimeMicros) * time.Microsecond
	if p.Control <= 0 || p.Control > maximum || p.Transport <= 0 || p.Transport > maximum {
		return ErrInvalidDeadlinePolicy
	}
	return nil
}

// Lifetime returns the configured lifetime for kind.
func (p DeadlinePolicy) Lifetime(kind wgpacket.Kind) time.Duration {
	if kind.Control() {
		return p.Control
	}
	return p.Transport
}

// Packet is one ingress-owned WireGuard packet awaiting session scheduling.
type Packet struct {
	Kind           wgpacket.Kind
	Payload        []byte
	DeadlineMicros uint64
}

// Validate verifies the packet classification, payload, and lifetime metadata.
func (p Packet) Validate() error {
	if !p.Kind.Accepted() || len(p.Payload) > protocol.MaxPacketSize ||
		wgpacket.Classify(p.Payload) != p.Kind || p.DeadlineMicros == 0 {
		return ErrInvalidPacket
	}
	return nil
}
