// Package datagram owns WireGuard-facing UDP endpoint behavior.
package datagram

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var (
	// ErrNoLocalPeer indicates that no WireGuard process has sent a local datagram yet.
	ErrNoLocalPeer = errors.New("no local UDP peer")
	// ErrDatagramDropped indicates a per-datagram rejection or failure that does not invalidate the UDP endpoint.
	ErrDatagramDropped = errors.New("UDP datagram dropped")
	// readBufferPool includes one sentinel byte so unsupported oversized datagrams cannot appear valid after truncation.
	readBufferPool = sync.Pool{New: func() any { return new([protocol.MaxPacketSize + 1]byte) }}
)

// Packet is one accepted WireGuard-looking UDP datagram.
type Packet struct {
	Kind    wgpacket.Kind
	Payload []byte
}

// Endpoint reads and writes one side of a WireGuard UDP data path.
type Endpoint interface {
	// Read returns one accepted packet with a caller-owned payload. Read calls on the same endpoint must not overlap.
	Read(context.Context) (Packet, error)
	// Write synchronously delivers one accepted WireGuard packet before deadline, or only the context deadline when
	// deadline is zero. It must not retain payload after returning.
	Write(context.Context, []byte, time.Time) error
	Close() error
}

// copyAcceptedPacket validates one complete read and returns an exact owned WireGuard packet.
func copyAcceptedPacket(buffer []byte, length int) (Packet, bool) {
	if length > protocol.MaxPacketSize {
		return Packet{}, false
	}
	kind := wgpacket.Classify(buffer[:length])
	if !kind.Accepted() {
		return Packet{}, false
	}
	payload := make([]byte, length)
	copy(payload, buffer[:length])
	return Packet{Kind: kind, Payload: payload}, true
}

// isSoftNetworkError reports per-datagram failures that do not invalidate a reusable UDP socket.
func isSoftNetworkError(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EMSGSIZE) || errors.Is(err, syscall.ENOBUFS) {
		return true
	}
	networkError, ok := errors.AsType[net.Error](err)
	return ok && networkError.Timeout()
}

// earlierDeadline returns the earlier nonzero deadline imposed by ctx or maximum.
func earlierDeadline(ctx context.Context, maximum time.Time) time.Time {
	deadline, ok := ctx.Deadline()
	if ok && (maximum.IsZero() || deadline.Before(maximum)) {
		return deadline
	}
	return maximum
}
