// Package datagram owns WireGuard-facing UDP endpoint behavior.
package datagram

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aofei/wirehop/internal/netsetup"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

const (
	// MaximumBatchSize bounds opportunistic packet vectors throughout the relay data path.
	MaximumBatchSize = 16
	// pooledPacketClassCount is the number of retained packet-buffer size classes.
	pooledPacketClassCount = 12
)

// pooledPacketSizes cover common WireGuard datagrams before progressing to larger bounded capacities.
var pooledPacketSizes = [pooledPacketClassCount]int{64, 96, 160, 256, 512, 1024, 1536, 2048, 4096, 8192, 16384, 32768}

var (
	// ErrNoLocalPeer indicates that no WireGuard process has sent a local datagram yet.
	ErrNoLocalPeer = errors.New("no local UDP peer")
	// ErrDatagramDropped indicates a per-datagram rejection or failure that does not invalidate the UDP endpoint.
	ErrDatagramDropped = errors.New("UDP datagram dropped")
	// readBufferPool includes one sentinel byte so unsupported oversized datagrams cannot appear valid after truncation.
	readBufferPool = sync.Pool{New: func() any { return new([protocol.MaxPacketSize + 1]byte) }}
	// packetBufferPools retain ordinary owned packet capacities by size class.
	packetBufferPools = newPacketBufferPools()
)

// packetBuffer is one reference-counted owned UDP payload allocation.
type packetBuffer struct {
	storage []byte
	refs    atomic.Int32
	class   int
}

// Packet is one accepted WireGuard-looking UDP datagram.
type Packet struct {
	Kind    wgpacket.Kind
	Payload []byte
	buffer  *packetBuffer
}

// Retain returns another ownership reference to p.
func (p Packet) Retain() Packet {
	if p.buffer != nil {
		p.buffer.retain()
	}
	return p
}

// Release relinquishes one ownership reference and clears p.
func (p *Packet) Release() {
	buffer := p.buffer
	*p = Packet{}
	if buffer == nil {
		return
	}
	if buffer.release() && buffer.class >= 0 {
		packetBufferPools[buffer.class].Put(buffer)
	}
}

// retain adds one live reference without reviving a returned pool object.
func (b *packetBuffer) retain() {
	for {
		refs := b.refs.Load()
		if refs == 0 {
			panic("retain released UDP packet")
		}
		if b.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

// release removes one live reference and reports whether the buffer became unowned.
func (b *packetBuffer) release() bool {
	for {
		refs := b.refs.Load()
		if refs == 0 {
			panic("release unowned UDP packet")
		}
		if b.refs.CompareAndSwap(refs, refs-1) {
			return refs == 1
		}
	}
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

// readBatchEndpoint extends [Endpoint] with ordered opportunistic vector reads. It transfers the returned packet prefix
// to the caller even when it also returns an error.
type readBatchEndpoint interface {
	ReadBatch(context.Context, []Packet) (int, error)
}

// writeBatchEndpoint extends [Endpoint] with ordered vector writes. It reports the successfully written prefix and must
// consume every payload when it returns a nil error.
type writeBatchEndpoint interface {
	WriteBatch(context.Context, [][]byte, time.Time) (int, error)
}

// udpMessage describes one UDP payload and peer.
type udpMessage struct {
	payload []byte
	peer    netip.AddrPort
}

// udpReadBatch retains one borrowed vector of received UDP messages.
type udpReadBatch interface {
	messages() []udpMessage
	release()
}

// udpBatchConn provides platform-specific nonblocking receive draining and vector writes.
type udpBatchConn interface {
	readAvailable(int) (udpReadBatch, error)
	write([]udpMessage) (int, error)
}

// ReadBatch reads one or more packets while falling back to scalar endpoint I/O. It transfers ownership of the returned
// packet prefix to the caller even when it also returns an error.
func ReadBatch(ctx context.Context, endpoint Endpoint, packets []Packet) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	if batch, ok := endpoint.(readBatchEndpoint); ok {
		return batch.ReadBatch(ctx, packets)
	}
	packet, err := endpoint.Read(ctx)
	if err != nil {
		return 0, err
	}
	packets[0] = packet
	return 1, nil
}

// WriteBatch writes an ordered successful prefix and reports the first failed packet. A nil error means every payload
// was written.
func WriteBatch(ctx context.Context, endpoint Endpoint, payloads [][]byte, deadline time.Time) (int, error) {
	if len(payloads) == 0 {
		return 0, nil
	}
	if batch, ok := endpoint.(writeBatchEndpoint); ok {
		return batch.WriteBatch(ctx, payloads, deadline)
	}
	for index, payload := range payloads {
		if err := endpoint.Write(ctx, payload, deadline); err != nil {
			return index, err
		}
	}
	return len(payloads), nil
}

// copyAcceptedPacket validates one complete read and returns an owned WireGuard packet.
func copyAcceptedPacket(buffer []byte, length int) (Packet, bool) {
	if length > protocol.MaxPacketSize {
		return Packet{}, false
	}
	kind := wgpacket.Classify(buffer[:length])
	if !kind.Accepted() {
		return Packet{}, false
	}
	return ownPacket(kind, buffer[:length]), true
}

// ownPacket copies payload into an owned size-classed packet buffer.
func ownPacket(kind wgpacket.Kind, payload []byte) Packet {
	class := packetBufferClass(len(payload))
	var buffer *packetBuffer
	if class >= 0 {
		buffer = packetBufferPools[class].Get().(*packetBuffer)
	} else {
		buffer = &packetBuffer{storage: make([]byte, len(payload)), class: -1}
	}
	buffer.refs.Store(1)
	copy(buffer.storage, payload)
	return Packet{Kind: kind, Payload: buffer.storage[:len(payload)], buffer: buffer}
}

// packetBufferClass returns the smallest retained capacity that can hold size.
func packetBufferClass(size int) int {
	for class, capacity := range pooledPacketSizes {
		if size <= capacity {
			return class
		}
	}
	return -1
}

// newPacketBufferPools creates size-classed pools without copying a live [sync.Pool].
func newPacketBufferPools() [pooledPacketClassCount]*sync.Pool {
	var pools [pooledPacketClassCount]*sync.Pool
	for class := range pools {
		size := pooledPacketSizes[class]
		pools[class] = &sync.Pool{New: func() any {
			return &packetBuffer{storage: make([]byte, size), class: class}
		}}
	}
	return pools
}

// isSoftNetworkError reports per-datagram failures that do not invalidate a reusable UDP socket.
func isSoftNetworkError(err error) bool {
	if errno, ok := netsetup.SocketErrno(err); ok {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EHOSTUNREACH, syscall.ENETUNREACH,
			syscall.EMSGSIZE, syscall.ENOBUFS, syscall.ENOMEM, syscall.EACCES, syscall.EPERM, syscall.ENETDOWN,
			syscall.EHOSTDOWN, syscall.ETIMEDOUT:
			return true
		case syscall.EINVAL:
			// Linux blackhole routes return EINVAL even for a valid datagram on a reusable socket.
			return runtime.GOOS == "linux"
		}
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
