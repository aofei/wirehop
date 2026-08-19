package datagram

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

// Local is a UDP listener that tracks the latest valid local WireGuard source.
type Local struct {
	conn          *net.UDPConn
	readInterrupt readInterrupt
	mu            sync.RWMutex
	peer          netip.AddrPort
}

// ListenLocal binds a local WireGuard UDP listener.
func ListenLocal(address netip.AddrPort) (*Local, error) {
	conn, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(address))
	if err != nil {
		return nil, fmt.Errorf("bind local UDP listener: %w", err)
	}
	return &Local{conn: conn}, nil
}

// NewLocal wraps an already bound local WireGuard UDP listener.
func NewLocal(conn *net.UDPConn) *Local {
	return &Local{conn: conn}
}

// Read returns the next structurally valid local WireGuard datagram and remembers its source.
func (e *Local) Read(ctx context.Context) (Packet, error) {
	if err := e.readInterrupt.prepare(ctx, e.conn); err != nil {
		return Packet{}, err
	}
	pooled := readBufferPool.Get().(*[protocol.MaxPacketSize + 1]byte)
	defer readBufferPool.Put(pooled)
	buffer := pooled[:]
	for {
		length, peer, err := e.conn.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return Packet{}, ctx.Err()
			}
			if isSoftNetworkError(err) {
				continue
			}
			return Packet{}, fmt.Errorf("read local UDP datagram: %w", err)
		}
		packet, ok := copyAcceptedPacket(buffer, length)
		if !ok {
			continue
		}
		e.mu.Lock()
		e.peer = peer
		e.mu.Unlock()
		return packet, nil
	}
}

// Write writes one reply to the latest valid local WireGuard source by deadline.
func (e *Local) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.RLock()
	peer := e.peer
	e.mu.RUnlock()
	if !peer.IsValid() {
		return ErrNoLocalPeer
	}
	if err := e.conn.SetWriteDeadline(earlierDeadline(ctx, deadline)); err != nil {
		return fmt.Errorf("set local UDP write deadline: %w", err)
	}
	written, err := e.conn.WriteToUDPAddrPort(payload, peer)
	if err != nil {
		if isSoftNetworkError(err) {
			return fmt.Errorf("%w: write local UDP datagram: %w", ErrDatagramDropped, err)
		}
		return fmt.Errorf("write local UDP datagram: %w", err)
	}
	if written != len(payload) {
		return fmt.Errorf("write local UDP datagram: %w", io.ErrShortWrite)
	}
	return nil
}

// LocalAddr returns the bound local WireGuard UDP listener endpoint.
func (e *Local) LocalAddr() netip.AddrPort {
	return e.conn.LocalAddr().(*net.UDPAddr).AddrPort()
}

// Close closes the local WireGuard UDP listener.
func (e *Local) Close() error {
	e.readInterrupt.close()
	return e.conn.Close()
}

// readInterrupt retains cancellation and deadline state across reads that share a context.
type readInterrupt struct {
	mu          sync.Mutex
	done        <-chan struct{}
	stop        func() bool
	deadline    time.Time
	deadlineSet bool
	closed      bool
}

// prepare installs changed cancellation state and applies a changed read deadline.
func (i *readInterrupt) prepare(ctx context.Context, conn *net.UDPConn) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	done := ctx.Done()
	deadline, _ := ctx.Deadline()
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return net.ErrClosed
	}
	if i.done != done {
		if i.stop != nil {
			i.stop()
		}
		i.done = done
		i.stop = nil
		i.deadlineSet = false
		if done != nil {
			i.stop = context.AfterFunc(ctx, func() { i.interrupt(conn, done) })
		}
	}
	if !i.deadlineSet || !i.deadline.Equal(deadline) {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return fmt.Errorf("set UDP read deadline: %w", err)
		}
		i.deadline = deadline
		i.deadlineSet = true
	}
	if err := ctx.Err(); err != nil {
		conn.SetReadDeadline(time.Now())
		i.deadlineSet = false
		return err
	}
	return nil
}

// interrupt advances the socket deadline only for the context that remains current.
func (i *readInterrupt) interrupt(conn *net.UDPConn, done <-chan struct{}) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.done != done {
		return
	}
	conn.SetReadDeadline(time.Now())
	i.deadlineSet = false
}

// close prevents future hooks and releases the retained context registration.
func (i *readInterrupt) close() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	i.done = nil
	i.deadlineSet = false
	if i.stop != nil {
		i.stop()
		i.stop = nil
	}
}
