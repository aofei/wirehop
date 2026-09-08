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
	"github.com/aofei/wirehop/internal/wgpacket"
)

// Local is a UDP listener that tracks the latest valid local WireGuard source.
type Local struct {
	conn          *net.UDPConn
	batch         udpBatchConn
	readInterrupt readInterrupt
	writeMu       sync.Mutex
	writeMessages [MaximumBatchSize]udpMessage
	mu            sync.RWMutex
	peer          netip.AddrPort
}

// ListenLocal binds a local WireGuard UDP listener.
func ListenLocal(address netip.AddrPort) (*Local, error) {
	conn, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(address))
	if err != nil {
		return nil, fmt.Errorf("bind local UDP listener: %w", err)
	}
	return NewLocal(conn), nil
}

// NewLocal wraps an already bound local WireGuard UDP listener.
func NewLocal(conn *net.UDPConn) *Local {
	return &Local{conn: conn, batch: newUDPBatchConn(conn)}
}

// Read returns the next structurally valid local WireGuard datagram and remembers its source.
func (e *Local) Read(ctx context.Context) (Packet, error) {
	packet, peer, err := e.readOne(ctx)
	if err != nil {
		return Packet{}, err
	}
	e.rememberPeer(peer)
	return packet, nil
}

// ReadBatch returns one blocking packet plus currently queued valid local WireGuard datagrams.
func (e *Local) ReadBatch(ctx context.Context, packets []Packet) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	packet, latestPeer, err := e.readOne(ctx)
	if err != nil {
		return 0, err
	}
	packets[0] = packet
	count := 1
	var drainErr error
	if e.batch != nil && len(packets) > 1 {
		var batch udpReadBatch
		batch, drainErr = e.batch.readAvailable(len(packets) - 1)
		if batch != nil {
			for _, message := range batch.messages() {
				kind := wgpacket.Classify(message.payload)
				if !kind.Accepted() {
					continue
				}
				packets[count] = ownPacket(kind, message.payload)
				count++
				latestPeer = message.peer
			}
			batch.release()
		}
	}
	if isSoftNetworkError(drainErr) {
		drainErr = nil
	}
	e.rememberPeer(latestPeer)
	return count, drainErr
}

// readOne returns one blocking accepted packet and its source.
func (e *Local) readOne(ctx context.Context) (Packet, netip.AddrPort, error) {
	if err := e.readInterrupt.prepare(ctx, e.conn); err != nil {
		return Packet{}, netip.AddrPort{}, err
	}
	pooled := readBufferPool.Get().(*[protocol.MaxPacketSize + 1]byte)
	defer readBufferPool.Put(pooled)
	buffer := pooled[:]
	for {
		length, peer, err := e.conn.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return Packet{}, netip.AddrPort{}, ctx.Err()
			}
			if isSoftNetworkError(err) {
				continue
			}
			return Packet{}, netip.AddrPort{}, fmt.Errorf("read local UDP datagram: %w", err)
		}
		packet, ok := copyAcceptedPacket(buffer, length)
		if ok {
			return packet, peer, nil
		}
	}
}

// rememberPeer records the latest accepted local WireGuard source.
func (e *Local) rememberPeer(peer netip.AddrPort) {
	e.mu.Lock()
	e.peer = peer
	e.mu.Unlock()
}

// Write writes one reply to the latest valid local WireGuard source by deadline.
func (e *Local) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	written, err := e.WriteBatch(ctx, [][]byte{payload}, deadline)
	if err != nil {
		return err
	}
	if written != 1 {
		return fmt.Errorf("write local UDP datagram: %w", io.ErrShortWrite)
	}
	return nil
}

// WriteBatch writes an ordered packet prefix to the latest valid local WireGuard source.
func (e *Local) WriteBatch(ctx context.Context, payloads [][]byte, deadline time.Time) (int, error) {
	if len(payloads) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.RLock()
	peer := e.peer
	e.mu.RUnlock()
	if !peer.IsValid() {
		return 0, ErrNoLocalPeer
	}
	if err := e.conn.SetWriteDeadline(earlierDeadline(ctx, deadline)); err != nil {
		return 0, fmt.Errorf("set local UDP write deadline: %w", err)
	}
	written := 0
	for written < len(payloads) {
		count := min(MaximumBatchSize, len(payloads)-written)
		if e.batch == nil {
			for index := range count {
				payload := payloads[written+index]
				length, err := e.conn.WriteToUDPAddrPort(payload, peer)
				if err != nil {
					if isSoftNetworkError(err) {
						return written + index, fmt.Errorf("%w: write local UDP datagram: %w", ErrDatagramDropped, err)
					}
					return written + index, fmt.Errorf("write local UDP datagram: %w", err)
				}
				if length != len(payload) {
					return written + index, fmt.Errorf("write local UDP datagram: %w", io.ErrShortWrite)
				}
			}
			written += count
			continue
		}
		messages := e.writeMessages[:count]
		for index := range count {
			messages[index] = udpMessage{payload: payloads[written+index], peer: peer}
		}
		countWritten, err := e.batch.write(messages)
		clear(messages)
		written += countWritten
		if err != nil {
			if isSoftNetworkError(err) {
				return written, fmt.Errorf("%w: write local UDP datagram batch: %w", ErrDatagramDropped, err)
			}
			return written, fmt.Errorf("write local UDP datagram batch: %w", err)
		}
	}
	return written, nil
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
