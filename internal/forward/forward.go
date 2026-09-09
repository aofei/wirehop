// Package forward directly forwards WireGuard UDP packets between local and target endpoints.
package forward

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/netsetup"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

// defaultUDPWriteTimeout bounds one direct UDP delivery operation.
const defaultUDPWriteTimeout = time.Second

// ErrInvalidConfig indicates a missing or invalid forwarding endpoint.
var ErrInvalidConfig = errors.New("invalid forward configuration")

// Config defines one local listener and upstream WireGuard target.
type Config struct {
	Listen             netip.AddrPort
	Target             target.Endpoint
	Reserved           wgpacket.Reserved
	Resolver           target.Resolver
	TargetListenConfig net.ListenConfig
	Logger             *slog.Logger
}

// Forwarder owns one direct bidirectional WireGuard UDP forwarding path.
type Forwarder struct {
	local     datagram.Endpoint
	localAddr netip.AddrPort
	cancel    context.CancelFunc
	ready     chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	err       error
}

// Start binds the local UDP endpoint and prepares the target asynchronously.
func Start(parent context.Context, config Config) (*Forwarder, error) {
	if !validListenAddress(config.Listen) || !config.Target.Valid() {
		return nil, ErrInvalidConfig
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	local, err := datagram.ListenLocal(config.Listen, config.Logger)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	forwarder := &Forwarder{
		local: datagram.WithReservedTranslation(local, config.Reserved), localAddr: local.LocalAddr(),
		cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}),
	}
	go forwarder.run(ctx, config)
	return forwarder, nil
}

// LocalAddr returns the bound local WireGuard endpoint.
func (f *Forwarder) LocalAddr() netip.AddrPort {
	return f.localAddr
}

// WaitReady waits until the target is resolved and its UDP socket is prepared, or forwarding ends.
func (f *Forwarder) WaitReady(ctx context.Context) error {
	select {
	case <-f.ready:
		return nil
	case <-f.done:
		return f.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait blocks until forwarding ends and returns its terminal result.
func (f *Forwarder) Wait() error {
	<-f.done
	return f.err
}

// Close stops forwarding and waits for target preparation and both UDP directions to exit.
func (f *Forwarder) Close() error {
	f.stop()
	<-f.done
	return nil
}

// run owns target preparation and both forwarding workers.
func (f *Forwarder) run(parent context.Context, config Config) {
	defer close(f.done)
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	var destination atomic.Pointer[datagram.Remote]
	results := make(chan error, 2)
	go func() {
		err := f.copyLocalPackets(ctx, &destination)
		cancel(err)
		results <- err
	}()
	var remote *datagram.Remote
	err := netsetup.RetryDNS(ctx, config.Logger, func() error {
		var err error
		remote, err = datagram.OpenRemote(ctx, config.Target, datagram.RemoteConfig{
			Resolver: config.Resolver, ListenConfig: config.TargetListenConfig, Logger: config.Logger,
		})
		return err
	})
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		f.stop()
		<-results
		f.err = fmt.Errorf("prepare forward target: %w", err)
		return
	}
	destination.Store(remote)
	close(f.ready)
	go func() { results <- copyPackets(ctx, "target", remote, f.local) }()
	err = <-results
	if cause := context.Cause(ctx); cause != nil {
		err = cause
	}
	f.stop()
	remote.Close()
	<-results
	f.err = err
}

// stop cancels forwarding and closes the local UDP endpoint exactly once.
func (f *Forwarder) stop() {
	f.stopOnce.Do(func() {
		f.cancel()
		f.local.Close()
	})
}

// copyLocalPackets discards traffic until target preparation finishes, then forwards without readiness checks.
func (f *Forwarder) copyLocalPackets(ctx context.Context, destination *atomic.Pointer[datagram.Remote]) error {
	var packets [datagram.MaximumBatchSize]datagram.Packet
	var payloads [datagram.MaximumBatchSize][]byte
	for {
		count, err := datagram.ReadBatch(ctx, f.local, packets[:])
		remote := destination.Load()
		if remote == nil {
			for index := range count {
				packets[index].Release()
			}
		} else if writeErr := writePackets(ctx, "local", remote, packets[:count], payloads[:count]); writeErr != nil {
			return writeErr
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read local UDP endpoint: %w", err)
		}
		if remote != nil {
			return copyPackets(ctx, "local", f.local, remote)
		}
	}
}

// copyPackets synchronously forwards accepted packets until one endpoint or the context fails.
func copyPackets(ctx context.Context, sourceName string, source, destination datagram.Endpoint) error {
	var packets [datagram.MaximumBatchSize]datagram.Packet
	var payloads [datagram.MaximumBatchSize][]byte
	for {
		count, readErr := datagram.ReadBatch(ctx, source, packets[:])
		if err := writePackets(ctx, sourceName, destination, packets[:count], payloads[:count]); err != nil {
			return err
		}
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read %s UDP endpoint: %w", sourceName, readErr)
		}
	}
}

// writePackets delivers one batch and releases every packet, including intentionally dropped datagrams.
func writePackets(ctx context.Context, sourceName string, destination datagram.Endpoint,
	packets []datagram.Packet, payloads [][]byte) error {
	for index := range packets {
		payloads[index] = packets[index].Payload
	}
	offset := 0
	deadline := time.Now().Add(defaultUDPWriteTimeout)
	for offset < len(packets) {
		written, err := datagram.WriteBatch(ctx, destination, payloads[offset:], deadline)
		for index := offset; index < offset+written; index++ {
			packets[index].Release()
			payloads[index] = nil
		}
		offset += written
		if err == nil {
			continue
		}
		if errors.Is(err, datagram.ErrNoLocalPeer) || errors.Is(err, datagram.ErrDatagramDropped) {
			packets[offset].Release()
			payloads[offset] = nil
			offset++
			continue
		}
		for index := offset; index < len(packets); index++ {
			packets[index].Release()
			payloads[index] = nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("forward %s UDP packet: %w", sourceName, err)
	}
	return nil
}

// validListenAddress reports whether address is a canonical IP literal bind address.
func validListenAddress(address netip.AddrPort) bool {
	return address.IsValid() && address.Addr() == address.Addr().Unmap() && !address.Addr().IsMulticast()
}
