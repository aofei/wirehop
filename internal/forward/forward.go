// Package forward directly forwards WireGuard UDP packets between local and target endpoints.
package forward

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

const (
	// defaultUDPWriteTimeout bounds one direct UDP delivery operation.
	defaultUDPWriteTimeout = time.Second
)

var (
	// ErrInvalidConfig indicates a missing or invalid forwarding endpoint.
	ErrInvalidConfig = errors.New("invalid forward configuration")
)

// Config defines one local listener and upstream WireGuard target.
type Config struct {
	Listen             netip.AddrPort
	Target             target.Endpoint
	Reserved           wgpacket.Reserved
	Resolver           target.Resolver
	TargetListenConfig net.ListenConfig
}

// Forwarder owns one direct bidirectional WireGuard UDP forwarding path.
type Forwarder struct {
	local     datagram.Endpoint
	remote    *datagram.Remote
	localAddr netip.AddrPort
	cancel    context.CancelFunc
	done      chan struct{}
	stopOnce  sync.Once
	err       error
}

// Start validates config, prepares both UDP endpoints, and starts forwarding.
func Start(parent context.Context, config Config) (*Forwarder, error) {
	if !validListenAddress(config.Listen) || !config.Target.Valid() {
		return nil, ErrInvalidConfig
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	local, err := datagram.ListenLocal(config.Listen)
	if err != nil {
		cancel()
		return nil, err
	}
	remote, err := datagram.OpenRemote(ctx, config.Target, datagram.RemoteConfig{
		Resolver: config.Resolver, ListenConfig: config.TargetListenConfig,
	})
	if err != nil {
		cancel()
		local.Close()
		return nil, err
	}
	forwarder := &Forwarder{
		local: datagram.WithReservedTranslation(local, config.Reserved), remote: remote,
		localAddr: local.LocalAddr(), cancel: cancel, done: make(chan struct{}),
	}
	go forwarder.run(ctx)
	return forwarder, nil
}

// LocalAddr returns the bound local WireGuard endpoint.
func (f *Forwarder) LocalAddr() netip.AddrPort {
	return f.localAddr
}

// Wait blocks until forwarding ends and returns its terminal result.
func (f *Forwarder) Wait() error {
	<-f.done
	return f.err
}

// Close stops forwarding and waits for both UDP directions to exit.
func (f *Forwarder) Close() error {
	f.stop()
	<-f.done
	return nil
}

// run owns both forwarding workers and preserves their first terminal result.
func (f *Forwarder) run(ctx context.Context) {
	results := make(chan error, 2)
	go func() { results <- copyPackets(ctx, "local", f.local, f.remote) }()
	go func() { results <- copyPackets(ctx, "target", f.remote, f.local) }()
	err := <-results
	f.stop()
	<-results
	f.err = err
	close(f.done)
}

// stop cancels forwarding and closes both UDP endpoints exactly once.
func (f *Forwarder) stop() {
	f.stopOnce.Do(func() {
		f.cancel()
		f.local.Close()
		f.remote.Close()
	})
}

// copyPackets synchronously forwards accepted packets until one endpoint or the context fails.
func copyPackets(ctx context.Context, sourceName string, source, destination datagram.Endpoint) error {
	for {
		packet, err := source.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read %s UDP endpoint: %w", sourceName, err)
		}
		if err := destination.Write(ctx, packet.Payload, time.Now().Add(defaultUDPWriteTimeout)); err != nil {
			if errors.Is(err, datagram.ErrNoLocalPeer) || errors.Is(err, datagram.ErrDatagramDropped) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("forward %s UDP packet: %w", sourceName, err)
		}
	}
}

// validListenAddress reports whether address is a canonical IP literal bind address.
func validListenAddress(address netip.AddrPort) bool {
	return address.IsValid() && address.Addr() == address.Addr().Unmap() && !address.Addr().IsMulticast()
}
