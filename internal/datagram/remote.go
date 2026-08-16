package datagram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

const (
	// targetResolveTimeout bounds one server-side DNS refresh.
	targetResolveTimeout = 5 * time.Second
	// targetRefreshInterval bounds DNS query frequency during WireGuard handshake retries.
	targetRefreshInterval = 5 * time.Second
	// targetCandidateGrace permits replies to handshakes sent just before a DNS candidate replacement.
	targetCandidateGrace = 15 * time.Second
	// targetRouteLifetime retains public WireGuard index affinity for one key lifetime.
	targetRouteLifetime = 3 * time.Minute
	// maximumTargetRoutes bounds unauthenticated public index state for one authorized session.
	maximumTargetRoutes = 1024
)

// targetRoute binds one public WireGuard index to the DNS candidate that emitted it.
type targetRoute struct {
	address netip.AddrPort
	expires time.Time
}

// remoteRead is one accepted target packet or terminal socket read failure.
type remoteRead struct {
	packet Packet
	err    error
}

// Remote is a server-side logical target with DNS candidates and WireGuard index affinity.
type Remote struct {
	target          target.Endpoint
	resolver        target.Resolver
	resolveTimeout  time.Duration
	refreshInterval time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	reads           chan remoteRead
	writeMu         sync.Mutex
	mu              sync.Mutex
	sockets         [2]*net.UDPConn
	candidates      []netip.AddrPort
	recent          map[netip.AddrPort]time.Time
	retained        map[netip.AddrPort]int
	current         netip.AddrPort
	handshakeRoutes map[uint32]targetRoute
	transportRoutes map[uint32]targetRoute
	nextRoutePrune  time.Time
	lastRefresh     time.Time
	refreshing      bool
	closed          bool
	workers         sync.WaitGroup
}

// OpenRemote resolves target and opens the required per-family UDP sockets.
func OpenRemote(parent context.Context, endpoint target.Endpoint, resolver target.Resolver) (*Remote, error) {
	return openRemote(parent, endpoint, resolver, targetResolveTimeout, targetRefreshInterval)
}

func openRemote(parent context.Context, endpoint target.Endpoint, resolver target.Resolver,
	resolveTimeout, refreshInterval time.Duration) (*Remote, error) {
	if parent == nil || resolveTimeout <= 0 || refreshInterval <= 0 {
		return nil, target.ErrInvalid
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	resolveContext, cancelResolve := context.WithTimeout(parent, resolveTimeout)
	addresses, err := target.Resolve(resolveContext, resolver, endpoint)
	cancelResolve()
	if err != nil {
		return nil, err
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	remote := &Remote{
		target: endpoint, resolver: resolver, resolveTimeout: resolveTimeout, refreshInterval: refreshInterval,
		ctx: ctx, cancel: cancel, reads: make(chan remoteRead),
		recent:          make(map[netip.AddrPort]time.Time),
		retained:        make(map[netip.AddrPort]int),
		handshakeRoutes: make(map[uint32]targetRoute), transportRoutes: make(map[uint32]targetRoute),
		lastRefresh: time.Now(),
	}
	if err := remote.ensureSockets(addresses); err != nil {
		cancel()
		return nil, err
	}
	remote.replaceCandidates(addresses)
	return remote, nil
}

// Read returns the next structurally valid WireGuard datagram from an authorized target candidate.
func (e *Remote) Read(ctx context.Context) (Packet, error) {
	select {
	case result := <-e.reads:
		return result.packet, result.err
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	case <-e.ctx.Done():
		return Packet{}, e.ctx.Err()
	}
}

// Write routes one WireGuard datagram to its indexed candidate or bounded handshake fan-out set.
func (e *Remote) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	header := wgpacket.Inspect(payload)
	if !header.Kind.Accepted() {
		return fmt.Errorf("write target UDP datagram: unsupported WireGuard packet")
	}
	if header.Kind == wgpacket.HandshakeInitiation {
		e.triggerRefresh()
	}
	var destinationBuffer [target.MaxCandidates]netip.AddrPort
	destinations := e.destinations(header, destinationBuffer[:0])
	if len(destinations) == 0 {
		return fmt.Errorf("%w: no target candidate", ErrDatagramDropped)
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	var lastErr error
	var terminalErr error
	succeeded := false
	for _, destination := range destinations {
		connection := e.socket(destination.Addr())
		if connection == nil {
			lastErr = fmt.Errorf("no UDP socket for %s target candidate", addressFamily(destination.Addr()))
			e.triggerRefresh()
			continue
		}
		if err := connection.SetWriteDeadline(earlierDeadline(ctx, deadline)); err != nil {
			lastErr = err
			terminalErr = err
			e.triggerRefresh()
			continue
		}
		written, err := connection.WriteToUDPAddrPort(payload, destination)
		if err != nil {
			lastErr = err
			e.triggerRefresh()
			if !isSoftNetworkError(err) {
				terminalErr = err
			}
			continue
		}
		if written != len(payload) {
			lastErr = io.ErrShortWrite
			terminalErr = io.ErrShortWrite
			continue
		}
		succeeded = true
	}
	if succeeded {
		return nil
	}
	if terminalErr != nil {
		return fmt.Errorf("write target UDP datagram: %w", terminalErr)
	}
	return fmt.Errorf("%w: write target UDP datagram: %w", ErrDatagramDropped, lastErr)
}

// Close stops resolution and closes all target sockets.
func (e *Remote) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.cancel()
	sockets := e.sockets
	e.sockets = [2]*net.UDPConn{}
	e.mu.Unlock()
	var errs []error
	for _, connection := range sockets {
		if connection != nil {
			errs = append(errs, connection.Close())
		}
	}
	e.workers.Wait()
	return errors.Join(errs...)
}

// ensureSockets opens one source socket for each candidate address family not already represented.
func (e *Remote) ensureSockets(addresses []netip.AddrPort) error {
	var required [2]bool
	for _, address := range addresses {
		required[familyIndex(address.Addr())] = true
	}
	var firstErr error
	for index, needed := range required {
		if !needed {
			continue
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return net.ErrClosed
		}
		alreadyOpen := e.sockets[index] != nil
		e.mu.Unlock()
		if alreadyOpen {
			continue
		}
		network := "udp4"
		address := &net.UDPAddr{IP: net.IPv4zero}
		if index == 1 {
			network = "udp6"
			address = &net.UDPAddr{IP: net.IPv6unspecified}
		}
		connection, err := net.ListenUDP(network, address)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("open %s target UDP socket: %w", network, err)
			}
			continue
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			connection.Close()
			return net.ErrClosed
		}
		if e.sockets[index] != nil {
			e.mu.Unlock()
			connection.Close()
			continue
		}
		e.sockets[index] = connection
		e.workers.Add(1)
		e.mu.Unlock()
		go e.readSocket(connection)
	}
	e.mu.Lock()
	usable := required[0] && e.sockets[0] != nil || required[1] && e.sockets[1] != nil
	e.mu.Unlock()
	if !usable {
		if firstErr == nil {
			firstErr = errors.New("no UDP socket for target candidate address families")
		}
		return firstErr
	}
	return nil
}

// readSocket receives and filters one address family's target traffic.
func (e *Remote) readSocket(connection *net.UDPConn) {
	defer e.workers.Done()
	pooled := readBufferPool.Get().(*[protocol.MaxPacketSize + 1]byte)
	defer readBufferPool.Put(pooled)
	buffer := pooled[:]
	for {
		length, peer, err := connection.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if e.ctx.Err() != nil {
				return
			}
			if isSoftNetworkError(err) {
				e.triggerRefresh()
				continue
			}
			available := e.retireSocket(connection)
			connection.Close()
			e.triggerRefresh()
			if !available {
				e.offerRead(remoteRead{err: fmt.Errorf("read target UDP datagram: %w", err)})
			}
			return
		}
		if length > protocol.MaxPacketSize {
			continue
		}
		header := wgpacket.Inspect(buffer[:length])
		if !header.Kind.Accepted() || !e.observeSource(peer, header) {
			continue
		}
		payload := make([]byte, length)
		copy(payload, buffer[:length])
		e.offerRead(remoteRead{packet: Packet{Kind: header.Kind, Payload: payload}})
	}
}

// retireSocket removes one failed family socket and reports whether another family remains usable.
func (e *Remote) retireSocket(connection *net.UDPConn) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for index, candidate := range e.sockets {
		if candidate == connection {
			e.sockets[index] = nil
			break
		}
	}
	return e.sockets[0] != nil || e.sockets[1] != nil
}

// offerRead publishes one target read unless the endpoint has closed.
func (e *Remote) offerRead(result remoteRead) {
	select {
	case e.reads <- result:
	case <-e.ctx.Done():
	}
}

// observeSource accepts only resolved or retained candidates and records their public WireGuard indexes.
func (e *Remote) observeSource(peer netip.AddrPort, header wgpacket.Header) bool {
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneRoutesLocked(now)
	if !e.acceptsSourceLocked(peer, now) {
		return false
	}
	switch header.Kind {
	case wgpacket.HandshakeInitiation:
		e.rememberRouteLocked(e.handshakeRoutes, header.SenderIndex, peer, now)
	case wgpacket.HandshakeResponse:
		e.rememberRouteLocked(e.transportRoutes, header.SenderIndex, peer, now)
	}
	return true
}

// destinations appends handshake fan-out or one public-index-affine target candidate to buffer.
func (e *Remote) destinations(header wgpacket.Header, buffer []netip.AddrPort) []netip.AddrPort {
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneRoutesLocked(now)
	var route targetRoute
	var found bool
	switch header.Kind {
	case wgpacket.HandshakeInitiation:
		return append(buffer, e.candidates...)
	case wgpacket.HandshakeResponse, wgpacket.CookieReply:
		route, found = e.handshakeRoutes[header.ReceiverIndex]
	case wgpacket.TransportData:
		route, found = e.transportRoutes[header.ReceiverIndex]
		if !found {
			route, found = e.handshakeRoutes[header.ReceiverIndex]
		}
	}
	if found {
		e.current = route.address
		return append(buffer, route.address)
	}
	if header.Kind == wgpacket.HandshakeResponse || header.Kind == wgpacket.CookieReply {
		return nil
	}
	if e.current.IsValid() {
		return append(buffer, e.current)
	}
	return nil
}

// triggerRefresh starts one rate-limited DNS refresh without delaying the current packet.
func (e *Remote) triggerRefresh() {
	if !e.target.IsDomain() {
		return
	}
	now := time.Now()
	e.mu.Lock()
	if e.closed || e.refreshing || now.Sub(e.lastRefresh) < e.refreshInterval {
		e.mu.Unlock()
		return
	}
	e.refreshing = true
	e.lastRefresh = now
	e.workers.Add(1)
	e.mu.Unlock()
	go e.refresh()
}

// refresh replaces the fan-out candidates after one successful bounded lookup.
func (e *Remote) refresh() {
	defer e.workers.Done()
	ctx, cancel := context.WithTimeout(e.ctx, e.resolveTimeout)
	addresses, err := target.Resolve(ctx, e.resolver, e.target)
	cancel()
	if err == nil {
		err = e.ensureSockets(addresses)
	}
	if err == nil {
		e.replaceCandidates(addresses)
	}
	e.mu.Lock()
	e.refreshing = false
	e.mu.Unlock()
}

// replaceCandidates atomically installs one successful DNS result while retaining established affinity.
func (e *Remote) replaceCandidates(addresses []netip.AddrPort) {
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneRecentLocked(now)
	for _, previous := range e.candidates {
		if !slices.Contains(addresses, previous) {
			e.recent[previous] = now.Add(targetCandidateGrace)
		}
	}
	for _, address := range addresses {
		delete(e.recent, address)
	}
	e.candidates = append(e.candidates[:0], addresses...)
	if !e.current.IsValid() || e.socketLocked(e.current.Addr()) == nil {
		e.current = netip.AddrPort{}
		for _, address := range addresses {
			if e.socketLocked(address.Addr()) != nil {
				e.current = address
				break
			}
		}
	}
}

// rememberRouteLocked inserts or refreshes one bounded index route.
func (e *Remote) rememberRouteLocked(routes map[uint32]targetRoute, index uint32, address netip.AddrPort,
	now time.Time) {
	previous, exists := routes[index]
	if !exists && len(routes) >= maximumTargetRoutes {
		var oldestIndex uint32
		var oldestExpiry time.Time
		for candidateIndex, route := range routes {
			if oldestExpiry.IsZero() || route.expires.Before(oldestExpiry) {
				oldestIndex = candidateIndex
				oldestExpiry = route.expires
			}
		}
		e.deleteRouteLocked(routes, oldestIndex)
	}
	if !exists || previous.address != address {
		if exists {
			e.releaseRouteAddressLocked(previous.address)
		}
		e.retained[address]++
	}
	expires := now.Add(targetRouteLifetime)
	routes[index] = targetRoute{address: address, expires: expires}
	if e.nextRoutePrune.IsZero() || expires.Before(e.nextRoutePrune) {
		e.nextRoutePrune = expires
	}
}

// pruneRoutesLocked removes expired public index affinity when its next deadline passes.
func (e *Remote) pruneRoutesLocked(now time.Time) {
	if e.nextRoutePrune.IsZero() || now.Before(e.nextRoutePrune) {
		return
	}
	e.nextRoutePrune = time.Time{}
	for _, routes := range []map[uint32]targetRoute{e.handshakeRoutes, e.transportRoutes} {
		for index, route := range routes {
			if !now.Before(route.expires) {
				e.deleteRouteLocked(routes, index)
				continue
			}
			if e.nextRoutePrune.IsZero() || route.expires.Before(e.nextRoutePrune) {
				e.nextRoutePrune = route.expires
			}
		}
	}
}

// acceptsSourceLocked reports whether peer is a current, recently replaced, or index-affine candidate.
func (e *Remote) acceptsSourceLocked(peer netip.AddrPort, now time.Time) bool {
	if peer == e.current || e.retained[peer] != 0 {
		return true
	}
	if expiry, ok := e.recent[peer]; ok {
		if now.Before(expiry) {
			return true
		}
		delete(e.recent, peer)
	}
	return slices.Contains(e.candidates, peer)
}

// pruneRecentLocked removes expired DNS candidates retained for in-flight handshake replies.
func (e *Remote) pruneRecentLocked(now time.Time) {
	for address, expiry := range e.recent {
		if !now.Before(expiry) {
			delete(e.recent, address)
		}
	}
}

// deleteRouteLocked removes one route and releases its source-address retention.
func (e *Remote) deleteRouteLocked(routes map[uint32]targetRoute, index uint32) {
	route := routes[index]
	delete(routes, index)
	e.releaseRouteAddressLocked(route.address)
}

// releaseRouteAddressLocked releases one source address after its final route expires or is replaced.
func (e *Remote) releaseRouteAddressLocked(address netip.AddrPort) {
	if e.retained[address] == 1 {
		delete(e.retained, address)
		return
	}
	e.retained[address]--
}

// socket returns the source socket for address's family.
func (e *Remote) socket(address netip.Addr) *net.UDPConn {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.socketLocked(address)
}

// socketLocked returns the source socket for address's family while e.mu is held.
func (e *Remote) socketLocked(address netip.Addr) *net.UDPConn {
	return e.sockets[familyIndex(address)]
}

// familyIndex maps IPv4 and IPv6 addresses to their source socket slots.
func familyIndex(address netip.Addr) int {
	if address.Is4() {
		return 0
	}
	return 1
}

// addressFamily returns the diagnostic name for an IP address family.
func addressFamily(address netip.Addr) string {
	if address.Is4() {
		return "IPv4"
	}
	return "IPv6"
}
