package datagram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/netsetup"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

const (
	// targetRefreshInterval bounds DNS query frequency during WireGuard handshake retries.
	targetRefreshInterval = 5 * time.Second
	// targetCandidateGrace permits replies to handshakes sent just before a DNS candidate replacement.
	targetCandidateGrace = 15 * time.Second
	// targetRouteLifetime retains public WireGuard index affinity for one key lifetime.
	targetRouteLifetime = 3 * time.Minute
	// targetHandshakeRetryInterval gives an established target one WireGuard handshake attempt before failover.
	targetHandshakeRetryInterval = 5 * time.Second
	// maximumTargetRoutes bounds unauthenticated public index state for one target endpoint.
	maximumTargetRoutes = 1024
)

// targetRoute binds one public WireGuard index to the DNS candidate that emitted it.
type targetRoute struct {
	address   netip.AddrPort
	created   time.Time
	expires   time.Time
	confirmed bool
}

// remoteRead is one accepted target packet vector or terminal socket read failure.
type remoteRead struct {
	packets [MaximumBatchSize]Packet
	count   int
	err     error
}

// release relinquishes every unread packet in r.
func (r *remoteRead) release() {
	for index := range r.count {
		r.packets[index].Release()
	}
	r.count = 0
}

// RemoteConfig defines target resolution and upstream UDP socket creation.
type RemoteConfig struct {
	Resolver     target.Resolver
	ListenConfig net.ListenConfig
	Logger       *slog.Logger
}

// Remote is an upstream logical target with DNS candidates and WireGuard index affinity.
type Remote struct {
	target           target.Endpoint
	resolver         target.Resolver
	listenConfig     net.ListenConfig
	resolveTimeout   time.Duration
	refreshInterval  time.Duration
	ctx              context.Context
	cancel           context.CancelFunc
	reads            chan remoteRead
	readMu           sync.Mutex
	pending          remoteRead
	pendingOffset    int
	writeMu          sync.Mutex
	writeMessages    [MaximumBatchSize]udpMessage
	mu               sync.Mutex
	sockets          [2]*net.UDPConn
	batchSockets     [2]udpBatchConn
	candidates       []netip.AddrPort
	recent           map[netip.AddrPort]time.Time
	retained         map[netip.AddrPort]int
	current          netip.AddrPort
	currentRouteAt   time.Time
	handshakeStarted time.Time
	handshakeRoutes  map[uint32]targetRoute
	transportRoutes  map[uint32]targetRoute
	nextRoutePrune   time.Time
	lastRefresh      time.Time
	refreshing       bool
	closed           bool
	workers          sync.WaitGroup
	notice           udpNotice
}

// OpenRemote resolves target and opens its per-family UDP sockets. The context bounds preparation only. The caller
// must close a successful result to stop its readers and DNS refreshes.
func OpenRemote(parent context.Context, endpoint target.Endpoint, config RemoteConfig) (*Remote, error) {
	return openRemote(parent, endpoint, config, netsetup.ResolveTimeout, targetRefreshInterval)
}

// openRemote opens a target endpoint with injectable resolution timings for tests.
func openRemote(parent context.Context, endpoint target.Endpoint, config RemoteConfig,
	resolveTimeout, refreshInterval time.Duration) (*Remote, error) {
	if parent == nil || resolveTimeout <= 0 || refreshInterval <= 0 {
		return nil, target.ErrInvalid
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if config.Resolver == nil {
		config.Resolver = net.DefaultResolver
	}
	resolveContext, cancelResolve := context.WithTimeout(parent, resolveTimeout)
	addresses, err := target.Resolve(resolveContext, config.Resolver, endpoint)
	cancelResolve()
	if err != nil {
		return nil, err
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	remote := &Remote{
		target: endpoint, resolver: config.Resolver, listenConfig: config.ListenConfig,
		resolveTimeout: resolveTimeout, refreshInterval: refreshInterval,
		ctx: ctx, cancel: cancel, reads: make(chan remoteRead),
		recent:          make(map[netip.AddrPort]time.Time),
		retained:        make(map[netip.AddrPort]int),
		handshakeRoutes: make(map[uint32]targetRoute), transportRoutes: make(map[uint32]targetRoute),
		lastRefresh: time.Now(),
		notice:      udpNotice{logger: config.Logger},
	}
	if err := remote.ensureSockets(parent, addresses); err != nil {
		remote.Close()
		return nil, err
	}
	if err := parent.Err(); err != nil {
		remote.Close()
		return nil, err
	}
	remote.replaceCandidates(addresses)
	return remote, nil
}

// Read returns the next structurally valid WireGuard datagram from an accepted target candidate.
func (e *Remote) Read(ctx context.Context) (Packet, error) {
	var packets [1]Packet
	count, err := e.ReadBatch(ctx, packets[:])
	if count > 0 {
		return packets[0], nil
	}
	return Packet{}, err
}

// ReadBatch returns an accepted target packet vector from either address family.
func (e *Remote) ReadBatch(ctx context.Context, packets []Packet) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	e.readMu.Lock()
	defer e.readMu.Unlock()
	if e.pendingOffset < e.pending.count {
		return e.consumePending(packets), nil
	}
	select {
	case result := <-e.reads:
		if result.err != nil {
			return 0, result.err
		}
		e.pending = result
		e.pendingOffset = 0
		return e.consumePending(packets), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-e.ctx.Done():
		return 0, e.ctx.Err()
	}
}

// consumePending transfers as much pending packet ownership as destination can hold.
func (e *Remote) consumePending(destination []Packet) int {
	count := min(len(destination), e.pending.count-e.pendingOffset)
	copy(destination, e.pending.packets[e.pendingOffset:e.pendingOffset+count])
	clear(e.pending.packets[e.pendingOffset : e.pendingOffset+count])
	e.pendingOffset += count
	if e.pendingOffset == e.pending.count {
		e.pending = remoteRead{}
		e.pendingOffset = 0
	}
	return count
}

// Write routes one WireGuard datagram to its indexed candidate or bounded handshake fan-out set.
func (e *Remote) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	written, err := e.WriteBatch(ctx, [][]byte{payload}, deadline)
	if err != nil {
		return err
	}
	if written != 1 {
		return fmt.Errorf("write target UDP datagram: %w", io.ErrShortWrite)
	}
	return nil
}

// WriteBatch writes an ordered packet prefix using vector I/O for single-candidate runs.
func (e *Remote) WriteBatch(ctx context.Context, payloads [][]byte, deadline time.Time) (int, error) {
	if len(payloads) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	written := 0
	for written < len(payloads) {
		now := time.Now()
		var destinationBuffer [target.MaxCandidates]netip.AddrPort
		destinations, err := e.prepareDestinations(payloads[written], destinationBuffer[:0], now)
		if err != nil {
			return written, err
		}
		if len(destinations) != 1 {
			if err := e.writeDestinationsLocked(ctx, payloads[written], destinations, deadline); err != nil {
				return written, err
			}
			written++
			continue
		}
		connection, batchConnection := e.socketPair(destinations[0].Addr())
		if connection == nil {
			e.triggerRefresh()
			return written, fmt.Errorf("%w: no UDP socket for %s target candidate", ErrDatagramDropped,
				addressFamily(destinations[0].Addr()))
		}
		messages := e.writeMessages[:]
		messages[0] = udpMessage{payload: payloads[written], peer: destinations[0]}
		count := 1
		for count < len(messages) && written+count < len(payloads) {
			var nextBuffer [target.MaxCandidates]netip.AddrPort
			next, err := e.prepareDestinations(payloads[written+count], nextBuffer[:0], now)
			if err != nil || len(next) != 1 {
				break
			}
			nextConnection, _ := e.socketPair(next[0].Addr())
			if nextConnection != connection {
				break
			}
			messages[count] = udpMessage{payload: payloads[written+count], peer: next[0]}
			count++
		}
		if err := connection.SetWriteDeadline(earlierDeadline(ctx, deadline)); err != nil {
			clear(messages[:count])
			e.triggerRefresh()
			return written, fmt.Errorf("write target UDP datagram batch: %w", err)
		}
		countWritten, err := writeUDPMessageBatch(connection, batchConnection, messages[:count])
		clear(messages[:count])
		written += countWritten
		if err != nil {
			e.triggerRefresh()
			if isSoftNetworkError(err) {
				e.notice.report("write target", err)
				return written, fmt.Errorf("%w: write target UDP datagram batch: %w", ErrDatagramDropped, err)
			}
			return written, fmt.Errorf("write target UDP datagram batch: %w", err)
		}
	}
	return written, nil
}

// prepareDestinations validates one packet and returns its current target candidates.
func (e *Remote) prepareDestinations(payload []byte, buffer []netip.AddrPort, now time.Time) ([]netip.AddrPort, error) {
	header := wgpacket.Inspect(payload)
	if !header.Kind.Accepted() {
		return nil, fmt.Errorf("write target UDP datagram: unsupported WireGuard packet")
	}
	if header.Kind == wgpacket.HandshakeInitiation {
		e.triggerRefresh()
	}
	destinations := e.destinations(header, buffer, now)
	if len(destinations) == 0 {
		return nil, fmt.Errorf("%w: no target candidate", ErrDatagramDropped)
	}
	return destinations, nil
}

// writeDestinationsLocked writes one packet to its fan-out set while e.writeMu is held.
func (e *Remote) writeDestinationsLocked(ctx context.Context, payload []byte, destinations []netip.AddrPort,
	deadline time.Time) error {
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
			e.notice.report("write target", err)
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

// writeUDPMessageBatch uses vector I/O when available and otherwise preserves scalar ordering.
func writeUDPMessageBatch(connection *net.UDPConn, batchConnection udpBatchConn, messages []udpMessage) (int, error) {
	if batchConnection != nil {
		return batchConnection.write(messages)
	}
	for index, message := range messages {
		written, err := connection.WriteToUDPAddrPort(message.payload, message.peer)
		if err != nil {
			return index, err
		}
		if written != len(message.payload) {
			return index, io.ErrShortWrite
		}
	}
	return len(messages), nil
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
	e.batchSockets = [2]udpBatchConn{}
	e.mu.Unlock()
	var errs []error
	for _, connection := range sockets {
		if connection != nil {
			errs = append(errs, connection.Close())
		}
	}
	e.workers.Wait()
	e.readMu.Lock()
	e.pending.release()
	e.pendingOffset = 0
	e.readMu.Unlock()
	return errors.Join(errs...)
}

// ensureSockets attempts to open one source socket for each candidate address family not already represented.
func (e *Remote) ensureSockets(ctx context.Context, addresses []netip.AddrPort) error {
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
		packetConnection, err := e.listenConfig.ListenPacket(ctx, network, address.String())
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("open %s target UDP socket: %w", network, err)
			}
			continue
		}
		connection := packetConnection.(*net.UDPConn)
		configureReceiveBuffer(connection, &e.notice)
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
		batchConnection := newUDPBatchConn(connection)
		e.sockets[index] = connection
		e.batchSockets[index] = batchConnection
		e.workers.Add(1)
		e.mu.Unlock()
		go e.readSocket(connection, batchConnection)
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
func (e *Remote) readSocket(connection *net.UDPConn, batchConnection udpBatchConn) {
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
				e.notice.report("read target", err)
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
		now := time.Now()
		header := wgpacket.Inspect(buffer[:length])
		if !header.Kind.Accepted() || !e.observeSource(peer, header, now) {
			continue
		}
		result := remoteRead{count: 1}
		result.packets[0] = ownPacket(header.Kind, buffer[:length])
		var drainErr error
		if batchConnection != nil {
			var batch udpReadBatch
			batch, drainErr = batchConnection.readAvailable(MaximumBatchSize - 1)
			if batch != nil {
				for _, message := range batch.messages() {
					header := wgpacket.Inspect(message.payload)
					if !header.Kind.Accepted() || !e.observeSource(message.peer, header, now) {
						continue
					}
					result.packets[result.count] = ownPacket(header.Kind, message.payload)
					result.count++
				}
				batch.release()
			}
		}
		e.offerRead(result)
		if drainErr == nil {
			continue
		}
		if isSoftNetworkError(drainErr) {
			e.notice.report("read target", drainErr)
			e.triggerRefresh()
			continue
		}
		available := e.retireSocket(connection)
		connection.Close()
		e.triggerRefresh()
		if !available {
			e.offerRead(remoteRead{err: fmt.Errorf("read target UDP datagram batch: %w", drainErr)})
		}
		return
	}
}

// retireSocket removes one failed family socket and reports whether another family remains usable.
func (e *Remote) retireSocket(connection *net.UDPConn) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for index, candidate := range e.sockets {
		if candidate == connection {
			e.sockets[index] = nil
			e.batchSockets[index] = nil
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
		result.release()
	}
}

// observeSource accepts only resolved or retained candidates and records their public WireGuard indexes.
func (e *Remote) observeSource(peer netip.AddrPort, header wgpacket.Header, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneRoutesLocked(now)
	if !e.acceptsSourceLocked(peer, now) {
		return false
	}
	switch header.Kind {
	case wgpacket.HandshakeInitiation:
		// An alternate backend can still remember a previous handshake. Do not let its unsolicited rekey move
		// healthy inner connections away from the target selected by the local WireGuard implementation.
		if peer != e.current && !e.currentRouteAt.IsZero() && !e.handshakeFailoverLocked(now) {
			return false
		}
		e.rememberRouteLocked(e.handshakeRoutes, header.SenderIndex, peer, now)
	case wgpacket.HandshakeResponse:
		e.rememberRouteLocked(e.transportRoutes, header.SenderIndex, peer, now)
	}
	return true
}

// destinations appends an established target, handshake discovery candidates, or one index-affine target to buffer.
func (e *Remote) destinations(header wgpacket.Header, buffer []netip.AddrPort, now time.Time) []netip.AddrPort {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneRoutesLocked(now)
	var route targetRoute
	var found bool
	switch header.Kind {
	case wgpacket.HandshakeInitiation:
		if !e.currentRouteAt.IsZero() {
			if e.handshakeStarted.IsZero() {
				e.handshakeStarted = now
			}
			if !e.handshakeFailoverLocked(now) {
				return append(buffer, e.current)
			}
		}
		return append(buffer, e.candidates...)
	case wgpacket.HandshakeResponse:
		route, found = e.handshakeRoutes[header.ReceiverIndex]
	case wgpacket.CookieReply:
		route, found = e.handshakeRoutes[header.ReceiverIndex]
		if !found {
			route, found = e.transportRoutes[header.ReceiverIndex]
		}
	case wgpacket.TransportData:
		routes := e.transportRoutes
		route, found = routes[header.ReceiverIndex]
		if !found {
			routes = e.handshakeRoutes
			route, found = routes[header.ReceiverIndex]
		}
		if found && !route.confirmed {
			route.confirmed = true
			routes[header.ReceiverIndex] = route
			if !route.created.Before(e.currentRouteAt) {
				e.current = route.address
				e.currentRouteAt = route.created
				e.handshakeStarted = time.Time{}
			}
		}
	}
	if found {
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

// handshakeFailoverLocked reports whether an established target has had a complete unanswered handshake attempt.
func (e *Remote) handshakeFailoverLocked(now time.Time) bool {
	return !e.handshakeStarted.IsZero() && now.Sub(e.handshakeStarted) >= targetHandshakeRetryInterval
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
		err = e.ensureSockets(e.ctx, addresses)
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
		e.currentRouteAt = time.Time{}
		e.handshakeStarted = time.Time{}
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
	route := targetRoute{address: address, created: now, expires: expires}
	if exists && previous.address == address {
		route.created = previous.created
		route.confirmed = previous.confirmed
	}
	routes[index] = route
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

// socketPair returns the scalar and batch connections for address's family.
func (e *Remote) socketPair(address netip.Addr) (*net.UDPConn, udpBatchConn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	index := familyIndex(address)
	return e.sockets[index], e.batchSockets[index]
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
