// Package client implements WireHop client startup and session ownership.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
	"github.com/aofei/wirehop/internal/wsheader"
	"github.com/coder/websocket"
)

var (
	// ErrInvalidConfig indicates missing address, authentication, timing, or queue configuration.
	ErrInvalidConfig = errors.New("invalid client configuration")
	// ErrUnexpectedServerResponse indicates a valid but contextually impossible handshake response.
	ErrUnexpectedServerResponse = errors.New("unexpected server response")
	// ErrSessionGone indicates that the server no longer retains a session being joined.
	ErrSessionGone = errors.New("relay session is gone")
	// ErrStartupTimeout indicates that no configured lane created a session within the startup budget.
	ErrStartupTimeout = errors.New("relay session startup timed out")
	// ErrLaneRejected indicates a permanent carrier-specific admission rejection.
	ErrLaneRejected = errors.New("relay lane rejected")
)

// RejectionError is an authenticated server rejection.
type RejectionError struct {
	Code       protocol.ErrorCode
	Class      protocol.ErrorClass
	Scope      protocol.ErrorScope
	Diagnostic string
}

// Error returns the stable rejection diagnostic.
func (e *RejectionError) Error() string {
	return fmt.Sprintf("server rejected admission with code %d: %s", e.Code, e.Diagnostic)
}

// Config defines the carrier lanes and local WireGuard listener for one relay session.
type Config struct {
	Lanes               []lanespec.Spec
	Listen              netip.AddrPort
	Target              target.Endpoint
	Reserved            wgpacket.Reserved
	Token               []byte
	Clock               relay.Clock
	WallClock           func() time.Time
	Dialer              *net.Dialer
	Proxy               func(*http.Request) (*neturl.URL, error)
	TLSConfig           *tls.Config
	Logger              *slog.Logger
	HandshakeTimeout    time.Duration
	StartupTimeout      time.Duration
	MaxLanes            int
	IngressLimits       packetqueue.Limits
	LaneLimits          packetqueue.Limits
	Deadlines           relay.DeadlinePolicy
	DeduplicationWindow int
}

// Client owns the early-bound local UDP socket and asynchronous relay session.
type Client struct {
	config        Config
	lanes         []clientLane
	endpoint      datagram.Endpoint
	localAddr     netip.AddrPort
	queue         *packetqueue.Queue[relay.Packet]
	retention     *retention.Budget
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	gracefulOnce  sync.Once
	closeOnce     sync.Once
	mu            sync.RWMutex
	err           error
	sessionID     protocol.SessionID
	sessionSecret protocol.SessionSecret
	scheduler     *relay.Scheduler
}

// clientLane is one configured occurrence with stable session-local identity.
type clientLane struct {
	spec                lanespec.Spec
	laneID              protocol.LaneID
	pathGroupID         protocol.PathGroupID
	authenticationClock *authenticationClock
}

// pathGroupKey identifies lanes with the same logical endpoint and fixed resolution.
type pathGroupKey struct {
	url       string
	resolveIP netip.Addr
}

// Start validates config, binds UDP immediately, and starts carrier session establishment.
func Start(parent context.Context, config Config) (*Client, error) {
	if config.StartupTimeout == 0 {
		config.StartupTimeout = defaultStartupTimeout
	}
	if config.MaxLanes == 0 {
		config.MaxLanes = defaultMaxLanes
	}
	specs, err := validateConfig(config)
	if err != nil {
		return nil, err
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if config.Clock == nil {
		config.Clock = monotime.New()
	}
	if config.WallClock == nil {
		config.WallClock = time.Now
	}
	if config.Dialer == nil {
		config.Dialer = &net.Dialer{}
	} else {
		dialer := *config.Dialer
		config.Dialer = &dialer
	}
	if config.TLSConfig != nil {
		config.TLSConfig = config.TLSConfig.Clone()
	}
	config.IngressLimits.ControlPreemption = true
	config.LaneLimits.ControlPreemption = false
	config.Token = append([]byte(nil), config.Token...)
	lanes := buildLanes(specs, config.WallClock)
	retentionBudget, err := retention.NewBudget(retention.Limits{
		Packets: defaultRetainedPackets,
		Bytes:   defaultRetainedBytes,
	})
	if err != nil {
		return nil, err
	}
	local, err := datagram.ListenLocal(config.Listen)
	if err != nil {
		return nil, err
	}
	endpoint := datagram.WithReservedTranslation(local, config.Reserved)
	queue, err := packetqueue.NewWithBudget[relay.Packet](config.IngressLimits, retentionBudget)
	if err != nil {
		endpoint.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	client := &Client{
		config: config, lanes: lanes, endpoint: endpoint, localAddr: local.LocalAddr(), queue: queue,
		retention: retentionBudget,
		ctx:       ctx, cancel: cancel,
		done: make(chan struct{}),
	}
	go func() {
		err := client.run()
		client.mu.Lock()
		client.err = err
		client.mu.Unlock()
		client.stop()
		close(client.done)
	}()
	return client, nil
}

// LocalAddr returns the already-bound local WireGuard endpoint.
func (c *Client) LocalAddr() netip.AddrPort {
	return c.localAddr
}

// SessionID returns the created session identifier or zero before successful admission.
func (c *Client) SessionID() protocol.SessionID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionID
}

// Wait blocks until the relay session ends and returns its terminal result.
func (c *Client) Wait() error {
	<-c.done
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

// Close stops the client and closes its local UDP listener.
func (c *Client) Close() error {
	c.gracefulOnce.Do(func() {
		c.mu.RLock()
		scheduler := c.scheduler
		sessionID := c.sessionID
		c.mu.RUnlock()
		if scheduler != nil && !sessionID.IsZero() {
			closeContext, cancel := context.WithTimeout(context.Background(), gracefulCloseTimeout)
			scheduler.CloseSession(closeContext, protocol.CloseClientShutdown)
			cancel()
		}
	})
	c.stop()
	<-c.done
	return nil
}

// stop idempotently releases resources that unblock startup and data-plane workers.
func (c *Client) stop() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.queue.Close()
		c.endpoint.Close()
		c.clearSession()
	})
}

// creationAttempt binds one admission request to its lane authentication clock.
type creationAttempt struct {
	laneID              protocol.LaneID
	pathGroupID         protocol.PathGroupID
	generation          uint64
	nonce               protocol.Nonce
	unixSeconds         int64
	monotonicMicros     uint64
	authenticationClock *authenticationClock
}

// creationResult contains authenticated or carrier-protected server admission state.
type creationResult struct {
	sessionID     protocol.SessionID
	sessionSecret protocol.SessionSecret
	receiveMicros uint64
	sendMicros    uint64
}

// acceptedLane is one admitted carrier generation and its clock-bootstrap state.
type acceptedLane struct {
	connection   carrier.Conn
	mapping      clockmap.Mapping
	initialFrame protocol.Frame
}

// preparationResult is one concurrently prepared first-hop carrier connection.
type preparationResult struct {
	index      int
	connection net.Conn
	err        error
}

// webSocketRoute binds one WebSocket attempt to its selected first hop and proxy policy.
type webSocketRoute struct {
	address string
	proxy   *neturl.URL
}

// preparedWebSocketConnection retains route and TLS endpoint metadata for a prepared first-hop socket.
type preparedWebSocketConnection struct {
	net.Conn
	proxy           *neturl.URL
	firstHopAddress string
	targetAddress   string
}

// namespacedClientSessionCache prevents independent TLS endpoints with the same server name from replacing each other.
type namespacedClientSessionCache struct {
	cache     tls.ClientSessionCache
	namespace string
}

// Get retrieves one endpoint-scoped TLS session.
func (c namespacedClientSessionCache) Get(sessionKey string) (*tls.ClientSessionState, bool) {
	return c.cache.Get(c.key(sessionKey))
}

// Put stores one endpoint-scoped TLS session.
func (c namespacedClientSessionCache) Put(sessionKey string, state *tls.ClientSessionState) {
	c.cache.Put(c.key(sessionKey), state)
}

// key combines a role and socket endpoint with the TLS package's logical identity key.
func (c namespacedClientSessionCache) key(sessionKey string) string {
	return c.namespace + "\x00" + sessionKey
}

// NetConn returns the underlying socket for abortive carrier teardown.
func (c *preparedWebSocketConnection) NetConn() net.Conn {
	return c.Conn
}

// laneResult reports one permanently ended lane supervisor.
type laneResult struct {
	index int
	err   error
}

// configuredLaneError identifies the declaration responsible for one lane failure.
type configuredLaneError struct {
	index int
	spec  lanespec.Spec
	err   error
}

// Error returns the lane declaration and underlying failure.
func (e *configuredLaneError) Error() string {
	if resolveIP := e.spec.ResolveIP(); resolveIP.IsValid() {
		return fmt.Sprintf("lane %d (url=%s, resolve=%s): %v", e.index+1, e.spec.URL(), resolveIP, e.err)
	}
	return fmt.Sprintf("lane %d (%s): %v", e.index+1, e.spec.URL(), e.err)
}

// Unwrap returns the underlying lane failure.
func (e *configuredLaneError) Unwrap() error {
	return e.err
}

// failureDisposition determines the ownership scope of one lane failure.
type failureDisposition uint8

const (
	// failureRetry retries the same stable lane identity with a new generation.
	failureRetry failureDisposition = iota
	// failureCloseLane permanently closes only the affected configured lane.
	failureCloseLane
	// failureCloseSession ends or recreates the complete relay session.
	failureCloseSession
)

const (
	// initialReconnectDelay is the first per-lane retry delay.
	initialReconnectDelay = 100 * time.Millisecond
	// maximumReconnectDelay bounds per-lane retry backoff.
	maximumReconnectDelay = 5 * time.Second
	// defaultStartupTimeout bounds one complete session-creation attempt across all configured lanes.
	defaultStartupTimeout = 15 * time.Second
	// reconnectStabilityInterval resets backoff after sustained healthy service.
	reconnectStabilityInterval = 30 * time.Second
	// defaultMaxLanes bounds concurrent carrier preparation and session membership.
	defaultMaxLanes = 16
	// defaultRetainedPackets bounds aggregate client packet retention.
	defaultRetainedPackets = 131_072
	// defaultRetainedBytes bounds aggregate client accounted packet bytes.
	defaultRetainedBytes = 256 * 1024 * 1024
	// gracefulCloseTimeout bounds best-effort explicit session cleanup.
	gracefulCloseTimeout = 200 * time.Millisecond
	// maximumWebSocketResponseHeaderBytes bounds relay and proxy handshake metadata.
	maximumWebSocketResponseHeaderBytes = 16 * 1024
)

// run keeps UDP ingress alive while relay sessions are created or recreated.
func (c *Client) run() error {
	ingress, err := relay.NewIngress(c.endpoint, c.queue, c.config.Clock, c.config.Deadlines)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(c.ctx)
	ingressResult := make(chan error, 1)
	ingressDone := make(chan struct{})
	go func() {
		defer close(ingressDone)
		err := ingress.Run(ctx)
		ingressResult <- err
		cancel(err)
	}()
	defer func() {
		cancel(context.Canceled)
		<-ingressDone
	}()
	startupDeadline := time.Now().Add(c.config.StartupTimeout)
	everEstablished := false
	retryDelay := initialReconnectDelay
	var lastStartupFailure error
	for {
		startupBudget := c.config.StartupTimeout
		if !everEstablished {
			startupBudget = time.Until(startupDeadline)
			if startupBudget <= 0 {
				if lastStartupFailure != nil {
					return fmt.Errorf("%w: %w", ErrStartupTimeout, lastStartupFailure)
				}
				return ErrStartupTimeout
			}
		}
		activeFor, err := c.runSession(ctx, ingressResult, startupBudget)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		hasSession := !c.SessionID().IsZero()
		if hasSession {
			everEstablished = true
			retryDelay = reconnectDelayAfterUptime(retryDelay, activeFor)
		} else if !everEstablished {
			lastStartupFailure = err
		}
		if hasSession && sessionReplacementFailure(err) {
			c.clearSession()
			if err := waitReconnect(ctx, retryDelay); err != nil {
				return err
			}
			retryDelay = nextReconnectDelay(retryDelay)
			continue
		}
		if !hasSession && (classifyLaneFailure(err) == failureRetry || retryableSessionFailure(err)) {
			delay := retryDelay
			if !everEstablished {
				remaining := time.Until(startupDeadline)
				if remaining <= 0 {
					return fmt.Errorf("%w: %w", ErrStartupTimeout, err)
				}
				delay = min(delay, remaining)
			}
			if err := waitReconnect(ctx, delay); err != nil {
				return err
			}
			retryDelay = nextReconnectDelay(retryDelay)
			continue
		}
		return err
	}
}

// runSession creates one session, supervises its lanes, and reports established uptime.
func (c *Client) runSession(ctx context.Context, ingressResult <-chan error,
	startupTimeout time.Duration) (time.Duration, error) {
	scheduler, err := relay.NewScheduler(c.queue)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.scheduler = scheduler
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.scheduler == scheduler {
			c.scheduler = nil
		}
		c.mu.Unlock()
	}()
	sessionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	schedulerResult := make(chan error, 1)
	go func() { schedulerResult <- scheduler.Run(sessionContext) }()
	receiver, err := relay.NewReceiver(relay.ReceiverConfig{
		Endpoint: c.endpoint, Clock: c.config.Clock, DeduplicationSize: c.config.DeduplicationWindow,
	})
	if err != nil {
		return 0, err
	}
	startupContext, startupCancel := context.WithTimeout(sessionContext, startupTimeout)
	startupDeadline, _ := startupContext.Deadline()
	preparations := make(chan preparationResult, len(c.lanes))
	for index, configured := range c.lanes {
		go func() {
			connection, err := c.prepareLane(startupContext, configured.spec)
			preparations <- preparationResult{index: index, connection: connection, err: err}
		}()
	}
	var accepted acceptedLane
	var result creationResult
	creatorIndex := -1
	var creationError error
	var retryableCreationError error
	consumed := make([]bool, len(c.lanes))
	receivedPreparations := 0
	cleanupPreparations := true
	defer func() {
		startupCancel()
		if !cleanupPreparations {
			return
		}
		for receivedPreparations < len(c.lanes) {
			prepared := <-preparations
			receivedPreparations++
			if prepared.connection != nil {
				prepared.connection.Close()
			}
		}
	}()
	for receivedPreparations < len(c.lanes) {
		prepared := <-preparations
		receivedPreparations++
		consumed[prepared.index] = true
		if prepared.err != nil {
			failure := c.configuredLaneError(prepared.index, prepared.err)
			creationError = failure
			if classifyLaneFailure(failure) == failureRetry {
				retryableCreationError = failure
			}
			continue
		}
		creator := c.lanes[prepared.index]
		attempt := c.newCreationAttempt(creator, 1)
		accepted, result, err = c.openPreparedCreation(
			startupContext, creator.spec.URL(), attempt, prepared.connection,
		)
		if err == nil {
			creatorIndex = prepared.index
			break
		}
		failure := c.configuredLaneError(prepared.index, err)
		creationError = failure
		if classifyLaneFailure(failure) == failureRetry {
			retryableCreationError = failure
		}
	}
	if creatorIndex < 0 {
		startupError := startupContext.Err()
		if startupError != nil || !time.Now().Before(startupDeadline) {
			if startupError == nil {
				startupError = context.DeadlineExceeded
			}
			if creationError != nil {
				return 0, fmt.Errorf("%w: %w", ErrStartupTimeout, creationError)
			}
			return 0, fmt.Errorf("%w: %w", ErrStartupTimeout, startupError)
		}
		if retryableCreationError != nil {
			return 0, retryableCreationError
		}
		return 0, creationError
	}
	preparedLanes := make([]chan preparationResult, len(c.lanes))
	for index := range preparedLanes {
		if index != creatorIndex && !consumed[index] {
			preparedLanes[index] = make(chan preparationResult, 1)
		}
	}
	var preparationsDone <-chan struct{}
	if remaining := len(c.lanes) - receivedPreparations; remaining > 0 {
		done := make(chan struct{})
		preparationsDone = done
		go func() {
			defer close(done)
			defer startupCancel()
			c.distributePreparations(sessionContext, preparations, preparedLanes, remaining)
		}()
	} else {
		startupCancel()
	}
	cleanupPreparations = false
	receiver.UpdateClock(accepted.mapping.Inverse())
	c.mu.Lock()
	c.sessionID = result.sessionID
	c.sessionSecret = result.sessionSecret
	c.mu.Unlock()
	establishedAt := time.Now()
	laneResults := make(chan laneResult, len(c.lanes))
	var laneWait sync.WaitGroup
	for index, configured := range c.lanes {
		laneWait.Go(func() {
			var initial *acceptedLane
			if index == creatorIndex {
				initial = &accepted
			}
			if err := c.superviseLane(
				sessionContext, configured, result, initial, preparedLanes[index], receiver, scheduler,
			); err != nil {
				select {
				case laneResults <- laneResult{index: index, err: err}:
				case <-sessionContext.Done():
				}
			}
		})
	}
	var resultError error
	schedulerConsumed := false
	remainingLanes := len(c.lanes)
	for resultError == nil {
		select {
		case resultError = <-schedulerResult:
			schedulerConsumed = true
		case ended := <-laneResults:
			remainingLanes--
			if laneFailureEndsSession(ended.err, remainingLanes) {
				resultError = c.configuredLaneError(ended.index, ended.err)
			} else if sessionContext.Err() == nil {
				c.logDisabledLane(ended.index, ended.err)
			}
		case resultError = <-ingressResult:
		case <-ctx.Done():
			resultError = context.Cause(ctx)
		}
	}
	if ctx.Err() != nil {
		resultError = context.Cause(ctx)
	}
	cancel()
	laneWait.Wait()
	if preparationsDone != nil {
		<-preparationsDone
	}
	closeUnusedPreparations(preparedLanes)
	if !schedulerConsumed {
		<-schedulerResult
	}
	return time.Since(establishedAt), resultError
}

// configuredLaneError wraps one failure with its stable command-line declaration.
func (c *Client) configuredLaneError(index int, err error) error {
	return &configuredLaneError{index: index, spec: c.lanes[index].spec, err: err}
}

// logDisabledLane reports permanent multipath degradation without exposing peer-controlled diagnostics.
func (c *Client) logDisabledLane(index int, err error) {
	if c.config.Logger == nil {
		return
	}
	lane := c.lanes[index]
	attributes := []any{"lane", index + 1, "url", lane.spec.URL()}
	if resolveIP := lane.spec.ResolveIP(); resolveIP.IsValid() {
		attributes = append(attributes, "resolve", resolveIP)
	}
	if rejection, ok := errors.AsType[*RejectionError](err); ok {
		attributes = append(attributes,
			"code", rejection.Code, "class", rejection.Class, "scope", rejection.Scope)
	} else if remote, ok := errors.AsType[*relay.RemoteError](err); ok {
		attributes = append(attributes,
			"code", remote.Value.Code, "class", remote.Value.Class, "scope", remote.Value.Scope)
	} else {
		attributes = append(attributes, "error", err)
	}
	c.config.Logger.Warn("relay lane disabled", attributes...)
}

// laneFailureEndsSession reports whether one ended supervisor invalidates the shared session.
func laneFailureEndsSession(err error, remainingLanes int) bool {
	if sessionGoneFailure(err) {
		return remainingLanes == 0
	}
	return classifyLaneFailure(err) == failureCloseSession || remainingLanes == 0
}

// sessionReplacementFailure reports whether a fresh session can safely replace the failed identity space.
func sessionReplacementFailure(err error) bool {
	return errors.Is(err, relay.ErrCounterExhausted) || sessionGoneFailure(err) || retryableSessionFailure(err)
}

// retryableSessionFailure reports a temporary typed failure that invalidates the complete session.
func retryableSessionFailure(err error) bool {
	if rejection, ok := errors.AsType[*RejectionError](err); ok {
		return rejection.Class == protocol.ErrorRetryable && rejection.Scope == protocol.ErrorScopeSession
	}
	remote, ok := errors.AsType[*relay.RemoteError](err)
	return ok && remote.Value.Class == protocol.ErrorRetryable &&
		remote.Value.Scope == protocol.ErrorScopeSession
}

// sessionGoneFailure reports whether a local handshake or typed remote error says the retained session is absent.
func sessionGoneFailure(err error) bool {
	if errors.Is(err, ErrSessionGone) {
		return true
	}
	remote, ok := errors.AsType[*relay.RemoteError](err)
	return ok && remote.Value.Class == protocol.ErrorSessionGone &&
		remote.Value.Scope == protocol.ErrorScopeSession
}

// distributePreparations routes outstanding first-hop results to their stable lane supervisors.
func (c *Client) distributePreparations(ctx context.Context, input <-chan preparationResult,
	outputs []chan preparationResult, remaining int) {
	for range remaining {
		prepared := <-input
		output := outputs[prepared.index]
		if output == nil || ctx.Err() != nil {
			if prepared.connection != nil {
				prepared.connection.Close()
			}
			continue
		}
		output <- prepared
	}
}

// closeUnusedPreparations releases first-hop connections not claimed before session shutdown.
func closeUnusedPreparations(preparations []chan preparationResult) {
	for _, preparation := range preparations {
		if preparation == nil {
			continue
		}
		select {
		case prepared := <-preparation:
			if prepared.connection != nil {
				prepared.connection.Close()
			}
		default:
		}
	}
}

// clearSession erases client-held ephemeral credentials between session generations.
func (c *Client) clearSession() {
	c.mu.Lock()
	c.sessionID = protocol.SessionID{}
	clear(c.sessionSecret[:])
	c.mu.Unlock()
}

// newCreationAttempt generates fresh authentication fields for one stable lane generation.
func (c *Client) newCreationAttempt(lane clientLane, generation uint64) creationAttempt {
	return creationAttempt{
		laneID: lane.laneID, pathGroupID: lane.pathGroupID, nonce: protocol.NewNonce(),
		unixSeconds:         lane.authenticationClock.Unix(),
		generation:          generation,
		monotonicMicros:     c.config.Clock.NowMicros(),
		authenticationClock: lane.authenticationClock,
	}
}

// superviseLane keeps one stable lane identifier attached with increasing generations.
func (c *Client) superviseLane(ctx context.Context, configured clientLane, session creationResult,
	initial *acceptedLane, preparation <-chan preparationResult, receiver *relay.Receiver,
	scheduler *relay.Scheduler) error {
	generation := uint64(1)
	delay := initialReconnectDelay
	for {
		accepted := initial
		initial = nil
		if accepted == nil {
			var connection net.Conn
			var err error
			if preparation != nil {
				var prepared preparationResult
				select {
				case prepared = <-preparation:
				case <-ctx.Done():
					return ctx.Err()
				}
				preparation = nil
				connection = prepared.connection
				err = prepared.err
				if err == nil && ctx.Err() != nil {
					prepared.connection.Close()
					return ctx.Err()
				}
			} else {
				connection, err = c.prepareLane(ctx, configured.spec)
			}
			var joined acceptedLane
			if err == nil {
				attempt := c.newCreationAttempt(configured, generation)
				joined, err = c.openPreparedJoin(ctx, configured.spec.URL(), attempt, session, connection)
			}
			if err != nil {
				if classifyLaneFailure(err) != failureRetry {
					return err
				}
				if err := advanceGeneration(&generation); err != nil {
					return err
				}
				if err := waitReconnect(ctx, delay); err != nil {
					return err
				}
				delay = nextReconnectDelay(delay)
				continue
			}
			accepted = &joined
		}
		receiver.UpdateClock(accepted.mapping.Inverse())
		started := time.Now()
		err := c.runLaneGeneration(ctx, configured, generation, *accepted, receiver, scheduler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if classifyLaneFailure(err) != failureRetry {
			return err
		}
		delay = reconnectDelayAfterUptime(delay, time.Since(started))
		if err := advanceGeneration(&generation); err != nil {
			return err
		}
		if err := waitReconnect(ctx, delay); err != nil {
			return err
		}
		delay = nextReconnectDelay(delay)
	}
}

// advanceGeneration increments one stable lane generation without permitting zero-value reuse.
func advanceGeneration(generation *uint64) error {
	if *generation == ^uint64(0) {
		return relay.ErrCounterExhausted
	}
	*generation++
	return nil
}

// runLaneGeneration registers and runs one admitted lane connection generation.
func (c *Client) runLaneGeneration(ctx context.Context, configured clientLane, generation uint64,
	accepted acceptedLane, receiver *relay.Receiver, scheduler *relay.Scheduler) error {
	defer accepted.connection.Close()
	store, err := relay.NewTransmissionStoreWithBudget(c.config.LaneLimits, c.retention)
	if err != nil {
		return err
	}
	laneContext, cancelLane := context.WithCancelCause(ctx)
	abandon := func() { cancelLane(relay.ErrLaneAbandoned) }
	defer cancelLane(context.Canceled)
	lane, err := relay.NewLane(relay.LaneConfig{
		Carrier: accepted.connection, Receiver: receiver, Store: store, Clock: c.config.Clock,
		Observer: scheduler, LaneID: configured.laneID, Generation: generation,
		InitialFrames: []protocol.Frame{accepted.initialFrame},
	})
	if err != nil {
		return err
	}
	if err := scheduler.Register(ctx, relay.LaneRegistration{
		LaneID: configured.laneID, Generation: generation, PathGroupID: configured.pathGroupID, Store: store,
		Abandon: abandon, SendControl: lane.SendControl, ValidateProbeProgress: lane.ValidateProbeProgress,
	}); err != nil {
		return err
	}
	defer func() {
		removeContext, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		scheduler.Remove(removeContext, configured.laneID, generation)
	}()
	return lane.Run(laneContext)
}

// prepareLane establishes one carrier's first hop without performing session admission.
func (c *Client) prepareLane(ctx context.Context, spec lanespec.Spec) (net.Conn, error) {
	url := spec.URL()
	if !url.Scheme().WebSocket() {
		return c.dialStream(ctx, spec)
	}
	route, err := c.webSocketRoute(spec)
	if err != nil {
		return nil, err
	}
	handshakeContext, cancel := context.WithTimeout(ctx, c.config.HandshakeTimeout)
	defer cancel()
	connection, err := c.config.Dialer.DialContext(handshakeContext, "tcp", route.address)
	if err != nil {
		return nil, fmt.Errorf("dial %s lane first hop: %w", url.Scheme(), err)
	}
	stopClose := context.AfterFunc(ctx, func() { connection.Close() })
	defer stopClose()
	if tcp, ok := connection.(*net.TCPConn); ok {
		if err := carrier.ConfigureTCP(tcp); err != nil {
			connection.Close()
			return nil, err
		}
	}
	firstHopAddress := connection.RemoteAddr().String()
	targetAddress := spec.DialAddress()
	if route.proxy == nil {
		targetAddress = firstHopAddress
	}
	return &preparedWebSocketConnection{
		Conn: connection, proxy: route.proxy, firstHopAddress: firstHopAddress, targetAddress: targetAddress,
	}, nil
}

// openPreparedCreation admits one prepared carrier lane and creates a session.
func (c *Client) openPreparedCreation(ctx context.Context, url laneurl.URL, attempt creationAttempt,
	connection net.Conn) (acceptedLane, creationResult, error) {
	if url.Scheme().WebSocket() {
		webSocket, result, mapping, initialFrame, err := c.createWebSocketSession(
			ctx, url, attempt, connection,
		)
		if err != nil {
			connection.Close()
		}
		return acceptedLane{connection: webSocket, mapping: mapping, initialFrame: initialFrame}, result, err
	}
	stopClose := context.AfterFunc(ctx, func() { connection.Close() })
	defer stopClose()
	result, mapping, initialFrame, err := c.createRawSession(ctx, connection, attempt)
	if err != nil {
		connection.Close()
		return acceptedLane{}, creationResult{}, err
	}
	return acceptedLane{
		connection: carrier.NewStreamConn(connection), mapping: mapping, initialFrame: initialFrame,
	}, result, nil
}

// openPreparedJoin authenticates one prepared carrier as an additional or reconnecting generation.
func (c *Client) openPreparedJoin(ctx context.Context, url laneurl.URL, attempt creationAttempt,
	session creationResult, connection net.Conn) (acceptedLane, error) {
	if url.Scheme().WebSocket() {
		accepted, err := c.joinWebSocketSession(ctx, url, attempt, session, connection)
		if err != nil {
			connection.Close()
		}
		return accepted, err
	}
	stopClose := context.AfterFunc(ctx, func() { connection.Close() })
	defer stopClose()
	mapping, initialFrame, err := c.joinRawSession(ctx, connection, attempt, session)
	if err != nil {
		connection.Close()
		return acceptedLane{}, err
	}
	return acceptedLane{
		connection: carrier.NewStreamConn(connection), mapping: mapping, initialFrame: initialFrame,
	}, nil
}

// dialStream connects and prepares one raw TCP or TLS stream in route-safe order.
func (c *Client) dialStream(ctx context.Context, spec lanespec.Spec) (net.Conn, error) {
	url := spec.URL()
	handshakeContext, cancel := context.WithTimeout(ctx, c.config.HandshakeTimeout)
	defer cancel()
	connection, err := c.config.Dialer.DialContext(handshakeContext, "tcp", spec.DialAddress())
	if err != nil {
		return nil, fmt.Errorf("dial %s lane: %w", url.Scheme(), err)
	}
	if tcp, ok := connection.(*net.TCPConn); ok {
		if err := carrier.ConfigureTCP(tcp); err != nil {
			connection.Close()
			return nil, err
		}
	}
	if url.Scheme() == laneurl.TCP {
		return connection, nil
	}
	secure := tls.Client(connection, c.tlsConfig(url, connection.RemoteAddr().String()))
	if err := secure.HandshakeContext(handshakeContext); err != nil {
		connection.Close()
		return nil, fmt.Errorf("perform TLS lane handshake: %w", err)
	}
	return secure, nil
}

// createRawSession performs one authenticated raw TCP or TLS creation exchange.
func (c *Client) createRawSession(ctx context.Context, connection net.Conn,
	attempt creationAttempt) (creationResult, clockmap.Mapping, protocol.Frame, error) {
	hello := protocol.ClientHello{
		Mode: protocol.HelloCreate, UnixSeconds: attempt.unixSeconds, MonotonicMicros: attempt.monotonicMicros,
		Nonce: attempt.nonce, LaneID: attempt.laneID, Generation: attempt.generation, PathGroupID: attempt.pathGroupID,
		Target: c.config.Target,
	}
	if err := protocol.SignClientHello(&hello, c.config.Token); err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := connection.SetDeadline(operationDeadline(ctx, c.config.HandshakeTimeout)); err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := protocol.WriteClientHello(connection, hello); err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	response, err := protocol.ReadServerHello(connection)
	clientReceiveMicros := c.config.Clock.NowMicros()
	if err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := protocol.VerifyServerHello(response, c.config.Token); err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := observeServerHello(attempt, response); err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	if response.Result == protocol.ServerRejected {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, &RejectionError{
			Code: response.ErrorCode, Class: response.ErrorClass, Scope: response.ErrorScope,
			Diagnostic: response.Diagnostic,
		}
	}
	if response.Result != protocol.ServerSessionCreated || response.PathGroupID != attempt.pathGroupID {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, ErrUnexpectedServerResponse
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	result := creationResult{
		sessionID: response.SessionID, sessionSecret: response.SessionSecret,
		receiveMicros: response.ReceiveMicros, sendMicros: response.SendMicros,
	}
	mapping, syncFrame, err := completeCreation(attempt.monotonicMicros, clientReceiveMicros, result)
	return result, mapping, syncFrame, err
}

// joinRawSession performs one session-secret-authenticated raw stream join.
func (c *Client) joinRawSession(ctx context.Context, connection net.Conn, attempt creationAttempt,
	session creationResult) (clockmap.Mapping, protocol.Frame, error) {
	hello := protocol.ClientHello{
		Mode: protocol.HelloJoin, UnixSeconds: attempt.unixSeconds, MonotonicMicros: attempt.monotonicMicros,
		Nonce: attempt.nonce, LaneID: attempt.laneID, Generation: attempt.generation,
		PathGroupID: attempt.pathGroupID, SessionID: session.sessionID,
	}
	if err := protocol.SignClientHello(&hello, session.sessionSecret[:]); err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := connection.SetDeadline(operationDeadline(ctx, c.config.HandshakeTimeout)); err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := protocol.WriteClientHello(connection, hello); err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	response, err := protocol.ReadServerHello(connection)
	clientReceiveMicros := c.config.Clock.NowMicros()
	if err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := protocol.VerifyServerHello(response, session.sessionSecret[:]); err != nil {
		if response.Result == protocol.ServerRejected &&
			protocol.VerifyServerHello(response, c.config.Token) == nil &&
			response.ErrorCode == protocol.ErrorSessionNotFound &&
			response.ErrorClass == protocol.ErrorSessionGone &&
			response.ErrorScope == protocol.ErrorScopeSession {
			if observeErr := observeServerHello(attempt, response); observeErr != nil {
				return clockmap.Mapping{}, protocol.Frame{}, observeErr
			}
			return clockmap.Mapping{}, protocol.Frame{}, ErrSessionGone
		}
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	if err := observeServerHello(attempt, response); err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	if response.Result == protocol.ServerRejected {
		rejection := &RejectionError{
			Code: response.ErrorCode, Class: response.ErrorClass, Scope: response.ErrorScope,
			Diagnostic: response.Diagnostic,
		}
		if response.ErrorClass == protocol.ErrorSessionGone {
			return clockmap.Mapping{}, protocol.Frame{}, fmt.Errorf("%w: %w", ErrSessionGone, rejection)
		}
		return clockmap.Mapping{}, protocol.Frame{}, rejection
	}
	if response.Result != protocol.ServerLaneAccepted || response.SessionID != session.sessionID ||
		response.PathGroupID != attempt.pathGroupID {
		return clockmap.Mapping{}, protocol.Frame{}, ErrUnexpectedServerResponse
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	result := creationResult{
		sessionID:     response.SessionID,
		receiveMicros: response.ReceiveMicros, sendMicros: response.SendMicros,
	}
	return completeCreation(attempt.monotonicMicros, clientReceiveMicros, result)
}

// createWebSocketSession performs header admission and reads the session-created frame.
func (c *Client) createWebSocketSession(ctx context.Context,
	url laneurl.URL, attempt creationAttempt,
	prepared net.Conn) (carrier.Conn, creationResult, clockmap.Mapping, protocol.Frame, error) {
	headers, err := wsheader.Headers(wsheader.Create{
		Token: string(c.config.Token), Target: c.config.Target, LaneID: attempt.laneID,
		Generation:  attempt.generation,
		PathGroupID: attempt.pathGroupID, Nonce: attempt.nonce, UnixSeconds: attempt.unixSeconds,
		MonotonicMicros: attempt.monotonicMicros,
	})
	if err != nil {
		return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	connection, response, handshakeContext, cancel, err := c.dialWebSocket(ctx, url, headers, prepared)
	defer cancel()
	if err != nil {
		if response != nil {
			if rejection := authenticatedWebSocketRejection(
				response, attempt, c.config.Token, nil,
			); rejection != nil {
				return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{}, rejection
			}
			if permanentHTTPRejection(response.StatusCode) {
				return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{},
					fmt.Errorf("%w: WebSocket admission returned HTTP %d: %v", ErrLaneRejected,
						response.StatusCode, err)
			}
			return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{},
				fmt.Errorf("WebSocket admission returned HTTP %d: %w", response.StatusCode, err)
		}
		return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	frame, err := connection.ReadFrame(handshakeContext)
	clientReceiveMicros := c.config.Clock.NowMicros()
	if err != nil {
		connection.Close()
		return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	created, err := protocol.ParseSessionCreated(frame)
	if err != nil || created.PathGroupID != attempt.pathGroupID {
		connection.Close()
		return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{}, ErrUnexpectedServerResponse
	}
	result := creationResult{
		sessionID: created.SessionID, sessionSecret: created.SessionSecret,
		receiveMicros: created.ReceiveMicros, sendMicros: created.SendMicros,
	}
	mapping, syncFrame, err := completeCreation(attempt.monotonicMicros, clientReceiveMicros, result)
	if err != nil {
		connection.Close()
		return nil, creationResult{}, clockmap.Mapping{}, protocol.Frame{}, err
	}
	return connection, result, mapping, syncFrame, nil
}

// joinWebSocketSession performs one signed WebSocket lane join.
func (c *Client) joinWebSocketSession(ctx context.Context, url laneurl.URL, attempt creationAttempt,
	session creationResult, prepared net.Conn) (acceptedLane, error) {
	join := wsheader.Join{
		Method: "GET", Path: url.EscapedPath(), SessionID: session.sessionID, LaneID: attempt.laneID,
		Generation: attempt.generation, PathGroupID: attempt.pathGroupID, Nonce: attempt.nonce,
		UnixSeconds: attempt.unixSeconds, MonotonicMicros: attempt.monotonicMicros,
	}
	if err := wsheader.SignJoin(&join, session.sessionSecret); err != nil {
		return acceptedLane{}, err
	}
	headers, err := wsheader.JoinHeaders(join)
	if err != nil {
		return acceptedLane{}, err
	}
	connection, response, handshakeContext, cancel, err := c.dialWebSocket(ctx, url, headers, prepared)
	defer cancel()
	if err != nil {
		if response != nil {
			if rejection := authenticatedWebSocketRejection(
				response, attempt, session.sessionSecret[:], c.config.Token,
			); rejection != nil {
				return acceptedLane{}, rejection
			}
			if response.StatusCode == http.StatusGone {
				return acceptedLane{}, ErrSessionGone
			}
			if permanentHTTPRejection(response.StatusCode) {
				return acceptedLane{}, fmt.Errorf("%w: WebSocket join returned HTTP %d: %v", ErrLaneRejected,
					response.StatusCode, err)
			}
			return acceptedLane{}, fmt.Errorf("WebSocket join returned HTTP %d: %w", response.StatusCode, err)
		}
		return acceptedLane{}, err
	}
	frame, err := connection.ReadFrame(handshakeContext)
	clientReceiveMicros := c.config.Clock.NowMicros()
	if err != nil {
		connection.Close()
		return acceptedLane{}, err
	}
	accepted, err := protocol.ParseLaneAccepted(frame)
	if err != nil || accepted.SessionID != session.sessionID || accepted.PathGroupID != attempt.pathGroupID {
		connection.Close()
		return acceptedLane{}, ErrUnexpectedServerResponse
	}
	result := creationResult{
		sessionID:     accepted.SessionID,
		receiveMicros: accepted.ReceiveMicros, sendMicros: accepted.SendMicros,
	}
	mapping, initialFrame, err := completeCreation(attempt.monotonicMicros, clientReceiveMicros, result)
	if err != nil {
		connection.Close()
		return acceptedLane{}, err
	}
	return acceptedLane{connection: connection, mapping: mapping, initialFrame: initialFrame}, nil
}

// authenticatedWebSocketRejection returns nil for an untrusted response and otherwise validates and classifies it.
func authenticatedWebSocketRejection(response *http.Response, attempt creationAttempt,
	key, sessionGoneKey []byte) error {
	rejection, err := wsheader.ParseRejection(response.Header)
	if err != nil {
		return nil
	}
	if protocol.VerifyServerHello(rejection, key) != nil {
		if len(sessionGoneKey) == 0 ||
			protocol.VerifyServerHello(rejection, sessionGoneKey) != nil ||
			rejection.ErrorCode != protocol.ErrorSessionNotFound ||
			rejection.ErrorClass != protocol.ErrorSessionGone ||
			rejection.ErrorScope != protocol.ErrorScopeSession {
			return nil
		}
	}
	if err := observeServerHello(attempt, rejection); err != nil {
		return err
	}
	rejectionError := &RejectionError{
		Code: rejection.ErrorCode, Class: rejection.ErrorClass, Scope: rejection.ErrorScope,
		Diagnostic: rejection.Diagnostic,
	}
	if rejection.ErrorClass == protocol.ErrorSessionGone {
		return fmt.Errorf("%w: %w", ErrSessionGone, rejectionError)
	}
	return rejectionError
}

// dialWebSocket performs an HTTP/1.1 binary WebSocket handshake over a configured TCP socket.
func (c *Client) dialWebSocket(ctx context.Context, url laneurl.URL,
	headers http.Header, prepared net.Conn) (carrier.Conn, *http.Response, context.Context, context.CancelFunc, error) {
	preparedWebSocket := prepared.(*preparedWebSocketConnection)
	handshakeContext, cancel := context.WithTimeout(ctx, c.config.HandshakeTimeout)
	proxyURL := preparedWebSocket.proxy
	httpTransport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL), ForceAttemptHTTP2: false,
		TLSHandshakeTimeout:    c.config.HandshakeTimeout,
		MaxResponseHeaderBytes: maximumWebSocketResponseHeaderBytes,
	}
	availableConnection := net.Conn(preparedWebSocket)
	var takeMutex sync.Mutex
	httpTransport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		takeMutex.Lock()
		defer takeMutex.Unlock()
		if availableConnection == nil {
			return nil, errors.New("prepared WebSocket connection already consumed")
		}
		prepared := availableConnection
		availableConnection = nil
		return prepared, nil
	}
	if url.Scheme() == laneurl.WSS {
		httpTransport.TLSClientConfig = c.tlsConfig(url, preparedWebSocket.targetAddress)
	}
	if proxyURL != nil && proxyURL.Scheme == "https" {
		dialProxy := httpTransport.DialContext
		httpTransport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, err := dialProxy(ctx, network, address)
			if err != nil {
				return nil, err
			}
			secure := tls.Client(connection, c.proxyTLSConfig(proxyURL.Hostname(), preparedWebSocket.firstHopAddress))
			if err := secure.HandshakeContext(ctx); err != nil {
				connection.Close()
				return nil, fmt.Errorf("perform HTTPS proxy TLS handshake: %w", err)
			}
			return secure, nil
		}
	}
	webSocket, response, err := websocket.Dial(handshakeContext, url.String(), &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport:     httpTransport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		HTTPHeader:   headers,
		Subprotocols: []string{wsheader.Subprotocol}, CompressionMode: websocket.CompressionDisabled,
	})
	httpTransport.CloseIdleConnections()
	if err != nil {
		if availableConnection != nil {
			availableConnection.Close()
		}
		return nil, response, handshakeContext, cancel, err
	}
	if webSocket.Subprotocol() != wsheader.Subprotocol {
		webSocket.CloseNow()
		return nil, response, handshakeContext, cancel, ErrUnexpectedServerResponse
	}
	connection := carrier.NewWebSocketConn(webSocket)
	connection.SetAbortConnection(preparedWebSocket)
	return connection, response, handshakeContext, cancel, nil
}

// webSocketFirstHop returns the direct destination or configured HTTP proxy socket address.
func (c *Client) webSocketFirstHop(spec lanespec.Spec) (string, error) {
	route, err := c.webSocketRoute(spec)
	return route.address, err
}

// webSocketRoute selects and validates one immutable first-hop route for a WebSocket attempt.
func (c *Client) webSocketRoute(spec lanespec.Spec) (webSocketRoute, error) {
	url := spec.URL()
	parsed, err := neturl.Parse(url.String())
	if err != nil {
		return webSocketRoute{}, err
	}
	proxy, err := c.proxy(&http.Request{URL: parsed})
	if err != nil {
		return webSocketRoute{}, fmt.Errorf("select WebSocket proxy: %w", err)
	}
	if proxy == nil {
		return webSocketRoute{address: spec.DialAddress()}, nil
	}
	if spec.ResolveIP().IsValid() {
		return webSocketRoute{}, errors.New("resolved WebSocket lane requires a direct connection and a matching NO_PROXY entry")
	}
	address, err := webSocketProxyAddress(proxy)
	if err != nil {
		return webSocketRoute{}, err
	}
	return webSocketRoute{address: address, proxy: proxy}, nil
}

// webSocketProxyAddress validates and canonicalizes one selected proxy socket address.
func webSocketProxyAddress(proxy *neturl.URL) (string, error) {
	switch proxy.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return "", fmt.Errorf("unsupported WebSocket proxy scheme %q", proxy.Scheme)
	}
	hostname := proxy.Hostname()
	if hostname == "" {
		return "", errors.New("WebSocket proxy has no host")
	}
	if _, err := netip.ParseAddr(hostname); err != nil &&
		(strings.ContainsAny(hostname, "[]:") || strings.HasPrefix(proxy.Host, "[")) {
		return "", fmt.Errorf("WebSocket proxy has invalid host %q", hostname)
	}
	port := proxy.Port()
	if port == "" {
		hostOnly := hostname
		if strings.Contains(hostOnly, ":") {
			hostOnly = "[" + hostOnly + "]"
		}
		if proxy.Host != hostOnly {
			return "", fmt.Errorf("WebSocket proxy has invalid authority %q", proxy.Host)
		}
		switch proxy.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		case "socks5", "socks5h":
			port = "1080"
		}
	} else {
		portNumber, err := strconv.ParseUint(port, 10, 16)
		if err != nil || portNumber == 0 {
			return "", fmt.Errorf("WebSocket proxy port %q must be between 1 and 65535", port)
		}
		port = strconv.FormatUint(portNumber, 10)
	}
	return net.JoinHostPort(hostname, port), nil
}

// proxy selects one WebSocket forward proxy from explicit client policy or the process environment.
func (c *Client) proxy(request *http.Request) (*neturl.URL, error) {
	requestCopy := request.Clone(request.Context())
	urlCopy := *request.URL
	requestCopy.URL = &urlCopy
	switch requestCopy.URL.Scheme {
	case "ws":
		requestCopy.URL.Scheme = "http"
	case "wss":
		requestCopy.URL.Scheme = "https"
	}
	var proxyURL *neturl.URL
	var err error
	if c.config.Proxy != nil {
		proxyURL, err = c.config.Proxy(requestCopy)
	} else {
		proxyURL, err = http.ProxyFromEnvironment(requestCopy)
	}
	if err != nil || proxyURL == nil {
		return proxyURL, err
	}
	proxyCopy := *proxyURL
	proxyCopy.Scheme = strings.ToLower(proxyCopy.Scheme)
	if proxyCopy.Scheme == "" {
		proxyCopy.Scheme = "http"
	}
	return &proxyCopy, nil
}

// permanentHTTPRejection reports client-side HTTP status classes that retrying cannot repair.
func permanentHTTPRejection(status int) bool {
	return status >= 100 && status < 500 && status != http.StatusRequestTimeout &&
		status != http.StatusTooManyRequests
}

// tlsConfig returns a per-connection TLS configuration with hostname validation and endpoint-scoped resumption.
func (c *Client) tlsConfig(url laneurl.URL, address string) *tls.Config {
	config := c.baseTLSConfig()
	if config.ServerName == "" {
		config.ServerName = url.Hostname()
	}
	namespaceClientSessionCache(config, string(url.Scheme())+"\x00"+address)
	return config
}

// proxyTLSConfig returns a per-connection configuration bound to the HTTPS proxy rather than the lane target.
func (c *Client) proxyTLSConfig(serverName, address string) *tls.Config {
	config := c.baseTLSConfig()
	config.ServerName = serverName
	config.NextProtos = []string{"http/1.1"}
	namespaceClientSessionCache(config, "proxy\x00"+address)
	return config
}

// namespaceClientSessionCache isolates TLS tickets while retaining the caller's aggregate cache bound.
func namespaceClientSessionCache(config *tls.Config, namespace string) {
	if config.ClientSessionCache != nil {
		config.ClientSessionCache = namespacedClientSessionCache{
			cache: config.ClientSessionCache, namespace: namespace,
		}
	}
}

// baseTLSConfig clones caller state and enforces the carrier protocol floor.
func (c *Client) baseTLSConfig() *tls.Config {
	var config *tls.Config
	if c.config.TLSConfig == nil {
		config = &tls.Config{}
	} else {
		config = c.config.TLSConfig.Clone()
	}
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	return config
}

// classifyLaneFailure maps one error to retry, lane, or session scope.
func classifyLaneFailure(err error) failureDisposition {
	if errors.Is(err, ErrSessionGone) || errors.Is(err, relay.ErrCounterExhausted) ||
		errors.Is(err, relay.ErrEndpointFailure) {
		return failureCloseSession
	}
	if rejection, ok := errors.AsType[*RejectionError](err); ok {
		switch rejection.Class {
		case protocol.ErrorRetryable:
			if rejection.Scope == protocol.ErrorScopeSession {
				return failureCloseSession
			}
			return failureRetry
		case protocol.ErrorSessionGone, protocol.ErrorSessionRejected:
			return failureCloseSession
		default:
			return failureCloseLane
		}
	}
	if remote, ok := errors.AsType[*relay.RemoteError](err); ok {
		if remote.Value.Class == protocol.ErrorRetryable {
			if remote.Value.Scope == protocol.ErrorScopeSession {
				return failureCloseSession
			}
			return failureRetry
		}
		if remote.Value.Scope == protocol.ErrorScopeSession {
			return failureCloseSession
		}
		return failureCloseLane
	}
	if errors.Is(err, protocol.ErrAuthenticationFailed) || errors.Is(err, ErrUnexpectedServerResponse) ||
		errors.Is(err, ErrLaneRejected) || errors.Is(err, os.ErrPermission) ||
		relay.IsProtocolViolation(err) {
		return failureCloseLane
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return failureCloseLane
	}
	if _, ok := errors.AsType[tls.RecordHeaderError](err); ok {
		return failureCloseLane
	}
	if _, ok := errors.AsType[x509.HostnameError](err); ok {
		return failureCloseLane
	}
	if _, ok := errors.AsType[x509.UnknownAuthorityError](err); ok {
		return failureCloseLane
	}
	return failureRetry
}

// waitReconnect waits for one per-lane retry delay or cancellation.
func waitReconnect(ctx context.Context, delay time.Duration) error {
	jitter := rand.N(delay + 1)
	timer := time.NewTimer(jitter)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// nextReconnectDelay doubles delay up to the reconnect maximum.
func nextReconnectDelay(delay time.Duration) time.Duration {
	if delay >= maximumReconnectDelay/2 {
		return maximumReconnectDelay
	}
	return delay * 2
}

// reconnectDelayAfterUptime resets retry history only after sustained healthy service.
func reconnectDelayAfterUptime(delay, uptime time.Duration) time.Duration {
	if uptime >= reconnectStabilityInterval {
		return initialReconnectDelay
	}
	return delay
}

// observeServerHello accepts one authenticated time sample bound to attempt.
func observeServerHello(attempt creationAttempt, response protocol.ServerHello) error {
	if response.RequestNonce != attempt.nonce {
		return ErrUnexpectedServerResponse
	}
	attempt.authenticationClock.Observe(response.ServerUnixSeconds)
	return nil
}

// completeCreation derives the initial clock mapping and ordered synchronization frame.
func completeCreation(clientSendMicros,
	clientReceiveMicros uint64, result creationResult) (clockmap.Mapping, protocol.Frame, error) {
	mapping, err := clockmap.Estimate(clockmap.Sample{
		LocalSendMicros: clientSendMicros, RemoteReceiveMicros: result.receiveMicros,
		RemoteSendMicros: result.sendMicros, LocalReceiveMicros: clientReceiveMicros,
	})
	if err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	syncFrame, err := protocol.MarshalClockSync(protocol.ClockSync{
		ClientSendMicros: clientSendMicros, ServerReceiveMicros: result.receiveMicros,
		ServerSendMicros: result.sendMicros, ClientReceiveMicros: clientReceiveMicros,
	})
	if err != nil {
		return clockmap.Mapping{}, protocol.Frame{}, err
	}
	return mapping, syncFrame, nil
}

// buildLanes assigns one stable lane ID per occurrence and one path group per canonical lane route.
func buildLanes(specs []lanespec.Spec, wallClock func() time.Time) []clientLane {
	groups := make(map[pathGroupKey]protocol.PathGroupID)
	lanes := make([]clientLane, 0, len(specs))
	for _, spec := range specs {
		key := pathGroupKey{url: spec.URL().String(), resolveIP: spec.ResolveIP()}
		group := groups[key]
		if group.IsZero() {
			group = protocol.NewPathGroupID()
			groups[key] = group
		}
		lanes = append(lanes, clientLane{
			spec: spec, laneID: protocol.NewLaneID(), pathGroupID: group,
			authenticationClock: newAuthenticationClock(wallClock),
		})
	}
	return lanes
}

// validateConfig verifies client startup and resource limits before binding UDP.
func validateConfig(config Config) ([]lanespec.Spec, error) {
	if err := wsheader.ValidateBearerToken(string(config.Token)); err != nil {
		return nil, fmt.Errorf("%w: invalid WIREHOP_TOKEN", ErrInvalidConfig)
	}
	if config.TLSConfig != nil {
		if config.TLSConfig.InsecureSkipVerify {
			return nil, fmt.Errorf("%w: TLS certificate verification cannot be disabled", ErrInvalidConfig)
		}
		if config.TLSConfig.MaxVersion != 0 && config.TLSConfig.MaxVersion < tls.VersionTLS12 {
			return nil, fmt.Errorf("%w: TLS maximum version is below TLS 1.2", ErrInvalidConfig)
		}
		if config.TLSConfig.MinVersion != 0 && config.TLSConfig.MaxVersion != 0 &&
			config.TLSConfig.MinVersion > config.TLSConfig.MaxVersion {
			return nil, fmt.Errorf("%w: TLS minimum version exceeds maximum version", ErrInvalidConfig)
		}
	}
	if len(config.Lanes) == 0 || len(config.Lanes) > config.MaxLanes || config.MaxLanes < 1 ||
		!validListenAddress(config.Listen) || !config.Target.Valid() ||
		config.HandshakeTimeout <= 0 || config.IngressLimits.Packets <= 0 ||
		config.IngressLimits.Bytes <= 0 || config.LaneLimits.Packets <= 0 || config.LaneLimits.Bytes <= 0 ||
		config.DeduplicationWindow <= 0 || config.StartupTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	if err := config.Deadlines.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	specs := append([]lanespec.Spec(nil), config.Lanes...)
	validationClient := &Client{config: config}
	for index, spec := range specs {
		if !spec.Valid() {
			return nil, fmt.Errorf("%w: lane %d is invalid", ErrInvalidConfig, index+1)
		}
		url := spec.URL()
		if url.Scheme().WebSocket() {
			if _, err := validationClient.webSocketFirstHop(spec); err != nil {
				return nil, fmt.Errorf("%w: lane %d: %w", ErrInvalidConfig, index+1, err)
			}
		}
	}
	return specs, nil
}

// validListenAddress reports whether address is a canonical IP literal bind address.
func validListenAddress(address netip.AddrPort) bool {
	return address.IsValid() && address.Addr() == address.Addr().Unmap() && !address.Addr().IsMulticast()
}

// operationDeadline returns the earlier context or operation deadline.
func operationDeadline(ctx context.Context, timeout time.Duration) time.Time {
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}
