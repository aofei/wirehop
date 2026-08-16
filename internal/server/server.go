// Package server implements WireHop server admission and session ownership.
package server

import (
	"bufio"
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/auth"
	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wsheader"
	"github.com/coder/websocket"
)

var (
	// ErrInvalidConfig indicates missing authentication, policy, timing, or queue configuration.
	ErrInvalidConfig = errors.New("invalid server configuration")
	// ErrSessionLimit indicates that the server has reached its bounded session capacity.
	ErrSessionLimit = errors.New("server session limit reached")
	// ErrSessionIDCollision indicates that generated session identity was already retained.
	ErrSessionIDCollision = errors.New("generated session ID collision")
)

// Config defines carrier admission and per-session resource limits.
type Config struct {
	Token                []byte
	Targets              policy.TargetSet
	Clock                relay.Clock
	WallClock            func() time.Time
	Logger               *slog.Logger
	Resolver             target.Resolver
	AuthenticationSkew   time.Duration
	HandshakeTimeout     time.Duration
	ReplayEntries        int
	JoinNonceEntries     int
	MaxSessions          int
	MaxLanesPerSession   int
	MaxPendingAdmissions int
	ReconnectGrace       time.Duration
	IngressLimits        packetqueue.Limits
	LaneLimits           packetqueue.Limits
	RetentionLimits      retention.Limits
	Deadlines            relay.DeadlinePolicy
	DeduplicationWindow  int
}

// Server accepts authenticated carrier lanes and owns their relay sessions.
type Server struct {
	config     Config
	replay     *auth.ReplayCache
	retention  *retention.Budget
	mu         sync.Mutex
	sessions   map[protocol.SessionID]*serverSession
	creating   int
	admissions chan struct{}
}

// webSocketAdmissionListener reserves authentication capacity before HTTP allocates a connection worker.
type webSocketAdmissionListener struct {
	net.Listener
	owner *Server
}

// admissionConnection transfers one pre-header reservation into the HTTP request context.
type admissionConnection struct {
	net.Conn
	owner *Server
	once  sync.Once
}

// admissionContextKey identifies a pre-reserved WebSocket admission connection.
type admissionContextKey struct{}

// webSocketResponseWriter clears HTTP handshake deadlines when the upgraded socket is hijacked.
type webSocketResponseWriter struct {
	http.ResponseWriter
	connection *net.Conn
}

// reportableLaneError marks a local carrier or session failure that requires operator attention.
type reportableLaneError struct {
	cause error
}

// activeLaneError marks a failure returned after authentication and lane activation.
type activeLaneError struct {
	cause error
}

// Error returns the marked failure detail.
func (e *reportableLaneError) Error() string {
	return e.cause.Error()
}

// Unwrap returns the original local failure.
func (e *reportableLaneError) Unwrap() error {
	return e.cause
}

// Error returns the active lane failure detail.
func (e *activeLaneError) Error() string {
	return e.cause.Error()
}

// Unwrap returns the original active lane failure.
func (e *activeLaneError) Unwrap() error {
	return e.cause
}

// Hijack transfers the connection to WebSocket ownership without retaining HTTP request deadlines.
func (w webSocketResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	connection, readWriter, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		connection.Close()
		return nil, nil, fmt.Errorf("clear WebSocket handshake deadline: %w", err)
	}
	*w.connection = connection
	return connection, readWriter, nil
}

// Snapshot is a point-in-time view of bounded server resources.
type Snapshot struct {
	Sessions          int
	AttachedLanes     int
	Detached          int
	CreatingSessions  int
	PendingAdmissions int
	RetainedPackets   int
	RetainedBytes     int
}

// New validates config and returns a server.
func New(config Config) (*Server, error) {
	if err := wsheader.ValidateBearerToken(string(config.Token)); err != nil {
		return nil, fmt.Errorf("%w: invalid WIREHOP_TOKEN", ErrInvalidConfig)
	}
	if config.Targets.Len() == 0 || config.AuthenticationSkew < time.Second ||
		config.HandshakeTimeout <= 0 || config.ReplayEntries <= 0 || config.JoinNonceEntries <= 0 ||
		config.MaxSessions <= 0 || config.MaxLanesPerSession <= 0 || config.MaxPendingAdmissions <= 0 ||
		config.ReconnectGrace <= 0 ||
		config.IngressLimits.Packets <= 0 ||
		config.IngressLimits.Bytes <= 0 || config.LaneLimits.Packets <= 0 || config.LaneLimits.Bytes <= 0 ||
		config.RetentionLimits.Packets <= 0 || config.RetentionLimits.Bytes <= 0 ||
		config.DeduplicationWindow <= 0 {
		return nil, ErrInvalidConfig
	}
	if err := config.Deadlines.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	if config.Clock == nil {
		config.Clock = monotime.New()
	}
	if config.WallClock == nil {
		config.WallClock = time.Now
	}
	config.IngressLimits.ControlPreemption = true
	config.LaneLimits.ControlPreemption = false
	config.Token = append([]byte(nil), config.Token...)
	replay, err := auth.NewReplayCache(config.ReplayEntries)
	if err != nil {
		return nil, err
	}
	retentionBudget, err := retention.NewBudget(config.RetentionLimits)
	if err != nil {
		return nil, err
	}
	return &Server{
		config: config, replay: replay, retention: retentionBudget,
		sessions:   make(map[protocol.SessionID]*serverSession),
		admissions: make(chan struct{}, config.MaxPendingAdmissions),
	}, nil
}

// beginAdmission reserves one bounded unauthenticated handshake slot.
func (s *Server) beginAdmission() bool {
	select {
	case s.admissions <- struct{}{}:
		return true
	default:
		return false
	}
}

// endAdmission releases one unauthenticated handshake slot.
func (s *Server) endAdmission() {
	<-s.admissions
}

// WebSocketListener bounds TLS handshakes and HTTP header parsing with the shared admission limit. For WSS, apply this
// wrapper before the TLS listener so [http.Server] receives the resulting TLS connection directly.
func (s *Server) WebSocketListener(listener net.Listener) net.Listener {
	return &webSocketAdmissionListener{Listener: listener, owner: s}
}

// Accept rejects excess pre-header connections and returns the next connection with reserved capacity.
func (l *webSocketAdmissionListener) Accept() (net.Conn, error) {
	for {
		connection, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.owner.beginAdmission() {
			return &admissionConnection{Conn: connection, owner: l.owner}, nil
		}
		connection.Close()
	}
}

// release returns this connection's admission reservation exactly once.
func (c *admissionConnection) release() {
	c.once.Do(c.owner.endAdmission)
}

// Close closes the transport and releases any reservation not consumed by a handler.
func (c *admissionConnection) Close() error {
	err := c.Conn.Close()
	c.release()
	return err
}

// NetConn returns the wrapped connection for carrier socket configuration.
func (c *admissionConnection) NetConn() net.Conn {
	return c.Conn
}

// WebSocketConnContext attaches a pre-header admission reservation to its HTTP connection context.
func (s *Server) WebSocketConnContext(parent context.Context, connection net.Conn) context.Context {
	admitted, ok := connection.(*admissionConnection)
	if !ok {
		if provider, wrapped := connection.(interface{ NetConn() net.Conn }); wrapped {
			admitted, ok = provider.NetConn().(*admissionConnection)
		}
	}
	if ok && admitted.owner == s {
		return context.WithValue(parent, admissionContextKey{}, admitted)
	}
	return parent
}

// createSession reserves capacity and creates one target-owning relay session.
func (s *Server) createSession(ctx context.Context, endpointTarget target.Endpoint) (*serverSession, error) {
	s.mu.Lock()
	if len(s.sessions)+s.creating >= s.config.MaxSessions {
		s.mu.Unlock()
		return nil, ErrSessionLimit
	}
	s.creating++
	s.mu.Unlock()
	inserted := false
	defer func() {
		if !inserted {
			s.mu.Lock()
			s.creating--
			s.mu.Unlock()
		}
	}()
	endpoint, err := datagram.OpenRemote(ctx, endpointTarget, s.config.Resolver)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", relay.ErrEndpointFailure, err)
	}
	sessionID := protocol.NewSessionID()
	sessionSecret := protocol.NewSessionSecret()
	session, err := newServerSession(ctx, s, sessionID, sessionSecret, endpoint)
	if err != nil {
		endpoint.Close()
		return nil, err
	}
	s.mu.Lock()
	if s.sessions[sessionID] != nil {
		s.mu.Unlock()
		session.close()
		return nil, ErrSessionIDCollision
	}
	s.creating--
	s.sessions[sessionID] = session
	inserted = true
	s.mu.Unlock()
	session.start()
	return session, nil
}

// findSession returns one retained attached or detached session.
func (s *Server) findSession(id protocol.SessionID) *serverSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

// removeSession removes session only when the registry still points to the same instance.
func (s *Server) removeSession(id protocol.SessionID, session *serverSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[id] == session {
		delete(s.sessions, id)
	}
}

// Snapshot returns bounded aggregate session and lane counts without credential material.
func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	sessions := make([]*serverSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	creating := s.creating
	s.mu.Unlock()
	result := Snapshot{
		Sessions: len(sessions), CreatingSessions: creating, PendingAdmissions: len(s.admissions),
	}
	usage := s.retention.Usage()
	result.RetainedPackets = usage.Packets
	result.RetainedBytes = usage.Bytes
	for _, session := range sessions {
		session.mu.Lock()
		lanes := len(session.lanes)
		reservations := session.reservations
		session.mu.Unlock()
		result.AttachedLanes += lanes
		if lanes == 0 && reservations == 0 {
			result.Detached++
		}
	}
	return result
}

// Serve accepts binary stream carrier connections until context cancellation or listener failure.
func (s *Server) Serve(parent context.Context, listener net.Listener) error {
	if listener == nil {
		return ErrInvalidConfig
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopClose := context.AfterFunc(ctx, func() { listener.Close() })
	defer stopClose()
	var sessions sync.WaitGroup
	defer sessions.Wait()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			cancel()
			listener.Close()
			return fmt.Errorf("accept stream lane: %w", err)
		}
		if !s.beginAdmission() {
			connection.Close()
			continue
		}
		sessions.Go(func() {
			release := sync.OnceFunc(s.endAdmission)
			defer release()
			if err := s.serveConnection(ctx, connection, release); ctx.Err() == nil {
				s.logLaneError("stream lane ended", connection.RemoteAddr(), err)
			}
		})
	}
}

// serveConnection authenticates and runs one binary stream creator or joiner lane.
func (s *Server) serveConnection(ctx context.Context, connection net.Conn, releaseAdmission func()) error {
	defer connection.Close()
	stopClose := context.AfterFunc(ctx, func() { connection.Close() })
	defer stopClose()
	if tcp, ok := connection.(*net.TCPConn); ok {
		if err := carrier.ConfigureTCP(tcp); err != nil {
			return reportLaneError(fmt.Errorf("configure accepted TCP connection: %w", err))
		}
	}
	if err := connection.SetDeadline(operationDeadline(ctx, s.config.HandshakeTimeout)); err != nil {
		return fmt.Errorf("set server handshake deadline: %w", err)
	}
	hello, err := protocol.ReadClientHello(connection)
	receiveMicros := s.config.Clock.NowMicros()
	if err != nil {
		if errors.Is(err, protocol.ErrUnsupportedVersion) {
			s.reject(connection, receiveMicros, protocol.Nonce{}, protocol.ErrorUnsupportedVersion,
				protocol.ErrorSessionRejected, "protocol version is not supported")
		}
		return fmt.Errorf("read client hello: %w", err)
	}
	if hello.Mode == protocol.HelloJoin {
		return s.serveRawJoin(connection, hello, receiveMicros, releaseAdmission)
	}
	return s.serveRawCreate(ctx, connection, hello, receiveMicros, releaseAdmission)
}

// serveRawCreate authenticates, prepares, and attaches one session creator lane.
func (s *Server) serveRawCreate(ctx context.Context, connection net.Conn, hello protocol.ClientHello,
	receiveMicros uint64, releaseAdmission func()) error {
	if err := protocol.VerifyClientHello(hello, s.config.Token); err != nil {
		s.reject(connection, receiveMicros, hello.Nonce, protocol.ErrorAuthentication,
			protocol.ErrorSessionRejected,
			"authentication failed")
		return err
	}
	now := s.config.WallClock().Unix()
	if err := auth.ValidateTimestamp(hello.UnixSeconds, now, s.config.AuthenticationSkew); err != nil {
		s.rejectWithKey(connection, receiveMicros, hello.Nonce, protocol.ErrorClockSkew,
			protocol.ErrorRetryable, protocol.ErrorScopeLane,
			"request timestamp rejected", s.config.Token)
		return err
	}
	expires, err := auth.ReplayExpiry(hello.UnixSeconds, s.config.AuthenticationSkew)
	if err != nil {
		return err
	}
	if err := s.replay.CheckAndStore(hello.Nonce, now, expires); err != nil {
		code := protocol.ErrorReplay
		class := protocol.ErrorSessionRejected
		if errors.Is(err, auth.ErrReplayCacheFull) {
			code = protocol.ErrorRateLimited
			class = protocol.ErrorRetryable
		}
		s.reject(connection, receiveMicros, hello.Nonce, code, class, "request replay rejected")
		return err
	}
	if !s.config.Targets.Allows(hello.Target) {
		s.reject(connection, receiveMicros, hello.Nonce, protocol.ErrorTargetDenied,
			protocol.ErrorSessionRejected,
			"target is not allowed")
		return policy.ErrInvalidTarget
	}
	session, err := s.createSession(ctx, hello.Target)
	if err != nil {
		code := protocol.ErrorUnavailable
		if errors.Is(err, ErrSessionLimit) {
			code = protocol.ErrorSessionLimit
		}
		s.reject(connection, receiveMicros, hello.Nonce, code, protocol.ErrorRetryable, "session unavailable")
		if !errors.Is(err, ErrSessionLimit) {
			return reportLaneError(fmt.Errorf("create target session: %w", err))
		}
		return err
	}
	if err := session.reserveLane(hello.LaneID, hello.Generation, hello.PathGroupID); err != nil {
		session.close()
		code, class, scope := reservationRejection(err)
		s.rejectWithKey(connection, receiveMicros, hello.Nonce, code, class, scope,
			"lane generation rejected", s.config.Token)
		return err
	}
	sessionID, sessionSecret, ok := session.creationCredentials()
	if !ok {
		session.close()
		return ErrSessionClosed
	}
	response := protocol.ServerHello{
		Result: protocol.ServerSessionCreated, RequestNonce: hello.Nonce,
		ServerUnixSeconds: s.config.WallClock().Unix(), SessionID: sessionID, SessionSecret: sessionSecret,
		PathGroupID: hello.PathGroupID, ReceiveMicros: receiveMicros, SendMicros: s.config.Clock.NowMicros(),
	}
	if err := protocol.SignServerHello(&response, s.config.Token); err != nil {
		session.close()
		return reportLaneError(fmt.Errorf("sign session creation response: %w", err))
	}
	if err := protocol.WriteServerHello(connection, response); err != nil {
		session.close()
		return fmt.Errorf("write session creation response: %w", err)
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		session.close()
		return fmt.Errorf("clear server handshake deadline: %w", err)
	}
	releaseAdmission()
	return session.runLane(carrier.NewStreamConn(connection), hello.LaneID, hello.Generation, hello.PathGroupID)
}

// serveRawJoin authenticates and attaches one generation to a retained session.
func (s *Server) serveRawJoin(connection net.Conn, hello protocol.ClientHello,
	receiveMicros uint64, releaseAdmission func()) error {
	session := s.findSession(hello.SessionID)
	if session == nil {
		s.reject(connection, receiveMicros, hello.Nonce, protocol.ErrorSessionNotFound,
			protocol.ErrorSessionGone,
			"session is not available")
		return ErrSessionClosed
	}
	secret, ok := session.joinSecret()
	if !ok {
		s.reject(connection, receiveMicros, hello.Nonce, protocol.ErrorSessionNotFound,
			protocol.ErrorSessionGone,
			"session is not available")
		return ErrSessionClosed
	}
	if err := protocol.VerifyClientHello(hello, secret[:]); err != nil {
		s.rejectWithKey(connection, receiveMicros, hello.Nonce, protocol.ErrorAuthentication,
			protocol.ErrorLaneRejected, protocol.ErrorScopeLane, "authentication failed", secret[:])
		return err
	}
	now := s.config.WallClock().Unix()
	if err := auth.ValidateTimestamp(hello.UnixSeconds, now, s.config.AuthenticationSkew); err != nil {
		s.rejectWithKey(connection, receiveMicros, hello.Nonce, protocol.ErrorClockSkew,
			protocol.ErrorRetryable, protocol.ErrorScopeLane, "request timestamp rejected", secret[:])
		return err
	}
	if err := session.acceptJoinNonce(hello.Nonce, hello.UnixSeconds, now); err != nil {
		code := protocol.ErrorReplay
		class := protocol.ErrorLaneRejected
		if errors.Is(err, auth.ErrReplayCacheFull) {
			code = protocol.ErrorRateLimited
			class = protocol.ErrorRetryable
		}
		s.rejectWithKey(connection, receiveMicros, hello.Nonce, code, class, protocol.ErrorScopeLane,
			"request replay rejected", secret[:])
		return err
	}
	if err := session.reserveLane(hello.LaneID, hello.Generation, hello.PathGroupID); err != nil {
		code, class, scope := reservationRejection(err)
		s.rejectWithKey(connection, receiveMicros, hello.Nonce, code, class, scope,
			"lane generation rejected", secret[:])
		return err
	}
	response := protocol.ServerHello{
		Result: protocol.ServerLaneAccepted, RequestNonce: hello.Nonce,
		ServerUnixSeconds: s.config.WallClock().Unix(), SessionID: session.id, PathGroupID: hello.PathGroupID,
		ReceiveMicros: receiveMicros, SendMicros: s.config.Clock.NowMicros(),
	}
	if err := protocol.SignServerHello(&response, secret[:]); err != nil {
		session.rejectReservedLane()
		return err
	}
	if err := protocol.WriteServerHello(connection, response); err != nil {
		session.rejectReservedLane()
		return err
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		session.rejectReservedLane()
		return err
	}
	releaseAdmission()
	return session.runLane(carrier.NewStreamConn(connection), hello.LaneID, hello.Generation, hello.PathGroupID)
}

// reject writes one authenticated raw-stream rejection.
func (s *Server) reject(connection net.Conn, receiveMicros uint64, requestNonce protocol.Nonce,
	code protocol.ErrorCode, class protocol.ErrorClass, diagnostic string) error {
	return s.rejectWithKey(connection, receiveMicros, requestNonce, code, class, protocol.ErrorScopeSession,
		diagnostic, s.config.Token)
}

// rejectWithKey writes one authenticated raw-stream rejection with key.
func (s *Server) rejectWithKey(connection net.Conn, receiveMicros uint64, requestNonce protocol.Nonce,
	code protocol.ErrorCode, class protocol.ErrorClass, scope protocol.ErrorScope, diagnostic string,
	key []byte) error {
	response := protocol.ServerHello{
		Result: protocol.ServerRejected, RequestNonce: requestNonce,
		ServerUnixSeconds: s.config.WallClock().Unix(), ReceiveMicros: receiveMicros,
		SendMicros: s.config.Clock.NowMicros(),
		ErrorCode:  code, ErrorClass: class, ErrorScope: scope, Diagnostic: diagnostic,
	}
	if err := protocol.SignServerHello(&response, key); err != nil {
		return err
	}
	if err := protocol.WriteServerHello(connection, response); err != nil {
		return fmt.Errorf("write server rejection: %w", err)
	}
	return nil
}

// WebSocketHandler returns an HTTP handler for authenticated WebSocket creator lanes.
func (s *Server) WebSocketHandler(ctx context.Context) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		release, admitted := s.webSocketAdmission(request)
		if !admitted {
			http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		defer release()
		if err := s.serveWebSocket(ctx, writer, request, release); ctx.Err() == nil {
			s.logLaneError("WebSocket lane ended", request.RemoteAddr, err)
		}
	})
}

// webSocketAdmission consumes a pre-header reservation or acquires one for directly hosted handlers.
func (s *Server) webSocketAdmission(request *http.Request) (func(), bool) {
	if admitted, ok := request.Context().Value(admissionContextKey{}).(*admissionConnection); ok &&
		admitted.owner == s {
		return admitted.release, true
	}
	if !s.beginAdmission() {
		return nil, false
	}
	return sync.OnceFunc(s.endAdmission), true
}

// reportLaneError marks a local failure for warning-level diagnostics at the carrier boundary.
func reportLaneError(err error) error {
	return &reportableLaneError{cause: err}
}

// logLaneError emits one actionable lane failure without trusting peer-provided diagnostic text.
func (s *Server) logLaneError(message string, remote any, err error) {
	if s.config.Logger == nil || !shouldReportLaneError(err) {
		return
	}
	if remoteError, ok := errors.AsType[*relay.RemoteError](err); ok {
		s.config.Logger.Warn(message,
			"remote", remote,
			"error", "peer reported protocol violation",
			"error_code", remoteError.Value.Code,
			"error_class", remoteError.Value.Class,
			"error_scope", remoteError.Value.Scope,
		)
		return
	}
	s.config.Logger.Warn(message, "remote", remote, "error", redactTargetError(err))
}

// redactTargetError removes endpoint details that may contain the authorized target address.
func redactTargetError(err error) error {
	if errors.Is(err, relay.ErrEndpointFailure) {
		return relay.ErrEndpointFailure
	}
	return err
}

// shouldReportLaneError selects only actionable local failures and authenticated protocol violations.
func shouldReportLaneError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if _, ok := errors.AsType[*reportableLaneError](err); ok {
		return true
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, relay.ErrRemoteClosed) {
		return false
	}
	_, active := errors.AsType[*activeLaneError](err)
	if !active {
		return false
	}
	if relay.IsProtocolViolation(err) || errors.Is(err, relay.ErrEndpointFailure) {
		return true
	}
	remoteError, ok := errors.AsType[*relay.RemoteError](err)
	return ok && remoteError.Value.Code == protocol.ErrorProtocolViolation
}

// serveWebSocket dispatches creation and join authentication before upgrading.
func (s *Server) serveWebSocket(ctx context.Context, writer http.ResponseWriter, request *http.Request,
	releaseAdmission func()) error {
	if strings.HasPrefix(request.Header.Get("Authorization"), "WireHop-HMAC ") {
		return s.serveWebSocketJoin(ctx, writer, request, releaseAdmission)
	}
	return s.serveWebSocketCreate(ctx, writer, request, releaseAdmission)
}

// serveWebSocketCreate authenticates headers before upgrading and running one creator lane.
func (s *Server) serveWebSocketCreate(ctx context.Context, writer http.ResponseWriter, request *http.Request,
	releaseAdmission func()) error {
	creation, err := wsheader.ParseCreate(request)
	receiveMicros := s.config.Clock.NowMicros()
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return err
	}
	if !hmac.Equal([]byte(creation.Token), s.config.Token) {
		return s.rejectWebSocket(writer, receiveMicros, creation.Nonce, protocol.ErrorAuthentication,
			protocol.ErrorSessionRejected, protocol.ErrorScopeSession, "authentication failed", s.config.Token,
			protocol.ErrAuthenticationFailed)
	}
	now := s.config.WallClock().Unix()
	if err := auth.ValidateTimestamp(creation.UnixSeconds, now, s.config.AuthenticationSkew); err != nil {
		return s.rejectWebSocket(writer, receiveMicros, creation.Nonce, protocol.ErrorClockSkew,
			protocol.ErrorRetryable, protocol.ErrorScopeLane, "request timestamp rejected", s.config.Token, err)
	}
	expires, err := auth.ReplayExpiry(creation.UnixSeconds, s.config.AuthenticationSkew)
	if err != nil {
		return err
	}
	if err := s.replay.CheckAndStore(creation.Nonce, now, expires); err != nil {
		code := protocol.ErrorReplay
		class := protocol.ErrorSessionRejected
		if errors.Is(err, auth.ErrReplayCacheFull) {
			code = protocol.ErrorRateLimited
			class = protocol.ErrorRetryable
		}
		return s.rejectWebSocket(writer, receiveMicros, creation.Nonce, code, class,
			protocol.ErrorScopeSession, "request replay rejected", s.config.Token, err)
	}
	if !s.config.Targets.Allows(creation.Target) {
		return s.rejectWebSocket(writer, receiveMicros, creation.Nonce, protocol.ErrorTargetDenied,
			protocol.ErrorSessionRejected, protocol.ErrorScopeSession, "target is not allowed", s.config.Token,
			policy.ErrInvalidTarget)
	}
	session, err := s.createSession(ctx, creation.Target)
	if err != nil {
		code := protocol.ErrorUnavailable
		cause := err
		if errors.Is(err, ErrSessionLimit) {
			code = protocol.ErrorSessionLimit
		} else {
			cause = reportLaneError(fmt.Errorf("create target session: %w", err))
		}
		return s.rejectWebSocket(writer, receiveMicros, creation.Nonce, code, protocol.ErrorRetryable,
			protocol.ErrorScopeSession, "session unavailable", s.config.Token, cause)
	}
	if err := session.reserveLane(creation.LaneID, creation.Generation, creation.PathGroupID); err != nil {
		session.close()
		code, class, scope := reservationRejection(err)
		return s.rejectWebSocket(writer, receiveMicros, creation.Nonce, code, class, scope,
			"lane generation rejected", s.config.Token, err)
	}
	stream, err := acceptWebSocket(writer, request)
	if err != nil {
		session.close()
		return err
	}
	defer stream.Close()
	sessionID, sessionSecret, ok := session.creationCredentials()
	if !ok {
		session.close()
		return ErrSessionClosed
	}
	created, err := protocol.MarshalSessionCreated(protocol.SessionCreated{
		SessionID: sessionID, SessionSecret: sessionSecret, PathGroupID: creation.PathGroupID,
		ReceiveMicros: receiveMicros, SendMicros: s.config.Clock.NowMicros(),
	})
	if err != nil {
		session.close()
		return reportLaneError(fmt.Errorf("marshal session creation response: %w", err))
	}
	handshakeContext, cancel := context.WithTimeout(ctx, s.config.HandshakeTimeout)
	defer cancel()
	if err := stream.WriteFrames(handshakeContext, []protocol.Frame{created}); err != nil {
		session.close()
		return err
	}
	releaseAdmission()
	return session.runLane(stream, creation.LaneID, creation.Generation, creation.PathGroupID)
}

// serveWebSocketJoin authenticates a session-bound join before upgrading.
func (s *Server) serveWebSocketJoin(ctx context.Context, writer http.ResponseWriter, request *http.Request,
	releaseAdmission func()) error {
	join, err := wsheader.ParseJoin(request)
	receiveMicros := s.config.Clock.NowMicros()
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return err
	}
	session := s.findSession(join.SessionID)
	if session == nil {
		return s.rejectWebSocket(writer, receiveMicros, join.Nonce, protocol.ErrorSessionNotFound,
			protocol.ErrorSessionGone, protocol.ErrorScopeSession, "session is not available", s.config.Token,
			ErrSessionClosed)
	}
	secret, ok := session.joinSecret()
	if !ok {
		return s.rejectWebSocket(writer, receiveMicros, join.Nonce, protocol.ErrorSessionNotFound,
			protocol.ErrorSessionGone, protocol.ErrorScopeSession, "session is not available", s.config.Token,
			ErrSessionClosed)
	}
	if err := wsheader.VerifyJoin(join, secret); err != nil {
		return s.rejectWebSocket(writer, receiveMicros, join.Nonce, protocol.ErrorAuthentication,
			protocol.ErrorLaneRejected, protocol.ErrorScopeLane, "authentication failed", secret[:], err)
	}
	now := s.config.WallClock().Unix()
	if err := auth.ValidateTimestamp(join.UnixSeconds, now, s.config.AuthenticationSkew); err != nil {
		return s.rejectWebSocket(writer, receiveMicros, join.Nonce, protocol.ErrorClockSkew,
			protocol.ErrorRetryable, protocol.ErrorScopeLane, "request timestamp rejected", secret[:], err)
	}
	if err := session.acceptJoinNonce(join.Nonce, join.UnixSeconds, now); err != nil {
		code := protocol.ErrorReplay
		class := protocol.ErrorLaneRejected
		if errors.Is(err, auth.ErrReplayCacheFull) {
			code = protocol.ErrorRateLimited
			class = protocol.ErrorRetryable
		}
		return s.rejectWebSocket(writer, receiveMicros, join.Nonce, code, class, protocol.ErrorScopeLane,
			"request replay rejected", secret[:], err)
	}
	if err := session.reserveLane(join.LaneID, join.Generation, join.PathGroupID); err != nil {
		code, class, scope := reservationRejection(err)
		return s.rejectWebSocket(writer, receiveMicros, join.Nonce, code, class, scope,
			"lane generation rejected", secret[:], err)
	}
	stream, err := acceptWebSocket(writer, request)
	if err != nil {
		session.rejectReservedLane()
		return err
	}
	defer stream.Close()
	accepted, err := protocol.MarshalLaneAccepted(protocol.LaneAccepted{
		SessionID: session.id, PathGroupID: join.PathGroupID, ReceiveMicros: receiveMicros,
		SendMicros: s.config.Clock.NowMicros(),
	})
	if err != nil {
		session.rejectReservedLane()
		return reportLaneError(fmt.Errorf("marshal lane acceptance response: %w", err))
	}
	handshakeContext, cancel := context.WithTimeout(ctx, s.config.HandshakeTimeout)
	defer cancel()
	if err := stream.WriteFrames(handshakeContext, []protocol.Frame{accepted}); err != nil {
		session.rejectReservedLane()
		return err
	}
	releaseAdmission()
	return session.runLane(stream, join.LaneID, join.Generation, join.PathGroupID)
}

// rejectWebSocket writes one authenticated WebSocket admission rejection and returns cause.
func (s *Server) rejectWebSocket(writer http.ResponseWriter, receiveMicros uint64, requestNonce protocol.Nonce,
	code protocol.ErrorCode, class protocol.ErrorClass, scope protocol.ErrorScope, diagnostic string, key []byte,
	cause error) error {
	rejection := protocol.ServerHello{
		Result: protocol.ServerRejected, RequestNonce: requestNonce,
		ServerUnixSeconds: s.config.WallClock().Unix(), ReceiveMicros: receiveMicros,
		SendMicros: s.config.Clock.NowMicros(), ErrorCode: code, ErrorClass: class, ErrorScope: scope,
		Diagnostic: diagnostic,
	}
	if err := protocol.SignServerHello(&rejection, key); err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return reportLaneError(fmt.Errorf("sign WebSocket rejection: %w", err))
	}
	if err := wsheader.SetRejection(writer.Header(), rejection); err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return reportLaneError(fmt.Errorf("encode WebSocket rejection: %w", err))
	}
	status := webSocketRejectionStatus(code)
	http.Error(writer, http.StatusText(status), status)
	return cause
}

// webSocketRejectionStatus maps a protocol rejection to its conventional HTTP status.
func webSocketRejectionStatus(code protocol.ErrorCode) int {
	switch code {
	case protocol.ErrorMalformed, protocol.ErrorProtocolViolation:
		return http.StatusBadRequest
	case protocol.ErrorUnsupportedVersion:
		return http.StatusUpgradeRequired
	case protocol.ErrorAuthentication, protocol.ErrorClockSkew:
		return http.StatusUnauthorized
	case protocol.ErrorTargetDenied:
		return http.StatusForbidden
	case protocol.ErrorReplay, protocol.ErrorStaleGeneration, protocol.ErrorLaneLimit:
		return http.StatusConflict
	case protocol.ErrorSessionNotFound:
		return http.StatusGone
	case protocol.ErrorRateLimited:
		return http.StatusTooManyRequests
	case protocol.ErrorSessionLimit, protocol.ErrorUnavailable:
		return http.StatusServiceUnavailable
	case protocol.ErrorInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// reservationRejection maps lane reservation state to one stable protocol disposition.
func reservationRejection(err error) (protocol.ErrorCode, protocol.ErrorClass, protocol.ErrorScope) {
	switch {
	case errors.Is(err, ErrSessionClosed):
		return protocol.ErrorSessionNotFound, protocol.ErrorSessionGone, protocol.ErrorScopeSession
	case errors.Is(err, ErrLaneLimit):
		return protocol.ErrorLaneLimit, protocol.ErrorLaneRejected, protocol.ErrorScopeLane
	case errors.Is(err, ErrPathGroupMismatch):
		return protocol.ErrorProtocolViolation, protocol.ErrorLaneRejected, protocol.ErrorScopeLane
	default:
		return protocol.ErrorStaleGeneration, protocol.ErrorLaneRejected, protocol.ErrorScopeLane
	}
}

// acceptWebSocket upgrades one validated request with the exact WireHop subprotocol.
func acceptWebSocket(writer http.ResponseWriter, request *http.Request) (*carrier.WebSocketConn, error) {
	var networkConnection net.Conn
	webSocket, err := websocket.Accept(webSocketResponseWriter{
		ResponseWriter: writer, connection: &networkConnection,
	}, request,
		&websocket.AcceptOptions{
			Subprotocols: []string{wsheader.Subprotocol}, CompressionMode: websocket.CompressionDisabled,
		})
	if err != nil {
		return nil, err
	}
	if webSocket.Subprotocol() != wsheader.Subprotocol {
		webSocket.CloseNow()
		return nil, wsheader.ErrInvalid
	}
	connection := carrier.NewWebSocketConn(webSocket)
	connection.SetAbortConnection(networkConnection)
	return connection, nil
}

// operationDeadline returns the earlier context or operation deadline.
func operationDeadline(ctx context.Context, timeout time.Duration) time.Time {
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}
