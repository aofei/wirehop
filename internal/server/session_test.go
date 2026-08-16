package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/netip"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/auth"
	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	targetpkg "github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
	"github.com/aofei/wirehop/internal/wsheader"
	"github.com/coder/websocket"
)

type closeModeCarrier struct {
	closed  chan struct{}
	once    sync.Once
	aborted atomic.Bool
}

func newCloseModeCarrier() *closeModeCarrier {
	return &closeModeCarrier{closed: make(chan struct{})}
}

func (c *closeModeCarrier) ReadFrame(ctx context.Context) (protocol.Frame, error) {
	select {
	case <-ctx.Done():
		return protocol.Frame{}, ctx.Err()
	case <-c.closed:
		return protocol.Frame{}, net.ErrClosed
	}
}

func (c *closeModeCarrier) WriteFrames(ctx context.Context, _ []protocol.Frame) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return net.ErrClosed
	default:
		return nil
	}
}

func (c *closeModeCarrier) WriteDataBatch(ctx context.Context, _ []protocol.Data) error {
	return c.WriteFrames(ctx, nil)
}

func (c *closeModeCarrier) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *closeModeCarrier) Abort() error {
	c.aborted.Store(true)
	return c.Close()
}

func TestLaneReconnectSessionRecreationAndGracefulClose(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	instance := newSessionTestServer(t, token, target, 100*time.Millisecond)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- instance.Serve(ctx, carrier.NewTCPOptionsListener(listener)) }()
	defer func() {
		cancelServer()
		<-serverDone
	}()
	_, carrierPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	laneSpec, err := lanespec.Parse(
		"url=tcp://relay.invalid:" + carrierPort + ",resolve=127.0.0.1",
	)
	if err != nil {
		t.Fatal(err)
	}
	clientInstance, err := client.Start(context.Background(), client.Config{
		Lanes: []lanespec.Spec{laneSpec, laneSpec}, Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: target,
		Token: token, HandshakeTimeout: time.Second,
		IngressLimits:       packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		LaneLimits:          packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientInstance.Close()
	waitSessionCondition(t, func() bool { return instance.Snapshot().AttachedLanes == 2 })
	session, laneID, generation := firstSessionLane(t, instance)
	session.mu.Lock()
	laneCancel := session.lanes[laneID].cancel
	session.mu.Unlock()
	laneCancel()
	reconnectDeadline := time.Now().Add(3 * time.Second)
	for {
		session.mu.Lock()
		current, active := session.lanes[laneID]
		highest := session.history[laneID].highestGeneration
		laneCount := len(session.lanes)
		session.mu.Unlock()
		if laneCount == 2 && active && current.generation > generation && highest > generation {
			break
		}
		if time.Now().After(reconnectDeadline) {
			t.Fatalf("reconnect state: lanes=%d active=%t generation=%d highest=%d snapshot=%+v",
				laneCount, active, current.generation, highest, instance.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	oldSessionID := clientInstance.SessionID()
	session.close()
	waitSessionCondition(t, func() bool {
		newSessionID := clientInstance.SessionID()
		snapshot := instance.Snapshot()
		return !newSessionID.IsZero() && newSessionID != oldSessionID && snapshot.Sessions == 1 &&
			snapshot.AttachedLanes == 2
	})
	if err := clientInstance.Close(); err != nil {
		t.Fatal(err)
	}
	if err := clientInstance.Wait(); err != context.Canceled {
		t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
	}
	waitSessionCondition(t, func() bool { return instance.Snapshot().Sessions == 0 })
}

func TestCreateSessionResolvesDomainTarget(t *testing.T) {
	addressTarget, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	domainTarget := targetpkg.MustParse(fmt.Sprintf("wg.example.com:%d", addressTarget.Port()))
	instance := newSessionTestServer(t, []byte("test-token"), domainTarget, time.Second)
	resolver := &serverTestResolver{addresses: []netip.Addr{addressTarget.Address()}}
	instance.config.Resolver = resolver
	session, err := instance.createSession(context.Background(), domainTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	if resolver.host != "wg.example.com." {
		t.Fatalf("resolver host = %q", resolver.host)
	}
}

func TestSessionLaneCancellationClosesNormally(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	laneID := protocol.LaneID{1}
	pathGroupID := protocol.PathGroupID{1}
	if err := session.reserveLane(laneID, 1, pathGroupID); err != nil {
		t.Fatal(err)
	}
	connection := newCloseModeCarrier()
	result := make(chan error, 1)
	go func() { result <- session.runLane(connection, laneID, 1, pathGroupID) }()
	waitSessionCondition(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		return len(session.lanes) == 1
	})
	session.mu.Lock()
	stop := session.lanes[laneID].cancel
	session.mu.Unlock()
	stop()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("runLane() error = %v, want %v", err, context.Canceled)
	}
	if connection.aborted.Load() {
		t.Fatal("ordinary session lane cancellation used an abortive close")
	}
}

func TestSessionCloseRacesLaneAttachment(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	for index := range 200 {
		session, err := instance.createSession(context.Background(), target)
		if err != nil {
			t.Fatal(err)
		}
		laneID := protocol.LaneID{byte(index + 1)}
		pathGroupID := protocol.PathGroupID{1}
		if err := session.reserveLane(laneID, 1, pathGroupID); err != nil {
			t.Fatal(err)
		}
		connection := newCloseModeCarrier()
		result := make(chan error, 1)
		go func() { result <- session.runLane(connection, laneID, 1, pathGroupID) }()
		session.close()
		if err := <-result; err == nil {
			t.Fatal("runLane() succeeded after concurrent session closure")
		}
		session.mu.Lock()
		lanes := len(session.lanes)
		session.mu.Unlock()
		if lanes != 0 {
			t.Fatalf("closed session retained %d lanes", lanes)
		}
	}
}

func TestLaneReservationDefersDetachedExpiry(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	grace := 10 * time.Millisecond
	instance := newSessionTestServer(t, []byte("test-token"), target, grace)
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.reserveLane(protocol.LaneID{1}, 1, protocol.PathGroupID{1}); err != nil {
		t.Fatal(err)
	}
	if snapshot := instance.Snapshot(); snapshot.Detached != 0 {
		t.Fatalf("snapshot counted accepted reservation as detached: %+v", snapshot)
	}
	session.mu.Lock()
	session.startDetachTimerLocked()
	detach := session.detach
	session.mu.Unlock()
	if detach != nil {
		t.Fatal("pending lane reservation started detached expiry")
	}
	time.Sleep(3 * grace)
	if session.ctx.Err() != nil {
		t.Fatal("pending lane reservation expired the session")
	}
	session.rejectReservedLane()
	waitSessionCondition(t, func() bool { return session.ctx.Err() != nil })
}

func TestStaleDetachedExpiryCannotCloseReservedSession(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Hour)
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.startDetachTimerLocked()
	stale := session.detach
	session.mu.Unlock()
	if stale == nil {
		t.Fatal("detached expiry was not created")
	}
	if err := session.reserveLane(protocol.LaneID{1}, 1, protocol.PathGroupID{1}); err != nil {
		t.Fatal(err)
	}
	session.expireDetached(stale)
	if session.ctx.Err() != nil {
		t.Fatal("stale detached expiry closed a reserved session")
	}
	session.rejectReservedLane()
	session.mu.Lock()
	current := session.detach
	session.mu.Unlock()
	if current == nil || current == stale {
		t.Fatal("new detached interval did not receive a distinct token")
	}
	session.expireDetached(stale)
	if session.ctx.Err() != nil {
		t.Fatal("old detached expiry closed a newer detached interval")
	}
	session.expireDetached(current)
	if session.ctx.Err() == nil {
		t.Fatal("current detached expiry did not close the session")
	}
}

func TestAdmissionLimit(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	if !instance.beginAdmission() {
		t.Fatal("first admission was rejected")
	}
	if instance.beginAdmission() {
		t.Fatal("admission limit accepted excess work")
	}
	instance.endAdmission()
	if !instance.beginAdmission() {
		t.Fatal("released admission capacity was not reusable")
	}
	instance.endAdmission()
}

func TestCanceledSessionCreationDoesNotLeak(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := instance.createSession(ctx, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("createSession() error = %v, want %v", err, context.Canceled)
	}
	waitSessionCondition(t, func() bool {
		snapshot := instance.Snapshot()
		return snapshot.Sessions == 0 && snapshot.CreatingSessions == 0
	})
}

func TestSessionCloseReleasesRetainedCapacity(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 32)
	payload[0] = 4
	if err := session.ingressQueue.Push(packetqueue.Item[relay.Packet]{
		Value: relay.Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 1000},
		Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if snapshot := instance.Snapshot(); snapshot.RetainedPackets != 1 || snapshot.RetainedBytes != len(payload) {
		t.Fatalf("snapshot retention = %+v", snapshot)
	}
	session.close()
	waitSessionCondition(t, func() bool {
		snapshot := instance.Snapshot()
		return snapshot.Sessions == 0 && snapshot.RetainedPackets == 0 && snapshot.RetainedBytes == 0
	})
}

func TestCreationCredentials(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	sessionID, secret, ok := session.creationCredentials()
	if !ok || sessionID != session.id || secret == (protocol.SessionSecret{}) {
		t.Fatalf("creationCredentials() = %v, %x, %t", sessionID, secret, ok)
	}
	session.close()
	if sessionID, secret, ok := session.creationCredentials(); ok || !sessionID.IsZero() ||
		secret != (protocol.SessionSecret{}) {
		t.Fatalf("closed creationCredentials() = %v, %x, %t", sessionID, secret, ok)
	}
}

func TestWebSocketCreationReplayCapacityIsRetryable(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	instance := newSessionTestServer(t, token, target, time.Second)
	replay, err := auth.NewReplayCache(1)
	if err != nil {
		t.Fatal(err)
	}
	instance.replay = replay
	now := time.Now().Unix()
	if err := instance.replay.CheckAndStore(protocol.Nonce{1}, now, now+60); err != nil {
		t.Fatal(err)
	}
	headers, err := wsheader.Headers(wsheader.Create{
		Token: string(token), Target: target, LaneID: protocol.LaneID{1}, Generation: 1,
		PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{2}, UnixSeconds: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
	request.Header = headers
	response := httptest.NewRecorder()
	instance.WebSocketHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("WebSocket status = %d, want %d", response.Code, http.StatusTooManyRequests)
	}
	rejection, err := wsheader.ParseRejection(response.Header())
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyServerHello(rejection, token); err != nil {
		t.Fatal(err)
	}
	if rejection.RequestNonce != (protocol.Nonce{2}) || rejection.ErrorCode != protocol.ErrorRateLimited ||
		rejection.ErrorClass != protocol.ErrorRetryable ||
		rejection.ErrorScope != protocol.ErrorScopeSession {
		t.Fatalf("WebSocket rejection = %+v", rejection)
	}
}

func TestWebSocketCreationReplayIsTerminal(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	instance := newSessionTestServer(t, token, target, time.Second)
	now := time.Now().Unix()
	nonce := protocol.Nonce{2}
	if err := instance.replay.CheckAndStore(nonce, now, now+60); err != nil {
		t.Fatal(err)
	}
	headers, err := wsheader.Headers(wsheader.Create{
		Token: string(token), Target: target, LaneID: protocol.LaneID{1}, Generation: 1,
		PathGroupID: protocol.PathGroupID{1}, Nonce: nonce, UnixSeconds: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
	request.Header = headers
	response := httptest.NewRecorder()
	instance.WebSocketHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("WebSocket status = %d, want %d", response.Code, http.StatusConflict)
	}
	rejection, err := wsheader.ParseRejection(response.Header())
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyServerHello(rejection, token); err != nil {
		t.Fatal(err)
	}
	if rejection.ErrorCode != protocol.ErrorReplay ||
		rejection.ErrorClass != protocol.ErrorSessionRejected ||
		rejection.ErrorScope != protocol.ErrorScopeSession {
		t.Fatalf("WebSocket rejection = %+v", rejection)
	}
}

func TestWebSocketClockSkewIsRetryable(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	instance := newSessionTestServer(t, token, target, time.Second)
	instance.config.WallClock = func() time.Time { return time.Unix(2_000, 0) }
	headers, err := wsheader.Headers(wsheader.Create{
		Token: string(token), Target: target, LaneID: protocol.LaneID{1}, Generation: 1,
		PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{2}, UnixSeconds: 3_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
	request.Header = headers
	response := httptest.NewRecorder()
	instance.WebSocketHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("WebSocket status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want %q", got, "no-store")
	}
	rejection, err := wsheader.ParseRejection(response.Header())
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyServerHello(rejection, token); err != nil {
		t.Fatal(err)
	}
	if rejection.RequestNonce != (protocol.Nonce{2}) || rejection.ServerUnixSeconds != 2_000 ||
		rejection.ErrorCode != protocol.ErrorClockSkew || rejection.ErrorClass != protocol.ErrorRetryable ||
		rejection.ErrorScope != protocol.ErrorScopeLane {
		t.Fatalf("WebSocket rejection = %+v", rejection)
	}
	if err := instance.replay.CheckAndStore(protocol.Nonce{2}, 2_000, 2_061); err != nil {
		t.Fatalf("clock-skew rejection retained its nonce: %v", err)
	}
}

func TestWebSocketRejectionSurvivesReverseProxy(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	instance := newSessionTestServer(t, token, target, time.Second)
	instance.config.WallClock = func() time.Time { return time.Unix(2_000, 0) }
	upstream := httptest.NewServer(instance.WebSocketHandler(context.Background()))
	defer upstream.Close()
	upstreamURL, err := neturl.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(httputil.NewSingleHostReverseProxy(upstreamURL))
	defer proxy.Close()
	headers, err := wsheader.Headers(wsheader.Create{
		Token: string(token), Target: target, LaneID: protocol.LaneID{1}, Generation: 1,
		PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{2}, UnixSeconds: 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, proxy.URL+"/_wirehop", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header = headers
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("proxy response status = %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}
	if got := response.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("proxy Cache-Control = %q, want %q", got, "no-store")
	}
	rejection, err := wsheader.ParseRejection(response.Header)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyServerHello(rejection, token); err != nil {
		t.Fatal(err)
	}
	if rejection.ErrorCode != protocol.ErrorClockSkew {
		t.Fatalf("proxy rejection code = %d, want %d", rejection.ErrorCode, protocol.ErrorClockSkew)
	}
}

func TestWebSocketLaneLimitIsTerminal(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	instance.config.MaxLanesPerSession = 1
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	if err := session.reserveLane(protocol.LaneID{1}, 1, protocol.PathGroupID{1}); err != nil {
		t.Fatal(err)
	}
	join := wsheader.Join{
		Method: "GET", Path: "/_wirehop", SessionID: session.id, LaneID: protocol.LaneID{2}, Generation: 1,
		PathGroupID: protocol.PathGroupID{2}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
	}
	if err := wsheader.SignJoin(&join, session.secret); err != nil {
		t.Fatal(err)
	}
	headers, err := wsheader.JoinHeaders(join)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
	request.Header = headers
	response := httptest.NewRecorder()
	instance.WebSocketHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("WebSocket status = %d, want %d", response.Code, http.StatusConflict)
	}
	rejection, err := wsheader.ParseRejection(response.Header())
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyServerHello(rejection, session.secret[:]); err != nil {
		t.Fatal(err)
	}
	if rejection.ErrorCode != protocol.ErrorLaneLimit ||
		rejection.ErrorClass != protocol.ErrorLaneRejected ||
		rejection.ErrorScope != protocol.ErrorScopeLane {
		t.Fatalf("WebSocket rejection = %+v", rejection)
	}
}

func TestWebSocketRejectionStatus(t *testing.T) {
	for _, tt := range []struct {
		code protocol.ErrorCode
		want int
	}{
		{code: protocol.ErrorMalformed, want: http.StatusBadRequest},
		{code: protocol.ErrorUnsupportedVersion, want: http.StatusUpgradeRequired},
		{code: protocol.ErrorAuthentication, want: http.StatusUnauthorized},
		{code: protocol.ErrorReplay, want: http.StatusConflict},
		{code: protocol.ErrorTargetDenied, want: http.StatusForbidden},
		{code: protocol.ErrorSessionNotFound, want: http.StatusGone},
		{code: protocol.ErrorStaleGeneration, want: http.StatusConflict},
		{code: protocol.ErrorLaneLimit, want: http.StatusConflict},
		{code: protocol.ErrorSessionLimit, want: http.StatusServiceUnavailable},
		{code: protocol.ErrorProtocolViolation, want: http.StatusBadRequest},
		{code: protocol.ErrorUnavailable, want: http.StatusServiceUnavailable},
		{code: protocol.ErrorRateLimited, want: http.StatusTooManyRequests},
		{code: protocol.ErrorInternal, want: http.StatusInternalServerError},
		{code: protocol.ErrorClockSkew, want: http.StatusUnauthorized},
	} {
		if got := webSocketRejectionStatus(tt.code); got != tt.want {
			t.Fatalf("webSocketRejectionStatus(%d) = %d, want %d", tt.code, got, tt.want)
		}
	}
	if got := webSocketRejectionStatus(0); got != http.StatusInternalServerError {
		t.Fatalf("webSocketRejectionStatus(0) = %d, want %d", got, http.StatusInternalServerError)
	}
}

func TestAcceptWebSocketRejectsMissingSubprotocolPromptly(t *testing.T) {
	result := make(chan error, 1)
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := acceptWebSocket(writer, request)
		if connection != nil {
			connection.Close()
		}
		result <- err
	}))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(target.URL, "http"),
		&websocket.DialOptions{Subprotocols: []string{"other.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("WebSocket response = %v", response)
	}
	select {
	case err := <-result:
		if !errors.Is(err, wsheader.ErrInvalid) {
			t.Fatalf("acceptWebSocket() error = %v, want %v", err, wsheader.ErrInvalid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing-subprotocol rejection did not return promptly")
	}
}

func TestWebSocketListenerBoundsPreHeaderConnections(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	listener := instance.WebSocketListener(base)
	firstClient, err := net.Dial("tcp", base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer firstClient.Close()
	firstServer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer firstServer.Close()
	if instance.Snapshot().PendingAdmissions != 1 {
		t.Fatalf("pending admissions = %d, want 1", instance.Snapshot().PendingAdmissions)
	}
	result := make(chan net.Conn, 1)
	errorsChannel := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			errorsChannel <- err
			return
		}
		result <- connection
	}()
	secondClient, err := net.Dial("tcp", base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := secondClient.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := secondClient.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess pre-header connection remained open")
	} else {
		if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
			t.Fatal("excess pre-header connection was not rejected before the deadline")
		}
	}
	secondClient.Close()
	if err := firstServer.Close(); err != nil {
		t.Fatal(err)
	}
	thirdClient, err := net.Dial("tcp", base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer thirdClient.Close()
	select {
	case thirdServer := <-result:
		thirdServer.Close()
	case err := <-errorsChannel:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("released admission capacity was not reusable")
	}
}

func TestWebSocketListenerReleasesAdmissionAfterTLSFailure(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	temporary := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := temporary.TLS.Certificates[0]
	temporary.Close()
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := tls.NewListener(instance.WebSocketListener(base), &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
	})
	httpServer := &http.Server{
		Handler: http.NotFoundHandler(), ConnContext: instance.WebSocketConnContext,
		ErrorLog: log.New(io.Discard, "", 0),
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- httpServer.Serve(listener) }()
	defer func() {
		httpServer.Close()
		if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve() error = %v", err)
		}
	}()
	connection, err := net.Dial("tcp", base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	waitSessionCondition(t, func() bool { return instance.Snapshot().PendingAdmissions == 1 })
	if _, err := connection.Write([]byte("not TLS\n")); err != nil {
		t.Fatal(err)
	}
	waitSessionCondition(t, func() bool { return instance.Snapshot().PendingAdmissions == 0 })
}

func TestWebSocketListenerPreservesTLSStateAndAdmission(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	temporary := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := temporary.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	temporary.Close()
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := tls.NewListener(instance.WebSocketListener(base), &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
	})
	type observation struct {
		tls       bool
		admitted  bool
		remaining int
	}
	observed := make(chan observation, 1)
	httpServer := &http.Server{
		ReadHeaderTimeout: time.Second,
		ConnContext:       instance.WebSocketConnContext,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			release, admitted := instance.webSocketAdmission(request)
			if admitted {
				release()
			}
			observed <- observation{
				tls: request.TLS != nil, admitted: admitted,
				remaining: instance.Snapshot().PendingAdmissions,
			}
			writer.WriteHeader(http.StatusNoContent)
		}),
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- httpServer.Serve(listener) }()
	defer func() {
		httpServer.Close()
		if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve() error = %v", err)
		}
	}()
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Get("https://" + base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("HTTP status = %d, want %d", response.StatusCode, http.StatusNoContent)
	}
	got := <-observed
	if !got.tls || !got.admitted || got.remaining != 0 {
		t.Fatalf("TLS admission observation = %+v", got)
	}
}

func TestWebSocketUpgradeClearsHTTPDeadline(t *testing.T) {
	result := make(chan error, 1)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := acceptWebSocket(writer, request)
		if err != nil {
			result <- err
			return
		}
		defer connection.Close()
		time.Sleep(50 * time.Millisecond)
		frame, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 1, SendMicros: 1})
		if err == nil {
			err = connection.WriteFrames(request.Context(), []protocol.Frame{frame})
		}
		result <- err
	})
	httpServer := httptest.NewUnstartedServer(handler)
	httpServer.Config.WriteTimeout = 10 * time.Millisecond
	httpServer.Start()
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"),
		&websocket.DialOptions{Subprotocols: []string{wsheader.Subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	messageType, message, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.MessageBinary {
		t.Fatalf("message type = %d, want binary", messageType)
	}
	frames, err := protocol.ParseFrames(message)
	if err != nil || len(frames) != 1 || frames[0].Type != protocol.FramePing {
		t.Fatalf("frames = %+v, error = %v", frames, err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestLanePathGroupIsStable(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	session, err := instance.createSession(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	laneID := protocol.LaneID{1}
	if err := session.reserveLane(laneID, 1, protocol.PathGroupID{1}); err != nil {
		t.Fatal(err)
	}
	if err := session.reserveLane(laneID, 2, protocol.PathGroupID{2}); err != ErrPathGroupMismatch {
		t.Fatalf("reserveLane() error = %v, want %v", err, ErrPathGroupMismatch)
	}
}

func TestRawJoinRejectionUsesLaneScope(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()
	result := make(chan error, 1)
	go func() {
		defer serverConnection.Close()
		result <- instance.rejectWithKey(serverConnection, 1, protocol.Nonce{1}, protocol.ErrorStaleGeneration,
			protocol.ErrorLaneRejected, protocol.ErrorScopeLane, "lane generation rejected", []byte("join-secret"))
	}()
	response, err := protocol.ReadServerHello(clientConnection)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if response.ErrorClass != protocol.ErrorLaneRejected || response.ErrorScope != protocol.ErrorScopeLane {
		t.Fatalf("raw rejection class and scope = %d, %d", response.ErrorClass, response.ErrorScope)
	}
	if err := protocol.VerifyServerHello(response, []byte("join-secret")); err != nil {
		t.Fatal(err)
	}
}

func TestRawClockSkewIsRetryable(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	instance := newSessionTestServer(t, token, target, time.Second)
	instance.config.WallClock = func() time.Time { return time.Unix(2_000, 0) }
	hello := protocol.ClientHello{
		Mode: protocol.HelloCreate, UnixSeconds: 3_000, MonotonicMicros: 1,
		Nonce: protocol.Nonce{2}, LaneID: protocol.LaneID{1}, Generation: 1,
		PathGroupID: protocol.PathGroupID{1}, Target: target,
	}
	if err := protocol.SignClientHello(&hello, token); err != nil {
		t.Fatal(err)
	}
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()
	result := make(chan error, 1)
	go func() {
		result <- instance.serveConnection(context.Background(), serverConnection, func() {})
	}()
	if err := protocol.WriteClientHello(clientConnection, hello); err != nil {
		t.Fatal(err)
	}
	rejection, err := protocol.ReadServerHello(clientConnection)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyServerHello(rejection, token); err != nil {
		t.Fatal(err)
	}
	if rejection.RequestNonce != hello.Nonce || rejection.ServerUnixSeconds != 2_000 ||
		rejection.ErrorCode != protocol.ErrorClockSkew || rejection.ErrorClass != protocol.ErrorRetryable ||
		rejection.ErrorScope != protocol.ErrorScopeLane {
		t.Fatalf("raw rejection = %+v", rejection)
	}
	if err := <-result; !errors.Is(err, auth.ErrTimestampOutsideWindow) {
		t.Fatalf("serveConnection() error = %v, want %v", err, auth.ErrTimestampOutsideWindow)
	}
	if err := instance.replay.CheckAndStore(hello.Nonce, 2_000, 2_061); err != nil {
		t.Fatalf("clock-skew rejection retained its nonce: %v", err)
	}
}

func TestReservationRejection(t *testing.T) {
	for _, test := range []struct {
		err   error
		code  protocol.ErrorCode
		class protocol.ErrorClass
		scope protocol.ErrorScope
	}{
		{err: relay.ErrStaleLane, code: protocol.ErrorStaleGeneration,
			class: protocol.ErrorLaneRejected, scope: protocol.ErrorScopeLane},
		{err: ErrLaneLimit, code: protocol.ErrorLaneLimit,
			class: protocol.ErrorLaneRejected, scope: protocol.ErrorScopeLane},
		{err: ErrPathGroupMismatch, code: protocol.ErrorProtocolViolation,
			class: protocol.ErrorLaneRejected, scope: protocol.ErrorScopeLane},
		{err: ErrSessionClosed, code: protocol.ErrorSessionNotFound,
			class: protocol.ErrorSessionGone, scope: protocol.ErrorScopeSession},
	} {
		code, class, scope := reservationRejection(test.err)
		if code != test.code || class != test.class || scope != test.scope {
			t.Fatalf("reservationRejection(%v) = %d, %d, %d, want %d, %d, %d",
				test.err, code, class, scope, test.code, test.class, test.scope)
		}
	}
}

func TestRawUnsupportedVersionIsRejected(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	instance := newSessionTestServer(t, token, target, time.Second)
	hello := protocol.ClientHello{
		Mode: protocol.HelloCreate, UnixSeconds: time.Now().Unix(), Nonce: protocol.Nonce{1},
		LaneID: protocol.LaneID{1}, Generation: 1, PathGroupID: protocol.PathGroupID{1}, Target: target,
	}
	if err := protocol.SignClientHello(&hello, token); err != nil {
		t.Fatal(err)
	}
	encoded, err := protocol.MarshalClientHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	encoded[4], encoded[5] = 0, 2
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()
	result := make(chan error, 1)
	go func() {
		result <- instance.serveConnection(context.Background(), serverConnection, func() {})
	}()
	prefixSize := len(encoded) - len(hello.Target.String()) - len(hello.AuthTag)
	if _, err := clientConnection.Write(encoded[:prefixSize]); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.ReadServerHello(clientConnection)
	if err != nil {
		t.Fatal(err)
	}
	if response.ErrorCode != protocol.ErrorUnsupportedVersion ||
		response.ErrorClass != protocol.ErrorSessionRejected || response.ErrorScope != protocol.ErrorScopeSession {
		t.Fatalf("version rejection = %+v", response)
	}
	if err := protocol.VerifyServerHello(response, token); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("serveConnection() error = %v, want %v", err, protocol.ErrUnsupportedVersion)
	}
}

func TestShouldReportLaneError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "Nil"},
		{name: "Cancellation", err: context.Canceled},
		{name: "NetworkClose", err: net.ErrClosed},
		{name: "PeerClose", err: relay.ErrRemoteClosed},
		{name: "PingTimeout", err: relay.ErrPingTimeout},
		{name: "Authentication", err: protocol.ErrAuthenticationFailed},
		{name: "PreAdmissionProtocolViolation", err: protocol.ErrInvalidFrameType},
		{name: "ActiveProtocolViolation", err: &activeLaneError{cause: protocol.ErrInvalidFrameType}, want: true},
		{name: "ActiveEndpointFailure", err: &activeLaneError{cause: relay.ErrEndpointFailure}, want: true},
		{
			name: "ActiveRemoteProtocolViolation",
			err: &activeLaneError{cause: &relay.RemoteError{Value: protocol.ErrorFrame{
				Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected,
				Scope: protocol.ErrorScopeLane, LaneID: protocol.LaneID{1}, Generation: 1,
				Diagnostic: "private diagnostic",
			}}},
			want: true,
		},
		{name: "LocalFailure", err: reportLaneError(errors.New("local failure")), want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldReportLaneError(test.err); got != test.want {
				t.Fatalf("shouldReportLaneError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestServerLogLaneErrorRedactsRemoteDiagnostic(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	var output bytes.Buffer
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	instance.config.Logger = slog.New(slog.NewTextHandler(&output, nil))
	instance.logLaneError("stream lane ended", "192.0.2.1:1234", &activeLaneError{
		cause: &relay.RemoteError{Value: protocol.ErrorFrame{
			Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected,
			Scope: protocol.ErrorScopeLane, LaneID: protocol.LaneID{1}, Generation: 1,
			Diagnostic: "private diagnostic",
		}},
	})
	logged := output.String()
	for _, value := range []string{
		`level=WARN msg="stream lane ended"`,
		`remote=192.0.2.1:1234`,
		`error="peer reported protocol violation"`,
		`error_code=10`,
		`error_class=2`,
		`error_scope=1`,
	} {
		if !strings.Contains(logged, value) {
			t.Fatalf("log output %q does not contain %q", logged, value)
		}
	}
	if strings.Contains(logged, "private diagnostic") {
		t.Fatalf("log output exposed remote diagnostic: %q", logged)
	}
}

func TestServerLogLaneErrorRedactsTargetEndpoint(t *testing.T) {
	var output bytes.Buffer
	instance := &Server{config: Config{Logger: slog.New(slog.NewTextHandler(&output, nil))}}
	target := "203.0.113.10:51820"
	endpointError := &net.OpError{
		Op: "write", Net: "udp", Addr: &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 51820},
		Err: errors.New("test failure"),
	}
	instance.logLaneError("stream lane ended", "192.0.2.1:1234", &activeLaneError{
		cause: fmt.Errorf("%w: %w", relay.ErrEndpointFailure, endpointError),
	})
	logged := output.String()
	if !strings.Contains(logged, `error="relay UDP endpoint failure"`) {
		t.Fatalf("log output %q does not contain the endpoint error class", logged)
	}
	if strings.Contains(logged, target) || strings.Contains(logged, "test failure") {
		t.Fatalf("log output exposed target endpoint details: %q", logged)
	}
}

func TestWebSocketAdmissionFailureIsSilent(t *testing.T) {
	target, stopTarget := startSessionEchoTarget(t)
	defer stopTarget()
	var output bytes.Buffer
	instance := newSessionTestServer(t, []byte("test-token"), target, time.Second)
	instance.config.Logger = slog.New(slog.NewTextHandler(&output, nil))
	request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
	response := httptest.NewRecorder()
	instance.WebSocketHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || output.Len() != 0 {
		t.Fatalf("response status = %d, log output = %q", response.Code, output.String())
	}
}

func TestNewRejectsZeroMaxPendingAdmissions(t *testing.T) {
	config := newSessionTestConfig(t, []byte("test-token"), targetpkg.MustParse("192.0.2.1:51820"), time.Second)
	config.MaxPendingAdmissions = 0
	instance, err := New(config)
	if instance != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() = %v, %v, want nil, %v", instance, err, ErrInvalidConfig)
	}
}

func newSessionTestConfig(t *testing.T, token []byte, target targetpkg.Endpoint, grace time.Duration) Config {
	t.Helper()
	targets, err := policy.NewTargetSet([]targetpkg.Endpoint{target})
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		Token: token, Targets: targets, AuthenticationSkew: time.Minute, HandshakeTimeout: time.Second,
		ReplayEntries: 1024, JoinNonceEntries: 1024, MaxSessions: 16, MaxLanesPerSession: 8, MaxPendingAdmissions: 1,
		ReconnectGrace: grace, IngressLimits: packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		LaneLimits:      packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		RetentionLimits: retention.Limits{Packets: 1024, Bytes: 4 * 1024 * 1024},
		Deadlines:       relay.DeadlinePolicy{Control: time.Second, Transport: time.Second}, DeduplicationWindow: 1024,
	}
}

func newSessionTestServer(t *testing.T, token []byte, target targetpkg.Endpoint, grace time.Duration) *Server {
	t.Helper()
	instance, err := New(newSessionTestConfig(t, token, target, grace))
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func firstSessionLane(t *testing.T, instance *Server) (*serverSession, protocol.LaneID, uint64) {
	t.Helper()
	instance.mu.Lock()
	defer instance.mu.Unlock()
	for _, session := range instance.sessions {
		session.mu.Lock()
		for laneID, lane := range session.lanes {
			generation := lane.generation
			session.mu.Unlock()
			return session, laneID, generation
		}
		session.mu.Unlock()
	}
	t.Fatal("no active session lane")
	return nil, protocol.LaneID{}, 0
}

func waitSessionCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for session condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func startSessionEchoTarget(t *testing.T) (targetpkg.Endpoint, func()) {
	t.Helper()
	connection, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, protocol.MaxPacketSize)
		for {
			length, peer, err := connection.ReadFromUDPAddrPort(buffer)
			if err != nil {
				return
			}
			if _, err := connection.WriteToUDPAddrPort(buffer[:length], peer); err != nil {
				return
			}
		}
	}()
	target, err := targetpkg.FromAddrPort(connection.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	return target, func() {
		connection.Close()
		<-done
	}
}

type serverTestResolver struct {
	addresses []netip.Addr
	host      string
}

func (r *serverTestResolver) LookupNetIP(_ context.Context, _ string, host string) ([]netip.Addr, error) {
	r.host = host
	return append([]netip.Addr(nil), r.addresses...), nil
}
