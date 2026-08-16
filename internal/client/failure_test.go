package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
)

type notifyingClock struct {
	called chan struct{}
}

func (c *notifyingClock) NowMicros() uint64 {
	c.called <- struct{}{}
	return 1
}

func TestLaneFailureEndsSession(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		remaining int
		want      bool
	}{
		{name: "SessionGoneWithPeer", err: ErrSessionGone, remaining: 1},
		{name: "SessionGoneLastLane", err: ErrSessionGone, want: true},
		{name: "LaneFailureWithPeer", err: ErrUnexpectedServerResponse, remaining: 1},
		{name: "CounterExhausted", err: relay.ErrCounterExhausted, remaining: 1, want: true},
		{name: "LastRejectedLane", err: ErrLaneRejected, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := laneFailureEndsSession(test.err, test.remaining); got != test.want {
				t.Fatalf("laneFailureEndsSession() = %t, want %t", got, test.want)
			}
		})
	}
	wrapped := errors.Join(errors.New("join failed"), ErrSessionGone)
	if laneFailureEndsSession(wrapped, 1) {
		t.Fatal("wrapped session-gone error ended a session with another lane")
	}
	remoteGone := &relay.RemoteError{Value: protocol.ErrorFrame{
		Code: protocol.ErrorSessionNotFound, Class: protocol.ErrorSessionGone, Scope: protocol.ErrorScopeSession,
	}}
	if !sessionGoneFailure(remoteGone) || laneFailureEndsSession(remoteGone, 1) {
		t.Fatal("typed remote session-gone error was not isolated while another lane remained")
	}
	if !sessionReplacementFailure(relay.ErrCounterExhausted) ||
		sessionReplacementFailure(protocol.ErrAuthenticationFailed) {
		t.Fatal("session replacement failures were classified incorrectly")
	}
	remoteLane := &relay.RemoteError{Value: protocol.ErrorFrame{
		Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected, Scope: protocol.ErrorScopeLane,
		LaneID: protocol.LaneID{1}, Generation: 1,
	}}
	if classifyLaneFailure(remoteLane) != failureCloseLane {
		t.Fatal("typed lane-scoped rejection did not close only its lane")
	}
	remoteSessionRetry := &relay.RemoteError{Value: protocol.ErrorFrame{
		Code: protocol.ErrorUnavailable, Class: protocol.ErrorRetryable, Scope: protocol.ErrorScopeSession,
	}}
	if classifyLaneFailure(remoteSessionRetry) != failureCloseSession ||
		!laneFailureEndsSession(remoteSessionRetry, 1) || !sessionReplacementFailure(remoteSessionRetry) {
		t.Fatal("retryable session-scoped error did not replace the complete session")
	}
	remoteLaneRetry := &relay.RemoteError{Value: protocol.ErrorFrame{
		Code: protocol.ErrorUnavailable, Class: protocol.ErrorRetryable, Scope: protocol.ErrorScopeLane,
		LaneID: protocol.LaneID{1}, Generation: 1,
	}}
	if classifyLaneFailure(remoteLaneRetry) != failureRetry || sessionReplacementFailure(remoteLaneRetry) {
		t.Fatal("retryable lane-scoped error escaped its lane")
	}
	rejectedSessionRetry := &RejectionError{
		Code: protocol.ErrorUnavailable, Class: protocol.ErrorRetryable, Scope: protocol.ErrorScopeSession,
	}
	if classifyLaneFailure(rejectedSessionRetry) != failureCloseSession ||
		!sessionReplacementFailure(rejectedSessionRetry) {
		t.Fatal("retryable session-scoped admission error did not request session replacement")
	}
	if classifyLaneFailure(protocol.ErrUnsupportedVersion) != failureCloseLane ||
		classifyLaneFailure(tls.RecordHeaderError{}) != failureCloseLane ||
		classifyLaneFailure(os.ErrPermission) != failureCloseLane ||
		classifyLaneFailure(relay.ErrUnexpectedFrame) != failureCloseLane ||
		classifyLaneFailure(relay.ErrPingTimeout) != failureRetry ||
		classifyLaneFailure(relay.ErrEndpointFailure) != failureCloseSession {
		t.Fatal("protocol and transport failures received incorrect retry dispositions")
	}
}

func TestLogDisabledLaneRedactsRemoteDiagnostic(t *testing.T) {
	spec, err := lanespec.Parse("wss://relay.example/_wirehop")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	instance := &Client{
		config: Config{Logger: slog.New(slog.NewTextHandler(&output, nil))},
		lanes:  []clientLane{{spec: spec}},
	}
	instance.logDisabledLane(0, &relay.RemoteError{Value: protocol.ErrorFrame{
		Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected, Scope: protocol.ErrorScopeLane,
		LaneID: protocol.LaneID{1}, Generation: 1, Diagnostic: "private diagnostic",
	}})
	logged := output.String()
	for _, value := range []string{
		`level=WARN msg="relay lane disabled"`, "lane=1", "url=wss://relay.example:443/_wirehop",
		"code=10", "class=2", "scope=1",
	} {
		if !strings.Contains(logged, value) {
			t.Fatalf("log output %q does not contain %q", logged, value)
		}
	}
	if strings.Contains(logged, "private diagnostic") {
		t.Fatalf("log output exposed remote diagnostic: %q", logged)
	}
}

func TestTLSConfigEnforcesMinimumVersion(t *testing.T) {
	configured := &tls.Config{MinVersion: tls.VersionTLS10, NextProtos: []string{"custom"}}
	instance := &Client{config: Config{TLSConfig: configured}}
	url, err := laneurl.ParseDial("tls://relay.example:443")
	if err != nil {
		t.Fatal(err)
	}
	got := instance.tlsConfig(url, url.Address())
	if got.MinVersion != tls.VersionTLS12 || got.ServerName != "relay.example" ||
		!slices.Equal(got.NextProtos, []string{"custom"}) {
		t.Fatalf("TLS config minimum and server name = %d, %q", got.MinVersion, got.ServerName)
	}
	if configured.MinVersion != tls.VersionTLS10 || configured.ServerName != "" {
		t.Fatal("TLS config input was modified")
	}
	proxy := instance.proxyTLSConfig("proxy.example", "proxy.example:443")
	if proxy.MinVersion != tls.VersionTLS12 || proxy.ServerName != "proxy.example" ||
		!slices.Equal(proxy.NextProtos, []string{"http/1.1"}) {
		t.Fatalf("proxy TLS config minimum and server name = %d, %q", proxy.MinVersion, proxy.ServerName)
	}
}

func TestTLSClientSessionCacheIsolatesEndpoints(t *testing.T) {
	configured := &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(4)}
	instance := &Client{config: Config{TLSConfig: configured}}
	url, err := laneurl.ParseDial("tls://relay.example:443")
	if err != nil {
		t.Fatal(err)
	}

	state := new(tls.ClientSessionState)
	first := instance.tlsConfig(url, "192.0.2.1:443")
	first.ClientSessionCache.Put(first.ServerName, state)

	same := instance.tlsConfig(url, "192.0.2.1:443")
	if got, ok := same.ClientSessionCache.Get(same.ServerName); !ok || got != state {
		t.Fatal("same target endpoint did not share its TLS session cache entry")
	}

	other := instance.tlsConfig(url, "192.0.2.2:443")
	if _, ok := other.ClientSessionCache.Get(other.ServerName); ok {
		t.Fatal("distinct target endpoints shared a TLS session cache entry")
	}

	webSocketURL, err := laneurl.ParseDial("wss://relay.example:443/_wirehop")
	if err != nil {
		t.Fatal(err)
	}
	webSocket := instance.tlsConfig(webSocketURL, "192.0.2.1:443")
	if _, ok := webSocket.ClientSessionCache.Get(webSocket.ServerName); ok {
		t.Fatal("raw TLS and secure WebSocket carriers shared a TLS session cache entry")
	}

	proxy := instance.proxyTLSConfig("relay.example", "192.0.2.1:443")
	if _, ok := proxy.ClientSessionCache.Get(proxy.ServerName); ok {
		t.Fatal("target and proxy roles shared a TLS session cache entry")
	}
}

func TestAdvanceGeneration(t *testing.T) {
	generation := uint64(1)
	if err := advanceGeneration(&generation); err != nil || generation != 2 {
		t.Fatalf("advanceGeneration() = %d, %v, want 2, nil", generation, err)
	}
	generation = math.MaxUint64
	if err := advanceGeneration(&generation); !errors.Is(err, relay.ErrCounterExhausted) ||
		generation != math.MaxUint64 {
		t.Fatalf("exhausted advanceGeneration() = %d, %v", generation, err)
	}
}

func TestWaitReconnectPreservesCancellationCause(t *testing.T) {
	want := errors.New("terminal failure")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(want)
	if err := waitReconnect(ctx, time.Hour); !errors.Is(err, want) {
		t.Fatalf("waitReconnect() error = %v, want %v", err, want)
	}
}

func TestReconnectDelay(t *testing.T) {
	for _, test := range []struct {
		name      string
		delay     time.Duration
		uptime    time.Duration
		wantDelay time.Duration
		wantNext  time.Duration
	}{
		{
			name: "Unstable", delay: 2 * time.Second, uptime: reconnectStabilityInterval - time.Nanosecond,
			wantDelay: 2 * time.Second, wantNext: 4 * time.Second,
		},
		{
			name: "Stable", delay: maximumReconnectDelay, uptime: reconnectStabilityInterval,
			wantDelay: initialReconnectDelay, wantNext: 2 * initialReconnectDelay,
		},
		{
			name: "Capped", delay: maximumReconnectDelay, wantDelay: maximumReconnectDelay,
			wantNext: maximumReconnectDelay,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			delay := reconnectDelayAfterUptime(test.delay, test.uptime)
			if delay != test.wantDelay {
				t.Fatalf("reconnectDelayAfterUptime() = %v, want %v", delay, test.wantDelay)
			}
			if next := nextReconnectDelay(delay); next != test.wantNext {
				t.Fatalf("nextReconnectDelay() = %v, want %v", next, test.wantNext)
			}
		})
	}
}

func TestSuperviseLaneTimestampsAfterPreparation(t *testing.T) {
	clock := &notifyingClock{called: make(chan struct{})}
	instance := &Client{config: Config{Clock: clock}}
	preparation := make(chan preparationResult)
	result := make(chan error, 1)
	go func() {
		result <- instance.superviseLane(context.Background(), clientLane{
			laneID: protocol.LaneID{1}, pathGroupID: protocol.PathGroupID{1},
		}, creationResult{}, nil, preparation, nil, nil)
	}()
	select {
	case <-clock.called:
		preparation <- preparationResult{err: ErrLaneRejected}
		<-result
		t.Fatal("join timestamp was observed before carrier preparation completed")
	case preparation <- preparationResult{err: ErrLaneRejected}:
	case <-time.After(time.Second):
		t.Fatal("superviseLane() did not wait for carrier preparation")
	}
	if err := <-result; !errors.Is(err, ErrLaneRejected) {
		t.Fatalf("superviseLane() error = %v, want %v", err, ErrLaneRejected)
	}
}

func TestSuperviseLanePreparationCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	instance := &Client{}
	result := make(chan error, 1)
	go func() {
		result <- instance.superviseLane(ctx, clientLane{}, creationResult{}, nil,
			make(chan preparationResult), nil, nil)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("superviseLane() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("superviseLane() remained blocked on preparation after cancellation")
	}
}
