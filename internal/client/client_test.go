package client_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/server"
	targetpkg "github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestRawTCPRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	serverAddress, stopServer := startRawServer(t, token, []netip.AddrPort{target}, nil)
	defer stopServer()
	testRelay(t, "tcp://"+serverAddress, target, token, nil)
}

func TestNonzeroReservedRawTCPRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	serverAddress, stopServer := startRawServer(t, token, []netip.AddrPort{target}, nil)
	defer stopServer()
	payload := transportPacket()
	copy(payload[1:4], []byte{1, 2, 3})
	testRelayConfigPayload(t, clientConfig(t, "tcp://"+serverAddress, target, token, nil), payload)
}

func TestReservedTranslationRawTCPRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	serverAddress, stopServer := startRawServer(t, token, []netip.AddrPort{target}, nil)
	defer stopServer()
	config := clientConfig(t, "tcp://"+serverAddress, target, token, nil)
	config.Reserved = wgpacket.Reserved{1, 2, 3}
	testRelayConfig(t, config)
}

func TestTLSRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	serverTLS, clientTLS := testTLSConfigs(t)
	serverAddress, stopServer := startRawServer(t, token, []netip.AddrPort{target}, serverTLS)
	defer stopServer()
	testRelay(t, "tls://"+serverAddress, target, token, clientTLS)
}

func TestWebSocketRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	laneURL, _, stopServer := startWebSocketServer(t, token, []netip.AddrPort{target}, false)
	defer stopServer()
	testRelay(t, laneURL, target, token, nil)
}

func TestEscapedWebSocketPathRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	instance := newServer(t, token, []netip.AddrPort{target})
	laneURL, _, stopServer := startWebSocketServerInstance(t, instance, false)
	defer stopServer()
	laneURL = strings.TrimSuffix(laneURL, "/_wirehop") + "/%2fwirehop"
	testMultipathRelay(t, instance, []string{laneURL, laneURL}, target, token)
}

func TestSecureWebSocketRelay(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	laneURL, clientTLS, stopServer := startWebSocketServer(t, token, []netip.AddrPort{target}, true)
	defer stopServer()
	testRelay(t, laneURL, target, token, clientTLS)
}

func TestAuthenticationClockSkewRecovery(t *testing.T) {
	for _, offset := range []time.Duration{-8 * time.Hour, 8 * time.Hour} {
		offsetName := "Behind"
		if offset > 0 {
			offsetName = "Ahead"
		}
		t.Run(offsetName, func(t *testing.T) {
			for _, carrierName := range []string{"TCP", "TLS", "WebSocket", "SecureWebSocket"} {
				t.Run(carrierName, func(t *testing.T) {
					target, stopTarget := startEchoTarget(t)
					defer stopTarget()
					token := []byte("a-sufficiently-long-test-authentication-token")
					var laneURL string
					var clientTLS *tls.Config
					var stopServer func()
					switch carrierName {
					case "TCP":
						address, stop := startRawServer(t, token, []netip.AddrPort{target}, nil)
						laneURL, stopServer = "tcp://"+address, stop
					case "TLS":
						serverTLS, secureClient := testTLSConfigs(t)
						address, stop := startRawServer(t, token, []netip.AddrPort{target}, serverTLS)
						laneURL, clientTLS, stopServer = "tls://"+address, secureClient, stop
					case "WebSocket":
						laneURL, clientTLS, stopServer = startWebSocketServer(
							t, token, []netip.AddrPort{target}, false,
						)
					case "SecureWebSocket":
						laneURL, clientTLS, stopServer = startWebSocketServer(
							t, token, []netip.AddrPort{target}, true,
						)
					}
					defer stopServer()
					config := clientConfig(t, laneURL, target, token, clientTLS)
					config.WallClock = func() time.Time { return time.Now().Add(offset) }
					testRelayConfig(t, config)
				})
			}
		})
	}
}

func TestAuthenticationFailureRemainsTerminal(t *testing.T) {
	for _, carrierName := range []string{"TCP", "WebSocket"} {
		t.Run(carrierName, func(t *testing.T) {
			target, stopTarget := startEchoTarget(t)
			defer stopTarget()
			serverToken := []byte("a-sufficiently-long-server-authentication-token")
			var laneURL string
			var stopServer func()
			if carrierName == "TCP" {
				address, stop := startRawServer(t, serverToken, []netip.AddrPort{target}, nil)
				laneURL, stopServer = "tcp://"+address, stop
			} else {
				laneURL, _, stopServer = startWebSocketServer(t, serverToken, []netip.AddrPort{target}, false)
			}
			defer stopServer()
			config := clientConfig(
				t, laneURL, target, []byte("a-sufficiently-long-client-authentication-token"), nil,
			)
			config.StartupTimeout = 2 * time.Second
			started := time.Now()
			instance, err := client.Start(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.Close()
			err = instance.Wait()
			if err == nil || errors.Is(err, client.ErrStartupTimeout) {
				t.Fatalf("Wait() error = %v, want terminal authentication rejection", err)
			}
			if elapsed := time.Since(started); elapsed >= config.StartupTimeout {
				t.Fatalf("authentication rejection took %v, startup timeout %v", elapsed, config.StartupTimeout)
			}
		})
	}
}

func TestWebSocketRuntimeClockSkewRecovery(t *testing.T) {
	for _, tt := range []struct {
		name   string
		secure bool
	}{
		{name: "WebSocket"},
		{name: "SecureWebSocket", secure: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			target, stopTarget := startEchoTarget(t)
			defer stopTarget()
			token := []byte("a-sufficiently-long-test-authentication-token")
			serverInstance := newServer(t, token, []netip.AddrPort{target})
			serverContext, stopServer := context.WithCancel(context.Background())
			httpServer := httptest.NewUnstartedServer(serverInstance.WebSocketHandler(serverContext))
			listener := &recordingListener{
				Listener: httpServer.Listener,
				accepted: make(chan net.Conn, 16),
			}
			httpServer.Listener = listener
			var clientTLS *tls.Config
			if tt.secure {
				httpServer.StartTLS()
				roots := x509.NewCertPool()
				roots.AddCert(httpServer.Certificate())
				clientTLS = &tls.Config{RootCAs: roots}
			} else {
				httpServer.Start()
			}
			defer func() {
				stopServer()
				httpServer.Close()
			}()

			var wallClockOffset atomic.Int64
			config := clientConfig(
				t, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/_wirehop", target, token, clientTLS,
			)
			config.WallClock = func() time.Time {
				return time.Now().Add(time.Duration(wallClockOffset.Load()))
			}
			instance, err := client.Start(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.Close()
			waitForServer(t, func(snapshot server.Snapshot) bool {
				return snapshot.Sessions == 1 && snapshot.AttachedLanes == 1
			}, serverInstance)
			sessionID := waitForClientSession(t, instance)
			initialConnection := receiveAcceptedConnection(t, listener.accepted)

			wallClockOffset.Store(int64(-8 * time.Hour))
			if err := initialConnection.Close(); err != nil {
				t.Fatal(err)
			}
			receiveAcceptedConnection(t, listener.accepted)
			receiveAcceptedConnection(t, listener.accepted)
			waitForServer(t, func(snapshot server.Snapshot) bool {
				return snapshot.Sessions == 1 && snapshot.AttachedLanes == 1
			}, serverInstance)
			if got := instance.SessionID(); got != sessionID {
				t.Fatalf("SessionID() = %v after clock-skew recovery, want retained %v", got, sessionID)
			}

			peer, err := net.ListenUDP(
				"udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			assertRelayExchange(t, peer, instance.LocalAddr())
		})
	}
}

func TestDuplicateRawTCPLanes(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	instance := newServer(t, token, []netip.AddrPort{target})
	serverAddress, stopServer := startRawServerInstance(t, instance, nil)
	defer stopServer()
	laneURL := "tcp://" + serverAddress
	testMultipathRelay(t, instance, []string{laneURL, laneURL}, target, token)
}

func TestMixedCarrierLanes(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	instance := newServer(t, token, []netip.AddrPort{target})
	serverAddress, stopRaw := startRawServerInstance(t, instance, nil)
	defer stopRaw()
	webSocketURL, _, stopWebSocket := startWebSocketServerInstance(t, instance, false)
	defer stopWebSocket()
	testMultipathRelay(t, instance, []string{"tcp://" + serverAddress, webSocketURL}, target, token)
}

func TestPreparedLanesJoinConcurrently(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	token := []byte("a-sufficiently-long-test-authentication-token")
	result := make(chan error, 1)
	go func() {
		type helloResult struct {
			connection net.Conn
			hello      protocol.ClientHello
			err        error
		}
		connections := make([]net.Conn, 0, 3)
		defer func() {
			for _, connection := range connections {
				connection.Close()
			}
		}()
		hellos := make(chan helloResult, 3)
		for range 3 {
			connection, err := listener.Accept()
			if err != nil {
				result <- err
				return
			}
			connections = append(connections, connection)
			go func() {
				connection.SetReadDeadline(time.Now().Add(2 * time.Second))
				hello, err := protocol.ReadClientHello(connection)
				hellos <- helloResult{connection: connection, hello: hello, err: err}
			}()
		}
		creator := <-hellos
		if creator.err != nil {
			result <- creator.err
			return
		}
		if creator.hello.Mode != protocol.HelloCreate {
			result <- fmt.Errorf("first hello mode = %d, want create", creator.hello.Mode)
			return
		}
		if err := protocol.VerifyClientHello(creator.hello, token); err != nil {
			result <- err
			return
		}
		sessionID := protocol.SessionID{1}
		sessionSecret := protocol.SessionSecret{1}
		response := protocol.ServerHello{
			Result: protocol.ServerSessionCreated, RequestNonce: creator.hello.Nonce,
			ServerUnixSeconds: time.Now().Unix(), SessionID: sessionID, SessionSecret: sessionSecret,
			PathGroupID: creator.hello.PathGroupID, ReceiveMicros: 1, SendMicros: 1,
		}
		if err := protocol.SignServerHello(&response, token); err != nil {
			result <- err
			return
		}
		if err := protocol.WriteServerHello(creator.connection, response); err != nil {
			result <- err
			return
		}
		seen := map[protocol.LaneID]struct{}{creator.hello.LaneID: {}}
		for range 2 {
			joined := <-hellos
			if joined.err != nil {
				result <- joined.err
				return
			}
			if joined.hello.Mode != protocol.HelloJoin || joined.hello.SessionID != sessionID ||
				joined.hello.PathGroupID != creator.hello.PathGroupID {
				result <- fmt.Errorf("invalid concurrent join: %+v", joined.hello)
				return
			}
			if err := protocol.VerifyClientHello(joined.hello, sessionSecret[:]); err != nil {
				result <- err
				return
			}
			if _, duplicate := seen[joined.hello.LaneID]; duplicate {
				result <- errors.New("concurrent join reused a lane ID")
				return
			}
			seen[joined.hello.LaneID] = struct{}{}
		}
		result <- nil
	}()

	laneURL := "tcp://" + listener.Addr().String()
	config := clientConfig(t, laneURL, netip.MustParseAddrPort("127.0.0.1:51820"), token, nil)
	config.Lanes = parseLaneSpecs(t, laneURL, laneURL, laneURL)
	ctx, cancel := context.WithCancel(context.Background())
	instance, err := client.Start(ctx, config)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("prepared lane joins did not start concurrently")
	}
	cancel()
	if err := instance.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
	}
}

func TestRejectedLaneDoesNotEndHealthySession(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	instance := newServer(t, token, []netip.AddrPort{target})
	rawAddress, stopRaw := startRawServerInstance(t, instance, nil)
	defer stopRaw()
	serverTLS, _ := testTLSConfigs(t)
	tlsAddress, stopTLS := startRawServerInstance(t, instance, serverTLS)
	defer stopTLS()
	config := clientConfig(t, "tcp://"+rawAddress, target, token, nil)
	config.Lanes = parseLaneSpecs(t, "tcp://"+rawAddress, "tls://"+tlsAddress)
	warnings := make(chan string, 1)
	config.Logger = slog.New(slog.NewTextHandler(clientChannelWriter{values: warnings}, nil))
	clientInstance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer clientInstance.Close()
	waitForServer(t, func(snapshot server.Snapshot) bool {
		return snapshot.Sessions == 1 && snapshot.AttachedLanes == 1
	}, instance)
	select {
	case warning := <-warnings:
		for _, value := range []string{`level=WARN msg="relay lane disabled"`, "lane=2", "url=tls://" + tlsAddress} {
			if !strings.Contains(warning, value) {
				t.Fatalf("lane warning %q does not contain %q", warning, value)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rejected lane did not produce a degradation warning")
	}
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	payload := transportPacket()
	if _, err := peer.WriteToUDPAddrPort(payload, clientInstance.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(payload))
	length, _, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil || string(buffer[:length]) != string(payload) {
		t.Fatalf("healthy-lane response length = %d, error = %v", length, err)
	}
}

func TestRawTCPRelayTargetDenied(t *testing.T) {
	allowed, stopAllowed := startEchoTarget(t)
	defer stopAllowed()
	denied, stopDenied := startEchoTarget(t)
	defer stopDenied()
	token := []byte("a-sufficiently-long-test-authentication-token")
	serverAddress, stopServer := startRawServer(t, token, []netip.AddrPort{allowed}, nil)
	defer stopServer()
	instance, err := client.Start(context.Background(), clientConfig(t, "tcp://"+serverAddress, denied, token, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	err = instance.Wait()
	rejection, ok := errors.AsType[*client.RejectionError](err)
	if !ok || rejection.Code != protocol.ErrorTargetDenied ||
		rejection.Class != protocol.ErrorSessionRejected {
		t.Fatalf("Wait() error = %v, want target-denied rejection", err)
	}
}

func TestRejectedCreatorCandidateFallsThrough(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	otherTarget, stopOtherTarget := startEchoTarget(t)
	defer stopOtherTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	rejectedAddress, stopRejected := startRawServer(t, token, []netip.AddrPort{otherTarget}, nil)
	defer stopRejected()
	serverTLS, clientTLS := testTLSConfigs(t)
	acceptedAddress, stopAccepted := startRawServer(t, token, []netip.AddrPort{target}, serverTLS)
	defer stopAccepted()
	config := clientConfig(t, "tcp://"+rejectedAddress, target, token, clientTLS)
	config.Lanes = parseLaneSpecs(t, "tcp://"+rejectedAddress, "tls://"+acceptedAddress)
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	payload := transportPacket()
	if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(payload))
	length, _, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil || string(buffer[:length]) != string(payload) {
		t.Fatalf("fallback creator response length = %d, error = %v", length, err)
	}
}

func TestUnreachableFirstLaneFallsThrough(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	serverAddress, stopServer := startRawServer(t, token, []netip.AddrPort{target}, nil)
	defer stopServer()
	unreachable, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachableAddress := unreachable.Addr().String()
	if err := unreachable.Close(); err != nil {
		t.Fatal(err)
	}
	config := clientConfig(t, "tcp://"+unreachableAddress, target, token, nil)
	config.Lanes = parseLaneSpecs(t, "tcp://"+unreachableAddress, "tcp://"+serverAddress)
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	payload := transportPacket()
	if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(payload))
	length, _, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil || string(buffer[:length]) != string(payload) {
		t.Fatalf("fallback response length = %d, error = %v", length, err)
	}
}

func TestRawTCPRelayBindsBeforeDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	config := clientConfig(
		t, "tcp://"+address, netip.MustParseAddrPort("127.0.0.1:51820"), []byte("test-token"), nil,
	)
	config.StartupTimeout = 100 * time.Millisecond
	started := time.Now()
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if !instance.LocalAddr().IsValid() || instance.LocalAddr().Port() == 0 {
		t.Fatalf("LocalAddr() = %v, want an allocated UDP port", instance.LocalAddr())
	}
	waitErr := instance.Wait()
	if waitErr == nil {
		t.Fatal("Wait() succeeded for an unavailable server")
	}
	if !strings.Contains(waitErr.Error(), "lane 1 (tcp://"+address+")") {
		t.Fatalf("Wait() error = %v, want configured lane context", waitErr)
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("unavailable server ended after %v without using the startup retry budget", elapsed)
	}
}

type clientChannelWriter struct {
	values chan<- string
}

func (w clientChannelWriter) Write(value []byte) (int, error) {
	w.values <- strings.TrimSpace(string(value))
	return len(value), nil
}

func TestSessionStartupTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			accepted <- connection
		}
	}()
	config := clientConfig(
		t, "tcp://"+listener.Addr().String(), netip.MustParseAddrPort("127.0.0.1:51820"),
		[]byte("test-token"), nil,
	)
	config.StartupTimeout = 50 * time.Millisecond
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	select {
	case connection := <-accepted:
		defer connection.Close()
	case <-time.After(time.Second):
		t.Fatal("client did not establish the stalled carrier")
	}
	if err := instance.Wait(); !errors.Is(err, client.ErrStartupTimeout) {
		t.Fatalf("Wait() error = %v, want %v", err, client.ErrStartupTimeout)
	}
}

func TestRetryableCreationRejectionHonorsStartupTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	token := []byte("test-token")
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			hello, readErr := protocol.ReadClientHello(connection)
			if readErr == nil {
				response := protocol.ServerHello{
					Result: protocol.ServerRejected, RequestNonce: hello.Nonce,
					ServerUnixSeconds: time.Now().Unix(), ReceiveMicros: hello.MonotonicMicros,
					SendMicros: hello.MonotonicMicros, ErrorCode: protocol.ErrorUnavailable,
					ErrorClass: protocol.ErrorRetryable, ErrorScope: protocol.ErrorScopeSession,
					Diagnostic: "temporarily unavailable",
				}
				if protocol.SignServerHello(&response, token) == nil {
					protocol.WriteServerHello(connection, response)
				}
			}
			connection.Close()
		}
	}()
	config := clientConfig(
		t, "tcp://"+listener.Addr().String(), netip.MustParseAddrPort("127.0.0.1:51820"), token, nil,
	)
	config.StartupTimeout = 100 * time.Millisecond
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Wait(); !errors.Is(err, client.ErrStartupTimeout) {
		t.Fatalf("Wait() error = %v, want %v", err, client.ErrStartupTimeout)
	}
	instance.Close()
	listener.Close()
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("retry rejection server did not stop")
	}
}

func TestRuntimeSessionReplacementOutlivesStartupTimeout(t *testing.T) {
	target, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("a-sufficiently-long-test-authentication-token")
	firstServer := newServer(t, token, []netip.AddrPort{target})
	firstListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := firstListener.Addr().String()
	firstContext, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstServer.Serve(firstContext, firstListener) }()

	config := clientConfig(t, "tcp://"+address, target, token, nil)
	config.StartupTimeout = 100 * time.Millisecond
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		stopFirst()
		t.Fatal(err)
	}
	defer instance.Close()
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		stopFirst()
		t.Fatal(err)
	}
	defer peer.Close()
	assertRelayExchange(t, peer, instance.LocalAddr())

	waitResult := make(chan error, 1)
	go func() { waitResult <- instance.Wait() }()
	stopFirst()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	goneListener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	goneResult := make(chan error, 1)
	go func() {
		connection, acceptErr := goneListener.Accept()
		if acceptErr != nil {
			goneResult <- acceptErr
			return
		}
		defer connection.Close()
		hello, readErr := protocol.ReadClientHello(connection)
		if readErr != nil {
			goneResult <- readErr
			return
		}
		response := protocol.ServerHello{
			Result: protocol.ServerRejected, RequestNonce: hello.Nonce, ServerUnixSeconds: time.Now().Unix(),
			ReceiveMicros: hello.MonotonicMicros, SendMicros: hello.MonotonicMicros,
			ErrorCode: protocol.ErrorSessionNotFound, ErrorClass: protocol.ErrorSessionGone,
			ErrorScope: protocol.ErrorScopeSession, Diagnostic: "session is not available",
		}
		if signErr := protocol.SignServerHello(&response, token); signErr != nil {
			goneResult <- signErr
			return
		}
		goneResult <- protocol.WriteServerHello(connection, response)
	}()
	select {
	case err := <-goneResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not attempt to join the expired session")
	}
	if err := goneListener.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * config.StartupTimeout)
	select {
	case err := <-waitResult:
		t.Fatalf("client exited during runtime recovery: %v", err)
	default:
	}

	replacementListener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	replacementServer := newServer(t, token, []netip.AddrPort{target})
	replacementContext, stopReplacement := context.WithCancel(context.Background())
	replacementDone := make(chan error, 1)
	go func() { replacementDone <- replacementServer.Serve(replacementContext, replacementListener) }()
	defer func() {
		stopReplacement()
		if err := <-replacementDone; err != nil {
			t.Errorf("replacement Serve() error = %v", err)
		}
	}()
	waitForServer(t, func(snapshot server.Snapshot) bool { return snapshot.Sessions == 1 }, replacementServer)
	assertRelayExchange(t, peer, instance.LocalAddr())
}

func testRelay(t *testing.T, laneURL string, target netip.AddrPort, token []byte, tlsConfig *tls.Config) {
	t.Helper()
	testRelayConfig(t, clientConfig(t, laneURL, target, token, tlsConfig))
}

func testRelayConfig(t *testing.T, config client.Config) {
	t.Helper()
	testRelayConfigPayload(t, config, transportPacket())
}

func testRelayConfigPayload(t *testing.T, config client.Config, payload []byte) {
	t.Helper()
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	assertRelayPayload(t, peer, instance.LocalAddr(), payload)
	if instance.SessionID().IsZero() {
		t.Fatal("session ID was not retained after successful admission")
	}
}

func assertRelayExchange(t *testing.T, peer *net.UDPConn, local netip.AddrPort) {
	t.Helper()
	assertRelayPayload(t, peer, local, transportPacket())
}

func assertRelayPayload(t *testing.T, peer *net.UDPConn, local netip.AddrPort, payload []byte) {
	t.Helper()
	if _, err := peer.WriteToUDPAddrPort(payload, local); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 2048)
	length, source, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if source != local || string(buffer[:length]) != string(payload) {
		t.Fatalf("relay response source = %v, payload length = %d", source, length)
	}
}

func testMultipathRelay(t *testing.T, serverInstance *server.Server, laneURLs []string, target netip.AddrPort,
	token []byte) {
	t.Helper()
	config := clientConfig(t, laneURLs[0], target, token, nil)
	config.Lanes = parseLaneSpecs(t, laneURLs...)
	instance, err := client.Start(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	waitForServer(t, func(snapshot server.Snapshot) bool {
		return snapshot.Sessions == 1 && snapshot.AttachedLanes == len(laneURLs)
	}, serverInstance)
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	payload := transportPacket()
	if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(payload))
	length, _, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil || string(buffer[:length]) != string(payload) {
		t.Fatalf("multipath relay response length = %d, error = %v", length, err)
	}
}

func clientConfig(t *testing.T, laneURL string, target netip.AddrPort, token []byte,
	tlsConfig *tls.Config) client.Config {
	t.Helper()
	endpoint, err := targetpkg.FromAddrPort(target)
	if err != nil {
		t.Fatal(err)
	}
	return client.Config{
		Lanes: parseLaneSpecs(t, laneURL), Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: endpoint, Token: token,
		TLSConfig:        tlsConfig,
		HandshakeTimeout: time.Second, IngressLimits: packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		LaneLimits:          packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1024,
	}
}

func parseLaneSpecs(t *testing.T, values ...string) []lanespec.Spec {
	t.Helper()
	specs := make([]lanespec.Spec, 0, len(values))
	for _, value := range values {
		spec, err := lanespec.Parse(value)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, spec)
	}
	return specs
}

func newServer(t *testing.T, token []byte, targets []netip.AddrPort) *server.Server {
	t.Helper()
	endpoints := make([]targetpkg.Endpoint, len(targets))
	for index, address := range targets {
		var err error
		endpoints[index], err = targetpkg.FromAddrPort(address)
		if err != nil {
			t.Fatal(err)
		}
	}
	allowlist, err := policy.NewTargetSet(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := server.New(server.Config{
		Token: token, Targets: allowlist, AuthenticationSkew: time.Minute, HandshakeTimeout: time.Second,
		ReplayEntries: 1024, JoinNonceEntries: 1024, MaxSessions: 64, MaxLanesPerSession: 8, MaxPendingAdmissions: 1,
		ReconnectGrace: time.Second, IngressLimits: packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		LaneLimits:          packetqueue.Limits{Packets: 64, Bytes: 256 * 1024},
		RetentionLimits:     retention.Limits{Packets: 1024, Bytes: 4 * 1024 * 1024},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func startRawServer(t *testing.T, token []byte, targets []netip.AddrPort,
	tlsConfig *tls.Config) (string, func()) {
	t.Helper()
	instance := newServer(t, token, targets)
	return startRawServerInstance(t, instance, tlsConfig)
}

func startRawServerInstance(t *testing.T, instance *server.Server, tlsConfig *tls.Config) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	configured := net.Listener(carrier.NewTCPOptionsListener(listener))
	if tlsConfig != nil {
		configured = tls.NewListener(configured, tlsConfig)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- instance.Serve(ctx, configured) }()
	stop := func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	}
	return listener.Addr().String(), stop
}

func startWebSocketServer(t *testing.T, token []byte,
	targets []netip.AddrPort, secure bool) (string, *tls.Config, func()) {
	t.Helper()
	instance := newServer(t, token, targets)
	return startWebSocketServerInstance(t, instance, secure)
}

func startWebSocketServerInstance(t *testing.T, instance *server.Server,
	secure bool) (string, *tls.Config, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var httpServer *httptest.Server
	if secure {
		httpServer = httptest.NewTLSServer(instance.WebSocketHandler(ctx))
	} else {
		httpServer = httptest.NewServer(instance.WebSocketHandler(ctx))
	}
	scheme := "ws://"
	var clientTLS *tls.Config
	if secure {
		scheme = "wss://"
		roots := x509.NewCertPool()
		roots.AddCert(httpServer.Certificate())
		clientTLS = &tls.Config{RootCAs: roots}
	}
	laneURL := scheme + strings.TrimPrefix(httpServer.URL, "http://")
	if secure {
		laneURL = scheme + strings.TrimPrefix(httpServer.URL, "https://")
	}
	stop := func() {
		cancel()
		httpServer.Close()
	}
	return laneURL + "/_wirehop", clientTLS, stop
}

// recordingListener reports every accepted connection without changing its behavior.
type recordingListener struct {
	net.Listener
	accepted chan net.Conn
}

// Accept returns and reports the next connection.
func (l *recordingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted <- connection
	return connection, nil
}

// receiveAcceptedConnection waits for one connection reported by a recording listener.
func receiveAcceptedConnection(t *testing.T, accepted <-chan net.Conn) net.Conn {
	t.Helper()
	select {
	case connection := <-accepted:
		return connection
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for an accepted WebSocket connection")
		return nil
	}
}

func waitForServer(t *testing.T, condition func(server.Snapshot) bool, instance *server.Server) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition(instance.Snapshot()) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for server state: %+v", instance.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForClientSession returns the first session identity published by client.
func waitForClientSession(t *testing.T, instance *client.Client) protocol.SessionID {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		sessionID := instance.SessionID()
		if !sessionID.IsZero() {
			return sessionID
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the client session")
		}
		time.Sleep(time.Millisecond)
	}
}

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	temporary := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := temporary.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	temporary.Close()
	return &tls.Config{Certificates: []tls.Certificate{certificate}}, &tls.Config{RootCAs: roots}
}

func startEchoTarget(t *testing.T) (netip.AddrPort, func()) {
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
	stop := func() {
		connection.Close()
		<-done
	}
	return connection.LocalAddr().(*net.UDPAddr).AddrPort(), stop
}

func transportPacket() []byte {
	packet := make([]byte, 1452)
	packet[0] = 4
	for index := 4; index < len(packet); index++ {
		packet[index] = byte(index)
	}
	return packet
}
