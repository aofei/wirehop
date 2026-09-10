package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/server"
)

func TestClientSharedOutageRecovery(t *testing.T) {
	const clients = 16
	const lanes = 4
	for _, test := range []struct {
		name      string
		scheme    string
		secure    bool
		webSocket bool
	}{
		{name: "TCP", scheme: "tcp"},
		{name: "TLS", scheme: "tls", secure: true},
		{name: "WebSocket", scheme: "ws", webSocket: true},
		{name: "SecureWebSocket", scheme: "wss", secure: true, webSocket: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, stopTarget := startEchoTarget(t)
			t.Cleanup(stopTarget)
			token := []byte("a-sufficiently-long-test-authentication-token")
			config := serverConfig(t, token, []netip.AddrPort{target})
			config.MaxSessions = clients
			config.MaxPendingAdmissions = 4
			config.HandshakeTimeout = 5 * time.Second
			var serverTLS, clientTLS *tls.Config
			if test.secure {
				serverTLS, clientTLS = testTLSConfigs(t)
			}
			first, address, stopFirst := startSharedOutageServer(t, config, "127.0.0.1:0", test.webSocket, serverTLS)
			laneURL := test.scheme + "://" + address
			if test.webSocket {
				laneURL += "/_wirehop"
			}
			instances := make([]*client.Client, clients)
			peers := make([]*net.UDPConn, clients)
			previousSessions := make([]protocol.SessionID, clients)
			results := make([]chan error, clients)
			for index := range clients {
				clientOptions := clientConfig(t, laneURL, target, token, clientTLS)
				clientOptions.HandshakeTimeout = config.HandshakeTimeout
				clientOptions.Lanes = parseLaneSpecs(t, laneURL, laneURL, laneURL, laneURL)
				instance, err := client.Start(t.Context(), clientOptions)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { instance.Close() })
				instances[index] = instance
				result := make(chan error, 1)
				results[index] = result
				go func() { result <- instance.Wait() }()
				peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { peer.Close() })
				peers[index] = peer
			}
			waitForSharedOutageState(t, first, results, func(snapshot server.Snapshot) bool {
				for _, instance := range instances {
					if instance.SessionID().IsZero() {
						return false
					}
				}
				return snapshot.Sessions == clients && snapshot.AttachedLanes == clients*lanes &&
					snapshot.CreatingSessions == 0 && snapshot.PendingAdmissions == 0
			})
			for index, instance := range instances {
				previousSessions[index] = instance.SessionID()
				assertRelayExchange(t, peers[index], instance.LocalAddr())
			}
			stopFirst()
			waitForSharedOutageState(t, first, results, func(snapshot server.Snapshot) bool {
				return snapshot == (server.Snapshot{})
			})
			replacement, _, _ := startSharedOutageServer(t, config, address, test.webSocket, serverTLS)
			waitForSharedOutageState(t, replacement, results, func(snapshot server.Snapshot) bool {
				for index, instance := range instances {
					current := instance.SessionID()
					if current.IsZero() || current == previousSessions[index] {
						return false
					}
				}
				return snapshot.Sessions == clients && snapshot.AttachedLanes == clients*lanes &&
					snapshot.CreatingSessions == 0 && snapshot.PendingAdmissions == 0
			})
			identities := make(map[protocol.SessionID]struct{}, clients)
			for index, instance := range instances {
				current := instance.SessionID()
				if current.IsZero() || current == previousSessions[index] {
					t.Fatalf("client %d did not replace its session", index)
				}
				if _, exists := identities[current]; exists {
					t.Fatalf("client %d shares another client's session", index)
				}
				identities[current] = struct{}{}
				payload := transportPacket()
				payload[len(payload)-1] = byte(index)
				assertRelayPayload(t, peers[index], instance.LocalAddr(), payload)
			}
			var closing sync.WaitGroup
			for _, instance := range instances {
				closing.Go(func() { instance.Close() })
			}
			closing.Wait()
			for index, result := range results {
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("client %d exited with %v, want cancellation", index, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("client %d did not stop", index)
				}
			}
			waitForSharedOutageState(t, replacement, nil, func(snapshot server.Snapshot) bool {
				return snapshot == (server.Snapshot{})
			})
		})
	}
}

func startSharedOutageServer(t *testing.T, config server.Config, address string, webSocket bool,
	tlsConfig *tls.Config) (*server.Server, string, func()) {
	t.Helper()
	instance, err := server.New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	address = listener.Addr().String()
	configured := net.Listener(carrier.NewTCPOptionsListener(listener))
	if webSocket {
		configured = instance.WebSocketListener(configured)
	}
	if tlsConfig != nil {
		configured = tls.NewListener(configured, tlsConfig)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var httpServer *http.Server
	if webSocket {
		httpServer = &http.Server{
			Handler: instance.WebSocketHandler(ctx), ConnContext: instance.WebSocketConnContext,
			ReadHeaderTimeout: config.HandshakeTimeout,
		}
		go func() { done <- httpServer.Serve(configured) }()
	} else {
		go func() { done <- instance.Serve(ctx, configured) }()
	}
	stop := sync.OnceFunc(func() {
		cancel()
		if httpServer != nil {
			httpServer.Close()
		}
		listener.Close()
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("shared outage server stopped with %v", err)
		}
	})
	t.Cleanup(stop)
	return instance, address, stop
}

func waitForSharedOutageState(t *testing.T, instance *server.Server, results []chan error,
	condition func(server.Snapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		for index, result := range results {
			select {
			case err := <-result:
				t.Fatalf("client %d exited during shared recovery: %v", index, err)
			default:
			}
		}
		snapshot := instance.Snapshot()
		if condition(snapshot) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared recovery did not reach the expected state: %+v", snapshot)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
