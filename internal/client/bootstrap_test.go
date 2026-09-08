package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/server"
	"github.com/aofei/wirehop/internal/target"
)

func TestClientBypassesStalledAdmission(t *testing.T) {
	for _, scheme := range []string{"TCP", "TLS", "WS", "WSS"} {
		t.Run(scheme, func(t *testing.T) {
			for _, failure := range []string{"None", "DNS", "Admission"} {
				t.Run(failure, func(t *testing.T) {
					testClientBypassesStalledAdmission(t, scheme, failure)
				})
			}
		})
	}
}

func testClientBypassesStalledAdmission(t *testing.T, scheme, failure string) {
	t.Helper()
	endpoint, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	config := serverConfig(t, token, []netip.AddrPort{endpoint})
	logicalTarget := target.MustParse(endpoint.String())
	var lookups atomic.Int32
	if failure == "Admission" {
		logicalTarget = target.MustParse("wg.example.com:" + strconv.Itoa(int(endpoint.Port())))
		var err error
		config.Targets, err = policy.NewTargetSet([]target.Endpoint{logicalTarget})
		if err != nil {
			t.Fatal(err)
		}
		config.Resolver = bootstrapResolver(func(_ context.Context, _, host string) ([]netip.Addr, error) {
			if lookups.Add(1) == 1 {
				return nil, &net.DNSError{Err: "temporary failure", Name: host, IsTemporary: true}
			}
			return []netip.Addr{endpoint.Addr()}, nil
		})
	}
	instance, err := server.New(config)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	notify := sync.OnceFunc(func() { close(started) })
	var goodURL, badURL string
	var clientTLS *tls.Config
	if scheme == "WS" || scheme == "WSS" {
		var stopGood func()
		goodURL, clientTLS, stopGood = startWebSocketServerInstance(t, instance, scheme == "WSS")
		defer stopGood()
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			notify()
			<-r.Context().Done()
		})
		var bad *httptest.Server
		if scheme == "WSS" {
			bad = httptest.NewTLSServer(handler)
		} else {
			bad = httptest.NewServer(handler)
		}
		defer bad.Close()
		badURL = strings.Replace(bad.URL, "http", "ws", 1) + "/_wirehop"
	} else {
		var serverTLS *tls.Config
		if scheme == "TLS" {
			serverTLS, clientTLS = testTLSConfigs(t)
		}
		goodAddress, stopGood := startRawServerInstance(t, instance, serverTLS)
		defer stopGood()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if serverTLS != nil {
			listener = tls.NewListener(listener, serverTLS)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				connection, err := listener.Accept()
				if err != nil {
					return
				}
				connection.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := protocol.ReadClientHello(connection); err == nil {
					notify()
					io.Copy(io.Discard, connection)
				}
				connection.Close()
			}
		}()
		defer func() {
			listener.Close()
			<-done
		}()
		goodURL = strings.ToLower(scheme) + "://" + goodAddress
		badURL = strings.ToLower(scheme) + "://" + listener.Addr().String()
	}
	clientConfig := clientConfig(t, badURL, endpoint, token, clientTLS)
	clientConfig.HandshakeTimeout = 5 * time.Second
	clientConfig.Target = logicalTarget
	clientConfig.Lanes = parseLaneSpecs(t, badURL, goodURL)
	goodAddress := clientConfig.Lanes[1].DialAddress()
	var goodAttempts atomic.Int32
	clientConfig.Dialer = &net.Dialer{ControlContext: func(ctx context.Context, _, address string, _ syscall.RawConn) error {
		if address == goodAddress {
			select {
			case <-started:
			case <-ctx.Done():
				return ctx.Err()
			}
			if goodAttempts.Add(1) == 1 && failure == "DNS" {
				return &net.DNSError{Err: "no such host", Name: "relay.example.com", IsNotFound: true}
			}
		}
		return nil
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	startedAt := time.Now()
	local, err := client.Start(ctx, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	waitForServer(t, func(snapshot server.Snapshot) bool {
		return !local.SessionID().IsZero() && snapshot.Sessions == 1 && snapshot.AttachedLanes == 1
	}, instance)
	peer, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(local.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	payload := transportPacket()
	if _, err := peer.Write(payload); err != nil {
		t.Fatal(err)
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, len(payload))
	count, err := peer.Read(buffer)
	if err != nil || !bytes.Equal(buffer[:count], payload) {
		t.Fatalf("healthy forwarding = %x, %v", buffer[:count], err)
	}
	t.Logf("healthy admission completed in %v", time.Since(startedAt))
	if failure != "None" && goodAttempts.Load() < 2 {
		t.Fatalf("healthy lane attempts = %d, want independent retry", goodAttempts.Load())
	}
	if failure == "Admission" && lookups.Load() != 2 {
		t.Fatalf("target lookups = %d, want two creation attempts", lookups.Load())
	}
}

func TestClientBootstrapTerminalCandidates(t *testing.T) {
	config := clientConfig(t, "tcp://127.0.0.1:1", netip.MustParseAddrPort("127.0.0.1:51820"), []byte("test-token"), nil)
	config.Lanes = parseLaneSpecs(t, "tcp://127.0.0.1:1", "tcp://127.0.0.1:2")
	var rejectedAttempts, recoverableAttempts atomic.Int32
	config.Dialer = &net.Dialer{ControlContext: func(_ context.Context, _, address string, _ syscall.RawConn) error {
		if address == config.Lanes[0].DialAddress() {
			rejectedAttempts.Add(1)
			return client.ErrLaneRejected
		}
		if recoverableAttempts.Add(1) < 3 {
			return &net.DNSError{Err: "no such host", Name: "relay.example.com", IsNotFound: true}
		}
		return os.ErrPermission
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	local, err := client.Start(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	err = local.Wait()
	if !errors.Is(err, client.ErrLaneRejected) && !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Wait() = %v, want a terminal candidate rejection", err)
	}
	if rejectedAttempts.Load() != 1 || recoverableAttempts.Load() != 3 {
		t.Fatalf("candidate attempts = %d and %d, want 1 and 3", rejectedAttempts.Load(), recoverableAttempts.Load())
	}
}

func TestClientConcurrentCandidatesRetainOneSession(t *testing.T) {
	endpoint, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	address := endpoint.LocalAddr().(*net.UDPAddr).AddrPort()
	domain := target.MustParse("wg.example.com:" + strconv.Itoa(int(address.Port())))
	token := []byte("test-token")
	config := serverConfig(t, token, []netip.AddrPort{address})
	config.Targets, err = policy.NewTargetSet([]target.Endpoint{domain})
	if err != nil {
		t.Fatal(err)
	}
	const lanes = 3
	config.MaxPendingAdmissions = lanes
	config.ReconnectGrace = time.Minute
	var lookups atomic.Int32
	ready := make(chan struct{})
	config.Resolver = bootstrapResolver(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
		if lookups.Add(1) == lanes {
			close(ready)
		}
		select {
		case <-ready:
			return []netip.Addr{address.Addr()}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	remote, err := server.New(config)
	if err != nil {
		t.Fatal(err)
	}
	carrierAddress, stopServer := startRawServerInstance(t, remote, nil)
	defer stopServer()
	url := "tcp://" + carrierAddress
	clientConfig := clientConfig(t, url, address, token, nil)
	clientConfig.Target = domain
	clientConfig.Lanes = parseLaneSpecs(t, url, url, url)
	local, err := client.Start(t.Context(), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	waitForServer(t, func(snapshot server.Snapshot) bool {
		return snapshot.Sessions == 1 && snapshot.AttachedLanes == lanes &&
			snapshot.CreatingSessions == 0 && snapshot.PendingAdmissions == 0
	}, remote)
	if got := lookups.Load(); got != lanes {
		t.Fatalf("target lookups = %d, want %d concurrent candidates", got, lanes)
	}
	peer, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(local.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	payload := transportPacket()
	buffer := make([]byte, len(payload))
	var source netip.AddrPort
	for range 16 {
		if _, err := peer.Write(payload); err != nil {
			t.Fatal(err)
		}
		endpoint.SetReadDeadline(time.Now().Add(time.Second))
		_, current, err := endpoint.ReadFromUDPAddrPort(buffer)
		if err != nil {
			t.Fatal(err)
		}
		if source.IsValid() && current != source {
			t.Fatalf("target source changed from %v to %v", source, current)
		}
		source = current
		if _, err := endpoint.WriteToUDPAddrPort(buffer, source); err != nil {
			t.Fatal(err)
		}
		peer.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := peer.Read(buffer); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientBootstrapCapacity(t *testing.T) {
	for _, scheme := range []string{"TCP", "TLS", "WS", "WSS"} {
		t.Run(scheme, func(t *testing.T) {
			for _, admissions := range []int{1, 16} {
				t.Run("Admissions"+strconv.Itoa(admissions), func(t *testing.T) {
					testClientBootstrapCapacity(t, scheme, admissions)
				})
			}
		})
	}
}

func testClientBootstrapCapacity(t *testing.T, scheme string, admissions int) {
	t.Helper()
	const lanes = 16
	endpoint, stopTarget := startEchoTarget(t)
	defer stopTarget()
	token := []byte("test-token")
	config := serverConfig(t, token, []netip.AddrPort{endpoint})
	config.MaxSessions = 1
	config.MaxLanesPerSession = lanes
	config.MaxPendingAdmissions = admissions
	config.HandshakeTimeout = 5 * time.Second
	remote, err := server.New(config)
	if err != nil {
		t.Fatal(err)
	}
	var url string
	var clientTLS *tls.Config
	if scheme == "WS" || scheme == "WSS" {
		ctx, cancel := context.WithCancel(t.Context())
		httpServer := httptest.NewUnstartedServer(remote.WebSocketHandler(ctx))
		defer func() {
			cancel()
			httpServer.Close()
		}()
		httpServer.Listener = remote.WebSocketListener(httpServer.Listener)
		httpServer.Config.ConnContext = remote.WebSocketConnContext
		httpServer.Config.ReadHeaderTimeout = config.HandshakeTimeout
		httpServer.Config.WriteTimeout = config.HandshakeTimeout
		httpServer.Config.ErrorLog = log.New(io.Discard, "", 0)
		if scheme == "WSS" {
			httpServer.StartTLS()
			roots := x509.NewCertPool()
			roots.AddCert(httpServer.Certificate())
			clientTLS = &tls.Config{RootCAs: roots}
		} else {
			httpServer.Start()
		}
		url = strings.Replace(httpServer.URL, "http", "ws", 1) + "/_wirehop"
	} else {
		var serverTLS *tls.Config
		if scheme == "TLS" {
			serverTLS, clientTLS = testTLSConfigs(t)
		}
		address, stopServer := startRawServerInstance(t, remote, serverTLS)
		defer stopServer()
		url = strings.ToLower(scheme) + "://" + address
	}
	clientConfig := clientConfig(t, url, endpoint, token, clientTLS)
	clientConfig.HandshakeTimeout = config.HandshakeTimeout
	clientConfig.MaxLanes = lanes
	urls := make([]string, lanes)
	for index := range urls {
		urls[index] = url
	}
	clientConfig.Lanes = parseLaneSpecs(t, urls...)
	local, err := client.Start(t.Context(), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	waitForClientSession(t, local)
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	assertRelayExchange(t, peer, local.LocalAddr())
	deadline := time.Now().Add(10 * time.Second)
	for {
		snapshot := remote.Snapshot()
		if snapshot.Sessions == 1 && snapshot.AttachedLanes == lanes &&
			snapshot.CreatingSessions == 0 && snapshot.PendingAdmissions == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lanes did not converge: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	for range lanes {
		assertRelayExchange(t, peer, local.LocalAddr())
	}
	local.Close()
	waitForServer(t, func(snapshot server.Snapshot) bool { return snapshot == (server.Snapshot{}) }, remote)
}

type bootstrapResolver func(context.Context, string, string) ([]netip.Addr, error)

func (f bootstrapResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}
