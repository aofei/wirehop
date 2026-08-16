package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	neturl "net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wsheader"
	"github.com/coder/websocket"
)

func TestStartValidatesBeforeBindingUDP(t *testing.T) {
	occupied, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	listen := occupied.LocalAddr().(*net.UDPAddr).AddrPort()
	instance, err := Start(context.Background(), Config{
		Lanes: []lanespec.Spec{{}}, Listen: listen,
		Target: target.MustParse("127.0.0.1:51820"), Token: []byte("test-token"),
		HandshakeTimeout:    time.Second,
		IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
		LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1,
	})
	if instance != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Start() = %v, %v, want nil, %v", instance, err, ErrInvalidConfig)
	}
}

func TestValidListenAddress(t *testing.T) {
	for _, test := range []struct {
		address netip.AddrPort
		want    bool
	}{
		{address: netip.MustParseAddrPort("127.0.0.1:51820"), want: true},
		{address: netip.MustParseAddrPort("0.0.0.0:51820"), want: true},
		{address: netip.MustParseAddrPort("[::]:51820"), want: true},
		{address: netip.MustParseAddrPort("224.0.0.1:51820")},
		{address: netip.MustParseAddrPort("[ff02::1]:51820")},
		{address: netip.MustParseAddrPort("[::ffff:127.0.0.1]:51820")},
	} {
		if got := validListenAddress(test.address); got != test.want {
			t.Fatalf("validListenAddress(%v) = %t, want %t", test.address, got, test.want)
		}
	}
}

func TestStartOwnsMutableConfig(t *testing.T) {
	dialer := &net.Dialer{Timeout: time.Second}
	tlsConfig := &tls.Config{ServerName: "relay.example"}
	instance, err := Start(context.Background(), Config{
		Lanes: testLaneSpecs(t, "tls://127.0.0.1:1"), Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: target.MustParse("127.0.0.1:51820"), Token: []byte("test-token"),
		Dialer: dialer, TLSConfig: tlsConfig, HandshakeTimeout: time.Second, MaxLanes: 1,
		IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
		LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	dialer.Timeout = 2 * time.Second
	tlsConfig.ServerName = "changed.example"
	if instance.config.Dialer == dialer || instance.config.TLSConfig == tlsConfig ||
		instance.config.Dialer.Timeout != time.Second || instance.config.TLSConfig.ServerName != "relay.example" {
		t.Fatal("Start() did not take an immutable snapshot of mutable connection config")
	}
}

func TestClientPreservesIngressFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	instance, err := Start(context.Background(), Config{
		Lanes: testLaneSpecs(t, "tcp://"+address), Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: target.MustParse("127.0.0.1:51820"), Token: []byte("test-token"),
		HandshakeTimeout: time.Second, StartupTimeout: time.Second, MaxLanes: 1,
		IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
		LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	err = instance.Wait()
	if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "read relay ingress") {
		t.Fatalf("Wait() error = %v, want preserved ingress failure", err)
	}
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateConfigRejectsInsecureTLS(t *testing.T) {
	config := Config{
		Lanes: testLaneSpecs(t, "tls://relay.example:443"), Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: target.MustParse("127.0.0.1:51820"), Token: []byte("test-token"),
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, HandshakeTimeout: time.Second, MaxLanes: 1,
		IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
		LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 1,
	}
	if _, err := validateConfig(config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("validateConfig() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestValidateConfigRejectsImpossibleTLSRange(t *testing.T) {
	for _, tlsConfig := range []*tls.Config{
		{MaxVersion: tls.VersionTLS11},
		{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS12},
	} {
		config := Config{
			Lanes: testLaneSpecs(t, "tls://relay.example:443"), Listen: netip.MustParseAddrPort("127.0.0.1:0"),
			Target: target.MustParse("127.0.0.1:51820"), Token: []byte("test-token"),
			TLSConfig: tlsConfig, HandshakeTimeout: time.Second, MaxLanes: 1,
			IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
			LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
			Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
			DeduplicationWindow: 1,
		}
		if _, err := validateConfig(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("validateConfig() error = %v, want %v", err, ErrInvalidConfig)
		}
	}
}

func TestPrepareLaneUsesResolvedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec := testLaneSpec(t, "url=tcp://relay.invalid:"+port+",resolve=127.0.0.1")
	instance := &Client{config: Config{Dialer: &net.Dialer{}, HandshakeTimeout: time.Second}}
	connection, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	if got, want := connection.RemoteAddr().String(), listener.Addr().String(); got != want {
		t.Fatalf("resolved lane remote address = %q, want %q", got, want)
	}
}

func TestPrepareTLSLaneUsesResolvedAddress(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	_, port, err := net.SplitHostPort(target.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec := testLaneSpec(t, "url=tls://example.com:"+port+",resolve=127.0.0.1")
	roots := x509.NewCertPool()
	roots.AddCert(target.Certificate())
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, TLSConfig: &tls.Config{RootCAs: roots}, HandshakeTimeout: time.Second,
	}}
	connection, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	secure, ok := connection.(*tls.Conn)
	if !ok {
		t.Fatalf("resolved TLS connection type = %T, want *tls.Conn", connection)
	}
	state := secure.ConnectionState()
	if state.ServerName != "example.com" || connection.RemoteAddr().String() != target.Listener.Addr().String() {
		t.Fatalf("resolved TLS state server name = %q, remote address = %q", state.ServerName,
			connection.RemoteAddr())
	}
}

func TestResolvedWSPreservesLogicalIdentity(t *testing.T) {
	observed := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{wsheader.Subprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		observed <- request.Host
		connection.Read(request.Context())
	}))
	defer target.Close()
	_, port, err := net.SplitHostPort(target.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec := testLaneSpec(t, "url=ws://relay.example:"+port+"/_wirehop,resolve=127.0.0.1")
	var proxySelections atomic.Int32
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, HandshakeTimeout: time.Second,
		Proxy: func(*http.Request) (*neturl.URL, error) {
			proxySelections.Add(1)
			return nil, nil
		},
	}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	connection, response, _, cancel, err := instance.dialWebSocket(
		context.Background(), spec.URL(), make(http.Header), prepared,
	)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("WebSocket response = %v", response)
	}
	select {
	case host := <-observed:
		want := net.JoinHostPort("relay.example", port)
		if host != want || proxySelections.Load() != 1 {
			t.Fatalf("resolved WS host = %q, proxy selections = %d, want %q and 1", host,
				proxySelections.Load(), want)
		}
	case <-time.After(time.Second):
		t.Fatal("resolved WS server did not observe the handshake")
	}
}

func TestResolvedWSSPreservesLogicalIdentity(t *testing.T) {
	type identity struct {
		host string
		sni  string
	}
	observed := make(chan identity, 1)
	target := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{wsheader.Subprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		observed <- identity{host: request.Host, sni: request.TLS.ServerName}
		connection.Read(request.Context())
	}))
	defer target.Close()
	_, port, err := net.SplitHostPort(target.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec := testLaneSpec(
		t, "url=wss://example.com:"+port+"/_wirehop,resolve=127.0.0.1",
	)
	roots := x509.NewCertPool()
	roots.AddCert(target.Certificate())
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, TLSConfig: &tls.Config{RootCAs: roots}, HandshakeTimeout: time.Second,
		Proxy: func(*http.Request) (*neturl.URL, error) {
			return nil, nil
		},
	}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	connection, response, _, cancel, err := instance.dialWebSocket(
		context.Background(), spec.URL(), make(http.Header), prepared,
	)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("WebSocket response = %v", response)
	}
	select {
	case got := <-observed:
		wantHost := net.JoinHostPort("example.com", port)
		if got.host != wantHost || got.sni != "example.com" {
			t.Fatalf("resolved WSS identity = %+v, want host %q and SNI example.com", got, wantHost)
		}
	case <-time.After(time.Second):
		t.Fatal("resolved WSS server did not observe the handshake")
	}
}

func TestDialWebSocketRejectsMissingSubprotocolPromptly(t *testing.T) {
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		<-release
	}))
	defer target.Close()
	defer close(release)
	spec := testLaneSpec(t, "ws"+strings.TrimPrefix(target.URL, "http"))
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, HandshakeTimeout: 10 * time.Second,
		Proxy: func(*http.Request) (*neturl.URL, error) {
			return nil, nil
		},
	}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	connection, response, _, cancel, err := instance.dialWebSocket(
		context.Background(), spec.URL(), make(http.Header), prepared,
	)
	cancel()
	if connection != nil || !errors.Is(err, ErrUnexpectedServerResponse) {
		t.Fatalf("dialWebSocket() = %v, %v, want nil, %v", connection, err, ErrUnexpectedServerResponse)
	}
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("WebSocket response = %v", response)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("missing-subprotocol rejection took %v", elapsed)
	}
}

func TestResolvedWebSocketRejectsForwardProxy(t *testing.T) {
	spec := testLaneSpec(t, "url=wss://relay.example/_wirehop,resolve=192.0.2.1")
	for _, test := range []struct {
		name   string
		scheme string
	}{
		{name: "HTTPProxy", scheme: "http"},
		{name: "HTTPSProxy", scheme: "https"},
		{name: "SOCKS5Proxy", scheme: "socks5"},
		{name: "SOCKS5HProxy", scheme: "socks5h"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := Config{
				Lanes:    []lanespec.Spec{spec},
				Listen:   netip.MustParseAddrPort("127.0.0.1:0"),
				Target:   target.MustParse("127.0.0.1:51820"),
				Token:    []byte("test-token"),
				Dialer:   &net.Dialer{},
				MaxLanes: 1,
				Proxy: func(*http.Request) (*neturl.URL, error) {
					return &neturl.URL{Scheme: test.scheme, Host: "proxy.example:8080"}, nil
				},
				HandshakeTimeout:    time.Second,
				IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
				LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
				Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
				DeduplicationWindow: 1,
			}
			_, err := validateConfig(config)
			if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "NO_PROXY") {
				t.Fatalf("validateConfig() error = %v, want direct-connection policy error", err)
			}
		})
	}
}

func TestDialWebSocketRejectsRedirect(t *testing.T) {
	var redirected atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirected" {
			redirected.Add(1)
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(writer, request, "/redirected", http.StatusFound)
	}))
	defer httpServer.Close()
	spec := testLaneSpec(t, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/_wirehop")
	url := spec.URL()
	instance := &Client{config: Config{Dialer: &net.Dialer{}, HandshakeTimeout: time.Second}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	connection, response, _, cancel, err := instance.dialWebSocket(
		context.Background(), url, make(http.Header), prepared,
	)
	cancel()
	if connection != nil || err == nil || response == nil || response.StatusCode != http.StatusFound {
		t.Fatalf("dialWebSocket() = %v, %v, %v", connection, response, err)
	}
	if redirected.Load() != 0 || !permanentHTTPRejection(response.StatusCode) {
		t.Fatalf("redirect count = %d, permanent = %t", redirected.Load(),
			permanentHTTPRejection(response.StatusCode))
	}
}

func TestDialWebSocketBoundsResponseHeaders(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Oversized", strings.Repeat("a", maximumWebSocketResponseHeaderBytes+1))
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer httpServer.Close()
	spec := testLaneSpec(t, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/_wirehop")
	url := spec.URL()
	instance := &Client{config: Config{Dialer: &net.Dialer{}, HandshakeTimeout: time.Second}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	connection, _, _, cancel, err := instance.dialWebSocket(context.Background(), url, make(http.Header), prepared)
	cancel()
	if connection != nil || err == nil || !strings.Contains(err.Error(), "response headers exceeded") {
		t.Fatalf("dialWebSocket() = %v, %v", connection, err)
	}
}

func TestPreparedWebSocketRetainsProxySelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{wsheader.Subprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		connection.Read(request.Context())
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec := testLaneSpec(t, "ws://localhost:"+port+"/_wirehop")
	url := spec.URL()
	var selections atomic.Int32
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, HandshakeTimeout: time.Second,
		Proxy: func(*http.Request) (*neturl.URL, error) {
			if selections.Add(1) != 1 {
				return nil, errors.New("proxy policy was selected more than once")
			}
			return nil, nil
		},
	}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	preparedWebSocket, ok := prepared.(*preparedWebSocketConnection)
	if !ok {
		t.Fatalf("prepared WebSocket connection type = %T", prepared)
	}
	wantEndpoint := server.Listener.Addr().String()
	if preparedWebSocket.firstHopAddress != wantEndpoint || preparedWebSocket.targetAddress != wantEndpoint {
		t.Fatalf("prepared WebSocket endpoints = %q, %q, want %q",
			preparedWebSocket.firstHopAddress, preparedWebSocket.targetAddress, wantEndpoint)
	}
	connection, _, _, cancel, err := instance.dialWebSocket(
		context.Background(), url, make(http.Header), prepared,
	)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if selections.Load() != 1 {
		t.Fatalf("proxy selections = %d, want 1", selections.Load())
	}
}

func TestPreparedWebSocketClosesOnHeaderError(t *testing.T) {
	url := testLaneSpec(t, "ws://relay.example/_wirehop").URL()
	attempt := creationAttempt{
		laneID: protocol.LaneID{1}, pathGroupID: protocol.PathGroupID{1}, generation: 1,
		nonce: protocol.Nonce{1},
	}
	instance := &Client{config: Config{
		Token: []byte("test-token"), Target: target.MustParse("127.0.0.1:51820"),
	}}
	for _, test := range []struct {
		name string
		open func(net.Conn) error
	}{
		{name: "Create", open: func(connection net.Conn) error {
			_, _, err := instance.openPreparedCreation(context.Background(), url, attempt, connection)
			return err
		}},
		{name: "Join", open: func(connection net.Conn) error {
			_, err := instance.openPreparedJoin(context.Background(), url, attempt, creationResult{
				sessionID: protocol.SessionID{1}, sessionSecret: protocol.SessionSecret{1},
			}, connection)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, peer := net.Pipe()
			defer peer.Close()
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := test.open(connection); err == nil {
				t.Fatal("WebSocket admission succeeded with an invalid timestamp")
			}
			if _, err := peer.Read(make([]byte, 1)); err == nil {
				t.Fatal("prepared connection remained open after admission header failure")
			}
		})
	}
}

func TestDialWebSocketThroughHTTPSProxy(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{wsheader.Subprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		connection.Read(request.Context())
	}))
	defer target.Close()
	proxyResumptions := make(chan bool, 2)
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodConnect {
			http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		proxyResumptions <- request.TLS.DidResume
		upstream, err := net.Dial("tcp", request.Host)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			return
		}
		clientConnection, buffered, err := http.NewResponseController(writer).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			clientConnection.Close()
			upstream.Close()
			return
		}
		if err := buffered.Flush(); err != nil {
			clientConnection.Close()
			upstream.Close()
			return
		}
		done := make(chan struct{})
		go func() {
			io.Copy(upstream, buffered)
			upstream.Close()
			close(done)
		}()
		io.Copy(clientConnection, upstream)
		clientConnection.Close()
		upstream.Close()
		<-done
	}))
	defer proxy.Close()
	proxyURL, err := neturl.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(target.Certificate())
	roots.AddCert(proxy.Certificate())
	spec := testLaneSpec(t, "wss"+strings.TrimPrefix(target.URL, "https")+"/_wirehop")
	url := spec.URL()
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, Proxy: http.ProxyURL(proxyURL), TLSConfig: &tls.Config{
			RootCAs: roots, MaxVersion: tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(4),
		},
		HandshakeTimeout: time.Second,
	}}
	for attempt := range 2 {
		prepared, err := instance.prepareLane(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		connection, response, _, cancel, err := instance.dialWebSocket(
			context.Background(), url, make(http.Header), prepared,
		)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("WebSocket response = %v", response)
		}
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
		if resumed := <-proxyResumptions; resumed != (attempt == 1) {
			t.Fatalf("HTTPS proxy TLS attempt %d resumed = %t", attempt+1, resumed)
		}
	}
}

func TestDialWebSocketThroughSOCKS5Proxy(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{wsheader.Subprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		connection.Read(request.Context())
	}))
	defer target.Close()
	_, port, err := net.SplitHostPort(target.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	requested := make(chan string, 1)
	proxyDone := make(chan error, 1)
	go func() {
		proxyDone <- serveSOCKS5Connection(proxy, target.Listener.Addr().String(), requested)
	}()
	proxyURL, err := neturl.Parse("socks5://" + proxy.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec := testLaneSpec(t, "ws://relay.example:"+port+"/_wirehop")
	instance := &Client{config: Config{
		Dialer: &net.Dialer{}, Proxy: http.ProxyURL(proxyURL), HandshakeTimeout: time.Second,
	}}
	prepared, err := instance.prepareLane(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	connection, response, _, cancel, err := instance.dialWebSocket(
		context.Background(), spec.URL(), make(http.Header), prepared,
	)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("WebSocket response = %v", response)
	}
	if got, want := <-requested, net.JoinHostPort("relay.example", port); got != want {
		t.Fatalf("SOCKS5 target = %q, want %q", got, want)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-proxyDone; err != nil {
		t.Fatal(err)
	}
}

func serveSOCKS5Connection(listener net.Listener, upstreamAddress string, requested chan<- string) error {
	client, err := listener.Accept()
	if err != nil {
		return err
	}
	defer client.Close()
	var greeting [3]byte
	if _, err := io.ReadFull(client, greeting[:]); err != nil {
		return err
	}
	if !bytes.Equal(greeting[:], []byte{5, 1, 0}) {
		return errors.New("unexpected SOCKS5 greeting")
	}
	if _, err := client.Write([]byte{5, 0}); err != nil {
		return err
	}
	var request [4]byte
	if _, err := io.ReadFull(client, request[:]); err != nil {
		return err
	}
	if !bytes.Equal(request[:3], []byte{5, 1, 0}) {
		return errors.New("unexpected SOCKS5 connect request")
	}
	host, err := readSOCKS5Host(client, request[3])
	if err != nil {
		return err
	}
	var port [2]byte
	if _, err := io.ReadFull(client, port[:]); err != nil {
		return err
	}
	requested <- net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:]))))
	upstream, err := net.Dial("tcp", upstreamAddress)
	if err != nil {
		return err
	}
	defer upstream.Close()
	if _, err := client.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		return err
	}
	copyDone := make(chan struct{})
	go func() {
		io.Copy(upstream, client)
		upstream.Close()
		close(copyDone)
	}()
	io.Copy(client, upstream)
	client.Close()
	<-copyDone
	return nil
}

func readSOCKS5Host(reader io.Reader, addressType byte) (string, error) {
	var size int
	switch addressType {
	case 1:
		size = net.IPv4len
	case 3:
		var length [1]byte
		if _, err := io.ReadFull(reader, length[:]); err != nil {
			return "", err
		}
		size = int(length[0])
	case 4:
		size = net.IPv6len
	default:
		return "", errors.New("unexpected SOCKS5 address type")
	}
	address := make([]byte, size)
	if _, err := io.ReadFull(reader, address); err != nil {
		return "", err
	}
	if addressType == 3 {
		return string(address), nil
	}
	return net.IP(address).String(), nil
}

func TestWebSocketFirstHopNormalizesProxyScheme(t *testing.T) {
	spec := testLaneSpec(t, "wss://relay.example:443/_wirehop")
	for _, test := range []struct {
		proxy string
		want  string
	}{
		{proxy: "HTTPS://proxy.example:8443", want: "proxy.example:8443"},
		{proxy: "//proxy.example", want: "proxy.example:80"},
		{proxy: "https://[2001:db8::1]", want: "[2001:db8::1]:443"},
		{proxy: "http://proxy.example:00080", want: "proxy.example:80"},
		{proxy: "socks5://proxy.example", want: "proxy.example:1080"},
		{proxy: "socks5h://proxy.example", want: "proxy.example:1080"},
	} {
		proxyURL, err := neturl.Parse(test.proxy)
		if err != nil {
			t.Fatal(err)
		}
		instance := &Client{config: Config{Proxy: func(request *http.Request) (*neturl.URL, error) {
			if request.URL.Scheme != "https" {
				t.Fatalf("proxy request scheme = %q, want https", request.URL.Scheme)
			}
			return proxyURL, nil
		}}}
		address, err := instance.webSocketFirstHop(spec)
		if err != nil {
			t.Fatal(err)
		}
		if address != test.want {
			t.Fatalf("webSocketFirstHop() = %q, want %q", address, test.want)
		}
	}
}

func TestValidateConfigRejectsInvalidProxy(t *testing.T) {
	for _, proxyURL := range []*neturl.URL{
		{Scheme: "ftp", Host: "proxy.example:21"},
		{Scheme: "https", Host: "proxy.example:0"},
		{Scheme: "https", Host: "proxy.example:65536"},
		{Scheme: "https", Host: "proxy.example:service"},
		{Scheme: "https", Host: "proxy.example:"},
		{Scheme: "https", Host: "]"},
	} {
		config := Config{
			Lanes:  testLaneSpecs(t, "wss://relay.example:443/_wirehop"),
			Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: target.MustParse("127.0.0.1:51820"),
			Token: []byte("test-token"), Proxy: func(*http.Request) (*neturl.URL, error) {
				return proxyURL, nil
			},
			HandshakeTimeout: time.Second, MaxLanes: 1,
			IngressLimits:       packetqueue.Limits{Packets: 1, Bytes: 2048},
			LaneLimits:          packetqueue.Limits{Packets: 1, Bytes: 2048},
			Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
			DeduplicationWindow: 1,
		}
		if _, err := validateConfig(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("validateConfig(proxy=%q) error = %v, want %v", proxyURL, err, ErrInvalidConfig)
		}
	}
}

func TestBuildLanesGroupsCanonicalRoutes(t *testing.T) {
	values := []string{
		"wss://EXAMPLE.COM/%7ewirehop",
		"wss://example.com:443/%7Ewirehop",
		"wss://example.com:443/other",
		"url=wss://example.com/%7Ewirehop,resolve=192.0.2.1",
		"url=wss://example.com/%7Ewirehop,resolve=192.0.2.2",
		"url=wss://EXAMPLE.COM:443/%7ewirehop,resolve=192.0.2.1",
	}
	lanes := buildLanes(testLaneSpecs(t, values...), time.Now)
	if len(lanes) != len(values) {
		t.Fatalf("buildLanes() returned %d lanes, want %d", len(lanes), len(values))
	}
	identities := make(map[protocol.LaneID]struct{}, len(lanes))
	for _, lane := range lanes {
		identities[lane.laneID] = struct{}{}
	}
	if len(identities) != len(lanes) {
		t.Fatal("buildLanes() reused a lane identity")
	}
	if lanes[0].pathGroupID != lanes[1].pathGroupID {
		t.Fatal("canonical-equivalent URLs received different path groups")
	}
	if lanes[0].pathGroupID == lanes[2].pathGroupID {
		t.Fatal("different canonical URLs received the same path group")
	}
	if lanes[0].pathGroupID == lanes[3].pathGroupID {
		t.Fatal("normal DNS and fixed resolution received the same path group")
	}
	if lanes[3].pathGroupID == lanes[4].pathGroupID {
		t.Fatal("different fixed resolutions received the same path group")
	}
	if lanes[3].pathGroupID != lanes[5].pathGroupID {
		t.Fatal("canonical-equivalent fixed resolutions received different path groups")
	}
}

func TestCloseUnusedPreparations(t *testing.T) {
	connection, peer := net.Pipe()
	defer peer.Close()
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	prepared := make(chan preparationResult, 1)
	prepared <- preparationResult{connection: connection}
	failed := make(chan preparationResult, 1)
	failed <- preparationResult{err: errors.New("preparation failed")}

	closeUnusedPreparations([]chan preparationResult{nil, prepared, failed, make(chan preparationResult, 1)})

	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("unused prepared connection read error = %v, want %v", err, io.EOF)
	}
	if len(prepared) != 0 || len(failed) != 0 {
		t.Fatalf("preparation channels retained %d and %d results", len(prepared), len(failed))
	}
}

func testLaneSpec(t *testing.T, value string) lanespec.Spec {
	t.Helper()
	return testLaneSpecs(t, value)[0]
}

func testLaneSpecs(t *testing.T, values ...string) []lanespec.Spec {
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
