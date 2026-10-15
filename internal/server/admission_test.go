package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/netsetup"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wsheader"
	"github.com/coder/websocket"
)

func TestServerWebSocketAuthenticationHasNoAdmissionSideEffects(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode protocol.HelloMode
	}{
		{name: "Create", mode: protocol.HelloCreate},
		{name: "Join", mode: protocol.HelloJoin},
	} {
		t.Run(mode.name, func(t *testing.T) {
			for _, tt := range []struct {
				name   string
				mutate func(*http.Request)
			}{
				{name: "Path", mutate: func(r *http.Request) { r.URL.Path = "/changed" }},
				{name: "EscapedPath", mutate: func(r *http.Request) {
					r.URL.Path, r.URL.RawPath = "/_wirehop", "/%5fwirehop"
				}},
				{name: "Generation", mutate: func(r *http.Request) { r.Header.Set("WireHop-Lane-Generation", "2") }},
				{name: "Timestamp", mutate: func(r *http.Request) { r.Header.Set("WireHop-Timestamp", "1") }},
				{name: "Nonce", mutate: func(r *http.Request) { r.Header.Set("WireHop-Nonce", protocol.Nonce{2}.String()) }},
				{name: "Tag", mutate: func(r *http.Request) {
					value := []byte(r.Header.Get("Authorization"))
					if value[len(value)-1] == '0' {
						value[len(value)-1] = '1'
					} else {
						value[len(value)-1] = '0'
					}
					r.Header.Set("Authorization", string(value))
				}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					endpoint := target.MustParse("127.0.0.1:51820")
					token := []byte("test-token")
					instance := newSessionTestServer(t, token, endpoint, time.Minute)
					now := time.Now().Unix()
					hello := protocol.ClientHello{
						Mode: mode.mode, LaneID: 1, Generation: 1, PathGroupID: 1,
						Nonce: protocol.Nonce{1}, UnixSeconds: now,
					}
					key := token
					class, scope := protocol.ErrorSessionRejected, protocol.ErrorScopeSession
					var retained *serverSession
					if mode.mode == protocol.HelloCreate {
						hello.Target = endpoint
					} else {
						var err error
						retained, err = instance.createSession(t.Context(), t.Context(), endpoint)
						if err != nil {
							t.Fatal(err)
						}
						defer retained.close()
						hello.SessionID, key = retained.id, retained.secret[:]
						class, scope = protocol.ErrorLaneRejected, protocol.ErrorScopeLane
					}
					before := instance.Snapshot()
					headers, err := wsheader.Headers("/_wirehop", hello, key)
					if err != nil {
						t.Fatal(err)
					}
					request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
					request.Header = headers
					tt.mutate(request)
					writer := httptest.NewRecorder()
					instance.WebSocketHandler(t.Context()).ServeHTTP(writer, request)
					if writer.Code != http.StatusUnauthorized {
						t.Fatalf("HTTP status = %d, want 401", writer.Code)
					}
					rejection, err := wsheader.ParseRejection(writer.Header())
					if err != nil {
						t.Fatal(err)
					}
					if err := protocol.VerifyServerHello(rejection, key); err != nil {
						t.Fatal(err)
					}
					if rejection.ErrorCode != protocol.ErrorAuthentication || rejection.ErrorClass != class || rejection.ErrorScope != scope {
						t.Fatalf("rejection = %+v", rejection)
					}
					if after := instance.Snapshot(); after != before {
						t.Fatalf("failed authentication changed resources: before %+v, after %+v", before, after)
					}
					if retained == nil {
						for _, nonce := range []protocol.Nonce{{1}, {2}} {
							if err := instance.replay.CheckAndStore(nonce, now, now+60); err != nil {
								t.Fatalf("failed authentication retained nonce: %v", err)
							}
						}
					}
					if retained != nil {
						if err := retained.reserveJoinLane(hello); err != nil {
							t.Fatalf("failed authentication reserved lane: %v", err)
						}
						retained.rejectReservedLane()
					}
				})
			}
		})
	}
}

func TestServerWebSocketRejectsBearerAndAmbiguousOperations(t *testing.T) {
	endpoint := target.MustParse("127.0.0.1:51820")
	instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Minute)
	hello := protocol.ClientHello{
		Mode: protocol.HelloCreate, Target: endpoint, LaneID: 1, Generation: 1,
		PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
	}
	for _, tt := range []struct {
		name   string
		mutate func(http.Header)
	}{
		{name: "Bearer", mutate: func(h http.Header) { h.Set("Authorization", "Bearer test-token") }},
		{name: "BothOperations", mutate: func(h http.Header) { h.Set("WireHop-Session-ID", protocol.SessionID{1}.String()) }},
		{name: "NeitherOperation", mutate: func(h http.Header) { h.Del("WireHop-Target") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			headers, err := wsheader.Headers("/_wirehop", hello, []byte("test-token"))
			if err != nil {
				t.Fatal(err)
			}
			tt.mutate(headers)
			request := httptest.NewRequest(http.MethodGet, "http://relay.example/_wirehop", nil)
			request.Header = headers
			writer := httptest.NewRecorder()
			instance.WebSocketHandler(t.Context()).ServeHTTP(writer, request)
			if writer.Code != http.StatusBadRequest {
				t.Fatalf("HTTP status = %d, want 400", writer.Code)
			}
		})
	}
	if err := instance.replay.CheckAndStore(hello.Nonce, hello.UnixSeconds, hello.UnixSeconds+60); err != nil {
		t.Fatalf("malformed admission retained nonce: %v", err)
	}
}

func TestServerCandidateRetention(t *testing.T) {
	for _, transport := range []string{"Raw", "WebSocket"} {
		t.Run(transport, func(t *testing.T) {
			for _, behavior := range []string{"Abandoned", "UnconfirmedTimeout", "Confirmed"} {
				t.Run(behavior, func(t *testing.T) {
					endpoint := target.MustParse("127.0.0.1:51820")
					instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Minute)
					instance.config.HandshakeTimeout = 200 * time.Millisecond
					parent, cancel := context.WithCancel(t.Context())
					defer cancel()
					var connection carrier.Conn
					var id protocol.SessionID
					var receiveMicros, sendMicros uint64
					if transport == "Raw" {
						stream, peer := net.Pipe()
						defer peer.Close()
						finished := make(chan struct{})
						if !instance.beginAdmission() {
							t.Fatal("could not reserve test admission")
						}
						go func() {
							defer close(finished)
							release := sync.OnceFunc(instance.endAdmission)
							defer release()
							instance.serveConnection(parent, stream, release)
						}()
						defer func() {
							cancel()
							<-finished
						}()
						hello := protocol.ClientHello{
							Mode: protocol.HelloCreate, Target: endpoint, LaneID: protocol.LaneID(1), Generation: 1,
							PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
						}
						if err := protocol.SignClientHello(&hello, []byte("test-token")); err != nil {
							t.Fatal(err)
						}
						if err := protocol.WriteClientHello(peer, hello); err != nil {
							t.Fatal(err)
						}
						response, err := protocol.ReadServerHello(peer)
						if err != nil || response.Result != protocol.ServerSessionCreated {
							t.Fatalf("creation response = %+v, %v", response, err)
						}
						id, receiveMicros, sendMicros = response.SessionID, response.ReceiveMicros, response.SendMicros
						connection = carrier.NewStreamConn(peer)
					} else {
						httpServer := httptest.NewServer(instance.WebSocketHandler(parent))
						defer httpServer.Close()
						defer cancel()
						headers, err := wsheader.Headers("/_wirehop", protocol.ClientHello{
							Mode: protocol.HelloCreate, Target: endpoint, LaneID: protocol.LaneID(1), Generation: 1,
							PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
						}, []byte("test-token"))
						if err != nil {
							t.Fatal(err)
						}
						stream, _, err := carrier.DialWebSocket(parent, httpServer.URL+"/_wirehop", &websocket.DialOptions{
							HTTPHeader: headers, Subprotocols: []string{wsheader.Subprotocol},
						})
						if err != nil {
							t.Fatal(err)
						}
						connection = stream
						defer connection.Close()
						frame, err := connection.ReadFrame(parent)
						if err != nil {
							t.Fatal(err)
						}
						response, err := protocol.ParseSessionCreated(frame)
						if err != nil {
							t.Fatal(err)
						}
						id, receiveMicros, sendMicros = response.SessionID, response.ReceiveMicros, response.SendMicros
					}
					defer connection.Close()
					session := instance.findSession(id)
					if session == nil {
						t.Fatal("created session is absent")
					}
					if behavior == "Confirmed" {
						frame, err := protocol.MarshalClockSync(protocol.ClockSync{
							ClientSendMicros: 1, ServerReceiveMicros: receiveMicros,
							ServerSendMicros: sendMicros, ClientReceiveMicros: sendMicros - receiveMicros + 1000,
						})
						if err != nil {
							t.Fatal(err)
						}
						if err := connection.WriteFrames(parent, []protocol.Frame{frame}); err != nil {
							t.Fatal(err)
						}
						deadline := time.Now().Add(time.Second)
						for {
							session.mu.Lock()
							confirmed := session.confirmed
							session.mu.Unlock()
							if confirmed {
								break
							}
							if time.Now().After(deadline) {
								t.Fatal("first clock sync did not confirm session")
							}
							time.Sleep(time.Millisecond)
						}
					}
					if behavior != "UnconfirmedTimeout" {
						connection.Close()
					}
					deadline := time.Now().Add(time.Second)
					for {
						snapshot := instance.Snapshot()
						if behavior == "Confirmed" && snapshot.Sessions == 1 && snapshot.Detached == 1 ||
							behavior != "Confirmed" && snapshot.Sessions == 0 && snapshot.CreatingSessions == 0 && snapshot.PendingAdmissions == 0 {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("unexpected candidate resources: %+v", snapshot)
						}
						time.Sleep(time.Millisecond)
					}
				})
			}
		})
	}
}

func TestServerPreparationCancellation(t *testing.T) {
	for _, transport := range []string{"TCP", "TLS", "WS", "WSS"} {
		t.Run(transport, func(t *testing.T) {
			endpoint := target.MustParse("wg.example.com:51820")
			instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Minute)
			started := make(chan struct{})
			canceled := make(chan struct{})
			instance.config.Resolver = admissionResolver(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			})
			parent, stop := context.WithCancel(t.Context())
			defer stop()
			finished := make(chan struct{})
			var disconnect func()
			if transport == "TCP" || transport == "TLS" {
				connection, peer := net.Pipe()
				if transport == "TLS" {
					temporary := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
					defer temporary.Close()
					roots := x509.NewCertPool()
					roots.AddCert(temporary.Certificate())
					connection = tls.Server(connection, temporary.TLS.Clone())
					peer = tls.Client(peer, &tls.Config{RootCAs: roots, ServerName: "example.com"})
				}
				defer peer.Close()
				if !instance.beginAdmission() {
					t.Fatal("could not reserve test admission")
				}
				go func() {
					defer close(finished)
					defer instance.endAdmission()
					instance.serveConnection(parent, connection, func() { t.Error("unexpected admission") })
				}()
				hello := protocol.ClientHello{
					Mode: protocol.HelloCreate, Target: endpoint, LaneID: protocol.LaneID(1), Generation: 1,
					PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
				}
				if err := protocol.SignClientHello(&hello, []byte("test-token")); err != nil {
					t.Fatal(err)
				}
				if err := protocol.WriteClientHello(peer, hello); err != nil {
					t.Fatal(err)
				}
				disconnect = func() { peer.Close() }
			} else {
				handler := instance.WebSocketHandler(parent)
				wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(finished)
					handler.ServeHTTP(w, r)
				})
				var httpServer *httptest.Server
				if transport == "WSS" {
					httpServer = httptest.NewTLSServer(wrapped)
				} else {
					httpServer = httptest.NewServer(wrapped)
				}
				defer httpServer.Close()
				defer stop()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/_wirehop", nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header, err = wsheader.Headers("/_wirehop", protocol.ClientHello{
					Mode: protocol.HelloCreate, Target: endpoint, LaneID: protocol.LaneID(1), Generation: 1,
					PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
				}, []byte("test-token"))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Connection", "Upgrade")
				request.Header.Set("Upgrade", "websocket")
				request.Header.Set("Sec-WebSocket-Version", "13")
				request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				requestDone := make(chan struct{})
				go func() {
					defer close(requestDone)
					response, _ := httpServer.Client().Do(request)
					if response != nil {
						response.Body.Close()
					}
				}()
				disconnect = func() {
					cancel()
					<-requestDone
				}
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("target resolution did not start")
			}
			disconnect()
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("abandoned preparation did not cancel target resolution")
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("abandoned admission did not return")
			}
			if snapshot := instance.Snapshot(); snapshot.Sessions != 0 || snapshot.CreatingSessions != 0 || snapshot.PendingAdmissions != 0 {
				t.Fatalf("abandoned request retained resources: %+v", snapshot)
			}
		})
	}
}

type admissionResolver func(context.Context, string, string) ([]netip.Addr, error)

func (f admissionResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

func TestCreationDeadlineRequiresAuthorization(t *testing.T) {
	for _, transport := range []string{"Raw", "WebSocket"} {
		t.Run(transport, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				token   string
				target  string
				allowed bool
			}{
				{name: "Authorized", token: "test-token", target: "wg.example.com:51820", allowed: true},
				{name: "WrongToken", token: "wrong-token", target: "wg.example.com:51820"},
				{name: "DeniedTarget", token: "test-token", target: "other.example.com:51820"},
			} {
				t.Run(test.name, func(t *testing.T) {
					instance := newSessionTestServer(t, []byte("test-token"), target.MustParse("wg.example.com:51820"), time.Second)
					resolver := &serverTestResolver{err: errors.New("test resolver failure")}
					instance.config.Resolver = resolver
					var deadline time.Time
					started := time.Now()
					if transport == "Raw" {
						hello := protocol.ClientHello{
							Mode: protocol.HelloCreate, Target: target.MustParse(test.target), LaneID: protocol.LaneID(1),
							Generation: 1, PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
						}
						if err := protocol.SignClientHello(&hello, []byte(test.token)); err != nil {
							t.Fatal(err)
						}
						stream, peer := net.Pipe()
						defer stream.Close()
						defer peer.Close()
						connection := &admissionTestConnection{Conn: stream}
						instance.serveRawCreate(t.Context(), connection, hello, 0, func() { t.Error("unexpected admission") })
						deadline = connection.deadline
					} else {
						headers, err := wsheader.Headers("/_wirehop", protocol.ClientHello{
							Mode: protocol.HelloCreate, Target: target.MustParse(test.target), LaneID: protocol.LaneID(1),
							Generation: 1, PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
						}, []byte(test.token))
						if err != nil {
							t.Fatal(err)
						}
						request := httptest.NewRequest(http.MethodGet, "http://relay.example.com/_wirehop", nil)
						request.Header = headers
						writer := &admissionTestWriter{ResponseRecorder: httptest.NewRecorder()}
						instance.WebSocketHandler(context.Background()).ServeHTTP(writer, request)
						deadline = writer.deadline
					}
					if test.allowed {
						if deadline.Before(started.Add(netsetup.ResolveTimeout)) || resolver.host != "wg.example.com." {
							t.Fatalf("authorized deadline = %v, resolved host = %q", deadline, resolver.host)
						}
					} else if !deadline.IsZero() || resolver.host != "" {
						t.Fatalf("rejected request extended deadline to %v or resolved %q", deadline, resolver.host)
					}
					if snapshot := instance.Snapshot(); snapshot.CreatingSessions != 0 || snapshot.PendingAdmissions != 0 {
						t.Fatalf("failed creation retained admission resources: %+v", snapshot)
					}
				})
			}
		})
	}
}

func TestServerWebSocketAdmissionRejectsNoncanonicalNumbers(t *testing.T) {
	now := time.Now().Unix()
	endpoint := target.MustParse("wg.example.com:51820")
	creation, err := wsheader.Headers("/_wirehop", protocol.ClientHello{
		Mode: protocol.HelloCreate, Target: endpoint, LaneID: 1, Generation: 1, PathGroupID: 1,
		Nonce: protocol.Nonce{1}, UnixSeconds: now, MonotonicMicros: 1,
	}, []byte("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	join := protocol.ClientHello{
		Mode: protocol.HelloJoin, SessionID: protocol.SessionID{1}, LaneID: 1,
		Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: now, MonotonicMicros: 1,
	}
	joining, err := wsheader.Headers("/_wirehop", join, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []struct {
		name    string
		headers http.Header
	}{
		{name: "Create", headers: creation},
		{name: "Join", headers: joining},
	} {
		t.Run(mode.name, func(t *testing.T) {
			for _, field := range []struct {
				name   string
				header string
				value  string
			}{
				{name: "Generation", header: "WireHop-Lane-Generation", value: "01"},
				{name: "Timestamp", header: "WireHop-Timestamp", value: "+" + strconv.FormatInt(now, 10)},
				{name: "MonotonicSend", header: "WireHop-Monotonic-Send", value: "00"},
			} {
				t.Run(field.name, func(t *testing.T) {
					instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Second)
					resolver := &serverTestResolver{err: errors.New("test resolver failure")}
					instance.config.Resolver = resolver
					request := httptest.NewRequest(http.MethodGet, "http://relay.example.com/_wirehop", nil)
					request.Header = mode.headers.Clone()
					request.Header.Set(field.header, field.value)
					response := httptest.NewRecorder()
					instance.WebSocketHandler(t.Context()).ServeHTTP(response, request)
					if response.Code != http.StatusBadRequest || response.Header().Get("WireHop-Rejection") != "" {
						t.Fatalf("malformed admission response = %d, headers %v", response.Code, response.Header())
					}
					if resolver.host != "" || instance.Snapshot() != (Snapshot{}) {
						t.Fatalf("malformed admission resolved %q or retained resources: %+v", resolver.host, instance.Snapshot())
					}
					if err := instance.replay.CheckAndStore(protocol.Nonce{1}, now, now+60); err != nil {
						t.Fatalf("malformed admission consumed creation nonce: %v", err)
					}
				})
			}
		})
	}
}

type admissionTestConnection struct {
	net.Conn
	output   bytes.Buffer
	deadline time.Time
}

func (c *admissionTestConnection) Write(p []byte) (int, error) {
	return c.output.Write(p)
}

func (c *admissionTestConnection) SetDeadline(deadline time.Time) error {
	c.deadline = deadline
	return nil
}

type admissionTestWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *admissionTestWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}
