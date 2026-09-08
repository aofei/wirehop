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
							Mode: protocol.HelloCreate, Target: endpoint, LaneID: protocol.LaneID{1}, Generation: 1,
							PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
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
						headers, err := wsheader.Headers(wsheader.Create{
							Token: "test-token", Target: endpoint, LaneID: protocol.LaneID{1}, Generation: 1,
							PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
						})
						if err != nil {
							t.Fatal(err)
						}
						stream, _, err := websocket.Dial(parent, httpServer.URL, &websocket.DialOptions{
							HTTPHeader: headers, Subprotocols: []string{wsheader.Subprotocol},
						})
						if err != nil {
							t.Fatal(err)
						}
						connection = carrier.NewWebSocketConn(stream)
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
					Mode: protocol.HelloCreate, Target: endpoint, LaneID: protocol.LaneID{1}, Generation: 1,
					PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
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
				request.Header, err = wsheader.Headers(wsheader.Create{
					Token: "test-token", Target: endpoint, LaneID: protocol.LaneID{1}, Generation: 1,
					PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
				})
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
							Mode: protocol.HelloCreate, Target: target.MustParse(test.target), LaneID: protocol.LaneID{1},
							Generation: 1, PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
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
						headers, err := wsheader.Headers(wsheader.Create{
							Token: test.token, Target: target.MustParse(test.target), LaneID: protocol.LaneID{1},
							Generation: 1, PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix(),
						})
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
