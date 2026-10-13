package server

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/auth"
	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wsheader"
	"github.com/coder/websocket"
)

func TestServerSessionReserveJoinLane(t *testing.T) {
	t.Run("ConcurrentReplay", func(t *testing.T) {
		endpoint := target.MustParse("127.0.0.1:51820")
		instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Minute)
		session, err := instance.createSession(t.Context(), t.Context(), endpoint)
		if err != nil {
			t.Fatal(err)
		}
		defer session.close()
		hello := protocol.ClientHello{Mode: protocol.HelloJoin, SessionID: session.id,
			LaneID: 1, Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix()}
		if err := protocol.SignClientHello(&hello, session.secret[:]); err != nil {
			t.Fatal(err)
		}
		const workers = 32
		start := make(chan struct{})
		results := make(chan error, workers)
		var group sync.WaitGroup
		for range workers {
			group.Go(func() {
				<-start
				if err := protocol.VerifyClientHello(hello, session.secret[:]); err != nil {
					results <- err
					return
				}
				results <- session.reserveJoinLane(hello)
			})
		}
		close(start)
		group.Wait()
		close(results)
		accepted := 0
		for err := range results {
			if err == nil {
				accepted++
			} else if !errors.Is(err, relay.ErrStaleLane) {
				t.Fatalf("concurrent join returned %v", err)
			}
		}
		if accepted != 1 {
			t.Fatalf("accepted %d copies of one signed join, want 1", accepted)
		}
		// A failed acceptance write releases its reservation without reopening its generation.
		session.rejectReservedLane()
		if err := session.reserveJoinLane(hello); !errors.Is(err, relay.ErrStaleLane) {
			t.Fatalf("replay after failed response returned %v, want stale generation", err)
		}
	})
	t.Run("TimestampUnderLockAndRollback", func(t *testing.T) {
		const initialUnix = int64(2_000)
		endpoint := target.MustParse("127.0.0.1:51820")
		instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Minute)
		var wall atomic.Int64
		wall.Store(initialUnix)
		instance.config.WallClock = func() time.Time { return time.Unix(wall.Load(), 0) }
		session, err := instance.createSession(t.Context(), t.Context(), endpoint)
		if err != nil {
			t.Fatal(err)
		}
		defer session.close()
		hello := protocol.ClientHello{LaneID: 1, Generation: 1, PathGroupID: 1, UnixSeconds: initialUnix}
		if err := session.reserveJoinLane(hello); err != nil {
			t.Fatal(err)
		}
		defer session.rejectReservedLane()
		advancedUnix := initialUnix + int64(instance.config.AuthenticationSkew/time.Second) + 1
		wall.Store(advancedUnix)
		instance.authenticationTime()
		wall.Store(initialUnix)
		instance.config.WallClock = func() time.Time {
			if session.mu.TryLock() {
				session.mu.Unlock()
				t.Error("join sampled authentication time outside the session mutex")
			}
			return time.Unix(wall.Load(), 0)
		}
		hello.Generation = 2
		if err := session.reserveJoinLane(hello); !errors.Is(err, auth.ErrTimestampOutsideWindow) {
			t.Fatalf("old timestamp after rollback returned %v, want clock skew", err)
		}
		hello.UnixSeconds = advancedUnix
		if err := session.reserveJoinLane(hello); err != nil {
			t.Fatalf("fresh timestamp could not reserve the rejected generation: %v", err)
		}
		defer session.rejectReservedLane()
	})
}

func TestServerJoinNonceReuse(t *testing.T) {
	for _, tt := range []struct {
		name      string
		webSocket bool
	}{
		{name: "Raw"},
		{name: "WebSocket", webSocket: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := target.MustParse("127.0.0.1:51820")
			instance := newSessionTestServer(t, []byte("test-token"), endpoint, time.Minute)
			session, err := instance.createSession(t.Context(), t.Context(), endpoint)
			if err != nil {
				t.Fatal(err)
			}
			defer session.close()
			hello := protocol.ClientHello{Mode: protocol.HelloJoin, SessionID: session.id,
				LaneID: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: time.Now().Unix()}
			var serverURL string
			if tt.webSocket {
				server := httptest.NewServer(instance.WebSocketHandler(t.Context()))
				defer server.Close()
				serverURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/_wirehop"
			}
			for attempt, generation := range []uint64{1, 2, 2} {
				wantAccepted := attempt < 2
				hello.Generation = generation
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				if tt.webSocket {
					headers, err := wsheader.Headers("/_wirehop", hello, session.secret[:])
					if err != nil {
						t.Fatal(err)
					}
					connection, response, err := websocket.Dial(ctx, serverURL, &websocket.DialOptions{
						HTTPHeader: headers, Subprotocols: []string{wsheader.Subprotocol},
					})
					if err != nil {
						if wantAccepted || response == nil {
							t.Fatalf("join generation %d failed: %v", generation, err)
						}
						rejection, err := wsheader.ParseRejection(response.Header)
						if err != nil || protocol.VerifyServerHello(rejection, session.secret[:]) != nil ||
							rejection.RequestNonce != hello.Nonce || rejection.ErrorCode != protocol.ErrorStaleGeneration {
							t.Fatalf("replayed join rejection = %+v, %v", rejection, err)
						}
					} else {
						stream := carrier.NewWebSocketConn(connection)
						frame, err := stream.ReadFrame(ctx)
						stream.Close()
						if !wantAccepted {
							t.Fatal("replayed join received a second acceptance")
						}
						if err != nil {
							t.Fatal(err)
						}
						if accepted, err := protocol.ParseLaneAccepted(frame); err != nil || accepted.SessionID != session.id {
							t.Fatalf("join acceptance = %+v, %v", accepted, err)
						}
					}
				} else {
					if err := protocol.SignClientHello(&hello, session.secret[:]); err != nil {
						t.Fatal(err)
					}
					serverConnection, clientConnection := net.Pipe()
					finished := make(chan error, 1)
					go func() {
						finished <- instance.serveConnection(ctx, serverConnection, func() {})
					}()
					if err := clientConnection.SetDeadline(time.Now().Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					if err := protocol.WriteClientHello(clientConnection, hello); err != nil {
						t.Fatal(err)
					}
					response, err := protocol.ReadServerHello(clientConnection)
					clientConnection.Close()
					<-finished
					if err != nil || protocol.VerifyServerHello(response, session.secret[:]) != nil || response.RequestNonce != hello.Nonce {
						t.Fatalf("join response = %+v, %v", response, err)
					}
					if wantAccepted && response.Result != protocol.ServerLaneAccepted ||
						!wantAccepted && (response.Result != protocol.ServerRejected || response.ErrorCode != protocol.ErrorStaleGeneration) {
						t.Fatalf("unexpected join response: %+v", response)
					}
				}
				cancel()
				waitSessionCondition(t, func() bool {
					session.mu.Lock()
					defer session.mu.Unlock()
					return session.reservations == 0
				})
			}
		})
	}
}
