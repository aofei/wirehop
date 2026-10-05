package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/auth"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
)

type sessionTimingClock struct {
	now atomic.Uint64
}

func (c *sessionTimingClock) NowMicros() uint64 {
	return c.now.Load()
}

func TestServerCreateSessionCapacity(t *testing.T) {
	for _, name := range []string{"EvictOldestDetached", "ProtectReservedJoin", "ProtectActiveLanes"} {
		t.Run(name, func(t *testing.T) {
			target, stop := startSessionEchoTarget(t)
			defer stop()
			instance := newSessionTestServer(t, []byte("test-token"), target, time.Hour)
			instance.config.MaxSessions = 2
			clock := &sessionTimingClock{}
			instance.config.Clock = clock
			var sessions []*serverSession
			for index := range 2 {
				clock.now.Store(uint64(index+1) * 1_000_000)
				session, err := instance.createSession(t.Context(), t.Context(), target)
				if err != nil {
					t.Fatal(err)
				}
				defer session.close()
				session.mu.Lock()
				session.confirmed = true
				if name == "ProtectActiveLanes" {
					session.lanes[protocol.LaneID{1}] = sessionLane{generation: 1, cancel: func() {}}
				}
				session.startDetachTimerLocked()
				session.mu.Unlock()
				sessions = append(sessions, session)
			}
			if name == "ProtectReservedJoin" {
				for _, session := range sessions {
					if err := session.reserveLane(protocol.LaneID{1}, 1, protocol.PathGroupID{1}); err != nil {
						t.Fatal(err)
					}
				}
			}
			session, err := instance.createSession(t.Context(), t.Context(), target)
			if session != nil {
				defer session.close()
			}
			if name != "EvictOldestDetached" {
				if session != nil || !errors.Is(err, ErrSessionLimit) || sessions[0].ctx.Err() != nil ||
					sessions[1].ctx.Err() != nil {
					t.Fatalf("reserved sessions evicted: new session %v, error %v", session, err)
				}
			} else if err != nil || sessions[0].ctx.Err() == nil || sessions[1].ctx.Err() != nil ||
				instance.findSession(sessions[0].id) != nil || instance.findSession(sessions[1].id) == nil {
				t.Fatalf("oldest detached eviction failed: %v", err)
			}
		})
	}
}

func TestServerSessionReserveLaneExpiredGrace(t *testing.T) {
	target, stop := startSessionEchoTarget(t)
	defer stop()
	instance := newSessionTestServer(t, []byte("test-token"), target, 2*time.Minute)
	clock := &sessionTimingClock{}
	instance.config.Clock = clock
	session, err := instance.createSession(t.Context(), t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	session.mu.Lock()
	session.confirmed = true
	session.startDetachTimerLocked()
	session.mu.Unlock()
	clock.now.Store(119_000_000)
	if err := session.reserveLane(protocol.LaneID{1}, 1, protocol.PathGroupID{1}); err != nil {
		t.Fatalf("join after 119 seconds = %v", err)
	}
	session.rejectReservedLane()
	clock.now.Store(240_000_000)
	if err := session.reserveLane(protocol.LaneID{1}, 2, protocol.PathGroupID{1}); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("join after protocol-clock grace elapsed = %v", err)
	}
}

func TestServerServeConnectionTLSPhaseBudgets(t *testing.T) {
	temporary := httptest.NewTLSServer(http.NotFoundHandler())
	defer temporary.Close()
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	token := []byte("test-token")
	endpoint := target.MustParse("127.0.0.1:51820")
	instance := newSessionTestServer(t, token, endpoint, time.Minute)
	instance.config.WallClock = func() time.Time { return time.Unix(2000, 0) }
	synctest.Test(t, func(t *testing.T) {
		left, right := net.Pipe()
		server := tls.Server(left, temporary.TLS.Clone())
		client := tls.Client(right, &tls.Config{RootCAs: roots, ServerName: "example.com"})
		defer client.Close()
		result := make(chan error, 1)
		go func() { result <- instance.serveConnection(t.Context(), server, func() {}) }()
		synctest.Wait()
		synctest.Sleep(600 * time.Millisecond)
		if err := client.HandshakeContext(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(600 * time.Millisecond)
		hello := protocol.ClientHello{
			Mode: protocol.HelloCreate, UnixSeconds: 3000, MonotonicMicros: 1,
			Nonce: protocol.Nonce{1}, LaneID: protocol.LaneID{1}, Generation: 1,
			PathGroupID: protocol.PathGroupID{1}, Target: endpoint,
		}
		if err := protocol.SignClientHello(&hello, token); err != nil {
			t.Fatal(err)
		}
		if err := protocol.WriteClientHello(client, hello); err != nil {
			t.Fatalf("request did not receive a fresh budget after TLS: %v", err)
		}
		if _, err := protocol.ReadServerHello(client); err != nil {
			t.Fatal(err)
		}
		if err := <-result; !errors.Is(err, auth.ErrTimestampOutsideWindow) {
			t.Fatalf("request after independent TLS and header delays = %v", err)
		}
	})
}
