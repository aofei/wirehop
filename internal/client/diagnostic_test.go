package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/target"
	"github.com/coder/websocket"
)

func TestRemoteErrorDiagnostics(t *testing.T) {
	for _, err := range []error{
		&RejectionError{Code: protocol.ErrorAuthentication, Class: protocol.ErrorSessionRejected,
			Scope: protocol.ErrorScopeSession, Diagnostic: "private diagnostic"},
		&relay.RemoteError{Value: protocol.ErrorFrame{Code: protocol.ErrorAuthentication,
			Class: protocol.ErrorSessionRejected, Scope: protocol.ErrorScopeSession, Diagnostic: "private diagnostic"}},
	} {
		if text := fmt.Errorf("runtime failure: %w", err).Error(); strings.Contains(text, "private diagnostic") {
			t.Errorf("runtime error exposed a peer diagnostic: %s", text)
		}
	}
}

func TestLogDisabledLaneRedactsWebSocketClose(t *testing.T) {
	var output bytes.Buffer
	instance := &Client{
		config: Config{Logger: slog.New(slog.NewTextHandler(&output, nil))},
		lanes:  []clientLane{{spec: testLaneSpec(t, "wss://relay.example/_wirehop")}},
	}
	instance.logDisabledLane(0, fmt.Errorf("read carrier: %w", websocket.CloseError{
		Code: websocket.StatusServiceRestart, Reason: "private diagnostic",
	}))
	if text := output.String(); strings.Contains(text, "private diagnostic") || !strings.Contains(text, "close_code=1012") {
		t.Fatalf("WebSocket close diagnostic was not reduced to its status: %s", text)
	}
}

func TestClientCreateCandidateRedactsHTTPResponse(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
	}{
		{name: "InvalidUpgrade", response: "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: private diagnostic\r\n\r\n"},
		{name: "MalformedStatus", response: "HTTP/1.1 private diagnostic\r\n\r\n"},
		{name: "MalformedHeader", response: "HTTP/1.1 502 Bad Gateway\r\nprivate diagnostic\r\n\r\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				connection, stream, err := http.NewResponseController(writer).Hijack()
				if err != nil {
					return
				}
				defer connection.Close()
				fmt.Fprint(stream, tt.response)
				stream.Flush()
			}))
			t.Cleanup(server.Close)
			instance := &Client{
				config: Config{
					Dialer: &net.Dialer{}, Clock: monotime.New(), HandshakeTimeout: time.Second,
					SessionAttemptTimeout: 2 * time.Second, Token: []byte("test-token"),
					Target: target.MustParse("127.0.0.1:51820"),
				},
				lanes: []clientLane{{
					spec:   testLaneSpec(t, "ws"+strings.TrimPrefix(server.URL, "http")),
					laneID: protocol.LaneID{1}, pathGroupID: protocol.PathGroupID{1},
					authenticationClock: newAuthenticationClock(time.Now),
				}},
			}
			candidate := instance.createCandidate(context.Background(), 0)
			if candidate.err == nil {
				candidate.accepted.connection.Close()
				t.Fatal("invalid response was accepted")
			}
			if strings.Contains(candidate.err.Error(), "private") {
				t.Fatalf("HTTP error exposed peer text: %v", candidate.err)
			}
		})
	}
}

func TestWebSocketHandshakeError(t *testing.T) {
	for _, cause := range []error{
		context.Canceled, context.DeadlineExceeded, syscall.EACCES,
		&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
		&tls.CertificateVerificationError{Err: errors.New("private diagnostic")},
		tls.RecordHeaderError{Msg: "private diagnostic"},
	} {
		err := &webSocketHandshakeError{cause: cause}
		if !errors.Is(err, cause) || classifyLaneFailure(err) != classifyLaneFailure(cause) {
			t.Fatalf("redaction changed failure classification for %T", cause)
		}
		if strings.Contains(err.Error(), "private") {
			t.Fatalf("handshake error exposed peer text: %v", err)
		}
	}
}
