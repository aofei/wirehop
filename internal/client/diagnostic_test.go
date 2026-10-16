package client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/big"
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

func TestClientCreateCandidateDiagnostics(t *testing.T) {
	t.Run("HTTPResponse", func(t *testing.T) {
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
						laneID: protocol.LaneID(1), pathGroupID: protocol.PathGroupID(1),
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
	})

	t.Run("TLSCertificate", func(t *testing.T) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			DNSNames: []string{"private diagnostic\x1b[31m\n.example"},
		}
		encoded, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		for _, scheme := range []string{"TLS", "WSS"} {
			t.Run(scheme, func(t *testing.T) {
				remote := httptest.NewUnstartedServer(http.NotFoundHandler())
				remote.Config.ErrorLog = log.New(io.Discard, "", 0)
				remote.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{encoded}, PrivateKey: key}}}
				remote.StartTLS()
				t.Cleanup(remote.Close)
				roots := x509.NewCertPool()
				roots.AddCert(remote.Certificate())
				instance := &Client{
					config: Config{
						Dialer: &net.Dialer{}, Clock: monotime.New(), HandshakeTimeout: time.Second,
						SessionAttemptTimeout: 2 * time.Second, Token: []byte("test-token"),
						Target:    target.MustParse("127.0.0.1:51820"),
						TLSConfig: &tls.Config{RootCAs: roots, ServerName: "relay.example"},
					},
					lanes: []clientLane{{
						spec:   testLaneSpec(t, strings.ToLower(scheme)+strings.TrimPrefix(remote.URL, "https")),
						laneID: 1, pathGroupID: 1, authenticationClock: newAuthenticationClock(time.Now),
					}},
				}
				candidate := instance.createCandidate(t.Context(), 0)
				if candidate.err == nil {
					candidate.accepted.connection.Close()
					t.Fatal("mismatched certificate was accepted")
				}
				if _, ok := errors.AsType[x509.HostnameError](candidate.err); !ok ||
					classifyLaneFailure(candidate.err) != failureCloseLane {
					t.Fatalf("certificate error lost its type or terminal classification: %q", candidate.err.Error())
				}
				if !strings.Contains(candidate.err.Error(), "TLS certificate verification failed") {
					t.Fatalf("certificate error lost its local diagnostic category: %q", candidate.err.Error())
				}
				var output bytes.Buffer
				instance.config.Logger = slog.New(slog.NewTextHandler(&output, nil))
				instance.logDisabledLane(0, candidate.err)
				for _, diagnostic := range []string{
					candidate.err.Error(), instance.configuredLaneError(0, candidate.err).Error(), output.String(),
				} {
					if strings.Contains(diagnostic, "private diagnostic") || strings.ContainsRune(diagnostic, '\x1b') {
						t.Fatalf("TLS error exposed certificate text: %q", diagnostic)
					}
				}
			})
		}
	})
}

func TestHandshakeError(t *testing.T) {
	for _, name := range []string{"TLS", "WebSocket"} {
		t.Run(name, func(t *testing.T) {
			for _, cause := range []error{
				context.Canceled, context.DeadlineExceeded, syscall.EACCES,
				&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
				&tls.CertificateVerificationError{Err: errors.New("private diagnostic")},
				tls.RecordHeaderError{Msg: "private diagnostic"}, errors.New("private diagnostic"),
			} {
				err := &handshakeError{carrier: name, cause: cause}
				if !errors.Is(err, cause) || classifyLaneFailure(err) != classifyLaneFailure(cause) {
					t.Fatalf("redaction changed failure classification for %T", cause)
				}
				if strings.Contains(err.Error(), "private") {
					t.Fatalf("handshake error exposed peer text: %v", err)
				}
			}
		})
	}
}
