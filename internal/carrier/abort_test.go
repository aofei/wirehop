package carrier

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func testTLSAbortWithFullSendBuffer(t *testing.T, useWebSocket bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	accepted := make(chan error, 1)
	peerFinished := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(peerFinished)
		connection, err := AcceptWebSocket(w, r, nil)
		accepted <- err
		if err == nil {
			defer connection.Close()
			<-release
		}
	}))
	server.Config.ConnContext = func(ctx context.Context, connection net.Conn) context.Context {
		if err := connection.(*tls.Conn).NetConn().(*net.TCPConn).SetReadBuffer(4096); err != nil {
			t.Error(err)
		}
		return ctx
	}
	server.StartTLS()
	var raw *net.TCPConn
	var abort, closeOwner func() error
	var abortFinished chan struct{}
	peerObserved := false
	t.Cleanup(func() {
		if raw != nil {
			raw.Close()
		}
		if closeOwner != nil {
			closeOwner()
		}
		close(release)
		server.Close()
		if peerObserved {
			<-peerFinished
		}
		if abortFinished != nil {
			<-abortFinished
		}
	})
	if useWebSocket {
		sockets := make(chan *net.TCPConn, 1)
		transport := server.Client().Transport.(*http.Transport).Clone()
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err == nil {
				sockets <- connection.(*net.TCPConn)
			}
			return connection, err
		}
		defer transport.CloseIdleConnections()
		connection, _, err := DialWebSocket(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
			HTTPClient: &http.Client{Transport: transport},
		})
		if err != nil {
			t.Fatal(err)
		}
		raw = <-sockets
		connection.SetNetworkConnection(raw)
		abort, closeOwner = connection.Abort, connection.Close
	} else {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			defer close(peerFinished)
			peer, err := listener.Accept()
			if err != nil {
				accepted <- err
				return
			}
			defer peer.Close()
			if err := peer.(*net.TCPConn).SetReadBuffer(4096); err != nil {
				accepted <- err
				return
			}
			secure := tls.Server(peer, server.TLS)
			err = secure.HandshakeContext(ctx)
			accepted <- err
			if err == nil {
				<-release
			}
		}()
		connection, err := (&tls.Dialer{Config: server.Client().Transport.(*http.Transport).TLSClientConfig}).
			DialContext(ctx, "tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		secure := connection.(*tls.Conn)
		raw = secure.NetConn().(*net.TCPConn)
		stream := NewStreamConn(secure)
		abort, closeOwner = stream.Abort, stream.Close
	}
	peerError := <-accepted
	peerObserved = true
	if peerError != nil {
		t.Fatal(peerError)
	}
	if err := raw.SetWriteBuffer(4096); err != nil {
		t.Fatal(err)
	}
	// Fill TCP directly so the completed TLS writer does not retain a prior write error.
	// A fresh deadline for each write distinguishes backpressure from time spent in successful writes.
	writeUntilBlocked := func(buffer []byte) int {
		written := 0
		for {
			if err := ctx.Err(); err != nil {
				t.Fatalf("fill TCP send buffer: %v", err)
			}
			if err := raw.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			n, err := raw.Write(buffer)
			written += n
			if err != nil {
				assertNetworkTimeout(t, err)
				return written
			}
		}
	}
	if writeUntilBlocked(make([]byte, 64*1024)) == 0 {
		t.Fatal("TCP send buffer accepted no bytes")
	}
	writeUntilBlocked([]byte{0})
	if err := raw.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	abortFinished = make(chan struct{})
	go func() {
		defer close(abortFinished)
		result <- abort()
	}()
	select {
	case <-ctx.Done():
		t.Fatalf("abort did not complete: %v", ctx.Err())
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("abort waited for TLS close_notify with a full TCP send buffer")
	}
}
