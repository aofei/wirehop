package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/target"
)

type failingAcceptListener struct {
	results   []error
	calls     int
	closed    chan struct{}
	closeOnce sync.Once
}

func (l *failingAcceptListener) Accept() (net.Conn, error) {
	l.calls++
	if len(l.results) > 0 {
		err := l.results[0]
		l.results = l.results[1:]
		return nil, err
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *failingAcceptListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (*failingAcceptListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func TestServerServeAcceptRecovery(t *testing.T) {
	for _, tt := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "ProcessDescriptorLimit", err: syscall.EMFILE, retryable: true},
		{name: "SystemDescriptorLimit", err: syscall.ENFILE, retryable: true},
		{name: "SocketBufferPressure", err: syscall.ENOBUFS, retryable: true},
		{name: "KernelMemoryPressure", err: syscall.ENOMEM, retryable: true},
		{name: "IncomingConnectionNetworkFailure", err: syscall.ENETDOWN, retryable: true},
		{name: "ClosedListener", err: net.ErrClosed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				instance := newSessionTestServer(t, []byte("test-token"), target.MustParse("127.0.0.1:51820"), time.Second)
				listener := &failingAcceptListener{
					results: []error{&net.OpError{Op: "accept", Net: "tcp", Err: tt.err}},
					closed:  make(chan struct{}),
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- instance.Serve(ctx, listener) }()
				synctest.Sleep(time.Second)
				if tt.retryable {
					select {
					case err := <-result:
						t.Fatalf("temporary accept failure stopped the server: %v", err)
					default:
					}
					if listener.calls != 2 {
						t.Fatalf("accept calls = %d, want recovery attempt", listener.calls)
					}
					cancel()
					if err := <-result; err != nil {
						t.Fatal(err)
					}
				} else if err := <-result; !errors.Is(err, tt.err) {
					t.Fatalf("terminal accept error = %v, want %v", err, tt.err)
				}
			})
		})
	}
}

func TestServerServeAcceptBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		instance := newSessionTestServer(t, []byte("test-token"), target.MustParse("127.0.0.1:51820"), time.Second)
		listener := &failingAcceptListener{results: make([]error, 1000), closed: make(chan struct{})}
		for index := range listener.results {
			listener.results[index] = &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- instance.Serve(ctx, listener) }()
		synctest.Sleep(10 * time.Second)
		if listener.calls < 10 || listener.calls > 20 {
			t.Fatalf("persistent resource pressure caused %d accept attempts in ten seconds", listener.calls)
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("cancellation did not interrupt the accept backoff")
		}
	})
}

func TestWebSocketListenerAcceptTemporaryError(t *testing.T) {
	instance := newSessionTestServer(t, []byte("test-token"), target.MustParse("127.0.0.1:51820"), time.Second)
	listener := &failingAcceptListener{
		results: []error{&net.OpError{Op: "accept", Net: "tcp", Err: syscall.ENOBUFS}},
		closed:  make(chan struct{}),
	}
	_, err := instance.WebSocketListener(listener).Accept()
	if temporary, ok := errors.AsType[net.Error](err); !ok || !temporary.Temporary() {
		t.Fatalf("HTTP would treat the recoverable accept error as terminal: %v", err)
	}
	if !errors.Is(err, syscall.ENOBUFS) {
		t.Fatalf("accept recovery hid the socket error: %v", err)
	}
}
