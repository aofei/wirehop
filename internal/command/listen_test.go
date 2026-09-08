package command

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func TestListenWithRetry(t *testing.T) {
	t.Run("MissingRecordsRecover", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			deadline := time.Now().Add(listenerStartupTimeout)
			got, err := listenWithRetry(context.Background(), "relay.example.com:443", deadline,
				func(ctx context.Context, network, address string) (net.Listener, error) {
					if network != "tcp" || address != "relay.example.com:443" {
						t.Fatalf("listen(%q, %q), want TCP with the logical listener address", network, address)
					}
					if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
						t.Fatalf("lookup deadline = %v, %t, want %v", got, ok, deadline)
					}
					calls++
					if calls == 1 {
						return nil, &net.OpError{Op: "listen", Net: network,
							Err: &net.DNSError{Err: "no such host", Name: "relay.example.com", IsNotFound: true}}
					}
					return listener, nil
				})
			if err != nil || got != listener || calls != 2 {
				t.Fatalf("listenWithRetry() = %v, %v after %d calls", got, err, calls)
			}
		})
	})

	t.Run("SharedPreparationDeadline", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			deadline := started.Add(listenerStartupTimeout)
			got, err := listenWithRetry(context.Background(), "first.example.com:443", deadline,
				func(ctx context.Context, _, _ string) (net.Listener, error) {
					time.Sleep(20 * time.Second)
					return listener, nil
				})
			if err != nil || got != listener {
				t.Fatalf("first listenWithRetry() = %v, %v", got, err)
			}
			calls := 0
			got, err = listenWithRetry(context.Background(), "second.example.com:443", deadline,
				func(ctx context.Context, _, _ string) (net.Listener, error) {
					attemptDeadline, ok := ctx.Deadline()
					if !ok || !attemptDeadline.Equal(deadline) || time.Until(attemptDeadline) != 10*time.Second {
						t.Fatalf("invalid lookup deadline: %v, %t", attemptDeadline, ok)
					}
					calls++
					<-ctx.Done()
					return nil, ctx.Err()
				})
			if got != nil || !errors.Is(err, context.DeadlineExceeded) || calls == 0 || time.Since(started) != 30*time.Second {
				t.Fatalf("second listenWithRetry() = %v, %v after %d calls, total %v", got, err, calls, time.Since(started))
			}
		})
	})

	t.Run("BindFailure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			calls := 0
			listener, err := listenWithRetry(context.Background(), "127.0.0.1:443", started.Add(listenerStartupTimeout),
				func(context.Context, string, string) (net.Listener, error) {
					calls++
					return nil, &net.OpError{Op: "listen", Net: "tcp", Err: os.ErrPermission}
				})
			if listener != nil || !errors.Is(err, os.ErrPermission) || calls != 1 || time.Since(started) != 0 {
				t.Fatalf("listenWithRetry() = %v, %v after %d calls in %v", listener, err, calls, time.Since(started))
			}
		})
	})

	t.Run("CancelLookup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listener, err := listenWithRetry(ctx, "relay.example.com:443", time.Now().Add(listenerStartupTimeout),
				func(ctx context.Context, _, _ string) (net.Listener, error) {
					cancel()
					<-ctx.Done()
					return nil, ctx.Err()
				})
			if listener != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("listenWithRetry() = %v, %v, want nil, context canceled", listener, err)
			}
		})
	})

	t.Run("OutlivesStartupDeadline", func(t *testing.T) {
		config := net.ListenConfig{}
		listener, err := listenWithRetry(context.Background(), "127.0.0.1:0", time.Now().Add(100*time.Millisecond), config.Listen)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		time.Sleep(150 * time.Millisecond)
		connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		connection.Close()
		accepted, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		accepted.Close()
	})
}
