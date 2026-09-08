package netsetup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/target"
)

func TestRetryDNS(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "Deadline", err: context.DeadlineExceeded},
		{name: "TemporaryDNS", err: &net.DNSError{Err: "temporary", IsTemporary: true}},
		{name: "OtherDNS", err: &net.DNSError{Err: "server failure"}},
		{name: "MissingDNS", err: &net.DNSError{Err: "no such host", IsNotFound: true}},
		{name: "NoAddresses", err: target.ErrNoAddresses},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				calls := 0
				err := RetryDNS(context.Background(), nil, func() error {
					calls++
					if calls <= 3 {
						return fmt.Errorf("resolve endpoint: %w", test.err)
					}
					return nil
				})
				if err != nil || calls != 4 {
					t.Fatalf("RetryDNS() = %v after %d calls, want nil after 4 calls", err, calls)
				}
				if elapsed := time.Since(started); elapsed >= 7*time.Second {
					t.Fatalf("RetryDNS() took %v, want less than 7s", elapsed)
				}
			})
		})
	}

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "Permission", err: os.ErrPermission},
		{name: "CanceledOperation", err: context.Canceled},
		{name: "NonDNSFailure", err: errors.New("no usable target addresses")},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				calls := 0
				err := RetryDNS(context.Background(), nil, func() error {
					calls++
					return test.err
				})
				if !errors.Is(err, test.err) || calls != 1 || time.Since(started) != 0 {
					t.Fatalf("RetryDNS() = %v after %d calls in %v", err, calls, time.Since(started))
				}
			})
		})
	}

	t.Run("TimeoutPreservesLastDNSFailure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), ResolveTimeout)
			defer cancel()
			resolverError := &net.DNSError{Err: "server failure", Name: "relay.example.com"}
			calls := 0
			err := RetryDNS(ctx, nil, func() error {
				calls++
				if calls == 1 {
					return resolverError
				}
				<-ctx.Done()
				return context.DeadlineExceeded
			})
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, resolverError) || calls != 2 {
				t.Fatalf("RetryDNS() = %v after %d calls, want timeout preserving DNS error after 2 calls", err, calls)
			}
			if elapsed := time.Since(started); elapsed != ResolveTimeout {
				t.Fatalf("RetryDNS() took %v, want %v", elapsed, ResolveTimeout)
			}
		})
	})

	t.Run("BackoffCeilings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			previous := started
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			err := RetryDNS(ctx, nil, func() error {
				if calls > 0 {
					ceiling := min(time.Second<<min(calls-1, 3), 5*time.Second)
					if delay := time.Since(previous); delay < 0 || delay >= ceiling {
						t.Fatalf("retry %d delay = %v, want [0, %v)", calls, delay, ceiling)
					}
				}
				previous = time.Now()
				calls++
				return &net.DNSError{Err: "no such host", IsNotFound: true}
			})
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != time.Minute || calls < 5 {
				t.Fatalf("RetryDNS() = %v after %d calls in %v", err, calls, time.Since(started))
			}
		})
	})

	t.Run("ExpiredDeadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		err := RetryDNS(ctx, nil, func() error {
			t.Fatal("operation started after deadline")
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RetryDNS() = %v, want deadline exceeded", err)
		}
	})

	t.Run("LookupExhaustsParentDeadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := RetryDNS(ctx, nil, func() error {
				<-ctx.Done()
				return ctx.Err()
			})
			if err != context.DeadlineExceeded {
				t.Fatalf("RetryDNS() = %v, want the unduplicated context error", err)
			}
		})
	})

	t.Run("CanceledBeforeAttempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := RetryDNS(ctx, nil, func() error {
			t.Fatal("operation started after cancellation")
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RetryDNS() = %v, want context canceled", err)
		}
	})

	t.Run("CancellationDuringBackoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var result error
			go func() {
				result = RetryDNS(ctx, nil, func() error {
					return &net.DNSError{Err: "no such host", IsNotFound: true}
				})
			}()
			synctest.Wait()
			cancel()
			synctest.Wait()
			if !errors.Is(result, context.Canceled) {
				t.Fatalf("RetryDNS() = %v, want context canceled", result)
			}
		})
	})

	t.Run("ParentDeadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := RetryDNS(ctx, nil, func() error {
				return &net.DNSError{Err: "no such host", IsNotFound: true}
			})
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != time.Second {
				t.Fatalf("RetryDNS() = %v after %v, want parent deadline after 1s", err, time.Since(started))
			}
		})
	})
}
