// Package netsetup defines network preparation deadlines and recoverable DNS retries.
package netsetup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"time"

	"github.com/aofei/backoff"
	"github.com/aofei/wirehop/internal/target"
)

const (
	// ResolveTimeout allows the system resolver to try alternate nameservers before a lookup is abandoned.
	ResolveTimeout = 30 * time.Second
	// DialTimeout bounds DNS resolution and TCP connection establishment, independently of protocol handshakes.
	DialTimeout = 30 * time.Second
)

// RetryDNS retries DNS failures, including missing records, with full jitter from a 1-second to a 5-second ceiling.
// Each operation must impose its own lookup deadline without shortening the lifetime of a successful resource.
func RetryDNS(ctx context.Context, logger *slog.Logger, operation func() error) error {
	notice := NewRetryNotice(logger, "DNS resolution")
	var lastErr error
	for range backoff.Attempts(ctx, math.MaxInt, time.Second, 5*time.Second) {
		err := operation()
		if err == nil {
			notice.Recovered()
			return nil
		}
		if lastErr == nil || !errors.Is(err, context.DeadlineExceeded) {
			lastErr = err
		}
		if ctx.Err() != nil {
			break
		}
		if _, dnsFailure := errors.AsType[*net.DNSError](err); !dnsFailure &&
			!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, target.ErrNoAddresses) {
			return err
		}
		notice.Failed("error", lastErr)
	}
	if lastErr == nil {
		return ctx.Err()
	}
	if errors.Is(lastErr, ctx.Err()) {
		return lastErr
	}
	return fmt.Errorf("%w: %w", ctx.Err(), lastErr)
}
