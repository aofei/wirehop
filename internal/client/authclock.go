package client

import (
	"math"
	"sync"
	"time"
)

// authenticationClock estimates one lane endpoint's wall time for admission timestamps.
type authenticationClock struct {
	mu         sync.Mutex
	now        func() time.Time
	sampledAt  int64
	serverUnix int64
	sampled    bool
}

// newAuthenticationClock returns an unsampled authentication clock.
func newAuthenticationClock(now func() time.Time) *authenticationClock {
	return &authenticationClock{now: now}
}

// Unix returns the estimated server Unix time or local wall time before the first sample.
func (c *authenticationClock) Unix() int64 {
	now := c.now().Unix()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sampled {
		return now
	}
	if now <= c.sampledAt {
		return c.serverUnix
	}
	elapsed := uint64(now) - uint64(c.sampledAt)
	return addUnixElapsed(c.serverUnix, elapsed)
}

// Observe replaces the estimate with one authenticated server timestamp.
func (c *authenticationClock) Observe(serverUnix int64) {
	now := c.now().Unix()
	c.mu.Lock()
	c.sampledAt = now
	c.serverUnix = serverUnix
	c.sampled = true
	c.mu.Unlock()
}

// addUnixElapsed adds an unsigned elapsed interval to a signed Unix time and saturates at the positive limit.
func addUnixElapsed(base int64, elapsed uint64) int64 {
	const signBit = uint64(1) << 63
	ordered := uint64(base) ^ signBit
	if elapsed > math.MaxUint64-ordered {
		return math.MaxInt64
	}
	return int64((ordered + elapsed) ^ signBit)
}
