//go:build !darwin && !linux

package monotime

import "time"

// Clock converts the platform monotonic clock into process-relative microseconds.
type Clock struct {
	origin time.Time
}

// New returns a monotonic clock whose zero point is the current platform clock reading.
func New() *Clock {
	return &Clock{origin: time.Now()}
}

// NowMicros returns elapsed monotonic microseconds since clock creation.
func (c *Clock) NowMicros() uint64 {
	elapsed := time.Since(c.origin)
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed / time.Microsecond)
}
