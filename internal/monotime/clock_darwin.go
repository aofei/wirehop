//go:build darwin

package monotime

import (
	"time"

	"golang.org/x/sys/unix"
)

// Clock converts the suspend-aware Darwin monotonic clock into process-relative microseconds.
type Clock struct {
	origin uint64
}

// New returns a monotonic clock whose zero point is the current platform clock reading.
func New() *Clock {
	return &Clock{origin: continuousMicros()}
}

// NowMicros returns elapsed monotonic microseconds since clock creation, including system suspend time.
func (c *Clock) NowMicros() uint64 {
	now := continuousMicros()
	if now <= c.origin {
		return 0
	}
	return now - c.origin
}

// continuousMicros returns Darwin CLOCK_MONOTONIC_RAW rounded down to whole microseconds.
func continuousMicros() uint64 {
	var value unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &value); err != nil {
		panic("read CLOCK_MONOTONIC_RAW: " + err.Error())
	}
	return uint64(value.Sec)*uint64(time.Second/time.Microsecond) +
		uint64(value.Nsec)/uint64(time.Microsecond)
}
