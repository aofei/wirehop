//go:build linux

package monotime

import (
	"time"

	"golang.org/x/sys/unix"
)

// Clock converts the suspend-aware boot clock into process-relative microseconds.
type Clock struct {
	origin uint64
}

// New returns a monotonic clock whose zero point is the current platform clock reading.
func New() *Clock {
	return &Clock{origin: bootMicros()}
}

// NowMicros returns elapsed monotonic microseconds since clock creation, including system suspend time.
func (c *Clock) NowMicros() uint64 {
	now := bootMicros()
	if now <= c.origin {
		return 0
	}
	return now - c.origin
}

// bootMicros returns Linux CLOCK_BOOTTIME rounded down to whole microseconds.
func bootMicros() uint64 {
	var value unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &value); err != nil {
		panic("read CLOCK_BOOTTIME: " + err.Error())
	}
	return uint64(value.Sec)*uint64(time.Second/time.Microsecond) +
		uint64(value.Nsec)/uint64(time.Microsecond)
}
