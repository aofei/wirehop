//go:build linux

package monotime

import (
	"syscall"
	"time"
	"unsafe"

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
	// CLOCK_BOOTTIME cannot block. The standard syscall entry point keeps value live without allowing a stack split.
	_, _, err := syscall.RawSyscall(unix.SYS_CLOCK_GETTIME, unix.CLOCK_BOOTTIME, uintptr(unsafe.Pointer(&value)), 0)
	if err != 0 {
		panic("read CLOCK_BOOTTIME: " + err.Error())
	}
	return uint64(value.Sec)*uint64(time.Second/time.Microsecond) +
		uint64(value.Nsec)/uint64(time.Microsecond)
}
