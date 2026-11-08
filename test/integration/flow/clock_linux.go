//go:build linux

package main

import "golang.org/x/sys/unix"

// bootSeconds reads the same suspend-aware monotonic clock in every endpoint process.
func bootSeconds() (float64, error) {
	var at unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &at); err != nil {
		return 0, err
	}
	return float64(at.Sec) + float64(at.Nsec)/1e9, nil
}
