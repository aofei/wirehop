//go:build !linux

package main

import "errors"

// bootSeconds rejects execution outside the Linux network-test environment.
func bootSeconds() (float64, error) {
	return 0, errors.New("the flow event collector requires Linux CLOCK_BOOTTIME")
}
