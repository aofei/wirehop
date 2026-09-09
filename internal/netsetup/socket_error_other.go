//go:build !windows && !linux

package netsetup

import (
	"errors"
	"syscall"
)

// SocketErrno extracts the native socket error for recovery and diagnostics.
func SocketErrno(err error) (syscall.Errno, bool) {
	return errors.AsType[syscall.Errno](err)
}
