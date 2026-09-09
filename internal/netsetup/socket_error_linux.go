package netsetup

import (
	"errors"
	"syscall"
)

// SocketErrno extracts socket errors and normalizes Linux-specific network unavailability.
func SocketErrno(err error) (syscall.Errno, bool) {
	errno, ok := errors.AsType[syscall.Errno](err)
	if errno == syscall.ENONET {
		errno = syscall.ENETDOWN
	}
	return errno, ok
}
