//go:build linux

package socketopts

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// markSupported reports whether the platform supports SO_MARK.
func markSupported() bool {
	return true
}

// setMark applies SO_MARK to one not-yet-connected socket.
func setMark(raw syscall.RawConn, mark uint32) error {
	var operationError error
	if err := raw.Control(func(fileDescriptor uintptr) {
		operationError = unix.SetsockoptInt(int(fileDescriptor), unix.SOL_SOCKET, unix.SO_MARK, int(mark))
	}); err != nil {
		return err
	}
	return operationError
}
