//go:build !linux

package socketopts

import "syscall"

// markSupported reports whether the platform supports SO_MARK.
func markSupported() bool {
	return false
}

// setMark rejects marks on platforms without SO_MARK.
func setMark(syscall.RawConn, uint32) error {
	return ErrUnsupportedMark
}
