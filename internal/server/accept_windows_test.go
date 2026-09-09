package server

import (
	"errors"
	"net"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsAcceptRecovery(t *testing.T) {
	for _, errno := range []windows.Errno{windows.WSAEMFILE, windows.WSAENOBUFS, windows.ERROR_NOT_ENOUGH_MEMORY} {
		err := &net.OpError{Op: "accept", Net: "tcp", Err: errno}
		if !temporaryAcceptError(err) {
			t.Errorf("socket resource error %d stopped the listener", errno)
		}
		listener := (&Server{}).WebSocketListener(&failingAcceptListener{results: []error{err}})
		_, acceptErr := listener.Accept()
		if temporary, ok := errors.AsType[net.Error](acceptErr); !ok || !temporary.Temporary() {
			t.Errorf("HTTP would not retry socket resource error %d", errno)
		}
	}
}
