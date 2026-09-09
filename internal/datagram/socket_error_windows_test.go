package datagram

import (
	"net"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsUDPRecovery(t *testing.T) {
	for _, errno := range []windows.Errno{
		windows.WSAECONNREFUSED, windows.WSAECONNRESET, windows.WSAENETRESET,
		windows.WSAEHOSTUNREACH, windows.WSAENETUNREACH, windows.WSAEMSGSIZE,
		windows.WSAENOBUFS, windows.WSAEACCES, windows.WSAENETDOWN, windows.WSAEHOSTDOWN,
		windows.WSAETIMEDOUT, windows.ERROR_PORT_UNREACHABLE, windows.ERROR_HOST_UNREACHABLE,
		windows.ERROR_NETWORK_UNREACHABLE, windows.ERROR_PROTOCOL_UNREACHABLE,
		windows.ERROR_NETWORK_ACCESS_DENIED, windows.ERROR_NETNAME_DELETED,
		windows.ERROR_NOT_ENOUGH_MEMORY, windows.ERROR_OUTOFMEMORY,
	} {
		err := &net.OpError{Op: "write", Net: "udp", Err: errno}
		if !isSoftNetworkError(err) {
			t.Errorf("UDP error %d ended a reusable socket", errno)
		}
	}
	for _, errno := range []windows.Errno{windows.WSAENOTSOCK, windows.WSAEINVAL, windows.ERROR_INVALID_HANDLE} {
		if isSoftNetworkError(errno) {
			t.Errorf("invalid socket error %d was ignored", errno)
		}
	}
}
