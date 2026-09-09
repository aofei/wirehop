package netsetup

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// SocketErrno maps Winsock and completion errors to the socket recovery categories used on other platforms.
func SocketErrno(err error) (syscall.Errno, bool) {
	errno, ok := errors.AsType[syscall.Errno](err)
	if !ok {
		return 0, false
	}
	switch errno {
	case windows.WSAEMFILE:
		return syscall.EMFILE, true
	case windows.ERROR_NOT_ENOUGH_MEMORY, windows.ERROR_OUTOFMEMORY:
		return syscall.ENOMEM, true
	case windows.WSAECONNREFUSED, windows.ERROR_PORT_UNREACHABLE:
		return syscall.ECONNREFUSED, true
	case windows.WSAECONNRESET, windows.WSAENETRESET, windows.ERROR_NETNAME_DELETED:
		return syscall.ECONNRESET, true
	case windows.WSAEHOSTUNREACH, windows.ERROR_HOST_UNREACHABLE:
		return syscall.EHOSTUNREACH, true
	case windows.WSAENETUNREACH, windows.ERROR_NETWORK_UNREACHABLE, windows.ERROR_PROTOCOL_UNREACHABLE:
		return syscall.ENETUNREACH, true
	case windows.WSAEMSGSIZE:
		return syscall.EMSGSIZE, true
	case windows.WSAENOBUFS:
		return syscall.ENOBUFS, true
	case windows.WSAEACCES, windows.ERROR_NETWORK_ACCESS_DENIED:
		return syscall.EACCES, true
	case windows.WSAENETDOWN:
		return syscall.ENETDOWN, true
	case windows.WSAEHOSTDOWN:
		return syscall.EHOSTDOWN, true
	case windows.WSAETIMEDOUT:
		return syscall.ETIMEDOUT, true
	default:
		return errno, true
	}
}
