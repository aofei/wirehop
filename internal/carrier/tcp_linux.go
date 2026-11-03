//go:build linux

package carrier

import (
	"net"

	"golang.org/x/sys/unix"
)

// tcpNotSentLowat limits kernel unsent admission independently from socket send-buffer autotuning.
const tcpNotSentLowat = 16 * 1024

// configureTCPWriteQueue requests the carrier's unsent admission threshold on one Linux TCP socket.
func configureTCPWriteQueue(conn *net.TCPConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var operationError error
	if err := raw.Control(func(descriptor uintptr) {
		operationError = unix.SetsockoptInt(int(descriptor), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, tcpNotSentLowat)
	}); err != nil {
		return err
	}
	// Some Linux socket types do not expose this optional queue control.
	if operationError == unix.ENOPROTOOPT || operationError == unix.EOPNOTSUPP {
		return nil
	}
	return operationError
}
