//go:build !linux

package datagram

import "net"

// newUDPBatchConn reports that the current platform uses scalar UDP I/O.
func newUDPBatchConn(*net.UDPConn) udpBatchConn {
	return nil
}
