//go:build !linux

package carrier

import "net"

// configureTCPWriteQueue preserves the native write-queue policy on other platforms.
func configureTCPWriteQueue(*net.TCPConn) error {
	return nil
}
