package datagram

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"
)

// udpReceiveBufferSize absorbs bounded receive bursts without changing system-wide socket limits.
const udpReceiveBufferSize = 4 << 20

// udpNotice reports socket failures without exposing peer addresses or logging every dropped datagram.
type udpNotice struct {
	logger     *slog.Logger
	mu         sync.Mutex
	next       time.Time
	suppressed uint64
}

// report emits the first socket failure and at most one further warning per minute for this endpoint.
func (n *udpNotice) report(operation string, err error) {
	if n.logger == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	if now.Before(n.next) {
		n.suppressed++
		return
	}
	reason := "socket error"
	var errno syscall.Errno
	var networkError net.Error
	switch {
	case errors.Is(err, syscall.EMSGSIZE):
		reason = "datagram too large, check WireGuard MTU and UDP path MTU"
	case errors.As(err, &errno):
		reason = errno.Error()
	case errors.As(err, &networkError) && networkError.Timeout():
		reason = "socket deadline exceeded"
	}
	n.logger.Warn("UDP socket failure", "operation", operation, "reason", reason, "suppressed", n.suppressed)
	n.next = now.Add(time.Minute)
	n.suppressed = 0
}

// configureReceiveBuffer requests bounded burst capacity, retaining the OS default if its limit rejects the request.
func configureReceiveBuffer(conn *net.UDPConn, notice *udpNotice) {
	if err := conn.SetReadBuffer(udpReceiveBufferSize); err != nil {
		notice.report("configure receive buffer", err)
	}
}
