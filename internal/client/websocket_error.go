package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/aofei/wirehop/internal/netsetup"
)

// proxyConnectError preserves a rejected tunnel's status without trusting the proxy's response text.
type proxyConnectError struct {
	statusCode int
}

// Error describes the proxy rejection using its numeric HTTP status.
func (e *proxyConnectError) Error() string {
	return fmt.Sprintf("WebSocket proxy CONNECT returned HTTP %d", e.statusCode)
}

// Is classifies permanent proxy rejections with the same policy as WebSocket upgrade failures.
func (e *proxyConnectError) Is(target error) bool {
	return target == ErrLaneRejected && permanentHTTPRejection(e.statusCode)
}

// webSocketHandshakeError preserves failure classification without formatting untrusted HTTP parser diagnostics.
type webSocketHandshakeError struct {
	cause error
}

// Error describes the failure using local transport categories instead of response text.
func (e *webSocketHandshakeError) Error() string {
	if proxyError, ok := errors.AsType[*proxyConnectError](e.cause); ok {
		return proxyError.Error()
	}
	reason := "transport or HTTP response error"
	if errors.Is(e.cause, context.Canceled) {
		reason = "operation canceled"
	} else if networkError, ok := errors.AsType[net.Error](e.cause); ok && networkError.Timeout() {
		reason = "operation timed out"
	} else if _, ok := errors.AsType[*tls.CertificateVerificationError](e.cause); ok {
		reason = "TLS certificate verification failed"
	} else if _, ok := errors.AsType[tls.RecordHeaderError](e.cause); ok {
		reason = "invalid TLS record"
	} else if errno, ok := netsetup.SocketErrno(e.cause); ok {
		reason = errno.Error()
	}
	return "WebSocket handshake failed: " + reason
}

// Unwrap retains the original error for retry, cancellation, and permanent TLS failure classification.
func (e *webSocketHandshakeError) Unwrap() error { return e.cause }
