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

// handshakeError preserves failure classification without formatting untrusted certificate or HTTP parser diagnostics.
type handshakeError struct {
	carrier string
	cause   error
}

// Error describes the failure using local transport categories instead of response text.
func (e *handshakeError) Error() string {
	if proxyError, ok := errors.AsType[*proxyConnectError](e.cause); ok {
		return proxyError.Error()
	}
	reason := "transport or protocol error"
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
	return e.carrier + " handshake failed: " + reason
}

// Unwrap retains the original error for retry, cancellation, and permanent TLS failure classification.
func (e *handshakeError) Unwrap() error { return e.cause }
