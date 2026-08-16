package protocol

// ErrorClass determines client retry and process-lifecycle behavior.
type ErrorClass uint8

const (
	// ErrorRetryable identifies a temporary transport or capacity failure.
	ErrorRetryable ErrorClass = iota + 1
	// ErrorLaneRejected identifies a terminal rejection scoped to one lane candidate.
	ErrorLaneRejected
	// ErrorSessionGone identifies a session that no longer exists on the server.
	ErrorSessionGone
	// ErrorSessionRejected identifies a terminal rejection of the authenticated session.
	ErrorSessionRejected
)

// Valid reports whether the error class is defined by this protocol version.
func (c ErrorClass) Valid() bool {
	return c >= ErrorRetryable && c <= ErrorSessionRejected
}

// ErrorCode identifies one machine-readable protocol, policy, or capacity failure.
type ErrorCode uint16

const (
	// ErrorMalformed identifies malformed handshake or frame input.
	ErrorMalformed ErrorCode = iota + 1
	// ErrorUnsupportedVersion identifies an incompatible wire protocol version.
	ErrorUnsupportedVersion
	// ErrorAuthentication identifies invalid authentication material.
	ErrorAuthentication
	// ErrorReplay identifies a reused authenticated handshake nonce.
	ErrorReplay
	// ErrorTargetDenied identifies a target outside server policy.
	ErrorTargetDenied
	// ErrorSessionNotFound identifies an unknown or expired session.
	ErrorSessionNotFound
	// ErrorStaleGeneration identifies a non-increasing lane connection generation.
	ErrorStaleGeneration
	// ErrorLaneLimit identifies a per-session lane limit.
	ErrorLaneLimit
	// ErrorSessionLimit identifies a server session limit.
	ErrorSessionLimit
	// ErrorProtocolViolation identifies invalid in-session behavior.
	ErrorProtocolViolation
	// ErrorUnavailable identifies a temporary server or target failure.
	ErrorUnavailable
	// ErrorRateLimited identifies a temporary admission or reconnect rate limit.
	ErrorRateLimited
	// ErrorInternal identifies an unexpected server failure.
	ErrorInternal
	// ErrorClockSkew identifies an authenticated timestamp outside the server's acceptance window.
	ErrorClockSkew
)

// Valid reports whether the error code is defined by this protocol version.
func (c ErrorCode) Valid() bool {
	return c >= ErrorMalformed && c <= ErrorClockSkew
}

// ErrorScope identifies the state affected by a protocol error.
type ErrorScope uint8

const (
	// ErrorScopeLane limits an error to one lane generation.
	ErrorScopeLane ErrorScope = iota + 1
	// ErrorScopeSession applies an error to the complete session.
	ErrorScopeSession
)

// Valid reports whether the error scope is defined by this protocol version.
func (s ErrorScope) Valid() bool {
	return s == ErrorScopeLane || s == ErrorScopeSession
}

// validErrorDisposition reports whether an error class can apply to scope.
func validErrorDisposition(class ErrorClass, scope ErrorScope) bool {
	if !class.Valid() || !scope.Valid() {
		return false
	}
	switch class {
	case ErrorLaneRejected:
		return scope == ErrorScopeLane
	case ErrorSessionGone, ErrorSessionRejected:
		return scope == ErrorScopeSession
	case ErrorRetryable:
		return true
	default:
		return false
	}
}
