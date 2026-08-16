package client

import (
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wsheader"
)

func TestAuthenticationClock(t *testing.T) {
	now := time.Unix(1_000, 0)
	clock := newAuthenticationClock(func() time.Time { return now })
	if got := clock.Unix(); got != 1_000 {
		t.Fatalf("Unix() = %d, want 1000", got)
	}
	clock.Observe(5_000)
	if got := clock.Unix(); got != 5_000 {
		t.Fatalf("Unix() after Observe() = %d, want 5000", got)
	}
	now = now.Add(2500 * time.Millisecond)
	if got := clock.Unix(); got != 5_002 {
		t.Fatalf("Unix() after elapsed time = %d, want 5002", got)
	}
	clock.Observe(9_000)
	if got := clock.Unix(); got != 9_000 {
		t.Fatalf("Unix() after replacement sample = %d, want 9000", got)
	}
	now = time.Unix(500, 0)
	if got := clock.Unix(); got != 9_000 {
		t.Fatalf("Unix() after backward wall-clock adjustment = %d, want 9000", got)
	}
	clock.Observe(math.MaxInt64)
	now = now.Add(time.Second)
	if got := clock.Unix(); got != math.MaxInt64 {
		t.Fatalf("Unix() after overflow = %d, want %d", got, int64(math.MaxInt64))
	}
}

func TestAddUnixElapsed(t *testing.T) {
	for _, tt := range []struct {
		name    string
		base    int64
		elapsed uint64
		want    int64
	}{
		{name: "NoElapsed", base: -1, want: -1},
		{name: "CrossZero", base: -1, elapsed: 1},
		{name: "MinimumToZero", base: math.MinInt64, elapsed: uint64(math.MaxInt64) + 1},
		{name: "FullRange", base: math.MinInt64, elapsed: math.MaxUint64, want: math.MaxInt64},
		{name: "PositiveLimit", base: math.MaxInt64 - 1, elapsed: 1, want: math.MaxInt64},
		{name: "PositiveOverflow", base: math.MaxInt64, elapsed: 1, want: math.MaxInt64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := addUnixElapsed(tt.base, tt.elapsed); got != tt.want {
				t.Fatalf("addUnixElapsed(%d, %d) = %d, want %d", tt.base, tt.elapsed, got, tt.want)
			}
		})
	}
}

func TestAuthenticatedWebSocketRejection(t *testing.T) {
	key := []byte("test key")
	for _, tt := range []struct {
		name              string
		signingKey        []byte
		responseNonce     protocol.Nonce
		mutate            func(*protocol.ServerHello)
		wantAuthenticated bool
		wantError         error
		wantUnix          int64
	}{
		{name: "Valid", signingKey: key, responseNonce: protocol.Nonce{1}, wantAuthenticated: true, wantUnix: 5_000},
		{name: "WrongKey", signingKey: []byte("wrong key"), responseNonce: protocol.Nonce{1}, wantUnix: 1_000},
		{name: "WrongNonce", signingKey: key, responseNonce: protocol.Nonce{2}, wantAuthenticated: true,
			wantError: ErrUnexpectedServerResponse, wantUnix: 1_000},
		{name: "TamperedTime", signingKey: key, responseNonce: protocol.Nonce{1},
			mutate: func(value *protocol.ServerHello) { value.ServerUnixSeconds++ }, wantUnix: 1_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(1_000, 0)
			clock := newAuthenticationClock(func() time.Time { return now })
			attempt := creationAttempt{nonce: protocol.Nonce{1}, authenticationClock: clock}
			rejection := protocol.ServerHello{
				Result: protocol.ServerRejected, RequestNonce: tt.responseNonce, ServerUnixSeconds: 5_000,
				ErrorCode: protocol.ErrorClockSkew, ErrorClass: protocol.ErrorRetryable,
				ErrorScope: protocol.ErrorScopeLane, Diagnostic: "request timestamp rejected",
			}
			if err := protocol.SignServerHello(&rejection, tt.signingKey); err != nil {
				t.Fatal(err)
			}
			if tt.mutate != nil {
				tt.mutate(&rejection)
			}
			headers := make(http.Header)
			if err := wsheader.SetRejection(headers, rejection); err != nil {
				t.Fatal(err)
			}
			got := authenticatedWebSocketRejection(
				&http.Response{StatusCode: http.StatusUnauthorized, Header: headers}, attempt, key, nil,
			)
			if (got != nil) != tt.wantAuthenticated {
				t.Fatalf("authenticatedWebSocketRejection() = %v, want authenticated %t", got, tt.wantAuthenticated)
			}
			if tt.wantError != nil && !errors.Is(got, tt.wantError) {
				t.Fatalf("authenticatedWebSocketRejection() error = %v, want %v", got, tt.wantError)
			}
			if tt.wantAuthenticated && tt.wantError == nil {
				if _, ok := errors.AsType[*RejectionError](got); !ok {
					t.Fatalf("authenticatedWebSocketRejection() error = %v, want RejectionError", got)
				}
			}
			if got := clock.Unix(); got != tt.wantUnix {
				t.Fatalf("authentication clock = %d, want %d", got, tt.wantUnix)
			}
		})
	}
}

func TestAuthenticatedWebSocketSessionGoneFallback(t *testing.T) {
	longTermKey := []byte("long-term key")
	sessionKey := []byte("session key")
	now := time.Unix(1_000, 0)
	clock := newAuthenticationClock(func() time.Time { return now })
	attempt := creationAttempt{nonce: protocol.Nonce{1}, authenticationClock: clock}
	rejection := protocol.ServerHello{
		Result: protocol.ServerRejected, RequestNonce: attempt.nonce, ServerUnixSeconds: 5_000,
		ErrorCode: protocol.ErrorSessionNotFound, ErrorClass: protocol.ErrorSessionGone,
		ErrorScope: protocol.ErrorScopeSession, Diagnostic: "session is not available",
	}
	if err := protocol.SignServerHello(&rejection, longTermKey); err != nil {
		t.Fatal(err)
	}
	headers := make(http.Header)
	if err := wsheader.SetRejection(headers, rejection); err != nil {
		t.Fatal(err)
	}
	got := authenticatedWebSocketRejection(
		&http.Response{StatusCode: http.StatusGone, Header: headers},
		attempt, sessionKey, longTermKey,
	)
	if !errors.Is(got, ErrSessionGone) {
		t.Fatalf("authenticatedWebSocketRejection() = %v, want session gone", got)
	}
	if got := clock.Unix(); got != 5_000 {
		t.Fatalf("authentication clock = %d, want 5000", got)
	}

	clock = newAuthenticationClock(func() time.Time { return now })
	attempt.authenticationClock = clock
	rejection.ErrorCode = protocol.ErrorAuthentication
	rejection.ErrorClass = protocol.ErrorSessionRejected
	rejection.Diagnostic = "authentication failed"
	if err := protocol.SignServerHello(&rejection, longTermKey); err != nil {
		t.Fatal(err)
	}
	headers = make(http.Header)
	if err := wsheader.SetRejection(headers, rejection); err != nil {
		t.Fatal(err)
	}
	got = authenticatedWebSocketRejection(
		&http.Response{StatusCode: http.StatusUnauthorized, Header: headers},
		attempt, sessionKey, longTermKey,
	)
	if got != nil {
		t.Fatalf("unrelated long-term rejection = %v, want untrusted", got)
	}
	if got := clock.Unix(); got != 1_000 {
		t.Fatalf("authentication clock = %d after untrusted rejection, want 1000", got)
	}
}
