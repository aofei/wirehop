package wsheader

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestJoinRoundTrip(t *testing.T) {
	secret := protocol.SessionSecret{9}
	join := Join{
		Method: "GET", Path: "/_wirehop", SessionID: protocol.SessionID{1}, LaneID: protocol.LaneID{2},
		Generation: 3, PathGroupID: protocol.PathGroupID{4}, Nonce: protocol.Nonce{5}, UnixSeconds: 6,
		MonotonicMicros: 7,
	}
	if err := SignJoin(&join, secret); err != nil {
		t.Fatal(err)
	}
	headers, err := JoinHeaders(join)
	if err != nil {
		t.Fatal(err)
	}
	request := &http.Request{Method: "GET", URL: &url.URL{Path: "/_wirehop"}, Header: headers}
	parsed, err := ParseJoin(request)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != join {
		t.Fatalf("ParseJoin() = %+v, want %+v", parsed, join)
	}
	if err := VerifyJoin(parsed, secret); err != nil {
		t.Fatal(err)
	}
	invalidMethod := join
	invalidMethod.Method = http.MethodPost
	if err := SignJoin(&invalidMethod, secret); err != ErrInvalid {
		t.Fatalf("SignJoin() method error = %v, want %v", err, ErrInvalid)
	}
	request.URL.ForceQuery = true
	if _, err := ParseJoin(request); err != ErrInvalid {
		t.Fatalf("ParseJoin() empty query error = %v, want %v", err, ErrInvalid)
	}
	request.URL.ForceQuery = false
	request.Method = http.MethodPost
	if _, err := ParseJoin(request); err != ErrInvalid {
		t.Fatalf("ParseJoin() method error = %v, want %v", err, ErrInvalid)
	}
	request.Method = http.MethodGet
	parsed.Generation++
	if err := VerifyJoin(parsed, secret); err != protocol.ErrAuthenticationFailed {
		t.Fatalf("VerifyJoin() error = %v, want %v", err, protocol.ErrAuthenticationFailed)
	}
}

func TestJoinPathBoundary(t *testing.T) {
	secret := protocol.SessionSecret{1}
	join := Join{
		Method: http.MethodGet, Path: "/" + strings.Repeat("a", MaxPathSize-1),
		SessionID: protocol.SessionID{1}, LaneID: protocol.LaneID{2}, Generation: 3,
		PathGroupID: protocol.PathGroupID{4}, Nonce: protocol.Nonce{5}, UnixSeconds: 6,
	}
	if err := SignJoin(&join, secret); err != nil {
		t.Fatalf("SignJoin() boundary error = %v", err)
	}
	join.Path += "a"
	if err := SignJoin(&join, secret); !errors.Is(err, ErrInvalid) {
		t.Fatalf("SignJoin() oversized path error = %v, want %v", err, ErrInvalid)
	}
}
