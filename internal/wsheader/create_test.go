package wsheader

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
)

func TestCreateRoundTrip(t *testing.T) {
	request := Create{
		Token: "secret-token", Target: target.MustParse("wg.example.com:51820"), LaneID: protocol.LaneID{1},
		Generation: 2, PathGroupID: protocol.PathGroupID{3}, Nonce: protocol.Nonce{4}, UnixSeconds: 5,
		MonotonicMicros: 6,
	}
	headers, err := Headers(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers}
	parsed, err := ParseCreate(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != request {
		t.Fatalf("ParseCreate() = %+v, want %+v", parsed, request)
	}
}

func TestCreateErrors(t *testing.T) {
	valid := Create{
		Token: "token", Target: target.MustParse("wg.example.com:51820"), LaneID: protocol.LaneID{1},
		Generation: 1, PathGroupID: protocol.PathGroupID{1}, Nonce: protocol.Nonce{1}, UnixSeconds: 1,
	}
	for _, mutate := range []func(*Create){
		func(value *Create) { value.Token = "" },
		func(value *Create) { value.Token = "token\nvalue" },
		func(value *Create) { value.Token = "token value" },
		func(value *Create) { value.Token = "token=bad" },
		func(value *Create) { value.Target = target.Endpoint{} },
		func(value *Create) { value.LaneID = protocol.LaneID{} },
		func(value *Create) { value.Generation = 0 },
		func(value *Create) { value.PathGroupID = protocol.PathGroupID{} },
		func(value *Create) { value.Nonce = protocol.Nonce{} },
		func(value *Create) { value.UnixSeconds = 0 },
	} {
		request := valid
		mutate(&request)
		if _, err := Headers(request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Headers(%+v) error = %v, want %v", request, err, ErrInvalid)
		}
	}
	headers, err := Headers(valid)
	if err != nil {
		t.Fatal(err)
	}
	headers.Add(headerTarget, valid.Target.String())
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() duplicate error = %v, want %v", err, ErrInvalid)
	}
	headers.Del(headerTarget)
	headers.Set(headerTarget, "WG.EXAMPLE.COM:51820")
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() noncanonical target error = %v, want %v", err, ErrInvalid)
	}
	headers.Set(headerTarget, valid.Target.String())
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop", ForceQuery: true}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() empty query error = %v, want %v", err, ErrInvalid)
	}
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodPost, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() method error = %v, want %v", err, ErrInvalid)
	}
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/" + strings.Repeat("a", MaxPathSize)}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() oversized path error = %v, want %v", err, ErrInvalid)
	}
}

func TestValidateBearerToken(t *testing.T) {
	for _, token := range []string{"a", "abc-._~+/", "YWJjZA=="} {
		if err := ValidateBearerToken(token); err != nil {
			t.Fatalf("ValidateBearerToken(%q) error = %v", token, err)
		}
	}
	for _, token := range []string{"", "=", "abc def", "abc=def", "abc,", strings.Repeat("a", 4097)} {
		if err := ValidateBearerToken(token); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ValidateBearerToken(%q) error = %v, want %v", token, err, ErrInvalid)
		}
	}
}
