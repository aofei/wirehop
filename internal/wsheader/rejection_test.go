package wsheader

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestRejectionRoundTrip(t *testing.T) {
	rejection := protocol.ServerHello{
		Result: protocol.ServerRejected, RequestNonce: protocol.Nonce{1}, ServerUnixSeconds: 1_700_000_000,
		ErrorCode: protocol.ErrorClockSkew, ErrorClass: protocol.ErrorRetryable,
		ErrorScope: protocol.ErrorScopeLane, Diagnostic: "request timestamp rejected",
	}
	if err := protocol.SignServerHello(&rejection, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	headers := make(http.Header)
	if err := SetRejection(headers, rejection); err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRejection(headers)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed, rejection) {
		t.Fatalf("ParseRejection() = %#v, want %#v", parsed, rejection)
	}
	if err := protocol.VerifyServerHello(parsed, []byte("test key")); err != nil {
		t.Fatal(err)
	}
}

func TestRejectionErrors(t *testing.T) {
	rejection := protocol.ServerHello{
		Result: protocol.ServerRejected, RequestNonce: protocol.Nonce{1}, ServerUnixSeconds: 1_700_000_000,
		ErrorCode: protocol.ErrorAuthentication, ErrorClass: protocol.ErrorSessionRejected,
		ErrorScope: protocol.ErrorScopeSession,
	}
	if err := protocol.SignServerHello(&rejection, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	headers := make(http.Header)
	if err := SetRejection(headers, rejection); err != nil {
		t.Fatal(err)
	}
	headers.Add(headerRejection, headers.Get(headerRejection))
	if _, err := ParseRejection(headers); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseRejection() duplicate error = %v, want %v", err, ErrInvalid)
	}
	headers = make(http.Header)
	if err := SetRejection(headers, rejection); err != nil {
		t.Fatal(err)
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	noncanonical := []byte(headers.Get(headerRejection))
	last := len(noncanonical) - 1
	index := strings.IndexByte(alphabet, noncanonical[last])
	if index < 0 || index%4 != 0 {
		t.Fatalf("canonical rejection ended with unexpected base64url byte %q", noncanonical[last])
	}
	noncanonical[last] = alphabet[index+1]
	headers.Set(headerRejection, string(noncanonical))
	if _, err := ParseRejection(headers); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseRejection() noncanonical error = %v, want %v", err, ErrInvalid)
	}
	for _, value := range []string{"***", string(make([]byte, maximumEncodedRejectionSize+1))} {
		headers = make(http.Header)
		headers.Set(headerRejection, value)
		if _, err := ParseRejection(headers); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ParseRejection(%q) error = %v, want %v", value, err, ErrInvalid)
		}
	}
}
