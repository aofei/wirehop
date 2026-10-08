package wsheader

import (
	"encoding/base64"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestRejectionRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name        string
		diagnostic  string
		encodedSize int
	}{
		{name: "Ordinary", diagnostic: "request timestamp rejected", encodedSize: 120},
		{name: "MaximumDiagnostic", diagnostic: strings.Repeat("x", protocol.MaxDiagnosticSize), encodedSize: 768},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rejection := protocol.ServerHello{
				Result: protocol.ServerRejected, RequestNonce: protocol.Nonce{1}, ServerUnixSeconds: 1_700_000_000,
				ErrorCode: protocol.ErrorClockSkew, ErrorClass: protocol.ErrorRetryable,
				ErrorScope: protocol.ErrorScopeLane, Diagnostic: tt.diagnostic,
			}
			if err := protocol.SignServerHello(&rejection, []byte("test key")); err != nil {
				t.Fatal(err)
			}
			headers := make(http.Header)
			if err := SetRejection(headers, rejection); err != nil {
				t.Fatal(err)
			}
			if size := len(headers.Get(headerRejection)); size != tt.encodedSize {
				t.Fatalf("encoded header size = %d, want %d", size, tt.encodedSize)
			}
			parsed, err := ParseRejection(headers)
			if err != nil || parsed != rejection {
				t.Fatalf("ParseRejection() = %#v, %v, want %#v", parsed, err, rejection)
			}
			if err := protocol.VerifyServerHello(parsed, []byte("test key")); err != nil {
				t.Fatal(err)
			}
		})
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
	rejection.Diagnostic = "x"
	if err := protocol.SignServerHello(&rejection, []byte("test key")); err != nil {
		t.Fatal(err)
	}
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
	for _, value := range []string{"***", strings.Repeat("A", 769)} {
		headers = make(http.Header)
		headers.Set(headerRejection, value)
		if _, err := ParseRejection(headers); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ParseRejection(%q) error = %v, want %v", value, err, ErrInvalid)
		}
	}
}

func TestParseRejectionLineBreaks(t *testing.T) {
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
	canonical := headers.Get(headerRejection)
	for _, tt := range []struct {
		name  string
		value string
	}{
		{name: "CarriageReturn", value: "\r"},
		{name: "LineFeed", value: "\n"},
		{name: "CRLF", value: "\r\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, offset := range []int{0, len(canonical) / 2, len(canonical)} {
				t.Run("Offset"+strconv.Itoa(offset), func(t *testing.T) {
					headers.Set(headerRejection, canonical[:offset]+tt.value+canonical[offset:])
					if _, err := ParseRejection(headers); !errors.Is(err, ErrInvalid) {
						t.Fatalf("noncanonical header error = %v, want %v", err, ErrInvalid)
					}
				})
			}
		})
	}
}

func FuzzParseRejection(f *testing.F) {
	var validSeeds []string
	for _, value := range []string{"", "***", strings.Repeat("A", 769)} {
		f.Add(value)
	}
	for _, diagnostic := range []string{"", strings.Repeat("x", protocol.MaxDiagnosticSize)} {
		rejection := protocol.ServerHello{
			Result: protocol.ServerRejected, RequestNonce: protocol.Nonce{1}, ServerUnixSeconds: 1_700_000_000,
			ErrorCode: protocol.ErrorAuthentication, ErrorClass: protocol.ErrorSessionRejected,
			ErrorScope: protocol.ErrorScopeSession, Diagnostic: diagnostic,
		}
		if err := protocol.SignServerHello(&rejection, []byte("test key")); err != nil {
			f.Fatal(err)
		}
		headers := make(http.Header)
		if err := SetRejection(headers, rejection); err != nil {
			f.Fatal(err)
		}
		value := headers.Get(headerRejection)
		validSeeds = append(validSeeds, value)
		f.Add(value)
		f.Add(value + "\r\n")
	}
	f.Fuzz(func(t *testing.T, value string) {
		headers := make(http.Header)
		headers.Set(headerRejection, value)
		parsed, err := ParseRejection(headers)
		if err != nil {
			if slices.Contains(validSeeds, value) {
				t.Fatalf("valid rejection seed was rejected: %v", err)
			}
			return
		}
		encoded, err := protocol.MarshalServerHello(parsed)
		if err != nil || len(encoded) > protocol.MaxServerHelloSize {
			t.Fatalf("accepted rejection cannot be bounded and re-encoded: %v", err)
		}
		if canonical := base64.RawURLEncoding.EncodeToString(encoded); value != canonical {
			t.Fatal("accepted rejection header is not canonical")
		}
	})
}
