package laneurl

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/wsheader"
)

func TestParseDial(t *testing.T) {
	for _, test := range []struct {
		value   string
		scheme  Scheme
		address string
		result  string
	}{
		{value: "tcp://example.com:80", scheme: TCP, address: "example.com:80", result: "tcp://example.com:80"},
		{value: "tls://[2001:db8::1]:443", scheme: TLS, address: "[2001:db8::1]:443",
			result: "tls://[2001:db8::1]:443"},
		{value: "tls://[fe80::1%25en0]:443", scheme: TLS, address: "[fe80::1%en0]:443",
			result: "tls://[fe80::1%25en0]:443"},
		{value: "ws://example.com:80", scheme: WS, address: "example.com:80", result: "ws://example.com:80/"},
		{value: "ws://example.com", scheme: WS, address: "example.com:80", result: "ws://example.com:80/"},
		{value: "wss://example.com/_wirehop", scheme: WSS, address: "example.com:443",
			result: "wss://example.com:443/_wirehop"},
		{value: "wss://[2001:db8::1]/_wirehop", scheme: WSS, address: "[2001:db8::1]:443",
			result: "wss://[2001:db8::1]:443/_wirehop"},
		{value: "wss://EXAMPLE.COM:00443/%7ewirehop", scheme: WSS, address: "example.com:443",
			result: "wss://example.com:443/%7Ewirehop"},
		{value: "WSS://EXAMPLE.COM/_wirehop", scheme: WSS, address: "example.com:443",
			result: "wss://example.com:443/_wirehop"},
		{value: "tls://[::ffff:192.0.2.1]:443", scheme: TLS, address: "192.0.2.1:443",
			result: "tls://192.0.2.1:443"},
	} {
		parsed, err := ParseDial(test.value)
		if err != nil {
			t.Fatalf("ParseDial(%q) error = %v", test.value, err)
		}
		if parsed.Scheme() != test.scheme || parsed.Address() != test.address || parsed.String() != test.result {
			t.Fatalf("ParseDial(%q) = %v, %q, %q", test.value, parsed.Scheme(), parsed.Address(), parsed.String())
		}
	}
}

func TestParseDialErrors(t *testing.T) {
	for _, test := range []struct {
		value  string
		reason string
	}{
		{reason: "carrier scheme is required"},
		{value: "udp://example.com:80", reason: `unsupported carrier scheme "udp"`},
		{value: "tcp://example.com", reason: "tcp URLs require an explicit port"},
		{value: "tls://example.com", reason: "tls URLs require an explicit port"},
		{value: "tcp://:80", reason: "client lane URL requires a host"},
		{value: "tcp://example.com:0", reason: "port must be between 1 and 65535"},
		{value: "tcp://example.com:65536", reason: "port must be between 1 and 65535"},
		{value: "ws://example.com:", reason: "port is required"},
		{value: "ws://]", reason: `invalid host "]"`},
		{value: "tcp://0.0.0.0:80", reason: "client lane URL requires a unicast IP address"},
		{value: "tls://[::]:443", reason: "client lane URL requires a unicast IP address"},
		{value: "tls://[fe80::1]:443", reason: "client lane IPv6 link-local address requires a zone identifier"},
		{value: "ws://224.0.0.1:80", reason: "client lane URL requires a unicast IP address"},
		{value: "wss://255.255.255.255:443", reason: "client lane URL requires a unicast IP address"},
		{value: "tcp://user@example.com:80", reason: "user information is not allowed"},
		{value: "tcp://example.com:80/path", reason: "tcp URLs cannot contain a path"},
		{value: "ws://example.com:80/path?query=1", reason: "query parameters are not allowed"},
		{value: "tcp://example.com:80?", reason: "query parameters are not allowed"},
		{value: "ws://example.com:80/path?", reason: "query parameters are not allowed"},
		{value: "ws://example.com:80/path#fragment", reason: "fragments are not allowed"},
		{
			value:  "wss://example.com:443/" + strings.Repeat("a", wsheader.MaxPathSize),
			reason: fmt.Sprintf("WebSocket path exceeds %d bytes", wsheader.MaxPathSize),
		},
	} {
		_, err := ParseDial(test.value)
		if !errors.Is(err, ErrInvalid) || err.Error() != test.reason {
			t.Fatalf("ParseDial(%q) error = %v, want %q", test.value, err, test.reason)
		}
	}
}

func TestParseDialSyntaxErrors(t *testing.T) {
	t.Run("InvalidPort", func(t *testing.T) {
		_, err := ParseDial("wss://example.com:invalid/_wirehop")
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "malformed URL: invalid port") ||
			strings.Contains(err.Error(), "wss://") {
			t.Fatalf("ParseDial() error = %v", err)
		}
	})
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "IPv6Address", value: "wss://2001:db8::1/_wirehop"},
		{name: "IPv6AddressWithApparentPort", value: "tls://2001:db8::1:443"},
		{name: "ZonedIPv6Address", value: "wss://fe80::1%25en0/_wirehop"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseDial(test.value)
			want := "IPv6 host must be enclosed in brackets"
			if !errors.Is(err, ErrInvalid) || err.Error() != want {
				t.Fatalf("ParseDial(%q) error = %v, want %q", test.value, err, want)
			}
		})
	}
}

func TestParseWebSocketPathBoundary(t *testing.T) {
	path := "/" + strings.Repeat("a", wsheader.MaxPathSize-1)
	if _, err := ParseDial("wss://example.com:443" + path); err != nil {
		t.Fatalf("ParseDial() boundary error = %v", err)
	}
	if _, err := ParseDial("wss://example.com:443" + path + "a"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseDial() oversized error = %v, want %v", err, ErrInvalid)
	}
}

func TestParseListen(t *testing.T) {
	for _, test := range []struct {
		value   string
		address string
	}{
		{value: "tcp://:8080", address: ":8080"},
		{value: "tls://127.0.0.1:8443", address: "127.0.0.1:8443"},
		{value: "ws://:80/_wirehop", address: ":80"},
		{value: "ws://127.0.0.1/_wirehop", address: "127.0.0.1:80"},
		{value: "wss://[::]/_wirehop", address: "[::]:443"},
		{value: "wss://[::]:443/_wirehop", address: "[::]:443"},
	} {
		parsed, err := ParseListen(test.value)
		if err != nil {
			t.Fatalf("ParseListen(%q) error = %v", test.value, err)
		}
		if parsed.Address() != test.address {
			t.Fatalf("ParseListen(%q).Address() = %q, want %q", test.value, parsed.Address(), test.address)
		}
	}
	for _, test := range []struct {
		value  string
		reason string
	}{
		{value: "tcp://:0", reason: "port must be between 1 and 65535"},
		{value: "tcp://localhost", reason: "tcp URLs require an explicit port"},
		{value: "tls://localhost", reason: "tls URLs require an explicit port"},
		{value: "tcp://:80/path", reason: "tcp URLs cannot contain a path"},
		{value: "ws://", reason: "server listener URL requires a host or port"},
	} {
		_, err := ParseListen(test.value)
		if !errors.Is(err, ErrInvalid) || err.Error() != test.reason {
			t.Fatalf("ParseListen(%q) error = %v, want %q", test.value, err, test.reason)
		}
	}
}

func FuzzParseDial(f *testing.F) {
	for _, value := range []string{
		"wss://relay.example/_wirehop",
		"WSS://EXAMPLE.COM:00443/%7ewirehop",
		"tls://[2001:db8::1]:443",
		"ws://127.0.0.1",
		"ws://]",
		"tcp://example.com:51820",
	} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		parsed, err := ParseDial(value)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("ParseDial(%q) error = %v, want %v", value, err, ErrInvalid)
			}
			return
		}
		assertCanonicalURL(t, parsed, ParseDial)
	})
}

func FuzzParseListen(f *testing.F) {
	for _, value := range []string{
		"wss://:443/_wirehop",
		"ws://127.0.0.1",
		"tls://[::]:443",
		"tcp://localhost:51820",
	} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		parsed, err := ParseListen(value)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("ParseListen(%q) error = %v, want %v", value, err, ErrInvalid)
			}
			return
		}
		assertCanonicalURL(t, parsed, ParseListen)
	})
}

func assertCanonicalURL(t *testing.T, parsed URL, parse func(string) (URL, error)) {
	t.Helper()
	if !parsed.Valid() || parsed.Address() == "" || parsed.Port() == "" {
		t.Fatalf("parsed URL is internally inconsistent: %q", parsed.String())
	}
	if parsed.Hostname() == "" && !strings.HasPrefix(parsed.Address(), ":") {
		t.Fatalf("parsed URL is internally inconsistent: %q", parsed.String())
	}
	roundTrip, err := parse(parsed.String())
	if err != nil {
		t.Fatalf("parse canonical URL %q: %v", parsed.String(), err)
	}
	if roundTrip.String() != parsed.String() || roundTrip.Scheme() != parsed.Scheme() ||
		roundTrip.Address() != parsed.Address() || roundTrip.EscapedPath() != parsed.EscapedPath() {
		t.Fatalf("canonical URL changed after round trip: %q to %q", parsed.String(), roundTrip.String())
	}
}
