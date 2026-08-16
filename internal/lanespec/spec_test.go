package lanespec

import (
	"encoding/csv"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/laneurl"
)

func TestParse(t *testing.T) {
	for _, test := range []struct {
		name        string
		value       string
		url         string
		resolveIP   string
		dialAddress string
	}{
		{
			name: "URLShorthand", value: "wss://EXAMPLE.COM/_wirehop",
			url: "wss://example.com:443/_wirehop", dialAddress: "example.com:443",
		},
		{
			name: "IPv4", value: "url=tls://relay.example:8443,resolve=192.0.2.1",
			url: "tls://relay.example:8443", resolveIP: "192.0.2.1", dialAddress: "192.0.2.1:8443",
		},
		{
			name: "IPv6", value: "resolve=2001:db8::1,url=wss://relay.example/path",
			url: "wss://relay.example:443/path", resolveIP: "2001:db8::1",
			dialAddress: "[2001:db8::1]:443",
		},
		{
			name: "MappedIPv4", value: "url=ws://relay.example/path,resolve=::ffff:192.0.2.1",
			url: "ws://relay.example:80/path", resolveIP: "192.0.2.1", dialAddress: "192.0.2.1:80",
		},
		{
			name: "QuotedComma", value: `"url=wss://relay.example/path,part",resolve=192.0.2.1`,
			url: "wss://relay.example:443/path,part", resolveIP: "192.0.2.1", dialAddress: "192.0.2.1:443",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec, err := Parse(test.value)
			if err != nil {
				t.Fatal(err)
			}
			var resolveIP string
			if spec.ResolveIP().IsValid() {
				resolveIP = spec.ResolveIP().String()
			}
			if !spec.Valid() || spec.URL().String() != test.url ||
				resolveIP != test.resolveIP || spec.DialAddress() != test.dialAddress {
				t.Fatalf("Parse(%q) = %q, %q, %q", test.value, spec.URL(), spec.ResolveIP(), spec.DialAddress())
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  string
		reason string
		urlErr bool
	}{
		{name: "MalformedURL", value: "not-a-lane-url", reason: "carrier scheme is required", urlErr: true},
		{name: "MalformedCSV", value: `"url=wss://relay.example/path,resolve=192.0.2.1`,
			reason: "malformed field list"},
		{name: "EmptyField", value: "url=wss://relay.example/path,,resolve=192.0.2.1",
			reason: "fields cannot be empty"},
		{name: "Whitespace", value: "url=wss://relay.example/path, resolve=192.0.2.1",
			reason: "surrounded by whitespace"},
		{name: "MissingValue", value: "url=,resolve=192.0.2.1", reason: `field "url" requires a value`},
		{name: "UnknownField", value: "url=wss://relay.example/path,address=192.0.2.1",
			reason: `unknown field "address"`},
		{name: "UnknownFirstField", value: "address=192.0.2.1,url=wss://relay.example/path",
			reason: `unknown field "address"`},
		{name: "DuplicateField",
			value:  "url=wss://relay.example/path,resolve=192.0.2.1,resolve=192.0.2.2",
			reason: `duplicate field "resolve"`},
		{name: "MissingURL", value: "resolve=192.0.2.1", reason: `requires field "url"`},
		{name: "MissingResolve", value: "url=wss://relay.example/path", reason: `requires field "resolve"`},
		{name: "IPURL", value: "url=wss://192.0.2.10/path,resolve=192.0.2.1",
			reason: "requires a URL hostname"},
		{name: "HostnameResolve", value: "url=wss://relay.example/path,resolve=other.example",
			reason: "unbracketed IP address"},
		{name: "PortResolve", value: "url=wss://relay.example/path,resolve=192.0.2.1:443",
			reason: "unbracketed IP address"},
		{name: "BracketedIPv6", value: "url=wss://relay.example/path,resolve=[2001:db8::1]",
			reason: "unbracketed IP address"},
		{name: "ZonedIPv6", value: "url=wss://relay.example/path,resolve=fe80::1%en0",
			reason: "cannot contain a zone"},
		{name: "LinkLocalIPv6", value: "url=wss://relay.example/path,resolve=fe80::1",
			reason: "cannot use an IPv6 link-local address"},
		{name: "UnspecifiedResolve", value: "url=wss://relay.example/path,resolve=0.0.0.0",
			reason: "unicast IP address"},
		{name: "MulticastResolve", value: "url=wss://relay.example/path,resolve=ff02::1",
			reason: "unicast IP address"},
		{name: "BroadcastResolve", value: "url=wss://relay.example/path,resolve=255.255.255.255",
			reason: "unicast IP address"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.value)
			if !errors.Is(err, ErrInvalid) || test.urlErr && !errors.Is(err, laneurl.ErrInvalid) ||
				err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("Parse(%q) error = %v, want reason %q", test.value, err, test.reason)
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, value := range []string{
		"wss://relay.example/_wirehop",
		"url=tls://relay.example:8443,resolve=192.0.2.1",
		"resolve=2001:db8::1,url=wss://relay.example/path",
		`"url=wss://relay.example/path,part",resolve=192.0.2.1`,
	} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		spec, err := Parse(value)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Parse(%q) error = %v, want %v", value, err, ErrInvalid)
			}
			return
		}
		if !spec.Valid() {
			t.Fatalf("Parse(%q) returned an invalid spec", value)
		}
		host, port, err := net.SplitHostPort(spec.DialAddress())
		if err != nil || port != spec.URL().Port() {
			t.Fatalf("Parse(%q) dial address = %q: %v", value, spec.DialAddress(), err)
		}
		if resolveIP := spec.ResolveIP(); resolveIP.IsValid() {
			if host != resolveIP.String() {
				t.Fatalf("Parse(%q) dial host = %q, want %q", value, host, resolveIP)
			}
		} else if spec.DialAddress() != spec.URL().Address() {
			t.Fatalf("Parse(%q) dial address = %q, want %q", value, spec.DialAddress(), spec.URL().Address())
		}

		canonical := marshalSpec(t, spec)
		roundTrip, err := Parse(canonical)
		if err != nil {
			t.Fatalf("parse canonical spec %q: %v", canonical, err)
		}
		if roundTrip.URL().String() != spec.URL().String() || roundTrip.ResolveIP() != spec.ResolveIP() ||
			roundTrip.DialAddress() != spec.DialAddress() {
			t.Fatalf("canonical spec changed after round trip: %q", canonical)
		}
	})
}

func marshalSpec(t *testing.T, spec Spec) string {
	t.Helper()
	if !spec.ResolveIP().IsValid() {
		return spec.URL().String()
	}
	var encoded strings.Builder
	writer := csv.NewWriter(&encoded)
	if err := writer.Write([]string{"url=" + spec.URL().String(), "resolve=" + spec.ResolveIP().String()}); err != nil {
		t.Fatal(err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(encoded.String(), "\n")
}
