package target

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"
)

type testResolver struct {
	addresses []netip.Addr
	err       error
	host      string
}

func (r *testResolver) LookupNetIP(_ context.Context, network, host string) ([]netip.Addr, error) {
	if network != "ip" {
		panic("unexpected network " + network)
	}
	r.host = host
	return append([]netip.Addr(nil), r.addresses...), r.err
}

func TestParse(t *testing.T) {
	maximumHostname := strings.Join([]string{
		strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61),
	}, ".")
	for _, tt := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "IPv4", value: "192.0.2.1:51820", want: "192.0.2.1:51820"},
		{name: "IPv6", value: "[2001:db8::1]:51820", want: "[2001:db8::1]:51820"},
		{name: "Hostname", value: "WG.Example.COM.:51820", want: "wg.example.com:51820"},
		{name: "SingleLabel", value: "wireguard:51820", want: "wireguard:51820"},
		{name: "CanonicalPort", value: "wg.example.com:051820", want: "wg.example.com:51820"},
		{name: "MaximumHostname", value: maximumHostname + ":65535", want: maximumHostname + ":65535"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Valid() || got.String() != tt.want {
				t.Fatalf("Parse(%q) = %q", tt.value, got)
			}
		})
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	for _, value := range []string{
		"", "example.com", ":51820", "example.com:0", "example.com:http", "-bad.example:51820",
		"bad_.example:51820", "bad..example:51820", "[::]:51820", "224.0.0.1:51820",
		"255.255.255.255:51820", "[fe80::1]:51820", "[fe80::1%en0]:51820",
		"[::ffff:192.0.2.1]:51820", "example.com:65536",
		"127.1:51820", "192.0.002.1:51820",
		"\u212a.example:51820",
		strings.Repeat("a", 64) + ".example:51820",
		strings.Repeat("a", MaxHostnameSize+1) + ":51820",
	} {
		if _, err := Parse(value); !errors.Is(err, ErrInvalid) {
			t.Errorf("Parse(%q) error = %v, want %v", value, err, ErrInvalid)
		}
	}
}

func TestResolve(t *testing.T) {
	resolver := &testResolver{addresses: []netip.Addr{
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("::ffff:192.0.2.1"),
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("ff02::1"),
	}}
	got, err := Resolve(context.Background(), resolver, MustParse("wg.example.com:51820"))
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{
		netip.MustParseAddrPort("192.0.2.1:51820"),
		netip.MustParseAddrPort("[2001:db8::1]:51820"),
	}
	if resolver.host != "wg.example.com." || !reflect.DeepEqual(got, want) {
		t.Fatalf("Resolve() = %v using %q, want %v", got, resolver.host, want)
	}
}

func TestResolveLiteral(t *testing.T) {
	got, err := Resolve(context.Background(), nil, MustParse("192.0.2.1:51820"))
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:51820")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Resolve() = %v, want %v", got, want)
	}
}

func TestResolveHonorsCancellationForLiteral(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Resolve(ctx, nil, MustParse("192.0.2.1:51820")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve() error = %v, want %v", err, context.Canceled)
	}
}

func TestResolveHonorsCancellationAfterLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resolver := cancelingResolver{cancel: cancel}
	if _, err := Resolve(ctx, resolver, MustParse("wg.example.com:51820")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve() error = %v, want %v", err, context.Canceled)
	}
}

func TestResolveRequiresUsableAddress(t *testing.T) {
	resolver := &testResolver{addresses: []netip.Addr{
		netip.MustParseAddr("fe80::1"),
		netip.MustParseAddr("ff02::1"),
	}}
	if _, err := Resolve(context.Background(), resolver, MustParse("wg.example.com:51820")); !errors.Is(err, ErrNoAddresses) {
		t.Fatalf("Resolve() error = %v, want %v", err, ErrNoAddresses)
	}
}

func TestResolveRetainsBothAddressFamiliesAtCandidateLimit(t *testing.T) {
	addresses := make([]netip.Addr, 0, MaxCandidates+11)
	for index := range MaxCandidates + 10 {
		addresses = append(addresses, netip.AddrFrom16([16]byte{0x20, 0x01, 0x0d, 0xb8, byte(index)}))
	}
	addresses = append(addresses, netip.MustParseAddr("192.0.2.1"))
	got, err := Resolve(context.Background(), &testResolver{addresses: addresses}, MustParse("wg.example.com:51820"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != MaxCandidates || !got[len(got)-1].Addr().Is4() {
		t.Fatalf("Resolve() = %v", got)
	}
}

func FuzzParse(f *testing.F) {
	for _, value := range []string{
		"wg.example.com:51820",
		"WG.Example.COM.:051820",
		"192.0.2.1:51820",
		"[2001:db8::1]:51820",
		"bad_.example:51820",
	} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		endpoint, err := Parse(value)
		if err != nil {
			return
		}
		if !endpoint.Valid() || endpoint.String() == "" {
			t.Fatalf("Parse(%q) returned invalid endpoint", value)
		}
		roundTrip, err := Parse(endpoint.String())
		if err != nil || roundTrip != endpoint {
			t.Fatalf("canonical round trip = %v, %v, want %v", roundTrip, err, endpoint)
		}
	})
}

type cancelingResolver struct {
	cancel context.CancelFunc
}

func (r cancelingResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.cancel()
	return []netip.Addr{netip.MustParseAddr("192.0.2.1")}, nil
}
