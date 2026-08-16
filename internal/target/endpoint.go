// Package target defines canonical WireGuard target identities and resolves their server-side addresses.
package target

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

const (
	// MaxHostnameSize is the longest supported DNS hostname without its terminating root label.
	MaxHostnameSize = 253
	// MaxTextSize is the longest canonical HOST:PORT representation.
	MaxTextSize = MaxHostnameSize + 1 + 5
	// MaxCandidates bounds handshake fan-out and retained DNS results for one target.
	MaxCandidates = 16
)

var (
	// ErrInvalid indicates a malformed or unsafe WireGuard target.
	ErrInvalid = errors.New("invalid UDP target")
	// ErrNoAddresses indicates that a target resolved to no usable unicast addresses.
	ErrNoAddresses = errors.New("target has no usable addresses")
)

// Resolver resolves target hostnames through the server's local name service.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// Endpoint is one canonical IP-literal or DNS WireGuard target with a numeric UDP port.
type Endpoint struct {
	host string
	addr netip.Addr
	port uint16
}

// Parse parses and canonicalizes an IP-literal or DNS target with an explicit, nonzero port.
func Parse(value string) (Endpoint, error) {
	host, portValue, err := net.SplitHostPort(value)
	if err != nil || host == "" {
		return Endpoint{}, fmt.Errorf("%w: expected HOST:PORT", ErrInvalid)
	}
	parsedPort, err := strconv.ParseUint(portValue, 10, 16)
	if err != nil || parsedPort == 0 {
		return Endpoint{}, fmt.Errorf("%w: port must be between 1 and 65535", ErrInvalid)
	}
	port := uint16(parsedPort)
	if address, err := netip.ParseAddr(host); err == nil {
		if reason := invalidAddressReason(address); reason != "" {
			return Endpoint{}, fmt.Errorf("%w: %s", ErrInvalid, reason)
		}
		return Endpoint{host: address.String(), addr: address, port: port}, nil
	}
	if numericHostname(host) {
		return Endpoint{}, fmt.Errorf("%w: malformed IP address", ErrInvalid)
	}
	canonical, err := canonicalHostname(host)
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{host: canonical, port: port}, nil
}

// FromAddrPort returns a canonical target for address.
func FromAddrPort(address netip.AddrPort) (Endpoint, error) {
	if !address.IsValid() || address.Port() == 0 || !ValidAddress(address.Addr()) {
		return Endpoint{}, ErrInvalid
	}
	return Endpoint{host: address.Addr().String(), addr: address.Addr(), port: address.Port()}, nil
}

// MustParse parses value and panics on failure.
func MustParse(value string) Endpoint {
	endpoint, err := Parse(value)
	if err != nil {
		panic(err)
	}
	return endpoint
}

// Valid reports whether endpoint is canonical and usable.
func (e Endpoint) Valid() bool {
	if e.host == "" || e.port == 0 {
		return false
	}
	if e.addr.IsValid() {
		return e.host == e.addr.String() && ValidAddress(e.addr)
	}
	if numericHostname(e.host) {
		return false
	}
	canonical, err := canonicalHostname(e.host)
	return err == nil && canonical == e.host
}

// Port returns the UDP port.
func (e Endpoint) Port() uint16 {
	return e.port
}

// Address returns the IP literal, or an invalid address for a DNS target.
func (e Endpoint) Address() netip.Addr {
	return e.addr
}

// IsDomain reports whether endpoint requires server-side DNS resolution.
func (e Endpoint) IsDomain() bool {
	return e.Valid() && !e.addr.IsValid()
}

// String returns the canonical HOST:PORT representation.
func (e Endpoint) String() string {
	if !e.Valid() {
		return ""
	}
	return net.JoinHostPort(e.host, strconv.FormatUint(uint64(e.port), 10))
}

// Resolve returns the bounded, deduplicated addresses currently represented by endpoint.
func Resolve(ctx context.Context, resolver Resolver, endpoint Endpoint) ([]netip.AddrPort, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !endpoint.Valid() {
		return nil, ErrInvalid
	}
	if endpoint.addr.IsValid() {
		return []netip.AddrPort{netip.AddrPortFrom(endpoint.addr, endpoint.port)}, nil
	}
	if resolver == nil {
		return nil, ErrNoAddresses
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", endpoint.host+".")
	if err != nil {
		return nil, fmt.Errorf("resolve target hostname: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved := make([]netip.AddrPort, 0, min(len(addresses), MaxCandidates))
	seen := make(map[netip.Addr]struct{}, min(len(addresses), MaxCandidates))
	var haveIPv4, haveIPv6 bool
	for _, address := range addresses {
		address = address.Unmap()
		if !ValidAddress(address) {
			continue
		}
		if len(resolved) == MaxCandidates {
			if haveIPv4 && haveIPv6 {
				break
			}
			if address.Is4() && !haveIPv4 || address.Is6() && !haveIPv6 {
				resolved[len(resolved)-1] = netip.AddrPortFrom(address, endpoint.port)
				break
			}
			continue
		}
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		candidate := netip.AddrPortFrom(address, endpoint.port)
		resolved = append(resolved, candidate)
		haveIPv4 = haveIPv4 || address.Is4()
		haveIPv6 = haveIPv6 || address.Is6()
	}
	if len(resolved) == 0 {
		return nil, ErrNoAddresses
	}
	return resolved, nil
}

// ValidAddress reports whether address can identify a unicast WireGuard target without an IPv6 zone.
func ValidAddress(address netip.Addr) bool {
	return invalidAddressReason(address) == ""
}

// invalidAddressReason describes an IP address rejected as a WireGuard target candidate.
func invalidAddressReason(address netip.Addr) string {
	if !address.IsValid() {
		return "invalid address"
	}
	if address != address.Unmap() {
		return "IPv4-mapped IPv6 addresses are not allowed"
	}
	if address.Zone() != "" {
		return "IPv6 zone identifiers are not allowed"
	}
	if address.Is6() && address.IsLinkLocalUnicast() {
		return "IPv6 link-local addresses are not allowed without a zone identifier"
	}
	if address.IsUnspecified() {
		return "unspecified addresses are not allowed"
	}
	if address.IsMulticast() {
		return "multicast addresses are not allowed"
	}
	if address.Is4() && address.As4() == [4]byte{255, 255, 255, 255} {
		return "the IPv4 limited broadcast address is not allowed"
	}
	return ""
}

// canonicalHostname validates an ASCII RFC 1123 hostname and removes case and root-label differences.
func canonicalHostname(host string) (string, error) {
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > MaxHostnameSize {
		return "", fmt.Errorf("%w: hostname length must be between 1 and %d", ErrInvalid, MaxHostnameSize)
	}
	for index := range len(host) {
		if host[index] >= 0x80 {
			return "", fmt.Errorf("%w: hostname must contain only ASCII characters", ErrInvalid)
		}
	}
	host = strings.ToLower(host)
	for label := range strings.SplitSeq(host, ".") {
		if label == "" || len(label) > 63 || !lowercaseLetterOrDigit(label[0]) ||
			!lowercaseLetterOrDigit(label[len(label)-1]) {
			return "", fmt.Errorf("%w: malformed hostname", ErrInvalid)
		}
		for index := 1; index < len(label)-1; index++ {
			if !lowercaseLetterOrDigit(label[index]) && label[index] != '-' {
				return "", fmt.Errorf("%w: malformed hostname", ErrInvalid)
			}
		}
	}
	return host, nil
}

// lowercaseLetterOrDigit reports whether value is a lowercase ASCII letter or decimal digit.
func lowercaseLetterOrDigit(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

// numericHostname reports whether host could be interpreted as a noncanonical IPv4 address.
func numericHostname(host string) bool {
	for index := range len(host) {
		if host[index] != '.' && (host[index] < '0' || host[index] > '9') {
			return false
		}
	}
	return true
}
