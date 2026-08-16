// Package laneurl parses WireHop carrier URLs.
package laneurl

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/aofei/wirehop/internal/wsheader"
)

var (
	// ErrInvalid indicates a malformed or unsupported WireHop lane URL.
	ErrInvalid = errors.New("invalid lane URL")
)

// invalidError describes one malformed lane URL while preserving its underlying cause.
type invalidError struct {
	message string
	cause   error
}

// Error returns the actionable validation detail.
func (e *invalidError) Error() string {
	return e.message
}

// Unwrap returns the underlying parser error when one exists.
func (e *invalidError) Unwrap() error {
	return e.cause
}

// Is classifies every invalidError as [ErrInvalid].
func (e *invalidError) Is(target error) bool {
	return target == ErrInvalid
}

// Scheme identifies one supported carrier protocol.
type Scheme string

const (
	// TCP identifies an insecure raw TCP carrier.
	TCP Scheme = "tcp"
	// TLS identifies a raw TCP carrier protected by TLS.
	TLS Scheme = "tls"
	// WS identifies an insecure WebSocket carrier.
	WS Scheme = "ws"
	// WSS identifies a WebSocket carrier protected by TLS.
	WSS Scheme = "wss"
)

// Valid reports whether scheme is supported by WireHop.
func (s Scheme) Valid() bool {
	return s == TCP || s == TLS || s == WS || s == WSS
}

// WebSocket reports whether scheme uses a WebSocket carrier.
func (s Scheme) WebSocket() bool {
	return s == WS || s == WSS
}

// Secure reports whether scheme protects the carrier with TLS.
func (s Scheme) Secure() bool {
	return s == TLS || s == WSS
}

// URL is one validated WireHop lane URL.
type URL struct {
	value  *url.URL
	scheme Scheme
}

// ParseDial validates a client lane URL and applies the standard WebSocket port when omitted.
func ParseDial(value string) (URL, error) {
	return parse(value, false)
}

// ParseListen validates a server lane URL and applies the standard WebSocket port when omitted.
func ParseListen(value string) (URL, error) {
	return parse(value, true)
}

// parse validates a dial or listen lane URL.
func parse(value string, listen bool) (URL, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		if parseError, ok := errors.AsType[*url.Error](err); ok {
			err = parseError.Err
		}
		return URL{}, newInvalidError(fmt.Sprintf("malformed URL: %v", err), err)
	}
	scheme := Scheme(strings.ToLower(parsed.Scheme))
	if scheme == "" {
		return URL{}, newInvalidError("carrier scheme is required", nil)
	}
	if !scheme.Valid() {
		return URL{}, newInvalidError(fmt.Sprintf("unsupported carrier scheme %q", scheme), nil)
	}
	parsed.Scheme = string(scheme)
	if parsed.User != nil {
		return URL{}, newInvalidError("user information is not allowed", nil)
	}
	if parsed.Host == "" {
		if listen {
			return URL{}, newInvalidError("server listener URL requires a host or port", nil)
		}
		return URL{}, newInvalidError("client lane URL requires a host", nil)
	}
	if parsed.ForceQuery || parsed.RawQuery != "" {
		return URL{}, newInvalidError("query parameters are not allowed", nil)
	}
	if parsed.Fragment != "" {
		return URL{}, newInvalidError("fragments are not allowed", nil)
	}
	host, port, err := splitAuthority(parsed.Host, scheme)
	if err != nil {
		return URL{}, err
	}
	if !listen && host == "" {
		return URL{}, newInvalidError("client lane URL requires a host", nil)
	}
	if port == "" {
		return URL{}, newInvalidError("port is required", nil)
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return URL{}, newInvalidError("port must be between 1 and 65535", err)
	}
	if address, err := netip.ParseAddr(host); err == nil {
		if !listen {
			address = address.Unmap()
			if address.Is6() && address.IsLinkLocalUnicast() && address.Zone() == "" {
				return URL{}, newInvalidError("client lane IPv6 link-local address requires a zone identifier", nil)
			}
			if !ValidDialIP(address) {
				return URL{}, newInvalidError("client lane URL requires a unicast IP address", nil)
			}
		}
		host = address.String()
	} else {
		if strings.ContainsAny(host, "[]:") || strings.HasPrefix(parsed.Host, "[") {
			return URL{}, newInvalidError(fmt.Sprintf("invalid host %q", host), nil)
		}
		host = strings.ToLower(host)
	}
	parsed.Host = net.JoinHostPort(host, strconv.FormatUint(portNumber, 10))
	if !scheme.WebSocket() && parsed.EscapedPath() != "" {
		return URL{}, newInvalidError(fmt.Sprintf("%s URLs cannot contain a path", scheme), nil)
	}
	if scheme.WebSocket() && parsed.Path == "" {
		parsed.Path = "/"
	}
	if len(parsed.EscapedPath()) > wsheader.MaxPathSize {
		return URL{}, newInvalidError(fmt.Sprintf("WebSocket path exceeds %d bytes", wsheader.MaxPathSize), nil)
	}
	parsed.RawPath = canonicalEscapes(parsed.EscapedPath())
	return URL{value: parsed, scheme: scheme}, nil
}

// ValidDialIP reports whether address can identify one unicast carrier destination.
func ValidDialIP(address netip.Addr) bool {
	if !address.IsValid() || address.IsUnspecified() || address.IsMulticast() {
		return false
	}
	if address.Is6() && address.IsLinkLocalUnicast() && address.Zone() == "" {
		return false
	}
	return !address.Is4() || address.As4() != [4]byte{255, 255, 255, 255}
}

// splitAuthority returns an explicit host and port, applying standard WebSocket defaults when the port is omitted.
func splitAuthority(authority string, scheme Scheme) (string, string, error) {
	host, port, err := net.SplitHostPort(authority)
	if err == nil {
		return host, port, nil
	}
	parsed := &url.URL{Host: authority}
	host = parsed.Hostname()
	expected := host
	if strings.Contains(host, ":") {
		expected = "[" + host + "]"
	}
	if host != "" && authority == expected {
		switch scheme {
		case WS:
			return host, "80", nil
		case WSS:
			return host, "443", nil
		default:
			return "", "", newInvalidError(fmt.Sprintf("%s URLs require an explicit port", scheme), nil)
		}
	}
	if address, parseErr := netip.ParseAddr(authority); parseErr == nil && address.Is6() {
		return "", "", newInvalidError("IPv6 host must be enclosed in brackets", nil)
	}
	return "", "", newInvalidError(fmt.Sprintf("invalid host or port: %v", err), err)
}

// newInvalidError creates a clean lane URL diagnostic classified by [ErrInvalid].
func newInvalidError(message string, cause error) error {
	return &invalidError{message: message, cause: cause}
}

// canonicalEscapes normalizes hexadecimal digits without decoding path-significant escapes.
func canonicalEscapes(path string) string {
	encoded := []byte(path)
	for index := 0; index+2 < len(encoded); index++ {
		if encoded[index] != '%' {
			continue
		}
		encoded[index+1] = upperHex(encoded[index+1])
		encoded[index+2] = upperHex(encoded[index+2])
		index += 2
	}
	return string(encoded)
}

// upperHex converts one lowercase hexadecimal digit to uppercase.
func upperHex(value byte) byte {
	if value >= 'a' && value <= 'f' {
		return value - ('a' - 'A')
	}
	return value
}

// Scheme returns the validated carrier scheme.
func (u URL) Scheme() Scheme {
	return u.scheme
}

// Valid reports whether u contains a parsed supported lane URL.
func (u URL) Valid() bool {
	return u.value != nil && u.scheme.Valid()
}

// Address returns the canonical host and port used for dialing or listening.
func (u URL) Address() string {
	return u.value.Host
}

// Hostname returns the destination hostname without brackets or port.
func (u URL) Hostname() string {
	return u.value.Hostname()
}

// Port returns the canonical destination port.
func (u URL) Port() string {
	return u.value.Port()
}

// EscapedPath returns the canonical HTTP request path for a WebSocket lane.
func (u URL) EscapedPath() string {
	return u.value.EscapedPath()
}

// String returns the canonical lane URL.
func (u URL) String() string {
	return u.value.String()
}
