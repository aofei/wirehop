// Package lanespec parses WireHop client lane declarations.
package lanespec

import (
	"encoding/csv"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/aofei/wirehop/internal/laneurl"
)

var (
	// ErrInvalid indicates a malformed or inconsistent client lane declaration.
	ErrInvalid = errors.New("invalid lane declaration")
)

// invalidError describes one malformed lane declaration while preserving its underlying cause.
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

// Spec is one immutable client lane declaration.
type Spec struct {
	url       laneurl.URL
	resolveIP netip.Addr
}

// Parse validates a URL shorthand or structured client lane declaration.
func Parse(value string) (Spec, error) {
	if !structured(value) {
		url, err := laneurl.ParseDial(value)
		if err != nil {
			return Spec{}, newInvalidError(err.Error(), err)
		}
		return Spec{url: url}, nil
	}
	fields, err := parseFields(value)
	if err != nil {
		return Spec{}, err
	}
	var urlValue string
	var resolveValue string
	for _, field := range fields {
		if field == "" || field != strings.TrimSpace(field) {
			return Spec{}, newInvalidError("fields cannot be empty or surrounded by whitespace", nil)
		}
		key, fieldValue, ok := strings.Cut(field, "=")
		if !ok || fieldValue == "" {
			return Spec{}, newInvalidError(fmt.Sprintf("field %q requires a value", key), nil)
		}
		switch key {
		case "url":
			if urlValue != "" {
				return Spec{}, newInvalidError(fmt.Sprintf("duplicate field %q", key), nil)
			}
			urlValue = fieldValue
		case "resolve":
			if resolveValue != "" {
				return Spec{}, newInvalidError(fmt.Sprintf("duplicate field %q", key), nil)
			}
			resolveValue = fieldValue
		default:
			return Spec{}, newInvalidError(fmt.Sprintf("unknown field %q", key), nil)
		}
	}
	if urlValue == "" {
		return Spec{}, newInvalidError(`structured declaration requires field "url"`, nil)
	}
	if resolveValue == "" {
		return Spec{}, newInvalidError(`structured declaration requires field "resolve"`, nil)
	}
	url, err := laneurl.ParseDial(urlValue)
	if err != nil {
		return Spec{}, newInvalidError(err.Error(), err)
	}
	if _, err := netip.ParseAddr(url.Hostname()); err == nil {
		return Spec{}, newInvalidError("resolve requires a URL hostname rather than an IP address", nil)
	}
	resolveIP, err := netip.ParseAddr(resolveValue)
	if err != nil {
		return Spec{}, newInvalidError("resolve must be an unbracketed IP address without a port", err)
	}
	if resolveIP.Zone() != "" {
		return Spec{}, newInvalidError("resolve IP cannot contain a zone", nil)
	}
	resolveIP = resolveIP.Unmap()
	if resolveIP.Is6() && resolveIP.IsLinkLocalUnicast() {
		return Spec{}, newInvalidError("resolve cannot use an IPv6 link-local address without a zone", nil)
	}
	if !laneurl.ValidDialIP(resolveIP) {
		return Spec{}, newInvalidError("resolve must be a unicast IP address", nil)
	}
	return Spec{url: url, resolveIP: resolveIP}, nil
}

// structured reports whether value uses key-value fields rather than URL shorthand.
func structured(value string) bool {
	if strings.HasPrefix(value, `"`) {
		return true
	}
	schemeIndex := strings.Index(value, "://")
	fieldIndex := strings.IndexByte(value, '=')
	return fieldIndex >= 0 && (schemeIndex < 0 || fieldIndex < schemeIndex)
}

// parseFields reads exactly one CSV record containing key-value fields.
func parseFields(value string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(value))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, newInvalidError(fmt.Sprintf("malformed field list: %v", err), err)
	}
	if len(records) != 1 {
		return nil, newInvalidError("structured declaration must contain one record", nil)
	}
	return records[0], nil
}

// newInvalidError creates a clean lane declaration diagnostic classified by [ErrInvalid].
func newInvalidError(message string, cause error) error {
	return &invalidError{message: message, cause: cause}
}

// Valid reports whether s contains a parsed client lane declaration.
func (s Spec) Valid() bool {
	return s.url.Valid()
}

// URL returns the logical carrier URL.
func (s Spec) URL() laneurl.URL {
	return s.url
}

// ResolveIP returns the fixed resolution result, or an invalid address when normal DNS applies.
func (s Spec) ResolveIP() netip.Addr {
	return s.resolveIP
}

// DialAddress returns the actual host and port supplied to the network dialer.
func (s Spec) DialAddress() string {
	if !s.resolveIP.IsValid() {
		return s.url.Address()
	}
	return net.JoinHostPort(s.resolveIP.String(), s.url.Port())
}
