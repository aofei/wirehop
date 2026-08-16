package wgpacket

import (
	"encoding/base64"
	"errors"
)

// reservedSize is the fixed WireGuard reserved field width.
const reservedSize = 3

// errInvalidReservedEncoding reports malformed reserved field configuration.
var errInvalidReservedEncoding = errors.New("expected canonical Base64 encoding of exactly three bytes")

// Reserved is an optional three-byte WireGuard reserved value whose zero value disables endpoint translation.
type Reserved [reservedSize]byte

// Enabled reports whether reserved enables endpoint translation.
func (r Reserved) Enabled() bool {
	return r != Reserved{}
}

// MarshalText returns the canonical Base64 representation, or an empty default representation when disabled.
func (r Reserved) MarshalText() ([]byte, error) {
	if !r.Enabled() {
		return nil, nil
	}
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(r)))
	base64.StdEncoding.Encode(encoded, r[:])
	return encoded, nil
}

// UnmarshalText parses and replaces the configured reserved value.
func (r *Reserved) UnmarshalText(encoded []byte) error {
	if len(encoded) != base64.StdEncoding.EncodedLen(reservedSize) {
		return errInvalidReservedEncoding
	}
	var parsed Reserved
	decoded, err := base64.StdEncoding.Strict().Decode(parsed[:], encoded)
	if err != nil || decoded != len(parsed) {
		return errInvalidReservedEncoding
	}
	if !parsed.Enabled() {
		return errors.New("value must not be all zero")
	}
	*r = parsed
	return nil
}
