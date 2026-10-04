package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
)

// ErrInvalidInteger indicates an overflowing or nonminimal unsigned LEB128 encoding.
var ErrInvalidInteger = errors.New("invalid unsigned integer encoding")

// uvarintSize returns the shortest unsigned LEB128 encoding length of value.
func uvarintSize(value uint64) int {
	return (bits.Len64(value|1) + 6) / 7
}

// parseUvarint decodes one shortest-form unsigned LEB128 integer.
func parseUvarint(encoded []byte) (uint64, int, error) {
	value, width := binary.Uvarint(encoded)
	if width == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	// A multi-byte shortest form cannot end in a zero value group.
	if width < 0 || width > 1 && encoded[width-1] == 0 {
		return 0, 0, ErrInvalidInteger
	}
	return value, width, nil
}

// integerPayload allocates a fixed-width prefix followed by canonical unsigned integers. The caller initializes the
// prefix before exposing the payload.
func integerPayload(prefixSize int, values ...uint64) []byte {
	size := prefixSize
	for _, value := range values {
		size += uvarintSize(value)
	}
	payload := make([]byte, size)
	offset := prefixSize
	for _, value := range values {
		offset += binary.PutUvarint(payload[offset:], value)
	}
	return payload
}

// parseIntegers consumes canonical unsigned integers and returns the remaining payload.
func parseIntegers(payload []byte, values ...*uint64) ([]byte, error) {
	for _, value := range values {
		decoded, width, err := parseUvarint(payload)
		if err != nil {
			return nil, err
		}
		*value = decoded
		payload = payload[width:]
	}
	return payload, nil
}
