package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

func TestParseUvarint(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   uint64
		encoded []byte
	}{
		{name: "Zero", encoded: []byte{0}},
		{name: "One", value: 1, encoded: []byte{1}},
		{name: "SevenBits", value: 127, encoded: []byte{0x7f}},
		{name: "EightBits", value: 128, encoded: []byte{0x80, 1}},
		{name: "FourteenBits", value: 16383, encoded: []byte{0xff, 0x7f}},
		{name: "FifteenBits", value: 16384, encoded: []byte{0x80, 0x80, 1}},
		{name: "ThirtyTwoBits", value: math.MaxUint32, encoded: []byte{0xff, 0xff, 0xff, 0xff, 0x0f}},
		{name: "HighBit", value: 1 << 63, encoded: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 1}},
		{name: "Maximum", value: math.MaxUint64, encoded: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			value, width, err := parseUvarint(append(bytes.Clone(tt.encoded), 42))
			if err != nil || value != tt.value || width != len(tt.encoded) {
				t.Fatalf("parseUvarint() = %d, %d, %v", value, width, err)
			}
			if uvarintSize(tt.value) != len(tt.encoded) || !bytes.Equal(binary.AppendUvarint(nil, tt.value), tt.encoded) {
				t.Fatal("integer size or encoding differs from the wire vector")
			}
		})
	}
	for _, tt := range []struct {
		name    string
		encoded []byte
		want    error
	}{
		{name: "Empty", want: io.ErrUnexpectedEOF},
		{name: "Truncated", encoded: []byte{0x80}, want: io.ErrUnexpectedEOF},
		{name: "NonminimalZero", encoded: []byte{0x80, 0}, want: ErrInvalidInteger},
		{name: "NonminimalOne", encoded: []byte{0x81, 0}, want: ErrInvalidInteger},
		{name: "Overflow", encoded: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}, want: ErrInvalidInteger},
		{name: "TooLong", encoded: bytes.Repeat([]byte{0x80}, 11), want: ErrInvalidInteger},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := parseUvarint(tt.encoded); !errors.Is(err, tt.want) {
				t.Fatalf("parseUvarint() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func FuzzParseUvarint(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{0x80, 1})
	f.Add(bytes.Repeat([]byte{0xff}, 10))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		value, width, err := parseUvarint(encoded)
		if err == nil && !bytes.Equal(binary.AppendUvarint(nil, value), encoded[:width]) {
			t.Fatal("accepted integer differs from its canonical encoding")
		}
	})
}
