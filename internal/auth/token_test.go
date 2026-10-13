package auth

import (
	"bytes"
	"errors"
	"testing"
)

func TestValidateToken(t *testing.T) {
	for _, tt := range []struct {
		name  string
		token []byte
		valid bool
	}{
		{name: "Empty"},
		{name: "SingleByte", token: []byte{0}, valid: true},
		{name: "Text", token: []byte("secret with spaces=and/padding"), valid: true},
		{name: "Binary", token: []byte{0, 10, 13, 127, 255}, valid: true},
		{name: "Maximum", token: bytes.Repeat([]byte{255}, 4096), valid: true},
		{name: "Oversized", token: bytes.Repeat([]byte{'a'}, 4097)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateToken(tt.token)
			if tt.valid && err != nil || !tt.valid && !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("ValidateToken() error = %v, want valid %t", err, tt.valid)
			}
		})
	}
}
