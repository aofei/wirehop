package auth

import "errors"

// ErrInvalidToken indicates an empty or oversized long-term authentication key.
var ErrInvalidToken = errors.New("invalid authentication token")

// ValidateToken bounds the configured HMAC key without imposing a wire character encoding.
func ValidateToken(token []byte) error {
	if len(token) == 0 || len(token) > 4096 {
		return ErrInvalidToken
	}
	return nil
}
