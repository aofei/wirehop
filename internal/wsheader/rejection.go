package wsheader

import (
	"encoding/base64"
	"net/http"

	"github.com/aofei/wirehop/internal/protocol"
)

const (
	// headerRejection carries one authenticated binary admission rejection.
	headerRejection = "WireHop-Rejection"
	// maximumEncodedRejectionSize bounds the base64url-encoded rejection header.
	maximumEncodedRejectionSize = 1024
)

// SetRejection encodes rejection into headers.
func SetRejection(headers http.Header, rejection protocol.ServerHello) error {
	if headers == nil || rejection.Result != protocol.ServerRejected {
		return ErrInvalid
	}
	encoded, err := protocol.MarshalServerHello(rejection)
	if err != nil {
		return err
	}
	value := base64.RawURLEncoding.EncodeToString(encoded)
	if len(value) > maximumEncodedRejectionSize {
		return ErrInvalid
	}
	headers.Set(headerRejection, value)
	return nil
}

// ParseRejection decodes one canonical rejection from headers.
func ParseRejection(headers http.Header) (protocol.ServerHello, error) {
	value, err := single(headers, headerRejection)
	if err != nil || len(value) > maximumEncodedRejectionSize {
		return protocol.ServerHello{}, ErrInvalid
	}
	encoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil {
		return protocol.ServerHello{}, ErrInvalid
	}
	rejection, err := protocol.ParseServerHello(encoded)
	if err != nil || rejection.Result != protocol.ServerRejected {
		return protocol.ServerHello{}, ErrInvalid
	}
	return rejection, nil
}
