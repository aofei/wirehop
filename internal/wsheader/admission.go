// Package wsheader encodes and validates WireHop WebSocket admission headers.
package wsheader

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
)

// ErrInvalid indicates missing, duplicated, malformed, or inconsistent admission headers.
var ErrInvalid = errors.New("invalid WebSocket admission headers")

const (
	// Subprotocol is the WebSocket subprotocol for wire protocol version one.
	Subprotocol = "wirehop.v1"
	// MaxPathSize leaves room for bounded admission fields within the direct server's request-header budget.
	MaxPathSize = 8 * 1024
	// headerTarget carries the creation target.
	headerTarget = "WireHop-Target"
	// headerSessionID carries the retained session identifier.
	headerSessionID = "WireHop-Session-ID"
	// headerLaneID carries the stable lane identifier.
	headerLaneID = "WireHop-Lane-ID"
	// headerLaneGeneration carries the connection generation.
	headerLaneGeneration = "WireHop-Lane-Generation"
	// headerPathGroupID carries the proposed path group identifier.
	headerPathGroupID = "WireHop-Path-Group-ID"
	// headerNonce carries the replay-resistant admission nonce.
	headerNonce = "WireHop-Nonce"
	// headerTimestamp carries the Unix authentication timestamp.
	headerTimestamp = "WireHop-Timestamp"
	// headerMonotonicSend carries the client's clock-bootstrap timestamp.
	headerMonotonicSend = "WireHop-Monotonic-Send"
)

// Headers signs hello for an HTTP GET to path and returns its canonical admission headers.
func Headers(path string, hello protocol.ClientHello, key []byte) (http.Header, error) {
	tag, err := authenticate(http.MethodGet, path, hello, key)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Authorization", "WireHop-HMAC "+hex.EncodeToString(tag[:]))
	if hello.Mode == protocol.HelloCreate {
		headers.Set(headerTarget, hello.Target.String())
	} else {
		headers.Set(headerSessionID, hello.SessionID.String())
	}
	headers.Set(headerLaneID, hello.LaneID.String())
	headers.Set(headerLaneGeneration, strconv.FormatUint(hello.Generation, 10))
	headers.Set(headerPathGroupID, hello.PathGroupID.String())
	headers.Set(headerNonce, hello.Nonce.String())
	headers.Set(headerTimestamp, strconv.FormatInt(hello.UnixSeconds, 10))
	headers.Set(headerMonotonicSend, strconv.FormatUint(hello.MonotonicMicros, 10))
	return headers, nil
}

// Parse decodes one creation or join request without authenticating it.
func Parse(request *http.Request) (protocol.ClientHello, error) {
	if request == nil || request.Method != http.MethodGet || request.URL == nil ||
		request.URL.ForceQuery || request.URL.RawQuery != "" ||
		request.URL.EscapedPath() == "" || len(request.URL.EscapedPath()) > MaxPathSize {
		return protocol.ClientHello{}, ErrInvalid
	}
	var hello protocol.ClientHello
	authorization, err := single(request.Header, "Authorization")
	if err != nil {
		return hello, err
	}
	value, found := strings.CutPrefix(authorization, "WireHop-HMAC ")
	if !found || len(value) != hex.EncodedLen(len(hello.AuthTag)) {
		return hello, ErrInvalid
	}
	if _, err := hex.Decode(hello.AuthTag[:], []byte(value)); err != nil {
		return hello, ErrInvalid
	}
	// The mutually exclusive target and session headers determine the authenticated operation.
	if len(request.Header.Values(headerTarget)) != 0 {
		if len(request.Header.Values(headerSessionID)) != 0 {
			return hello, ErrInvalid
		}
		value, err := single(request.Header, headerTarget)
		if err != nil {
			return hello, err
		}
		hello.Target, err = target.Parse(value)
		if err != nil || hello.Target.String() != value {
			return hello, ErrInvalid
		}
		hello.Mode = protocol.HelloCreate
	} else {
		value, err := single(request.Header, headerSessionID)
		if err != nil {
			return hello, err
		}
		hello.SessionID, err = protocol.ParseSessionID(value)
		if err != nil {
			return hello, ErrInvalid
		}
		hello.Mode = protocol.HelloJoin
	}
	for _, field := range []struct {
		name string
		out  *uint64
	}{
		{headerLaneID, (*uint64)(&hello.LaneID)},
		{headerLaneGeneration, &hello.Generation},
		{headerPathGroupID, (*uint64)(&hello.PathGroupID)},
		{headerMonotonicSend, &hello.MonotonicMicros},
	} {
		value, err := single(request.Header, field.name)
		if err != nil {
			return hello, err
		}
		number, err := strconv.ParseUint(value, 10, 64)
		if err != nil || strconv.FormatUint(number, 10) != value {
			return hello, ErrInvalid
		}
		*field.out = number
	}
	nonceValue, err := single(request.Header, headerNonce)
	if err != nil {
		return hello, err
	}
	hello.Nonce, err = protocol.ParseNonce(nonceValue)
	if err != nil {
		return hello, ErrInvalid
	}
	timestampValue, err := single(request.Header, headerTimestamp)
	if err != nil {
		return hello, err
	}
	hello.UnixSeconds, err = strconv.ParseInt(timestampValue, 10, 64)
	if err != nil || strconv.FormatInt(hello.UnixSeconds, 10) != timestampValue {
		return hello, ErrInvalid
	}
	if _, err := protocol.MarshalClientHello(hello); err != nil {
		return hello, ErrInvalid
	}
	return hello, nil
}

// Verify authenticates a parsed hello against the received method and escaped path.
func Verify(request *http.Request, hello protocol.ClientHello, key []byte) error {
	tag, err := authenticate(request.Method, request.URL.EscapedPath(), hello, key)
	if err != nil {
		return err
	}
	if !hmac.Equal(hello.AuthTag[:], tag[:]) {
		return protocol.ErrAuthenticationFailed
	}
	return nil
}

// authenticate binds the HTTP endpoint to the complete canonical unsigned client hello.
func authenticate(method, path string, hello protocol.ClientHello, key []byte) (protocol.AuthTag, error) {
	if len(key) == 0 {
		return protocol.AuthTag{}, protocol.ErrMissingAuthKey
	}
	if method != http.MethodGet || path == "" || len(path) > MaxPathSize {
		return protocol.AuthTag{}, ErrInvalid
	}
	encoded, err := protocol.MarshalClientHello(hello)
	if err != nil {
		return protocol.AuthTag{}, ErrInvalid
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte{'W', 'H', 'O', 'W', byte(len(method))})
	mac.Write([]byte(method))
	mac.Write(binary.BigEndian.AppendUint16(nil, uint16(len(path))))
	mac.Write([]byte(path))
	mac.Write(encoded[:len(encoded)-len(hello.AuthTag)])
	var tag protocol.AuthTag
	copy(tag[:], mac.Sum(nil))
	return tag, nil
}

// single returns one nonempty header value and rejects repeated fields.
func single(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) != 1 || values[0] == "" {
		return "", fmt.Errorf("%w: %s", ErrInvalid, name)
	}
	return values[0], nil
}
