// Package wsheader encodes and validates WireHop WebSocket admission headers.
package wsheader

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
)

var (
	// ErrInvalid indicates missing, duplicated, malformed, or inconsistent admission headers.
	ErrInvalid = errors.New("invalid WebSocket admission headers")
)

const (
	// Subprotocol is the WebSocket subprotocol for wire protocol version one.
	Subprotocol = "wirehop.v1"
	// MaxPathSize leaves room for bounded admission fields within the direct server's request-header budget.
	MaxPathSize = 8 * 1024
	// maximumBearerTokenSize keeps creation headers within the server admission budget.
	maximumBearerTokenSize = 4096
	// headerTarget carries the creation target.
	headerTarget = "WireHop-Target"
	// headerLaneID carries the stable lane identifier.
	headerLaneID = "WireHop-Lane-ID"
	// headerLaneGeneration carries the connection generation.
	headerLaneGeneration = "WireHop-Lane-Generation"
	// headerPathGroupID carries the proposed path group identifier.
	headerPathGroupID = "WireHop-Path-Group-ID"
	// headerNonce carries the replay-resistant creation nonce.
	headerNonce = "WireHop-Nonce"
	// headerTimestamp carries the Unix authentication timestamp.
	headerTimestamp = "WireHop-Timestamp"
	// headerMonotonicSend carries the client's clock-bootstrap timestamp.
	headerMonotonicSend = "WireHop-Monotonic-Send"
)

// Create is one WebSocket session-creation request.
type Create struct {
	Token           string
	Target          target.Endpoint
	LaneID          protocol.LaneID
	Generation      uint64
	PathGroupID     protocol.PathGroupID
	Nonce           protocol.Nonce
	UnixSeconds     int64
	MonotonicMicros uint64
}

// Headers returns canonical HTTP headers for request.
func Headers(request Create) (http.Header, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+request.Token)
	headers.Set(headerTarget, request.Target.String())
	headers.Set(headerLaneID, request.LaneID.String())
	headers.Set(headerLaneGeneration, strconv.FormatUint(request.Generation, 10))
	headers.Set(headerPathGroupID, request.PathGroupID.String())
	headers.Set(headerNonce, request.Nonce.String())
	headers.Set(headerTimestamp, strconv.FormatInt(request.UnixSeconds, 10))
	headers.Set(headerMonotonicSend, strconv.FormatUint(request.MonotonicMicros, 10))
	return headers, nil
}

// ParseCreate parses the required creation headers from request.
func ParseCreate(request *http.Request) (Create, error) {
	if request == nil || request.Method != http.MethodGet || request.URL == nil ||
		request.URL.ForceQuery || request.URL.RawQuery != "" ||
		request.URL.EscapedPath() == "" || len(request.URL.EscapedPath()) > MaxPathSize {
		return Create{}, ErrInvalid
	}
	authorization, err := single(request.Header, "Authorization")
	if err != nil {
		return Create{}, ErrInvalid
	}
	token, found := strings.CutPrefix(authorization, "Bearer ")
	if !found {
		return Create{}, ErrInvalid
	}
	targetValue, err := single(request.Header, headerTarget)
	if err != nil {
		return Create{}, err
	}
	endpoint, err := target.Parse(targetValue)
	if err != nil || endpoint.String() != targetValue {
		return Create{}, ErrInvalid
	}
	laneValue, err := single(request.Header, headerLaneID)
	if err != nil {
		return Create{}, err
	}
	laneID, err := protocol.ParseLaneID(laneValue)
	if err != nil {
		return Create{}, ErrInvalid
	}
	generationValue, err := single(request.Header, headerLaneGeneration)
	if err != nil {
		return Create{}, err
	}
	generation, err := strconv.ParseUint(generationValue, 10, 64)
	if err != nil {
		return Create{}, ErrInvalid
	}
	pathValue, err := single(request.Header, headerPathGroupID)
	if err != nil {
		return Create{}, err
	}
	pathGroupID, err := protocol.ParsePathGroupID(pathValue)
	if err != nil {
		return Create{}, ErrInvalid
	}
	nonceValue, err := single(request.Header, headerNonce)
	if err != nil {
		return Create{}, err
	}
	nonce, err := protocol.ParseNonce(nonceValue)
	if err != nil {
		return Create{}, ErrInvalid
	}
	timestampValue, err := single(request.Header, headerTimestamp)
	if err != nil {
		return Create{}, err
	}
	timestamp, err := strconv.ParseInt(timestampValue, 10, 64)
	if err != nil {
		return Create{}, ErrInvalid
	}
	monotonicValue, err := single(request.Header, headerMonotonicSend)
	if err != nil {
		return Create{}, err
	}
	monotonic, err := strconv.ParseUint(monotonicValue, 10, 64)
	if err != nil {
		return Create{}, ErrInvalid
	}
	parsed := Create{
		Token: token, Target: endpoint, LaneID: laneID, Generation: generation,
		PathGroupID: pathGroupID, Nonce: nonce, UnixSeconds: timestamp, MonotonicMicros: monotonic,
	}
	if err := parsed.Validate(); err != nil {
		return Create{}, err
	}
	return parsed, nil
}

// Validate verifies canonical request fields and an HTTP-safe bearer token.
func (r Create) Validate() error {
	if ValidateBearerToken(r.Token) != nil || !r.Target.Valid() || r.LaneID.IsZero() || r.Generation == 0 ||
		r.PathGroupID.IsZero() || r.Nonce == (protocol.Nonce{}) || r.UnixSeconds <= 0 {
		return ErrInvalid
	}
	return nil
}

// single returns one nonempty header value and rejects repeated fields.
func single(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) != 1 || values[0] == "" {
		return "", fmt.Errorf("%w: %s", ErrInvalid, name)
	}
	return values[0], nil
}

// ValidateBearerToken verifies the RFC 6750 b64token character grammar used by session creation.
func ValidateBearerToken(token string) error {
	if token == "" || len(token) > maximumBearerTokenSize {
		return ErrInvalid
	}
	padding := false
	characters := 0
	for _, value := range []byte(token) {
		if value == '=' {
			padding = true
			continue
		}
		if padding || !bearerCharacter(value) {
			return ErrInvalid
		}
		characters++
	}
	if characters == 0 {
		return ErrInvalid
	}
	return nil
}

// bearerCharacter reports whether value is one non-padding b64token byte.
func bearerCharacter(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' ||
		value == '-' || value == '.' || value == '_' || value == '~' || value == '+' || value == '/'
}
