package wsheader

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/aofei/wirehop/internal/protocol"
)

const (
	// headerSessionID carries the retained session identifier.
	headerSessionID = "WireHop-Session-ID"
	// joinCanonicalSize is the exact authenticated join input size excluding method and path bytes.
	joinCanonicalSize = 1 + 2 + protocol.SessionIDSize + protocol.LaneIDSize + 8 + protocol.PathGroupIDSize + protocol.NonceSize + 8 + 8
)

// Join is one WebSocket lane join request.
type Join struct {
	Method          string
	Path            string
	SessionID       protocol.SessionID
	LaneID          protocol.LaneID
	Generation      uint64
	PathGroupID     protocol.PathGroupID
	Nonce           protocol.Nonce
	UnixSeconds     int64
	MonotonicMicros uint64
	AuthTag         protocol.AuthTag
}

// SignJoin validates request and authenticates its canonical encoding.
func SignJoin(request *Join, secret protocol.SessionSecret) error {
	if secret == (protocol.SessionSecret{}) {
		return protocol.ErrMissingAuthKey
	}
	encoded, err := marshalJoinUnsigned(*request)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret[:])
	mac.Write(encoded)
	copy(request.AuthTag[:], mac.Sum(nil))
	return nil
}

// VerifyJoin verifies request against the ephemeral session secret.
func VerifyJoin(request Join, secret protocol.SessionSecret) error {
	if secret == (protocol.SessionSecret{}) {
		return protocol.ErrMissingAuthKey
	}
	encoded, err := marshalJoinUnsigned(request)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret[:])
	mac.Write(encoded)
	if !hmac.Equal(request.AuthTag[:], mac.Sum(nil)) {
		return protocol.ErrAuthenticationFailed
	}
	return nil
}

// JoinHeaders returns canonical HTTP headers for an already signed join.
func JoinHeaders(request Join) (http.Header, error) {
	if _, err := marshalJoinUnsigned(request); err != nil {
		return nil, err
	}
	if request.AuthTag == (protocol.AuthTag{}) {
		return nil, ErrInvalid
	}
	headers := make(http.Header)
	headers.Set("Authorization", "WireHop-HMAC "+hex.EncodeToString(request.AuthTag[:]))
	headers.Set(headerSessionID, request.SessionID.String())
	headers.Set(headerLaneID, request.LaneID.String())
	headers.Set(headerLaneGeneration, strconv.FormatUint(request.Generation, 10))
	headers.Set(headerPathGroupID, request.PathGroupID.String())
	headers.Set(headerNonce, request.Nonce.String())
	headers.Set(headerTimestamp, strconv.FormatInt(request.UnixSeconds, 10))
	headers.Set(headerMonotonicSend, strconv.FormatUint(request.MonotonicMicros, 10))
	return headers, nil
}

// ParseJoin parses the required signed join fields from request.
func ParseJoin(request *http.Request) (Join, error) {
	if request == nil || request.URL == nil || request.URL.ForceQuery || request.URL.RawQuery != "" {
		return Join{}, ErrInvalid
	}
	authorization, err := single(request.Header, "Authorization")
	if err != nil {
		return Join{}, ErrInvalid
	}
	authValue, found := strings.CutPrefix(authorization, "WireHop-HMAC ")
	if !found {
		return Join{}, ErrInvalid
	}
	if len(authValue) != hex.EncodedLen(len(protocol.AuthTag{})) {
		return Join{}, ErrInvalid
	}
	var authTag protocol.AuthTag
	if _, err := hex.Decode(authTag[:], []byte(authValue)); err != nil {
		return Join{}, ErrInvalid
	}
	sessionValue, err := single(request.Header, headerSessionID)
	if err != nil {
		return Join{}, err
	}
	sessionID, err := protocol.ParseSessionID(sessionValue)
	if err != nil {
		return Join{}, ErrInvalid
	}
	laneValue, err := single(request.Header, headerLaneID)
	if err != nil {
		return Join{}, err
	}
	laneID, err := protocol.ParseLaneID(laneValue)
	if err != nil {
		return Join{}, ErrInvalid
	}
	generationValue, err := single(request.Header, headerLaneGeneration)
	if err != nil {
		return Join{}, err
	}
	generation, err := strconv.ParseUint(generationValue, 10, 64)
	if err != nil {
		return Join{}, ErrInvalid
	}
	pathValue, err := single(request.Header, headerPathGroupID)
	if err != nil {
		return Join{}, err
	}
	pathGroupID, err := protocol.ParsePathGroupID(pathValue)
	if err != nil {
		return Join{}, ErrInvalid
	}
	nonceValue, err := single(request.Header, headerNonce)
	if err != nil {
		return Join{}, err
	}
	nonce, err := protocol.ParseNonce(nonceValue)
	if err != nil {
		return Join{}, ErrInvalid
	}
	timestampValue, err := single(request.Header, headerTimestamp)
	if err != nil {
		return Join{}, err
	}
	timestamp, err := strconv.ParseInt(timestampValue, 10, 64)
	if err != nil {
		return Join{}, ErrInvalid
	}
	monotonicValue, err := single(request.Header, headerMonotonicSend)
	if err != nil {
		return Join{}, err
	}
	monotonic, err := strconv.ParseUint(monotonicValue, 10, 64)
	if err != nil {
		return Join{}, ErrInvalid
	}
	path := request.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	join := Join{
		Method: request.Method, Path: path, SessionID: sessionID, LaneID: laneID, Generation: generation,
		PathGroupID: pathGroupID, Nonce: nonce, UnixSeconds: timestamp, MonotonicMicros: monotonic, AuthTag: authTag,
	}
	if _, err := marshalJoinUnsigned(join); err != nil {
		return Join{}, err
	}
	return join, nil
}

// marshalJoinUnsigned returns an unambiguous canonical join encoding.
func marshalJoinUnsigned(request Join) ([]byte, error) {
	if request.Method != http.MethodGet || request.Path == "" || len(request.Path) > MaxPathSize ||
		request.SessionID.IsZero() || request.LaneID.IsZero() || request.Generation == 0 ||
		request.PathGroupID.IsZero() || request.Nonce == (protocol.Nonce{}) || request.UnixSeconds <= 0 {
		return nil, ErrInvalid
	}
	encoded := make([]byte, joinCanonicalSize+len(request.Method)+len(request.Path))
	encoded[0] = byte(len(request.Method))
	copy(encoded[1:], request.Method)
	offset := 1 + len(request.Method)
	binary.BigEndian.PutUint16(encoded[offset:offset+2], uint16(len(request.Path)))
	offset += 2
	copy(encoded[offset:], request.Path)
	offset += len(request.Path)
	copy(encoded[offset:], request.SessionID[:])
	offset += protocol.SessionIDSize
	copy(encoded[offset:], request.LaneID[:])
	offset += protocol.LaneIDSize
	binary.BigEndian.PutUint64(encoded[offset:offset+8], request.Generation)
	offset += 8
	copy(encoded[offset:], request.PathGroupID[:])
	offset += protocol.PathGroupIDSize
	copy(encoded[offset:], request.Nonce[:])
	offset += protocol.NonceSize
	binary.BigEndian.PutUint64(encoded[offset:offset+8], uint64(request.UnixSeconds))
	offset += 8
	binary.BigEndian.PutUint64(encoded[offset:offset+8], request.MonotonicMicros)
	return encoded, nil
}
