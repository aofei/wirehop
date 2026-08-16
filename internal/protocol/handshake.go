package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/aofei/wirehop/internal/target"
)

const (
	// clientHelloUnsignedFixedSize is the authenticated prefix before the optional target text.
	clientHelloUnsignedFixedSize = 94
	// clientHelloMinimumSize is a join hello with no target text.
	clientHelloMinimumSize = clientHelloUnsignedFixedSize + sha256.Size
	// MaxClientHelloSize bounds a creation hello containing the longest canonical target.
	MaxClientHelloSize = clientHelloMinimumSize + target.MaxTextSize
	// serverHelloFixedSize is the encoded server response size excluding its diagnostic.
	serverHelloFixedSize = 146
	// serverHelloUnsignedFixedSize excludes the trailing authentication tag.
	serverHelloUnsignedFixedSize = serverHelloFixedSize - sha256.Size
	// MaxDiagnosticSize bounds a peer-controlled handshake diagnostic.
	MaxDiagnosticSize = 512
)

var (
	// ErrInvalidMagic indicates input that does not carry a WireHop protocol preface.
	ErrInvalidMagic = errors.New("invalid protocol magic")
	// ErrUnsupportedVersion indicates an incompatible WireHop protocol version.
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
	// ErrInvalidClientHello indicates inconsistent client hello fields.
	ErrInvalidClientHello = errors.New("invalid client hello")
	// ErrInvalidServerHello indicates inconsistent server hello fields.
	ErrInvalidServerHello = errors.New("invalid server hello")
	// ErrMissingAuthKey indicates an empty long-term token or session secret.
	ErrMissingAuthKey = errors.New("missing authentication key")
	// ErrAuthenticationFailed indicates a mismatched handshake authentication tag.
	ErrAuthenticationFailed = errors.New("authentication failed")
	// ErrDiagnosticTooLarge indicates a handshake diagnostic above its protocol limit.
	ErrDiagnosticTooLarge = errors.New("diagnostic too large")
)

var (
	// clientMagic identifies a raw-stream WireHop client hello.
	clientMagic = [4]byte{'W', 'H', 'O', 'P'}
	// serverMagic identifies a raw-stream WireHop server hello.
	serverMagic = [4]byte{'W', 'H', 'O', 'R'}
)

// AuthTag is an HMAC-SHA256 authentication tag.
type AuthTag [sha256.Size]byte

// HelloMode distinguishes session creation from lane join.
type HelloMode uint8

const (
	// HelloCreate creates a new authenticated session and its first lane.
	HelloCreate HelloMode = iota + 1
	// HelloJoin joins a new connection generation to an existing session.
	HelloJoin
)

// Valid reports whether the hello mode is defined by this protocol version.
func (m HelloMode) Valid() bool {
	return m == HelloCreate || m == HelloJoin
}

// ClientHello authenticates session creation or a lane connection generation.
type ClientHello struct {
	Mode            HelloMode
	UnixSeconds     int64
	MonotonicMicros uint64
	Nonce           Nonce
	LaneID          LaneID
	Generation      uint64
	PathGroupID     PathGroupID
	SessionID       SessionID
	Target          target.Endpoint
	AuthTag         AuthTag
}

// SignClientHello validates hello and authenticates its canonical encoding with key.
func SignClientHello(hello *ClientHello, key []byte) error {
	if len(key) == 0 {
		return ErrMissingAuthKey
	}
	encoded, err := marshalClientHelloUnsigned(*hello)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(encoded)
	copy(hello.AuthTag[:], mac.Sum(nil))
	return nil
}

// VerifyClientHello verifies hello against key without modifying it.
func VerifyClientHello(hello ClientHello, key []byte) error {
	if len(key) == 0 {
		return ErrMissingAuthKey
	}
	encoded, err := marshalClientHelloUnsigned(hello)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(encoded)
	if !hmac.Equal(hello.AuthTag[:], mac.Sum(nil)) {
		return ErrAuthenticationFailed
	}
	return nil
}

// MarshalClientHello returns the canonical variable-width encoding of hello.
func MarshalClientHello(hello ClientHello) ([]byte, error) {
	unsigned, err := marshalClientHelloUnsigned(hello)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, len(unsigned)+sha256.Size)
	copy(encoded, unsigned)
	copy(encoded[len(unsigned):], hello.AuthTag[:])
	return encoded, nil
}

// ParseClientHello parses and validates a canonical variable-width client hello.
func ParseClientHello(encoded []byte) (ClientHello, error) {
	if len(encoded) < clientHelloMinimumSize || len(encoded) > MaxClientHelloSize {
		return ClientHello{}, ErrInvalidClientHello
	}
	if [4]byte(encoded[:4]) != clientMagic {
		return ClientHello{}, ErrInvalidMagic
	}
	if binary.BigEndian.Uint16(encoded[4:6]) != Version {
		return ClientHello{}, ErrUnsupportedVersion
	}
	if encoded[7] != 0 {
		return ClientHello{}, ErrInvalidClientHello
	}

	hello := ClientHello{
		Mode:            HelloMode(encoded[6]),
		UnixSeconds:     int64(binary.BigEndian.Uint64(encoded[8:16])),
		MonotonicMicros: binary.BigEndian.Uint64(encoded[16:24]),
		Generation:      binary.BigEndian.Uint64(encoded[52:60]),
	}
	copy(hello.Nonce[:], encoded[24:36])
	copy(hello.LaneID[:], encoded[36:52])
	copy(hello.PathGroupID[:], encoded[60:76])
	copy(hello.SessionID[:], encoded[76:92])
	targetLength := int(binary.BigEndian.Uint16(encoded[92:94]))
	if targetLength > target.MaxTextSize || len(encoded) != clientHelloMinimumSize+targetLength {
		return ClientHello{}, ErrInvalidClientHello
	}
	if targetLength != 0 {
		targetValue := string(encoded[94 : 94+targetLength])
		parsed, err := target.Parse(targetValue)
		if err != nil || parsed.String() != targetValue {
			return ClientHello{}, ErrInvalidClientHello
		}
		hello.Target = parsed
	}
	copy(hello.AuthTag[:], encoded[len(encoded)-sha256.Size:])
	if err := validateClientHello(hello); err != nil {
		return ClientHello{}, err
	}
	return hello, nil
}

// ReadClientHello reads one complete variable-width client hello from reader.
func ReadClientHello(reader io.Reader) (ClientHello, error) {
	header := make([]byte, clientHelloUnsignedFixedSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return ClientHello{}, err
	}
	if [4]byte(header[:4]) != clientMagic {
		return ClientHello{}, ErrInvalidMagic
	}
	if binary.BigEndian.Uint16(header[4:6]) != Version {
		return ClientHello{}, ErrUnsupportedVersion
	}
	targetLength := int(binary.BigEndian.Uint16(header[92:94]))
	if targetLength > target.MaxTextSize {
		return ClientHello{}, ErrInvalidClientHello
	}
	encoded := make([]byte, clientHelloMinimumSize+targetLength)
	copy(encoded, header)
	if _, err := io.ReadFull(reader, encoded[len(header):]); err != nil {
		return ClientHello{}, err
	}
	return ParseClientHello(encoded)
}

// WriteClientHello writes one complete client hello to writer.
func WriteClientHello(writer io.Writer, hello ClientHello) error {
	encoded, err := MarshalClientHello(hello)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded, "write client hello")
}

// marshalClientHelloUnsigned returns the authenticated canonical hello prefix.
func marshalClientHelloUnsigned(hello ClientHello) ([]byte, error) {
	if err := validateClientHello(hello); err != nil {
		return nil, err
	}
	targetValue := hello.Target.String()
	encoded := make([]byte, clientHelloUnsignedFixedSize+len(targetValue))
	copy(encoded[:4], clientMagic[:])
	binary.BigEndian.PutUint16(encoded[4:6], Version)
	encoded[6] = byte(hello.Mode)
	binary.BigEndian.PutUint64(encoded[8:16], uint64(hello.UnixSeconds))
	binary.BigEndian.PutUint64(encoded[16:24], hello.MonotonicMicros)
	copy(encoded[24:36], hello.Nonce[:])
	copy(encoded[36:52], hello.LaneID[:])
	binary.BigEndian.PutUint64(encoded[52:60], hello.Generation)
	copy(encoded[60:76], hello.PathGroupID[:])
	copy(encoded[76:92], hello.SessionID[:])
	binary.BigEndian.PutUint16(encoded[92:94], uint16(len(targetValue)))
	copy(encoded[94:], targetValue)
	return encoded, nil
}

// validateClientHello validates field relationships independent from authentication.
func validateClientHello(hello ClientHello) error {
	if !hello.Mode.Valid() || hello.UnixSeconds <= 0 || hello.Nonce == (Nonce{}) || hello.LaneID.IsZero() ||
		hello.Generation == 0 || hello.PathGroupID.IsZero() {
		return ErrInvalidClientHello
	}
	switch hello.Mode {
	case HelloCreate:
		if !hello.SessionID.IsZero() || !hello.Target.Valid() {
			return ErrInvalidClientHello
		}
	case HelloJoin:
		if hello.SessionID.IsZero() || hello.Target.Valid() {
			return ErrInvalidClientHello
		}
	}
	return nil
}

// ServerHelloResult distinguishes successful creation, lane acceptance, and rejection.
type ServerHelloResult uint8

const (
	// ServerSessionCreated accepts a newly created session.
	ServerSessionCreated ServerHelloResult = iota + 1
	// ServerLaneAccepted accepts a joined lane connection generation.
	ServerLaneAccepted
	// ServerRejected rejects the client hello with a stable protocol error.
	ServerRejected
)

// Valid reports whether the server hello result is defined by this protocol version.
func (r ServerHelloResult) Valid() bool {
	return r >= ServerSessionCreated && r <= ServerRejected
}

// ServerHello authenticates an admission result or a pre-upgrade rejection.
type ServerHello struct {
	Result            ServerHelloResult
	RequestNonce      Nonce
	ServerUnixSeconds int64
	SessionID         SessionID
	SessionSecret     SessionSecret
	PathGroupID       PathGroupID
	ReceiveMicros     uint64
	SendMicros        uint64
	ErrorCode         ErrorCode
	ErrorClass        ErrorClass
	ErrorScope        ErrorScope
	Diagnostic        string
	AuthTag           AuthTag
}

// SignServerHello validates hello and authenticates its canonical encoding with key.
func SignServerHello(hello *ServerHello, key []byte) error {
	if len(key) == 0 {
		return ErrMissingAuthKey
	}
	encoded, err := marshalServerHelloUnsigned(*hello)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(encoded)
	copy(hello.AuthTag[:], mac.Sum(nil))
	return nil
}

// VerifyServerHello verifies hello against key without modifying it.
func VerifyServerHello(hello ServerHello, key []byte) error {
	if len(key) == 0 {
		return ErrMissingAuthKey
	}
	encoded, err := marshalServerHelloUnsigned(hello)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(encoded)
	if !hmac.Equal(hello.AuthTag[:], mac.Sum(nil)) {
		return ErrAuthenticationFailed
	}
	return nil
}

// MarshalServerHello returns the variable-width canonical encoding of hello.
func MarshalServerHello(hello ServerHello) ([]byte, error) {
	unsigned, err := marshalServerHelloUnsigned(hello)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, len(unsigned)+sha256.Size)
	copy(encoded, unsigned)
	copy(encoded[len(unsigned):], hello.AuthTag[:])
	return encoded, nil
}

// ParseServerHello parses and validates a canonical server hello.
func ParseServerHello(encoded []byte) (ServerHello, error) {
	if len(encoded) < serverHelloFixedSize {
		return ServerHello{}, ErrInvalidServerHello
	}
	if [4]byte(encoded[:4]) != serverMagic {
		return ServerHello{}, ErrInvalidMagic
	}
	if binary.BigEndian.Uint16(encoded[4:6]) != Version {
		return ServerHello{}, ErrUnsupportedVersion
	}
	if encoded[7] != 0 {
		return ServerHello{}, ErrInvalidServerHello
	}
	diagnosticLength := int(binary.BigEndian.Uint16(encoded[112:114]))
	if diagnosticLength > MaxDiagnosticSize || len(encoded) != serverHelloFixedSize+diagnosticLength {
		return ServerHello{}, ErrInvalidServerHello
	}

	hello := ServerHello{
		Result:            ServerHelloResult(encoded[6]),
		ServerUnixSeconds: int64(binary.BigEndian.Uint64(encoded[20:28])),
		ReceiveMicros:     binary.BigEndian.Uint64(encoded[92:100]),
		SendMicros:        binary.BigEndian.Uint64(encoded[100:108]),
		ErrorCode:         ErrorCode(binary.BigEndian.Uint16(encoded[108:110])),
		ErrorClass:        ErrorClass(encoded[110]),
		ErrorScope:        ErrorScope(encoded[111]),
		Diagnostic:        string(encoded[114 : 114+diagnosticLength]),
	}
	copy(hello.RequestNonce[:], encoded[8:20])
	copy(hello.SessionID[:], encoded[28:44])
	copy(hello.SessionSecret[:], encoded[44:76])
	copy(hello.PathGroupID[:], encoded[76:92])
	authOffset := len(encoded) - sha256.Size
	copy(hello.AuthTag[:], encoded[authOffset:])
	if err := validateServerHello(hello); err != nil {
		return ServerHello{}, err
	}
	return hello, nil
}

// ReadServerHello reads one variable-width server hello from reader.
func ReadServerHello(reader io.Reader) (ServerHello, error) {
	header := make([]byte, serverHelloUnsignedFixedSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return ServerHello{}, err
	}
	diagnosticLength := int(binary.BigEndian.Uint16(header[112:114]))
	if diagnosticLength > MaxDiagnosticSize {
		return ServerHello{}, ErrDiagnosticTooLarge
	}
	encoded := make([]byte, serverHelloFixedSize+diagnosticLength)
	copy(encoded, header)
	if _, err := io.ReadFull(reader, encoded[len(header):]); err != nil {
		return ServerHello{}, err
	}
	return ParseServerHello(encoded)
}

// WriteServerHello writes one complete server hello to writer.
func WriteServerHello(writer io.Writer, hello ServerHello) error {
	encoded, err := MarshalServerHello(hello)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded, "write server hello")
}

// marshalServerHelloUnsigned returns the authenticated canonical server hello prefix and diagnostic.
func marshalServerHelloUnsigned(hello ServerHello) ([]byte, error) {
	if len(hello.Diagnostic) > MaxDiagnosticSize {
		return nil, ErrDiagnosticTooLarge
	}
	if err := validateServerHello(hello); err != nil {
		return nil, err
	}
	diagnostic := []byte(hello.Diagnostic)
	encoded := make([]byte, serverHelloUnsignedFixedSize+len(diagnostic))
	copy(encoded[:4], serverMagic[:])
	binary.BigEndian.PutUint16(encoded[4:6], Version)
	encoded[6] = byte(hello.Result)
	copy(encoded[8:20], hello.RequestNonce[:])
	binary.BigEndian.PutUint64(encoded[20:28], uint64(hello.ServerUnixSeconds))
	copy(encoded[28:44], hello.SessionID[:])
	copy(encoded[44:76], hello.SessionSecret[:])
	copy(encoded[76:92], hello.PathGroupID[:])
	binary.BigEndian.PutUint64(encoded[92:100], hello.ReceiveMicros)
	binary.BigEndian.PutUint64(encoded[100:108], hello.SendMicros)
	binary.BigEndian.PutUint16(encoded[108:110], uint16(hello.ErrorCode))
	encoded[110] = byte(hello.ErrorClass)
	encoded[111] = byte(hello.ErrorScope)
	binary.BigEndian.PutUint16(encoded[112:114], uint16(len(diagnostic)))
	copy(encoded[114:], diagnostic)
	return encoded, nil
}

// validateServerHello validates field relationships independent from authentication.
func validateServerHello(hello ServerHello) error {
	if !hello.Result.Valid() || hello.ServerUnixSeconds <= 0 || hello.ReceiveMicros > hello.SendMicros ||
		!validDiagnostic(hello.Diagnostic) {
		return ErrInvalidServerHello
	}
	if hello.RequestNonce == (Nonce{}) &&
		(hello.Result != ServerRejected || hello.ErrorCode != ErrorUnsupportedVersion) {
		return ErrInvalidServerHello
	}
	switch hello.Result {
	case ServerSessionCreated:
		if hello.SessionID.IsZero() || hello.SessionSecret == (SessionSecret{}) || hello.PathGroupID.IsZero() ||
			hello.ErrorCode != 0 || hello.ErrorClass != 0 || hello.ErrorScope != 0 || hello.Diagnostic != "" {
			return ErrInvalidServerHello
		}
	case ServerLaneAccepted:
		if hello.SessionID.IsZero() || hello.SessionSecret != (SessionSecret{}) || hello.PathGroupID.IsZero() ||
			hello.ErrorCode != 0 || hello.ErrorClass != 0 || hello.ErrorScope != 0 || hello.Diagnostic != "" {
			return ErrInvalidServerHello
		}
	case ServerRejected:
		if !hello.ErrorCode.Valid() || !validErrorDisposition(hello.ErrorClass, hello.ErrorScope) ||
			!hello.SessionID.IsZero() || hello.SessionSecret != (SessionSecret{}) || !hello.PathGroupID.IsZero() {
			return ErrInvalidServerHello
		}
		if hello.ErrorCode == ErrorClockSkew &&
			(hello.ErrorClass != ErrorRetryable || hello.ErrorScope != ErrorScopeLane) {
			return ErrInvalidServerHello
		}
	}
	return nil
}

// validDiagnostic reports whether a bounded diagnostic is safe for terminal and structured-log output.
func validDiagnostic(diagnostic string) bool {
	if len(diagnostic) > MaxDiagnosticSize {
		return false
	}
	for index := range len(diagnostic) {
		if diagnostic[index] < 0x20 || diagnostic[index] > 0x7e {
			return false
		}
	}
	return true
}

// writeFull writes all bytes or returns a short-write error.
func writeFull(writer io.Writer, encoded []byte, operation string) error {
	for len(encoded) > 0 {
		written, err := writer.Write(encoded)
		if err != nil {
			return fmt.Errorf("%s: %w", operation, err)
		}
		if written <= 0 || written > len(encoded) {
			return fmt.Errorf("%s: %w", operation, io.ErrShortWrite)
		}
		encoded = encoded[written:]
	}
	return nil
}
