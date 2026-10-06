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
	// helloHeaderSize bounds the magic, version, and authenticated body length.
	helloHeaderSize = 8
	// clientHelloUnsignedFixedSize precedes the lane, generation, group, and target fields.
	clientHelloUnsignedFixedSize = helloHeaderSize + 45
	// clientHelloMinimumSize includes three single-byte integers and an authentication tag.
	clientHelloMinimumSize = clientHelloUnsignedFixedSize + 3 + sha256.Size
	// MaxClientHelloSize bounds all client fields and the longest canonical target.
	MaxClientHelloSize = clientHelloUnsignedFixedSize + 3*binary.MaxVarintLen64 + target.MaxTextSize + sha256.Size
	// serverHelloUnsignedFixedSize precedes the group, error code, and diagnostic fields.
	serverHelloUnsignedFixedSize = helloHeaderSize + 87
	// serverHelloMinimumSize includes two single-byte integers and an authentication tag.
	serverHelloMinimumSize = serverHelloUnsignedFixedSize + 2 + sha256.Size
	// MaxDiagnosticSize bounds a peer-controlled handshake diagnostic.
	MaxDiagnosticSize = 512
	// maxServerHelloSize bounds every server admission response.
	maxServerHelloSize = serverHelloUnsignedFixedSize + 2*binary.MaxVarintLen64 + MaxDiagnosticSize + sha256.Size
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
	if err := validateHelloEncoding(encoded, clientMagic, clientHelloMinimumSize, MaxClientHelloSize, ErrInvalidClientHello); err != nil {
		return ClientHello{}, err
	}
	hello := ClientHello{
		Mode: HelloMode(encoded[8]), UnixSeconds: int64(binary.BigEndian.Uint64(encoded[9:17])),
		MonotonicMicros: binary.BigEndian.Uint64(encoded[17:25]),
	}
	copy(hello.Nonce[:], encoded[25:37])
	copy(hello.SessionID[:], encoded[37:53])
	unsignedEnd := len(encoded) - sha256.Size
	targetBytes, err := parseIntegers(encoded[53:unsignedEnd], (*uint64)(&hello.LaneID), &hello.Generation, (*uint64)(&hello.PathGroupID))
	if err != nil || len(targetBytes) > target.MaxTextSize {
		return ClientHello{}, ErrInvalidClientHello
	}
	if len(targetBytes) != 0 {
		value := string(targetBytes)
		hello.Target, err = target.Parse(value)
		if err != nil || hello.Target.String() != value {
			return ClientHello{}, ErrInvalidClientHello
		}
	}
	copy(hello.AuthTag[:], encoded[unsignedEnd:])
	if err := validateClientHello(hello); err != nil {
		return ClientHello{}, err
	}
	return hello, nil
}

// ReadClientHello reads one complete variable-width client hello from reader.
func ReadClientHello(reader io.Reader) (ClientHello, error) {
	encoded, err := readHello(reader, clientMagic, clientHelloMinimumSize, MaxClientHelloSize, ErrInvalidClientHello)
	if err != nil {
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
	encoded := integerPayload(clientHelloUnsignedFixedSize, uint64(hello.LaneID), hello.Generation, uint64(hello.PathGroupID))
	encoded[8] = byte(hello.Mode)
	binary.BigEndian.PutUint64(encoded[9:17], uint64(hello.UnixSeconds))
	binary.BigEndian.PutUint64(encoded[17:25], hello.MonotonicMicros)
	copy(encoded[25:37], hello.Nonce[:])
	copy(encoded[37:53], hello.SessionID[:])
	encoded = append(encoded, hello.Target.String()...)
	encodeHelloHeader(encoded, clientMagic)
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
	if err := validateHelloEncoding(encoded, serverMagic, serverHelloMinimumSize, maxServerHelloSize, ErrInvalidServerHello); err != nil {
		return ServerHello{}, err
	}
	hello := ServerHello{
		Result: ServerHelloResult(encoded[8]), ServerUnixSeconds: int64(binary.BigEndian.Uint64(encoded[21:29])),
		ReceiveMicros: binary.BigEndian.Uint64(encoded[77:85]), SendMicros: binary.BigEndian.Uint64(encoded[85:93]),
		ErrorClass: ErrorClass(encoded[93]), ErrorScope: ErrorScope(encoded[94]),
	}
	copy(hello.RequestNonce[:], encoded[9:21])
	copy(hello.SessionID[:], encoded[29:45])
	copy(hello.SessionSecret[:], encoded[45:77])
	unsignedEnd := len(encoded) - sha256.Size
	var code uint64
	diagnostic, err := parseIntegers(encoded[95:unsignedEnd], (*uint64)(&hello.PathGroupID), &code)
	if err != nil || code > uint64(ErrorClockSkew) || len(diagnostic) > MaxDiagnosticSize {
		return ServerHello{}, ErrInvalidServerHello
	}
	hello.ErrorCode = ErrorCode(code)
	hello.Diagnostic = string(diagnostic)
	copy(hello.AuthTag[:], encoded[unsignedEnd:])
	if err := validateServerHello(hello); err != nil {
		return ServerHello{}, err
	}
	return hello, nil
}

// ReadServerHello reads one variable-width server hello from reader.
func ReadServerHello(reader io.Reader) (ServerHello, error) {
	encoded, err := readHello(reader, serverMagic, serverHelloMinimumSize, maxServerHelloSize, ErrInvalidServerHello)
	if err != nil {
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
	encoded := integerPayload(serverHelloUnsignedFixedSize, uint64(hello.PathGroupID), uint64(hello.ErrorCode))
	encoded[8] = byte(hello.Result)
	copy(encoded[9:21], hello.RequestNonce[:])
	binary.BigEndian.PutUint64(encoded[21:29], uint64(hello.ServerUnixSeconds))
	copy(encoded[29:45], hello.SessionID[:])
	copy(encoded[45:77], hello.SessionSecret[:])
	binary.BigEndian.PutUint64(encoded[77:85], hello.ReceiveMicros)
	binary.BigEndian.PutUint64(encoded[85:93], hello.SendMicros)
	encoded[93] = byte(hello.ErrorClass)
	encoded[94] = byte(hello.ErrorScope)
	encoded = append(encoded, hello.Diagnostic...)
	encodeHelloHeader(encoded, serverMagic)
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

// encodeHelloHeader binds the magic, version, and complete body length into the authenticated encoding.
func encodeHelloHeader(unsigned []byte, magic [4]byte) {
	copy(unsigned[:4], magic[:])
	binary.BigEndian.PutUint16(unsigned[4:6], Version)
	binary.BigEndian.PutUint16(unsigned[6:8], uint16(len(unsigned)+sha256.Size-helloHeaderSize))
}

// validateHelloEncoding checks the preface and bounded exact length before field decoding.
func validateHelloEncoding(encoded []byte, magic [4]byte, minimum, maximum int, invalid error) error {
	if len(encoded) < minimum || len(encoded) > maximum {
		return invalid
	}
	if [4]byte(encoded[:4]) != magic {
		return ErrInvalidMagic
	}
	if binary.BigEndian.Uint16(encoded[4:6]) != Version {
		return ErrUnsupportedVersion
	}
	if int(binary.BigEndian.Uint16(encoded[6:8])) != len(encoded)-helloHeaderSize {
		return invalid
	}
	return nil
}

// readHello rejects invalid prefaces and lengths before reading a variable body.
func readHello(reader io.Reader, magic [4]byte, minimum, maximum int, invalid error) ([]byte, error) {
	var header [helloHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	if [4]byte(header[:4]) != magic {
		return nil, ErrInvalidMagic
	}
	if binary.BigEndian.Uint16(header[4:6]) != Version {
		return nil, ErrUnsupportedVersion
	}
	size := helloHeaderSize + int(binary.BigEndian.Uint16(header[6:8]))
	if size < minimum || size > maximum {
		return nil, invalid
	}
	encoded := make([]byte, size)
	copy(encoded, header[:])
	if _, err := io.ReadFull(reader, encoded[helloHeaderSize:]); err != nil {
		return nil, err
	}
	return encoded, nil
}
