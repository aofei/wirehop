package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/target"
)

func TestClientHello(t *testing.T) {
	sessionID := testSessionID(1)
	maximumTarget := target.MustParse(strings.Join([]string{
		strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61),
	}, ".") + ":65535")
	for _, tt := range []struct {
		name  string
		hello ClientHello
	}{
		{name: "CreateIPv4", hello: testClientHello(HelloCreate, SessionID{}, target.MustParse("192.0.2.1:51820"))},
		{name: "CreateIPv6", hello: testClientHello(HelloCreate, SessionID{}, target.MustParse("[2001:db8::1]:51820"))},
		{name: "CreateDomain", hello: testClientHello(HelloCreate, SessionID{}, target.MustParse("wg.example.com:51820"))},
		{name: "CreateMaximumTarget", hello: testClientHello(HelloCreate, SessionID{}, maximumTarget)},
		{name: "Join", hello: testClientHello(HelloJoin, sessionID, target.Endpoint{})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hello := tt.hello
			if err := SignClientHello(&hello, []byte("test key")); err != nil {
				t.Fatal(err)
			}
			if err := VerifyClientHello(hello, []byte("test key")); err != nil {
				t.Fatal(err)
			}
			encoded, err := MarshalClientHello(hello)
			if err != nil {
				t.Fatal(err)
			}
			if want := clientHelloMinimumSize + len(hello.Target.String()); len(encoded) != want {
				t.Fatalf("MarshalClientHello() length = %d, want %d", len(encoded), want)
			}
			if hello.Target == maximumTarget && len(encoded) != MaxClientHelloSize {
				t.Fatalf("maximum MarshalClientHello() length = %d, want %d", len(encoded), MaxClientHelloSize)
			}
			got, err := ParseClientHello(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, hello) {
				t.Fatalf("ParseClientHello() = %#v, want %#v", got, hello)
			}

			encoded[30] ^= 1
			tampered, err := ParseClientHello(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyClientHello(tampered, []byte("test key")); !errors.Is(err, ErrAuthenticationFailed) {
				t.Fatalf("VerifyClientHello() error = %v", err)
			}
		})
	}
}

func TestClientHelloErrors(t *testing.T) {
	valid := testClientHello(HelloCreate, SessionID{}, target.MustParse("192.0.2.1:51820"))
	for _, tt := range []struct {
		name string
		edit func(*ClientHello)
		want error
	}{
		{name: "UnknownMode", edit: func(hello *ClientHello) { hello.Mode = 255 }, want: ErrInvalidClientHello},
		{name: "ZeroTimestamp", edit: func(hello *ClientHello) { hello.UnixSeconds = 0 }, want: ErrInvalidClientHello},
		{name: "ZeroNonce", edit: func(hello *ClientHello) { hello.Nonce = Nonce{} }, want: ErrInvalidClientHello},
		{name: "ZeroLane", edit: func(hello *ClientHello) { hello.LaneID = LaneID{} }, want: ErrInvalidClientHello},
		{name: "ZeroGeneration", edit: func(hello *ClientHello) { hello.Generation = 0 }, want: ErrInvalidClientHello},
		{name: "ZeroPathGroup", edit: func(hello *ClientHello) { hello.PathGroupID = PathGroupID{} }, want: ErrInvalidClientHello},
		{name: "CreateWithSession", edit: func(hello *ClientHello) { hello.SessionID = testSessionID(1) }, want: ErrInvalidClientHello},
		{name: "CreateWithoutTarget", edit: func(hello *ClientHello) { hello.Target = target.Endpoint{} }, want: ErrInvalidClientHello},
		{name: "JoinWithTarget", edit: func(hello *ClientHello) { hello.Mode = HelloJoin; hello.SessionID = testSessionID(1) }, want: ErrInvalidClientHello},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hello := valid
			tt.edit(&hello)
			_, err := MarshalClientHello(hello)
			if !errors.Is(err, tt.want) {
				t.Fatalf("MarshalClientHello() error = %v, want %v", err, tt.want)
			}
		})
	}

	encoded := make([]byte, clientHelloMinimumSize)
	if _, err := ParseClientHello(encoded); !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("ParseClientHello() error = %v", err)
	}
	hello := valid
	if err := SignClientHello(&hello, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalClientHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	encoded[92] = 255
	encoded[93] = 255
	if _, err := ParseClientHello(encoded); !errors.Is(err, ErrInvalidClientHello) {
		t.Fatalf("ParseClientHello() target length error = %v, want %v", err, ErrInvalidClientHello)
	}
	domain := testClientHello(HelloCreate, SessionID{}, target.MustParse("wg.example.com:51820"))
	if err := SignClientHello(&domain, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err = MarshalClientHello(domain)
	if err != nil {
		t.Fatal(err)
	}
	encoded[94] = 'W'
	if _, err := ParseClientHello(encoded); !errors.Is(err, ErrInvalidClientHello) {
		t.Fatalf("ParseClientHello() noncanonical target error = %v, want %v", err, ErrInvalidClientHello)
	}
}

func TestClientHelloAuthenticatesEveryEncodedByte(t *testing.T) {
	hello := testClientHello(HelloCreate, SessionID{}, target.MustParse("192.0.2.1:51820"))
	key := []byte("test key")
	if err := SignClientHello(&hello, key); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalClientHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	for offset := range encoded {
		modified := append([]byte(nil), encoded...)
		modified[offset] ^= 1
		parsed, err := ParseClientHello(modified)
		if err == nil && !errors.Is(VerifyClientHello(parsed, key), ErrAuthenticationFailed) {
			t.Fatalf("modified byte %d passed structural and authentication checks", offset)
		}
	}
}

func TestReadClientHelloRejectsUnsupportedVersionBeforeVariableBody(t *testing.T) {
	header := make([]byte, clientHelloUnsignedFixedSize)
	copy(header[:4], clientMagic[:])
	binary.BigEndian.PutUint16(header[4:6], Version+1)
	binary.BigEndian.PutUint16(header[92:94], uint16(target.MaxTextSize))
	if _, err := ReadClientHello(bytes.NewReader(header)); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("ReadClientHello() error = %v, want %v", err, ErrUnsupportedVersion)
	}
}

func TestServerHello(t *testing.T) {
	for _, tt := range []struct {
		name  string
		hello ServerHello
	}{
		{name: "Created", hello: ServerHello{
			Result: ServerSessionCreated, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
			ReceiveMicros: 100, SendMicros: 110,
		}},
		{name: "Accepted", hello: ServerHello{
			Result: ServerLaneAccepted, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			SessionID: testSessionID(1), PathGroupID: testPathGroupID(3), ReceiveMicros: 100, SendMicros: 110,
		}},
		{name: "Rejected", hello: ServerHello{
			Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			ErrorCode: ErrorAuthentication, ErrorClass: ErrorLaneRejected, ErrorScope: ErrorScopeLane,
			Diagnostic: "authentication failed", ReceiveMicros: 100, SendMicros: 110,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hello := tt.hello
			if err := SignServerHello(&hello, []byte("test key")); err != nil {
				t.Fatal(err)
			}
			encoded, err := MarshalServerHello(hello)
			if err != nil {
				t.Fatal(err)
			}
			if want := 146 + len(hello.Diagnostic); len(encoded) != want {
				t.Fatalf("MarshalServerHello() length = %d, want %d", len(encoded), want)
			}
			got, err := ParseServerHello(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, hello) {
				t.Fatalf("ParseServerHello() = %#v, want %#v", got, hello)
			}
			if err := VerifyServerHello(got, []byte("test key")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServerHelloRejectsInconsistentErrorScope(t *testing.T) {
	if _, err := MarshalServerHello(ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4),
		ErrorCode: ErrorAuthentication, ErrorClass: ErrorSessionRejected, ErrorScope: ErrorScopeSession,
	}); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("MarshalServerHello() error = %v for zero server time", err)
	}
	if _, err := MarshalServerHello(ServerHello{
		Result: ServerRejected, ServerUnixSeconds: 1_700_000_000,
		ErrorCode: ErrorAuthentication, ErrorClass: ErrorSessionRejected, ErrorScope: ErrorScopeSession,
	}); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("MarshalServerHello() error = %v for zero request nonce", err)
	}
	if _, err := MarshalServerHello(ServerHello{
		Result: ServerSessionCreated, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
		Diagnostic: "unexpected",
	}); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("MarshalServerHello() error = %v for diagnostic-bearing success", err)
	}
	if _, err := MarshalServerHello(ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		ErrorCode: ErrorAuthentication, ErrorClass: ErrorLaneRejected, ErrorScope: ErrorScopeSession,
	}); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("MarshalServerHello() error = %v, want %v", err, ErrInvalidServerHello)
	}
	if _, err := MarshalServerHello(ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		SessionID: testSessionID(1), ErrorCode: ErrorAuthentication, ErrorClass: ErrorSessionRejected,
		ErrorScope: ErrorScopeSession,
	}); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("MarshalServerHello() error = %v for state-bearing rejection", err)
	}
	if _, err := MarshalServerHello(ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		ErrorCode: ErrorClockSkew, ErrorClass: ErrorSessionRejected, ErrorScope: ErrorScopeSession,
	}); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("MarshalServerHello() error = %v for terminal clock skew", err)
	}
	hello := ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		ErrorCode: ErrorAuthentication, ErrorClass: ErrorSessionRejected, ErrorScope: ErrorScopeSession,
		Diagnostic: "authentication failed",
	}
	if err := SignServerHello(&hello, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalServerHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	encoded[114] = '\n'
	if _, err := ParseServerHello(encoded); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("ParseServerHello() error = %v for control-byte diagnostic", err)
	}
	hello.Diagnostic = strings.Repeat("x", MaxDiagnosticSize+1)
	if _, err := MarshalServerHello(hello); !errors.Is(err, ErrDiagnosticTooLarge) {
		t.Fatalf("MarshalServerHello() error = %v for oversized diagnostic", err)
	}
}

func TestServerHelloAuthenticatesRequestAndTime(t *testing.T) {
	hello := ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		ErrorCode: ErrorClockSkew, ErrorClass: ErrorRetryable, ErrorScope: ErrorScopeLane,
		Diagnostic: "request timestamp rejected",
	}
	if err := SignServerHello(&hello, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalServerHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{8, 20} {
		modified := append([]byte(nil), encoded...)
		modified[offset] ^= 1
		parsed, err := ParseServerHello(modified)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyServerHello(parsed, []byte("test key")); !errors.Is(err, ErrAuthenticationFailed) {
			t.Fatalf("VerifyServerHello() error = %v after modifying offset %d", err, offset)
		}
	}
}

func TestServerHelloAuthenticatesEveryEncodedByte(t *testing.T) {
	hello := ServerHello{
		Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		ErrorCode: ErrorAuthentication, ErrorClass: ErrorSessionRejected, ErrorScope: ErrorScopeSession,
		Diagnostic: "authentication failed",
	}
	key := []byte("test key")
	if err := SignServerHello(&hello, key); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalServerHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	for offset := range encoded {
		modified := append([]byte(nil), encoded...)
		modified[offset] ^= 1
		parsed, err := ParseServerHello(modified)
		if err == nil && !errors.Is(VerifyServerHello(parsed, key), ErrAuthenticationFailed) {
			t.Fatalf("modified byte %d passed structural and authentication checks", offset)
		}
	}
}

func FuzzParseClientHello(f *testing.F) {
	hello := testClientHello(HelloCreate, SessionID{}, target.MustParse("192.0.2.1:51820"))
	if err := SignClientHello(&hello, []byte("test key")); err != nil {
		f.Fatal(err)
	}
	encoded, err := MarshalClientHello(hello)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, input []byte) {
		parsed, err := ParseClientHello(input)
		if err == nil {
			if _, err := MarshalClientHello(parsed); err != nil {
				t.Fatalf("parsed hello cannot be marshaled: %v", err)
			}
		}
	})
}

func FuzzParseServerHello(f *testing.F) {
	hello := ServerHello{
		Result: ServerSessionCreated, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
		SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
		ReceiveMicros: 100, SendMicros: 110,
	}
	if err := SignServerHello(&hello, []byte("test key")); err != nil {
		f.Fatal(err)
	}
	encoded, err := MarshalServerHello(hello)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, input []byte) {
		parsed, err := ParseServerHello(input)
		if err == nil {
			if _, err := MarshalServerHello(parsed); err != nil {
				t.Fatalf("parsed server hello cannot be marshaled: %v", err)
			}
		}
	})
}

func testClientHello(mode HelloMode, sessionID SessionID, endpoint target.Endpoint) ClientHello {
	return ClientHello{
		Mode: mode, UnixSeconds: 1_700_000_000, MonotonicMicros: 1234, Nonce: testNonce(1),
		LaneID: testLaneID(2), Generation: 1, PathGroupID: testPathGroupID(3), SessionID: sessionID, Target: endpoint,
	}
}

func testSessionID(value byte) SessionID {
	var id SessionID
	id[0] = value
	return id
}

func testSessionSecret(value byte) SessionSecret {
	var secret SessionSecret
	secret[0] = value
	return secret
}

func testLaneID(value byte) LaneID {
	var id LaneID
	id[0] = value
	return id
}

func testPathGroupID(value byte) PathGroupID {
	var id PathGroupID
	id[0] = value
	return id
}

func testNonce(value byte) Nonce {
	var nonce Nonce
	nonce[0] = value
	return nonce
}
