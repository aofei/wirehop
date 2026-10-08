package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

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
			want := clientHelloMinimumSize + len(hello.Target.String())
			if hello.Mode == HelloJoin {
				want += len(hello.SessionID)
			}
			if len(encoded) != want {
				t.Fatalf("MarshalClientHello() length = %d, want %d", len(encoded), want)
			}
			if hello.Target == maximumTarget && len(encoded) > MaxClientHelloSize {
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
		{name: "ZeroLane", edit: func(hello *ClientHello) { hello.LaneID = LaneID(0) }, want: ErrInvalidClientHello},
		{name: "ZeroGeneration", edit: func(hello *ClientHello) { hello.Generation = 0 }, want: ErrInvalidClientHello},
		{name: "ZeroPathGroup", edit: func(hello *ClientHello) { hello.PathGroupID = PathGroupID(0) }, want: ErrInvalidClientHello},
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
	encoded[6] = 255
	encoded[7] = 255
	if _, err := ParseClientHello(encoded); !errors.Is(err, ErrInvalidClientHello) {
		t.Fatalf("ParseClientHello() body length error = %v, want %v", err, ErrInvalidClientHello)
	}
	domain := testClientHello(HelloCreate, SessionID{}, target.MustParse("wg.example.com:51820"))
	if err := SignClientHello(&domain, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err = MarshalClientHello(domain)
	if err != nil {
		t.Fatal(err)
	}
	encoded[clientHelloMinimumSize-sha256.Size] = 'W'
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
	header := make([]byte, helloHeaderSize)
	copy(header[:4], clientMagic[:])
	binary.BigEndian.PutUint16(header[4:6], Version+1)
	binary.BigEndian.PutUint16(header[6:8], uint16(target.MaxTextSize))
	if _, err := ReadClientHello(bytes.NewReader(header)); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("ReadClientHello() error = %v, want %v", err, ErrUnsupportedVersion)
	}
}

func TestServerHello(t *testing.T) {
	for _, tt := range []struct {
		name  string
		hello ServerHello
		size  int
	}{
		{name: "Created", hello: ServerHello{
			Result: ServerSessionCreated, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
			ReceiveMicros: 100, SendMicros: 110,
		}, size: 126},
		{name: "Accepted", hello: ServerHello{
			Result: ServerLaneAccepted, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			SessionID: testSessionID(1), PathGroupID: testPathGroupID(3), ReceiveMicros: 100, SendMicros: 110,
		}, size: 94},
		{name: "Rejected", hello: ServerHello{
			Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			ErrorCode: ErrorAuthentication, ErrorClass: ErrorLaneRejected, ErrorScope: ErrorScopeLane,
			Diagnostic: "authentication failed",
		}, size: 85},
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
			if len(encoded) != tt.size {
				t.Fatalf("MarshalServerHello() length = %d, want %d", len(encoded), tt.size)
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
	encoded[serverHelloMinimumSize-sha256.Size] = '\n'
	if _, err := ParseServerHello(encoded); !errors.Is(err, ErrInvalidServerHello) {
		t.Fatalf("ParseServerHello() error = %v for control-byte diagnostic", err)
	}
	hello.Diagnostic = strings.Repeat("x", MaxDiagnosticSize+1)
	if _, err := MarshalServerHello(hello); !errors.Is(err, ErrDiagnosticTooLarge) {
		t.Fatalf("MarshalServerHello() error = %v for oversized diagnostic", err)
	}
	t.Run("RejectedTiming", func(t *testing.T) {
		for _, timing := range [][2]uint64{{0, 1}, {1, 1}} {
			hello.Diagnostic = ""
			hello.ReceiveMicros, hello.SendMicros = timing[0], timing[1]
			if _, err := MarshalServerHello(hello); !errors.Is(err, ErrInvalidServerHello) {
				t.Fatalf("MarshalServerHello() error = %v for unused rejection timing %v", err, timing)
			}
		}
	})
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
	for _, offset := range []int{9, 21} {
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
	maximumTarget := target.MustParse(strings.Join([]string{
		strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61),
	}, ".") + ":65535")
	for _, hello := range []ClientHello{
		testClientHello(HelloCreate, SessionID{}, target.MustParse("192.0.2.1:51820")),
		testClientHello(HelloCreate, SessionID{}, target.MustParse("[2001:db8::1]:51820")),
		testClientHello(HelloCreate, SessionID{}, maximumTarget),
		testClientHello(HelloJoin, testSessionID(1), target.Endpoint{}),
	} {
		for _, selector := range []uint64{1, math.MaxUint64} {
			hello.LaneID = LaneID(selector)
			hello.Generation = selector
			hello.PathGroupID = PathGroupID(selector)
			if err := SignClientHello(&hello, []byte("test key")); err != nil {
				f.Fatal(err)
			}
			encoded, err := MarshalClientHello(hello)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(encoded)
		}
	}
	f.Add([]byte(nil))
	f.Fuzz(func(t *testing.T, input []byte) {
		parsed, err := ParseClientHello(input)
		if err != nil {
			return
		}
		encoded, err := MarshalClientHello(parsed)
		if err != nil || !bytes.Equal(encoded, input) {
			t.Fatalf("accepted client hello changed during canonical encoding: %v", err)
		}
		stream := bytes.NewReader(append(bytes.Clone(input), 0x42))
		got, err := ReadClientHello(iotest.OneByteReader(stream))
		if err != nil || got != parsed || stream.Len() != 1 {
			t.Fatalf("fragmented client hello differs: error %v, remaining %d", err, stream.Len())
		}
	})
}

func FuzzParseServerHello(f *testing.F) {
	for _, hello := range []ServerHello{
		{Result: ServerSessionCreated, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: math.MaxUint64,
			ReceiveMicros: 100, SendMicros: 110},
		{Result: ServerLaneAccepted, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			SessionID: testSessionID(1), PathGroupID: math.MaxUint64, ReceiveMicros: 100, SendMicros: 110},
		{Result: ServerRejected, RequestNonce: testNonce(4), ServerUnixSeconds: 1_700_000_000,
			ErrorCode: ErrorInternal, ErrorClass: ErrorRetryable, ErrorScope: ErrorScopeLane,
			Diagnostic: strings.Repeat("x", MaxDiagnosticSize)},
		{Result: ServerRejected, ServerUnixSeconds: 1_700_000_000,
			ErrorCode: ErrorUnsupportedVersion, ErrorClass: ErrorLaneRejected, ErrorScope: ErrorScopeLane},
	} {
		if err := SignServerHello(&hello, []byte("test key")); err != nil {
			f.Fatal(err)
		}
		encoded, err := MarshalServerHello(hello)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Add([]byte(nil))
	f.Fuzz(func(t *testing.T, input []byte) {
		parsed, err := ParseServerHello(input)
		if err != nil {
			return
		}
		encoded, err := MarshalServerHello(parsed)
		if err != nil || !bytes.Equal(encoded, input) {
			t.Fatalf("accepted server hello changed during canonical encoding: %v", err)
		}
		stream := bytes.NewReader(append(bytes.Clone(input), 0x42))
		got, err := ReadServerHello(iotest.OneByteReader(stream))
		if err != nil || got != parsed || stream.Len() != 1 {
			t.Fatalf("fragmented server hello differs: error %v, remaining %d", err, stream.Len())
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
	return LaneID(value)
}

func testPathGroupID(value byte) PathGroupID {
	return PathGroupID(value)
}

func testNonce(value byte) Nonce {
	var nonce Nonce
	nonce[0] = value
	return nonce
}
