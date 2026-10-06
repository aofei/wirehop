package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/target"
)

func TestClientHelloMaximumFields(t *testing.T) {
	endpoint := target.MustParse(strings.Join([]string{
		strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61),
	}, ".") + ":65535")
	hello := testClientHello(HelloCreate, SessionID{}, endpoint)
	hello.LaneID = math.MaxUint64
	hello.Generation = math.MaxUint64
	hello.PathGroupID = math.MaxUint64
	if err := SignClientHello(&hello, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalClientHello(hello)
	if err != nil || len(encoded) != MaxClientHelloSize {
		t.Fatalf("maximum encoding: %d bytes, error %v", len(encoded), err)
	}
	stream := bytes.NewReader(append(encoded, 0x42))
	got, err := ReadClientHello(stream)
	if err != nil || got != hello || stream.Len() != 1 {
		t.Fatalf("maximum read: %+v, error %v, remaining %d", got, err, stream.Len())
	}
	if err := VerifyClientHello(got, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	for end := range len(encoded) {
		if _, err := ReadClientHello(bytes.NewReader(encoded[:end])); err == nil {
			t.Fatalf("truncation at %d was accepted", end)
		}
	}
}

func TestReadHelloBounds(t *testing.T) {
	for _, tt := range []struct {
		name    string
		magic   [4]byte
		minimum int
		maximum int
		invalid error
		read    func(io.Reader) error
	}{
		{"Client", clientMagic, clientHelloMinimumSize, MaxClientHelloSize, ErrInvalidClientHello,
			func(r io.Reader) error { _, err := ReadClientHello(r); return err }},
		{"Server", serverMagic, serverHelloMinimumSize, maxServerHelloSize, ErrInvalidServerHello,
			func(r io.Reader) error { _, err := ReadServerHello(r); return err }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, size := range []int{helloHeaderSize, tt.minimum - 1, tt.maximum + 1, math.MaxUint16} {
				var header [helloHeaderSize]byte
				copy(header[:], tt.magic[:])
				binary.BigEndian.PutUint16(header[4:6], Version)
				binary.BigEndian.PutUint16(header[6:8], uint16(size-helloHeaderSize))
				if err := tt.read(bytes.NewReader(header[:])); !errors.Is(err, tt.invalid) {
					t.Fatalf("length %d: error %v, want %v before reading body", size, err, tt.invalid)
				}
			}
		})
	}
}

func TestServerHelloMaximumDiagnostic(t *testing.T) {
	hello := ServerHello{Result: ServerRejected, RequestNonce: testNonce(1), ServerUnixSeconds: 1,
		ErrorCode: ErrorInternal, ErrorClass: ErrorRetryable, ErrorScope: ErrorScopeLane,
		Diagnostic: strings.Repeat("x", MaxDiagnosticSize)}
	if err := SignServerHello(&hello, []byte("test key")); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalServerHello(hello)
	if err != nil || len(encoded) != 641 {
		t.Fatalf("maximum rejection: %d bytes, error %v", len(encoded), err)
	}
	stream := bytes.NewReader(append(encoded, 0x42))
	got, err := ReadServerHello(stream)
	if err != nil || got != hello || stream.Len() != 1 {
		t.Fatalf("maximum read: %+v, error %v, remaining %d", got, err, stream.Len())
	}
	for end := range len(encoded) {
		if _, err := ReadServerHello(bytes.NewReader(encoded[:end])); err == nil {
			t.Fatalf("truncation at %d was accepted", end)
		}
	}
}
