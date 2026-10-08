package protocol

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/aofei/wirehop/internal/target"
)

type helloWriterFunc func([]byte) (int, error)

func (f helloWriterFunc) Write(buffer []byte) (int, error) {
	return f(buffer)
}

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
		{"Server", serverMagic, serverHelloMinimumSize, MaxServerHelloSize, ErrInvalidServerHello,
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
	if err != nil || len(encoded) != 576 {
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

func TestHelloWireLayout(t *testing.T) {
	for _, tt := range []struct {
		name    string
		client  *ClientHello
		server  *ServerHello
		encoded string
	}{
		{name: "Create", client: &ClientHello{Mode: HelloCreate, UnixSeconds: 1, MonotonicMicros: 2,
			Nonce: testNonce(3), LaneID: 4, Generation: 5, PathGroupID: 6, Target: target.MustParse("a:1")},
			encoded: "57484f50000100430100000000000000010000000000000002030000000000000000000000040506613a31" +
				"517d183d41a4c75959d5778ea181523ccecdc7a7b8fbf220a2499cef87e0655e"},
		{name: "Join", client: &ClientHello{Mode: HelloJoin, UnixSeconds: 1, MonotonicMicros: 2,
			Nonce: testNonce(3), LaneID: 4, Generation: 5, PathGroupID: 6, SessionID: testSessionID(7)},
			encoded: "57484f50000100500200000000000000010000000000000002030000000000000000000000040506" +
				"07000000000000000000000000000000da592db0707acc468cf2570b4d88d4426a3331bcee5d1e93960b42b7b47e1156"},
		{name: "Created", server: &ServerHello{Result: ServerSessionCreated, ServerUnixSeconds: 1,
			RequestNonce: testNonce(3), ReceiveMicros: 9, SendMicros: 10, SessionID: testSessionID(7),
			SessionSecret: testSessionSecret(8), PathGroupID: 6},
			encoded: "57484f52000100760103000000000000000000000000000000000000010000000000000009000000000000000a" +
				"07000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000006" +
				"f21c932bc65048511cef08e3de9089d417c7940ad7965fdf650481c93a4f6f52"},
		{name: "Accepted", server: &ServerHello{Result: ServerLaneAccepted, ServerUnixSeconds: 1,
			RequestNonce: testNonce(3), ReceiveMicros: 9, SendMicros: 10, SessionID: testSessionID(7), PathGroupID: 6},
			encoded: "57484f52000100560203000000000000000000000000000000000000010000000000000009000000000000000a" +
				"070000000000000000000000000000000693ead232e5aacc7a26a4f5e0372ae52eee05a0c0c4b5f77cef544f87984d035b"},
		{name: "Rejected", server: &ServerHello{Result: ServerRejected, ServerUnixSeconds: 1,
			RequestNonce: testNonce(3), ErrorClass: ErrorLaneRejected,
			ErrorScope: ErrorScopeLane, ErrorCode: ErrorAuthentication, Diagnostic: "bad"},
			encoded: "57484f520001003b030300000000000000000000000000000000000001020103626164" +
				"2799b1d486ad253561110d1be81374372136aede6161e3e369f99b88ed6a7c41"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, err := hex.DecodeString(tt.encoded)
			if err != nil {
				t.Fatal(err)
			}
			key := []byte("test key")
			stream := bytes.NewReader(append(bytes.Clone(want), 0x22, 1, 2))
			var encoded []byte
			if tt.client != nil {
				if err := SignClientHello(tt.client, key); err != nil {
					t.Fatal(err)
				}
				encoded, err = MarshalClientHello(*tt.client)
				got, parseErr := ReadClientHello(iotest.OneByteReader(stream))
				if parseErr != nil || got != *tt.client || VerifyClientHello(got, key) != nil {
					t.Fatalf("literal client hello differs: %v", parseErr)
				}
			} else {
				if err := SignServerHello(tt.server, key); err != nil {
					t.Fatal(err)
				}
				encoded, err = MarshalServerHello(*tt.server)
				got, parseErr := ReadServerHello(iotest.OneByteReader(stream))
				if parseErr != nil || got != *tt.server || VerifyServerHello(got, key) != nil {
					t.Fatalf("literal server hello differs: %v", parseErr)
				}
			}
			if err != nil || !bytes.Equal(encoded, want) {
				t.Fatalf("literal hello encoding differs: %x, %v", encoded, err)
			}
			frame, err := ReadFrame(iotest.OneByteReader(stream))
			if err != nil {
				t.Fatalf("frame following hello: %v", err)
			}
			ping, err := ParseTimingPing(frame)
			if err != nil || ping != (TimingPing{ID: 1, SendMicros: 2}) || stream.Len() != 0 {
				t.Fatalf("frame following hello: %+v, error %v, remaining %d", ping, err, stream.Len())
			}
			for end := range len(want) {
				var err error
				if tt.client != nil {
					_, err = ReadClientHello(iotest.OneByteReader(bytes.NewReader(want[:end])))
				} else {
					_, err = ReadServerHello(iotest.OneByteReader(bytes.NewReader(want[:end])))
				}
				if err == nil {
					t.Fatalf("truncated hello at byte %d was accepted", end)
				}
			}
			write := func(writer io.Writer) error {
				if tt.client != nil {
					return WriteClientHello(writer, *tt.client)
				}
				return WriteServerHello(writer, *tt.server)
			}
			t.Run("PartialWrites", func(t *testing.T) {
				var output bytes.Buffer
				writer := helloWriterFunc(func(buffer []byte) (int, error) {
					return output.Write(buffer[:1])
				})
				if err := write(writer); err != nil || !bytes.Equal(output.Bytes(), want) {
					t.Fatalf("fragmented write differs from literal hello: %x, %v", output.Bytes(), err)
				}
			})
			t.Run("WriteFailure", func(t *testing.T) {
				var output bytes.Buffer
				writer := helloWriterFunc(func(buffer []byte) (int, error) {
					if output.Len() == 3 {
						return 0, io.ErrClosedPipe
					}
					return output.Write(buffer[:1])
				})
				if err := write(writer); !errors.Is(err, io.ErrClosedPipe) || !bytes.Equal(output.Bytes(), want[:3]) {
					t.Fatalf("failed write: %x, error %v", output.Bytes(), err)
				}
			})
			t.Run("NoWriteProgress", func(t *testing.T) {
				writer := helloWriterFunc(func([]byte) (int, error) { return 0, nil })
				if err := write(writer); !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("stalled write: error %v, want %v", err, io.ErrShortWrite)
				}
			})
			for offset := range want {
				modified := bytes.Clone(want)
				modified[offset] ^= 1
				if tt.client != nil {
					got, err := ParseClientHello(modified)
					if err == nil && VerifyClientHello(got, key) == nil {
						t.Fatalf("modified client byte %d passed authentication", offset)
					}
				} else {
					got, err := ParseServerHello(modified)
					if err == nil && VerifyServerHello(got, key) == nil {
						t.Fatalf("modified server byte %d passed authentication", offset)
					}
				}
			}
		})
	}
}

func TestHelloRejectsMalformedSelectors(t *testing.T) {
	key := []byte("test key")
	for _, tt := range []struct {
		name    string
		client  *ClientHello
		server  *ServerHello
		offsets []int
	}{
		{name: "Create", client: &ClientHello{Mode: HelloCreate, UnixSeconds: 1, Nonce: testNonce(1),
			LaneID: 1, Generation: 1, PathGroupID: 1, Target: target.MustParse("a:1")}, offsets: []int{37, 38, 39}},
		{name: "Join", client: &ClientHello{Mode: HelloJoin, UnixSeconds: 1, Nonce: testNonce(1),
			LaneID: 1, Generation: 1, PathGroupID: 1, SessionID: testSessionID(1)}, offsets: []int{37, 38, 39}},
		{name: "Created", server: &ServerHello{Result: ServerSessionCreated, ServerUnixSeconds: 1,
			RequestNonce: testNonce(1), SessionID: testSessionID(1), SessionSecret: testSessionSecret(1),
			PathGroupID: 1}, offsets: []int{93}},
		{name: "Accepted", server: &ServerHello{Result: ServerLaneAccepted, ServerUnixSeconds: 1,
			RequestNonce: testNonce(1), SessionID: testSessionID(1), PathGroupID: 1}, offsets: []int{61}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var encoded []byte
			var err error
			if tt.client != nil {
				encoded, err = MarshalClientHello(*tt.client)
			} else {
				encoded, err = MarshalServerHello(*tt.server)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, malformed := range []struct {
				name  string
				value []byte
			}{
				{name: "Zero", value: []byte{0}},
				{name: "NonminimalZero", value: []byte{0x80, 0}},
				{name: "NonminimalOne", value: []byte{0x81, 0}},
				{name: "Overflow", value: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}},
				{name: "TooLong", value: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 1}},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					for _, offset := range tt.offsets {
						modified := append(bytes.Clone(encoded[:offset]), malformed.value...)
						modified = append(modified, encoded[offset+1:]...)
						// Keep the length and HMAC valid so only the selector encoding is malformed.
						binary.BigEndian.PutUint16(modified[6:8], uint16(len(modified)-8))
						unsigned := modified[:len(modified)-sha256.Size]
						mac := hmac.New(sha256.New, key)
						mac.Write(unsigned)
						copy(modified[len(unsigned):], mac.Sum(nil))
						var parseErr error
						if tt.client != nil {
							_, parseErr = ParseClientHello(modified)
							if !errors.Is(parseErr, ErrInvalidClientHello) {
								t.Fatalf("selector at byte %d: error %v, want %v", offset, parseErr, ErrInvalidClientHello)
							}
						} else {
							_, parseErr = ParseServerHello(modified)
							if !errors.Is(parseErr, ErrInvalidServerHello) {
								t.Fatalf("selector at byte %d: error %v, want %v", offset, parseErr, ErrInvalidServerHello)
							}
						}
					}
				})
			}
		})
	}
}
