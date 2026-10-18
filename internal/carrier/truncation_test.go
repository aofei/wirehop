package carrier

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/coder/websocket"
)

func TestWebSocketConnReadFramesTransportTruncation(t *testing.T) {
	for _, route := range []struct {
		name     string
		secure   bool
		toServer bool
	}{
		{name: "WS/ToClient"},
		{name: "WS/ToServer", toServer: true},
		{name: "WSS/ToClient", secure: true},
		{name: "WSS/ToServer", secure: true, toServer: true},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, tt := range []struct {
				name       string
				cut        int
				large      bool
				fragmented bool
				split      int
				control    bool
				truncate   bool
				wantFrames int
			}{
				{name: "HeaderOnly"},
				{name: "PartialPrefix", cut: 1},
				{name: "CompletePrefix", cut: 2},
				{name: "CompleteFrame", cut: 3},
				{name: "PartialNextHeader", cut: 4},
				{name: "PartialPayload", cut: 6000},
				{name: "BufferedPrefix", cut: streamReadBufferSize + 1, large: true, wantFrames: 1},
				{name: "MissingContinuation", fragmented: true, wantFrames: 2},
				{name: "ContinuationBetweenFrames", split: 3, wantFrames: 2},
				{name: "ContinuationAcrossFrameHeader", split: 4, wantFrames: 2},
				{name: "ContinuationBeforeFrameExtension", split: 5, wantFrames: 2},
				{name: "ContinuationAcrossPayload", split: 6001, wantFrames: 2},
				{name: "ControlBeforeContinuation", split: 1, control: true, wantFrames: 2},
				{name: "TruncatedContinuation", split: 6001, truncate: true, wantFrames: 1},
				{name: "EmptyFinalContinuation", split: -1, control: true, wantFrames: 2},
			} {
				t.Run(tt.name, func(t *testing.T) {
					payloadSize := 12000
					if tt.large {
						payloadSize = 40000
					}
					want := []protocol.Frame{
						{Type: protocol.FramePing, Payload: []byte{1}},
						{Type: protocol.FrameData, Payload: make([]byte, payloadSize)},
					}
					var message []byte
					for _, frame := range want {
						var err error
						message, err = protocol.AppendFrame(message, frame)
						if err != nil {
							t.Fatal(err)
						}
					}
					var wire []byte
					appendFragment := func(opcode byte, payload []byte, length int) {
						wire = append(wire, opcode)
						lengthOffset := len(wire)
						if length < 126 {
							wire = append(wire, byte(length))
						} else {
							wire = append(wire, 126, byte(length>>8), byte(length))
						}
						if route.toServer {
							wire[lengthOffset] |= 0x80
							key := [...]byte{opcode, 127, 128, 255}
							wire = append(wire, key[:]...)
							for index, value := range payload {
								wire = append(wire, value^key[index%len(key)])
							}
						} else {
							wire = append(wire, payload...)
						}
					}
					if tt.split != 0 {
						split := tt.split
						if split < 0 {
							split = len(message)
						}
						appendFragment(0x02, message[:split], split)
						if tt.control {
							appendFragment(0x8a, []byte{1, 2, 3}, 3)
						}
						tail := message[split:]
						length := len(tail)
						if tt.truncate {
							tail = tail[:len(tail)/2]
						}
						appendFragment(0x80, tail, length)
					} else if tt.fragmented {
						appendFragment(0x02, message, len(message))
					} else {
						appendFragment(0x82, message[:tt.cut], len(message))
					}
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					type readResult struct {
						frames int
						err    error
					}
					corrupted := errors.New("transport truncation exposed corrupted frame bytes")
					receive := func(stream *WebSocketConn) readResult {
						defer stream.Close()
						var frames [maximumStreamReadBatchFrames]protocol.Frame
						received := 0
						for {
							count, err := stream.ReadFrames(ctx, frames[:])
							if received+count > len(want) || !reflect.DeepEqual(frames[:count], want[received:received+count]) {
								return readResult{frames: received, err: corrupted}
							}
							received += count
							if err != nil {
								return readResult{frames: received, err: err}
							}
						}
					}
					type connectionKey struct{}
					result := make(chan readResult, 1)
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						socket, err := AcceptWebSocket(w, r, nil)
						if err != nil {
							result <- readResult{err: err}
							return
						}
						defer socket.Close()
						if route.toServer {
							result <- receive(socket)
							return
						}
						_, err = r.Context().Value(connectionKey{}).(net.Conn).Write(wire)
						result <- readResult{err: err}
					}))
					server.Config.ConnContext = func(ctx context.Context, socket net.Conn) context.Context {
						return context.WithValue(ctx, connectionKey{}, socket)
					}
					if route.secure {
						server.StartTLS()
					} else {
						server.Start()
					}
					defer server.Close()
					transport := server.Client().Transport.(*http.Transport).Clone()
					defer transport.CloseIdleConnections()
					rawConnections := make(chan net.Conn, 1)
					transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
						socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
						if err == nil {
							rawConnections <- socket
						}
						return socket, err
					}
					transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
						socket, err := (&tls.Dialer{Config: transport.TLSClientConfig}).DialContext(ctx, network, address)
						if err == nil {
							rawConnections <- socket
						}
						return socket, err
					}
					socket, _, err := DialWebSocket(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
						HTTPClient: &http.Client{Transport: transport},
					})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					var received readResult
					if route.toServer {
						raw := <-rawConnections
						if _, err := raw.Write(wire); err != nil {
							t.Fatal(err)
						}
						raw.Close()
						received = <-result
					} else {
						received = receive(socket)
						if written := <-result; written.err != nil {
							t.Fatal(written.err)
						}
					}
					if received.frames != tt.wantFrames || errors.Is(received.err, corrupted) ||
						!errors.Is(received.err, io.ErrUnexpectedEOF) {
						t.Fatalf("truncated transport = %d frames, %v, want %d frames and unexpected EOF",
							received.frames, received.err, tt.wantFrames)
					}
					for _, violation := range []error{ErrInvalidWebSocketMessage, protocol.ErrTrailingFrameData,
						protocol.ErrInvalidFrameType, protocol.ErrInvalidInteger, protocol.ErrFrameTooLarge} {
						if errors.Is(received.err, violation) {
							t.Fatalf("truncated transport became a protocol violation: %v", received.err)
						}
					}
				})
			}
		})
	}
}
