package carrier

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
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

type bufferedUpgradeResponseWriter struct {
	http.ResponseWriter
	size int
}

func (w bufferedUpgradeResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	connection, buffered, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	// Fill the hijacked reader without assuming TCP preserves the preceding Write boundary.
	if _, err := buffered.Reader.Peek(w.size); err != nil {
		connection.Close()
		return nil, nil, err
	}
	return connection, buffered, nil
}

func TestWebSocketConnReadFramesBufferedUpgrade(t *testing.T) {
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
				payload    []byte
				length     int
				wantFrames int
				wantError  error
			}{
				{name: "CompleteMessage", payload: []byte{0x16, 0, 1, 0x17, 0, 2}, length: 6, wantFrames: 2,
					wantError: io.ErrUnexpectedEOF},
				{name: "TruncatedMessage", payload: []byte{0x16, 0, 1}, length: 6, wantError: io.ErrUnexpectedEOF},
				{name: "IncompleteFrame", payload: []byte{0x16, 0, 1, 0x80}, length: 4, wantFrames: 1,
					wantError: protocol.ErrTrailingFrameData},
				{name: "EmptyMessage", wantError: ErrInvalidWebSocketMessage},
			} {
				t.Run(tt.name, func(t *testing.T) {
					wire := []byte{0x82, byte(tt.length)}
					payload := append([]byte(nil), tt.payload...)
					if route.toServer {
						wire[1] |= 0x80
						key := [...]byte{6, 127, 128, 255}
						wire = append(wire, key[:]...)
						for index := range payload {
							payload[index] ^= key[index%len(key)]
						}
					}
					wire = append(wire, payload...)
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					type readResult struct {
						frames int
						err    error
					}
					corrupted := errors.New("buffered upgrade changed frame contents")
					receive := func(connection *WebSocketConn) readResult {
						want := []protocol.Frame{
							{Type: protocol.FramePing, Payload: []byte{1}},
							{Type: protocol.FramePong, Payload: []byte{2}},
						}
						var frames [maximumStreamReadBatchFrames]protocol.Frame
						received := 0
						for {
							count, err := connection.ReadFrames(ctx, frames[:])
							if received+count > len(want) || !reflect.DeepEqual(frames[:count], want[received:received+count]) {
								return readResult{frames: received, err: corrupted}
							}
							received += count
							if err != nil {
								return readResult{frames: received, err: err}
							}
						}
					}
					result := make(chan readResult, 1)
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var outcome readResult
						defer func() { result <- outcome }()
						if route.toServer {
							connection, err := AcceptWebSocket(bufferedUpgradeResponseWriter{
								ResponseWriter: w, size: len(wire),
							}, r, nil)
							if err != nil {
								outcome.err = err
								return
							}
							defer connection.Close()
							outcome = receive(connection)
							return
						}
						connection, _, err := http.NewResponseController(w).Hijack()
						if err != nil {
							outcome.err = err
							return
						}
						defer connection.Close()
						accept := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
						header := "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
							"Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(accept[:]) + "\r\n\r\n"
						_, outcome.err = connection.Write(append([]byte(header), wire...))
					}))
					if route.secure {
						server.StartTLS()
					} else {
						server.Start()
					}
					defer server.Close()
					var received readResult
					if route.toServer {
						var connection net.Conn
						var err error
						if route.secure {
							connection, err = (&tls.Dialer{Config: server.Client().Transport.(*http.Transport).TLSClientConfig}).
								DialContext(ctx, "tcp", server.Listener.Addr().String())
						} else {
							connection, err = (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
						}
						if err != nil {
							t.Fatal(err)
						}
						defer connection.Close()
						if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						header := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"+
							"Sec-WebSocket-Key: AAECAwQFBgcICQoLDA0ODw==\r\nSec-WebSocket-Version: 13\r\n\r\n", server.Listener.Addr())
						if _, err := connection.Write(append([]byte(header), wire...)); err != nil {
							t.Fatal(err)
						}
						if err := connection.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
							t.Fatal(err)
						}
						response, err := http.ReadResponse(bufio.NewReader(connection), nil)
						if err != nil {
							t.Fatal(err)
						}
						response.Body.Close()
						if response.StatusCode != http.StatusSwitchingProtocols {
							t.Fatalf("upgrade status = %d", response.StatusCode)
						}
						received = <-result
					} else {
						connection, _, err := DialWebSocket(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
							HTTPClient: server.Client(),
						})
						if err != nil {
							t.Fatal(err)
						}
						defer connection.Close()
						received = receive(connection)
						if written := <-result; written.err != nil {
							t.Fatal(written.err)
						}
					}
					if received.frames != tt.wantFrames || !errors.Is(received.err, tt.wantError) {
						t.Fatalf("buffered upgrade = %d frames, %v, want %d frames and %v",
							received.frames, received.err, tt.wantFrames, tt.wantError)
					}
				})
			}
		})
	}
}
