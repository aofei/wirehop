package carrier

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/coder/websocket"
)

func TestWebSocketConnReadFrames(t *testing.T) {
	t.Run("ProgressAndChangingCallContext", func(t *testing.T) {
		release := make(chan struct{})
		written := make(chan error, 1)
		finished := make(chan error, 1)
		payload := bytes.Repeat([]byte{42}, 6000)
		frame, err := protocol.MarshalFrame(protocol.Frame{Type: protocol.FrameDeliveryReport, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := websocket.Accept(w, r, nil)
			if err != nil {
				written <- err
				return
			}
			defer connection.CloseNow()
			writer, err := connection.Writer(r.Context(), websocket.MessageBinary)
			if err == nil {
				_, err = writer.Write(bytes.Repeat(frame, 2))
			}
			written <- err
			<-release
			if err == nil {
				err = writer.Close()
			}
			finished <- err
			connection.Read(r.Context())
		}))
		defer server.Close()
		defer close(release)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		stream := NewWebSocketConn(connection)
		defer stream.Close()
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		admission, stopAdmission := context.WithTimeout(ctx, time.Second)
		first, err := stream.ReadFrame(admission)
		stopAdmission()
		if err != nil || !bytes.Equal(first.Payload, payload) {
			t.Fatalf("frame before message end = %v, payload length %d", err, len(first.Payload))
		}
		release <- struct{}{}
		second, err := stream.ReadFrame(ctx)
		if err != nil || !bytes.Equal(second.Payload, payload) {
			t.Fatalf("frame after admission cancellation = %v, payload length %d", err, len(second.Payload))
		}
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ValidPrefixBeforeMalformedTail", func(t *testing.T) {
		prefix, err := protocol.MarshalFrame(protocol.Frame{Type: protocol.FrameDeliveryReport, Payload: []byte{7}})
		if err != nil {
			t.Fatal(err)
		}
		message := append(prefix, 0x85, 0)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer connection.CloseNow()
			connection.Write(r.Context(), websocket.MessageBinary, message)
			connection.Read(r.Context())
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		stream := NewWebSocketConn(connection)
		defer stream.Close()
		var frames [2]protocol.Frame
		count, err := stream.ReadFrames(ctx, frames[:])
		if count != 1 || !bytes.Equal(frames[0].Payload, []byte{7}) {
			t.Fatalf("valid prefix = %d frames, first frame %#v", count, frames[0])
		}
		if err == nil {
			_, err = stream.ReadFrame(ctx)
		}
		if !errors.Is(err, protocol.ErrInvalidInteger) {
			t.Fatalf("malformed tail = %v, want invalid integer", err)
		}
	})
}

func TestStreamConnWriteDataBatchWithin(t *testing.T) {
	for _, tt := range []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{name: "ThreeSeconds", timeout: 3 * time.Second, wantErr: true},
		{name: "TenSeconds", timeout: 10 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				left, right := net.Pipe()
				defer left.Close()
				defer right.Close()
				data := make([]protocol.Data, 16)
				var encoded []byte
				for index := range data {
					data[index] = protocol.Data{PacketID: uint64(index + 1), DeadlineMicros: 1_000_000,
						Payload: bytes.Repeat([]byte{byte(index)}, 1400)}
					var err error
					encoded, err = protocol.AppendDataFrame(encoded, data[index])
					if err != nil {
						t.Fatal(err)
					}
				}
				done := make(chan []byte, 1)
				go func() {
					var received []byte
					buffer := make([]byte, 1024)
					for len(received) < len(encoded) {
						synctest.Sleep(200 * time.Millisecond)
						count, err := right.Read(buffer)
						received = append(received, buffer[:count]...)
						if err != nil {
							break
						}
					}
					done <- received
				}()
				started := time.Now()
				err := WriteDataBatchWithin(t.Context(), NewStreamConn(left), data, tt.timeout)
				left.Close()
				received := <-done
				if tt.wantErr {
					if err == nil || len(received) == 0 || time.Since(started) < tt.timeout {
						t.Fatalf("bounded progressing write = %v, %d bytes, %v", err, len(received), time.Since(started))
					}
				} else if err != nil || !bytes.Equal(received, encoded) {
					t.Fatalf("slow complete write = %v, received %d of %d bytes", err, len(received), len(encoded))
				}
			})
		})
	}
}

func TestWebSocketConnReadCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := websocket.Accept(w, r, nil)
		if err == nil {
			defer connection.CloseNow()
			connection.Read(r.Context())
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := NewWebSocketConn(connection)
	defer stream.Close()
	readContext, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if _, err := stream.ReadFrame(readContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled blocking read = %v, want deadline exceeded", err)
	}
}

func TestWebSocketConnReadTransportTruncation(t *testing.T) {
	type connectionKey struct{}
	encoded, err := protocol.MarshalFrame(protocol.Frame{Type: protocol.FrameDeliveryReport, Payload: make([]byte, 12000)})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		writer, err := connection.Writer(r.Context(), websocket.MessageBinary)
		if err == nil {
			writer.Write(encoded[:6000])
		}
		r.Context().Value(connectionKey{}).(net.Conn).Close()
		connection.CloseNow()
	}))
	server.Config.ConnContext = func(ctx context.Context, connection net.Conn) context.Context {
		return context.WithValue(ctx, connectionKey{}, connection)
	}
	server.Start()
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := NewWebSocketConn(connection)
	defer stream.Close()
	if _, err := stream.ReadFrame(ctx); err == nil || errors.Is(err, protocol.ErrTrailingFrameData) ||
		errors.Is(err, ErrInvalidWebSocketMessage) {
		t.Fatalf("interrupted network read became a protocol violation: %v", err)
	}
}
