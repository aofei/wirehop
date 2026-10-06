package carrier

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/coder/websocket"
)

type benchmarkConn struct{}

var benchmarkFrameSink protocol.Frame

func (benchmarkConn) Read([]byte) (int, error)         { panic("unexpected read") }
func (benchmarkConn) Write(value []byte) (int, error)  { return len(value), nil }
func (benchmarkConn) Close() error                     { return nil }
func (benchmarkConn) LocalAddr() net.Addr              { return nil }
func (benchmarkConn) RemoteAddr() net.Addr             { return nil }
func (benchmarkConn) SetDeadline(time.Time) error      { return nil }
func (benchmarkConn) SetReadDeadline(time.Time) error  { return nil }
func (benchmarkConn) SetWriteDeadline(time.Time) error { return nil }

func BenchmarkStreamConnWriteDataBatch(b *testing.B) {
	stream := NewStreamConn(benchmarkConn{})
	batch := make([]protocol.Data, 16)
	for index := range batch {
		batch[index] = protocol.Data{PacketID: uint64(index + 1), DeadlineMicros: 1, Payload: make([]byte, 1420)}
	}
	if err := stream.WriteDataBatch(context.Background(), batch); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(batch) * 1420))
	for b.Loop() {
		if err := stream.WriteDataBatch(context.Background(), batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamConnReadFrame(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			accepted <- connection
		}
	}()
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	stream := NewStreamConn(connection)
	peer := <-accepted
	expected := protocol.Frame{Type: protocol.FramePing, Payload: make([]byte, 16)}
	encoded, err := protocol.MarshalFrame(expected)
	if err != nil {
		b.Fatal(err)
	}
	writeResult := make(chan error, 1)
	go func() {
		for {
			if _, err := peer.Write(encoded); err != nil {
				writeResult <- err
				return
			}
		}
	}()
	b.Cleanup(func() {
		stream.Close()
		peer.Close()
		<-writeResult
	})
	b.ReportAllocs()
	b.SetBytes(int64(len(expected.Payload)))
	for b.Loop() {
		frame, err := stream.ReadFrame(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if frame.Type != expected.Type || len(frame.Payload) != len(expected.Payload) {
			b.Fatalf("ReadFrame() = %#v, want %#v", frame, expected)
		}
		benchmarkFrameSink = frame
	}
}

func BenchmarkWebSocketConnReadFrames(b *testing.B) {
	for _, frameCount := range []int{1, 16} {
		b.Run(strconv.Itoa(frameCount), func(b *testing.B) {
			for _, cancelable := range []bool{false, true} {
				name := "Background"
				if cancelable {
					name = "Cancelable"
				}
				b.Run(name, func(b *testing.B) {
					var message []byte
					for range frameCount {
						var err error
						message, err = protocol.AppendFrame(message, protocol.Frame{
							Type: protocol.FrameDeliveryReport, Payload: make([]byte, 1420),
						})
						if err != nil {
							b.Fatal(err)
						}
					}
					done := make(chan struct{})
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						defer close(done)
						connection, err := websocket.Accept(w, r, nil)
						if err != nil {
							return
						}
						defer connection.CloseNow()
						for connection.Write(context.Background(), websocket.MessageBinary, message) == nil {
						}
					}))
					b.Cleanup(server.Close)
					connection, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
					if err != nil {
						b.Fatal(err)
					}
					stream := NewWebSocketConn(connection)
					b.Cleanup(func() {
						stream.Close()
						<-done
					})
					ctx := context.Background()
					if cancelable {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						b.Cleanup(cancel)
					}
					var frames [16]protocol.Frame
					b.ReportAllocs()
					b.SetBytes(int64(len(message)))
					for b.Loop() {
						for remaining := frameCount; remaining > 0; {
							count, err := stream.ReadFrames(ctx, frames[:remaining])
							if err != nil || count == 0 {
								b.Fatalf("read batch = %d, %v", count, err)
							}
							remaining -= count
							benchmarkFrameSink = frames[count-1]
							clear(frames[:count])
						}
					}
				})
			}
		})
	}
}
