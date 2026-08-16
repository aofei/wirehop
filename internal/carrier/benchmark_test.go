package carrier

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
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
