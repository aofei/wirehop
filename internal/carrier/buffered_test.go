package carrier

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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

func TestWebSocketConnReadFramesOwnsBufferedTail(t *testing.T) {
	for _, tt := range []struct {
		name    string
		size    int
		packets int
	}{
		{name: "OrdinaryBatch", size: 1452, packets: 15},
		{name: "NearBufferLimit", size: 8000, packets: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var encoded []byte
			var want []protocol.Frame
			for index := range tt.packets + 1 {
				size := tt.size
				if index == 0 {
					size = 32
				}
				payload := bytes.Repeat([]byte{byte(index + 1)}, size)
				payload[0] = 4
				data := protocol.Data{PacketID: 1_000_000_000 + uint64(index), DeadlineMicros: 86_405_000_000, Payload: payload}
				frame, err := protocol.MarshalData(data)
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, frame)
				encoded, err = protocol.AppendFrame(encoded, frame)
				if err != nil {
					t.Fatal(err)
				}
			}
			reader := bufio.NewReaderSize(bytes.NewReader(encoded), streamReadBufferSize)
			borrowed, err := reader.Peek(len(encoded))
			if err != nil {
				t.Fatal(err)
			}
			stream := WebSocketConn{messageReader: reader, messageOpen: true}
			var frames [maximumStreamReadBatchFrames]protocol.Frame
			count, err := stream.ReadFrames(context.Background(), frames[:])
			if err != nil || count != len(want) || !reflect.DeepEqual(frames[:count], want) {
				t.Fatalf("read batch = %d frames, %v", count, err)
			}
			if cap(stream.readBuffer) > streamReadBufferSize {
				t.Fatalf("tail capacity = %d bytes, exceeds the buffered prefix", cap(stream.readBuffer))
			}
			for index, offset := 1, 0; index < count; index++ {
				if &frames[index].Payload[0] != &stream.readBuffer[offset] {
					t.Fatal("buffered tail retains an additional backing allocation")
				}
				offset += len(frames[index].Payload)
			}
			clear(borrowed)
			if !reflect.DeepEqual(frames[:count], want) {
				t.Fatal("read-buffer reuse changed an owned payload")
			}
		})
	}
}

type fragmentedReadConn struct {
	net.Conn
	input *bytes.Reader
	limit int
}

func (c *fragmentedReadConn) Read(buffer []byte) (int, error) {
	return c.input.Read(buffer[:min(len(buffer), c.limit)])
}

func (*fragmentedReadConn) SetReadDeadline(time.Time) error { return nil }

func FuzzCarrierReadFrames(f *testing.F) {
	for _, encoded := range [][]byte{
		{}, {byte(protocol.FrameData)}, {0}, {0, 0},
		{byte(protocol.FramePing), 0}, {byte(protocol.FrameData), 128, 0},
		{byte(protocol.FramePing), 0, byte(protocol.FrameData), 128},
	} {
		f.Add(encoded, uint16(0), uint8(15))
	}
	for _, size := range []int{0, 32, 1452, streamReadBufferSize, 40_000, protocol.MaxFrameContentSize} {
		encoded, err := protocol.MarshalFrame(protocol.Frame{
			Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0xa5}, size),
		})
		if err != nil {
			f.Fatal(err)
		}
		for index := range 17 {
			encoded, err = protocol.AppendFrame(encoded, protocol.Frame{
				Type: protocol.FramePing, Payload: bytes.Repeat([]byte{byte(index + 1)}, 16),
			})
			if err != nil {
				f.Fatal(err)
			}
		}
		for _, maximumRead := range []uint16{0, 16_383, 32_767, 65_535} {
			for _, batchSize := range []uint8{0, 15} {
				f.Add(encoded, maximumRead, batchSize)
			}
		}
	}
	f.Fuzz(func(t *testing.T, encoded []byte, maximumRead uint16, batchSize uint8) {
		if len(encoded) > 2*protocol.MaxEncodedFrameSize {
			t.Skip()
		}
		for _, stream := range []frameBatchReader{
			NewStreamConn(&fragmentedReadConn{input: bytes.NewReader(encoded), limit: 1 + int(maximumRead)}),
			// Decode an open WebSocket message. Real message boundaries have separate carrier tests.
			&WebSocketConn{
				messageReader: bufio.NewReaderSize(&fragmentedReadConn{
					input: bytes.NewReader(encoded), limit: 1 + int(maximumRead),
				}, streamReadBufferSize),
				messageOpen: true,
			},
		} {
			reference := bytes.NewReader(encoded)
			var frames [maximumStreamReadBatchFrames]protocol.Frame
			for {
				count, err := stream.ReadFrames(context.Background(), frames[:1+int(batchSize)%len(frames)])
				if connection, ok := stream.(*WebSocketConn); ok && cap(connection.readBuffer) > streamReadBufferSize {
					t.Fatalf("WebSocket tail capacity = %d, exceeds the read buffer", cap(connection.readBuffer))
				}
				if count == 0 && err == nil {
					t.Fatalf("%T returned without a frame or an error", stream)
				}
				for _, frame := range frames[:count] {
					want, expectedErr := protocol.ReadFrame(reference)
					if expectedErr != nil || frame.Type != want.Type || !bytes.Equal(frame.Payload, want.Payload) {
						t.Fatalf("%T differs from the scalar reference: %v", stream, expectedErr)
					}
				}
				if err != nil {
					_, expectedErr := protocol.ReadFrame(reference)
					if !errors.Is(err, expectedErr) {
						t.Fatalf("%T error %v differs from reference error %v", stream, err, expectedErr)
					}
					break
				}
			}
		}
	})
}

func TestStreamConnBorrowedBatchStopsBeforeRefill(t *testing.T) {
	left, right := net.Pipe()
	stream := NewStreamConn(right)
	t.Cleanup(func() {
		left.Close()
		stream.Close()
	})
	want := []protocol.Frame{
		{Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0xa5}, 1452)},
		{Type: protocol.FramePing, Payload: bytes.Repeat([]byte{0x5a}, 16)},
		{Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0x96}, 128)},
	}
	var encoded []byte
	for _, frame := range want {
		var err error
		encoded, err = protocol.AppendFrame(encoded, frame)
		if err != nil {
			t.Fatal(err)
		}
	}
	cut := len(encoded) - 64
	resume := make(chan struct{})
	t.Cleanup(func() { close(resume) })
	result := make(chan error, 1)
	go func() {
		if _, err := left.Write(encoded[:cut]); err != nil {
			result <- err
			return
		}
		<-resume
		_, err := left.Write(encoded[cut:])
		result <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var frames [16]protocol.Frame
	count, err := stream.ReadFrames(ctx, frames[:])
	if err != nil || count != 2 || !reflect.DeepEqual(frames[:count], want[:2]) {
		t.Fatalf("complete prefix = %d frames, %v", count, err)
	}
	resume <- struct{}{}
	count, err = stream.ReadFrames(ctx, frames[:])
	if err != nil || count != 1 || !reflect.DeepEqual(frames[0], want[2]) {
		t.Fatalf("fragmented tail = %d frames, %v", count, err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestStreamConnBorrowedPrefixWithMalformedTail(t *testing.T) {
	left, right := net.Pipe()
	stream := NewStreamConn(right)
	t.Cleanup(func() {
		left.Close()
		stream.Close()
	})
	want := protocol.Frame{Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0xa5}, 1452)}
	encoded, err := protocol.MarshalFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 0, 0)
	go func() { left.Write(encoded) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var frames [16]protocol.Frame
	count, err := stream.ReadFrames(ctx, frames[:])
	if count != 1 || !errors.Is(err, protocol.ErrInvalidFrameType) || !reflect.DeepEqual(frames[0], want) {
		t.Fatalf("malformed tail lost its valid prefix: %d frames, %v", count, err)
	}
}

func TestStreamConnLargeFrameFallback(t *testing.T) {
	for _, tt := range []struct {
		name  string
		size  int
		mixed bool
	}{
		{name: "BufferBoundary", size: streamReadBufferSize, mixed: true},
		{name: "BeyondBuffer", size: 40_000, mixed: true},
		{name: "MaximumContent", size: protocol.MaxFrameContentSize},
	} {
		t.Run(tt.name, func(t *testing.T) {
			left, right := net.Pipe()
			stream := NewStreamConn(right)
			t.Cleanup(func() {
				left.Close()
				stream.Close()
			})
			want := []protocol.Frame{
				{Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0xa5}, tt.size)},
				{Type: protocol.FramePing, Payload: bytes.Repeat([]byte{0x5a}, 16)},
				{Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0x96}, 128)},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- NewStreamConn(left).WriteFrames(ctx, want) }()
			var frames [16]protocol.Frame
			for offset := 0; offset < len(want); {
				count, err := stream.ReadFrames(ctx, frames[:])
				if err != nil || count == 0 || offset+count > len(want) || !reflect.DeepEqual(frames[:count], want[offset:offset+count]) {
					t.Fatalf("large-frame batch at %d: %d frames, %v", offset, count, err)
				}
				if offset == 0 && tt.mixed && count != len(want) {
					t.Fatalf("owned and borrowed batch contains %d frames, want %d", count, len(want))
				}
				offset += count
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWebSocketConnLargeFrameBatch(t *testing.T) {
	for _, tt := range []struct {
		name string
		size int
	}{
		{name: "BufferBoundary", size: streamReadBufferSize},
		{name: "BeyondBuffer", size: 40_000},
		{name: "MaximumContent", size: protocol.MaxFrameContentSize},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want := []protocol.Frame{{Type: protocol.FrameData, Payload: bytes.Repeat([]byte{0xa5}, tt.size)}}
			for index := range 17 {
				want = append(want, protocol.Frame{Type: protocol.FramePing, Payload: bytes.Repeat([]byte{byte(index + 1)}, 16)})
			}
			var message []byte
			for _, frame := range want {
				var err error
				message, err = protocol.AppendFrame(message, frame)
				if err != nil {
					t.Fatal(err)
				}
			}
			last := protocol.Frame{Type: protocol.FramePong, Payload: bytes.Repeat([]byte{0x5a}, 24)}
			nextMessage, err := protocol.MarshalFrame(last)
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, last)
			written := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connection, err := websocket.Accept(w, r, nil)
				if err != nil {
					written <- err
					return
				}
				defer connection.CloseNow()
				err = connection.Write(r.Context(), websocket.MessageBinary, message)
				if err == nil {
					err = connection.Write(r.Context(), websocket.MessageBinary, nextMessage)
				}
				written <- err
				connection.Read(r.Context())
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			stream := NewWebSocketConn(connection)
			defer stream.Close()
			var frames [maximumStreamReadBatchFrames]protocol.Frame
			capacities := [...]int{16, 2, 1, 15}
			for offset, call := 0, 0; offset < len(want); call++ {
				readContext, stop := context.WithCancel(ctx)
				count, err := stream.ReadFrames(readContext, frames[:capacities[call%len(capacities)]])
				stop()
				if err != nil || count == 0 || offset+count > len(want) || !reflect.DeepEqual(frames[:count], want[offset:offset+count]) {
					t.Fatalf("WebSocket batch at %d = %d frames, %v", offset, count, err)
				}
				clear(frames[:count])
				offset += count
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}
