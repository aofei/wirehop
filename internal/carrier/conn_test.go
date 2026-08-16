package carrier

import (
	"bytes"
	"context"
	"crypto/tls"
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

func TestStreamConn(t *testing.T) {
	leftStream, rightStream := net.Pipe()
	left := NewStreamConn(leftStream)
	right := NewStreamConn(rightStream)
	t.Cleanup(func() {
		left.Close()
		right.Close()
	})

	want := []protocol.Frame{
		{Type: protocol.FramePing, Payload: []byte{1}},
		{Type: protocol.FrameProbe, Payload: []byte{2, 3}},
	}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- left.WriteFrames(context.Background(), want)
	}()
	for _, expected := range want {
		got, err := right.ReadFrame(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("ReadFrame() = %#v, want %#v", got, expected)
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	data := protocol.Data{
		PacketID: 1, DeadlineMicros: 100, Payload: []byte{4, 0, 0, 0, 9},
	}
	go func() { writeDone <- left.WriteDataBatch(context.Background(), []protocol.Data{data}) }()
	frame, err := right.ReadFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := protocol.ParseData(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, data) {
		t.Fatalf("WriteData() payload = %#v, want %#v", got, data)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestStreamConnDeadline(t *testing.T) {
	leftStream, rightStream := net.Pipe()
	left := NewStreamConn(leftStream)
	t.Cleanup(func() {
		left.Close()
		rightStream.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := left.ReadFrame(ctx)
	networkError, ok := errors.AsType[net.Error](err)
	if !ok || !networkError.Timeout() {
		t.Fatalf("ReadFrame() error = %v", err)
	}
}

func TestStreamConnClosedContext(t *testing.T) {
	leftStream, rightStream := net.Pipe()
	left := NewStreamConn(leftStream)
	t.Cleanup(func() {
		left.Close()
		rightStream.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := left.ReadFrame(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if err := left.WriteFrames(ctx, []protocol.Frame{{Type: protocol.FramePing}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteFrames() error = %v", err)
	}
}

func TestStreamConnAbort(t *testing.T) {
	for _, tt := range []struct {
		name string
		wrap func(net.Conn) net.Conn
	}{
		{name: "TCP", wrap: func(connection net.Conn) net.Conn { return connection }},
		{name: "TLS", wrap: func(connection net.Conn) net.Conn {
			return tls.Client(connection, &tls.Config{InsecureSkipVerify: true})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				connection, err := listener.Accept()
				if err == nil {
					accepted <- connection
				}
			}()
			connection, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			peer := <-accepted
			defer peer.Close()
			if err := NewStreamConn(tt.wrap(connection)).Abort(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWebSocketConnAbort(t *testing.T) {
	serverResult := make(chan error, 1)
	accepted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.CloseNow()
		close(accepted)
		_, _, err = connection.Read(request.Context())
		serverResult <- err
	}))
	defer server.Close()

	rawConnections := make(chan net.Conn, 1)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			rawConnections <- connection
		}
		return connection, err
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	stream := NewWebSocketConn(connection)
	stream.SetAbortConnection(<-rawConnections)
	<-accepted
	if err := stream.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Abort(); err != nil {
		t.Fatalf("second Abort() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() after Abort() error = %v", err)
	}
	select {
	case err := <-serverResult:
		if err == nil {
			t.Fatal("peer read succeeded after abortive close")
		}
	case <-time.After(time.Second):
		t.Fatal("peer read remained blocked after abortive close")
	}
}

func TestWebSocketConnReadLimit(t *testing.T) {
	t.Run("LargeFrame", func(t *testing.T) {
		result := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				result <- err
				return
			}
			defer connection.Close(websocket.StatusNormalClosure, "")
			frame, err := NewWebSocketConn(connection).ReadFrame(request.Context())
			if err == nil {
				var data protocol.Data
				data, err = protocol.ParseData(frame)
				if err == nil && len(data.Payload) != protocol.MaxPacketSize {
					err = errors.New("large data payload was truncated")
				}
			}
			result <- err
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		stream := NewWebSocketConn(connection)
		defer stream.Close()
		if err := stream.WriteDataBatch(ctx, []protocol.Data{{
			PacketID: 1, DeadlineMicros: 100, Payload: make([]byte, protocol.MaxPacketSize),
		}}); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("OversizedMessage", func(t *testing.T) {
		result := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				result <- err
				return
			}
			defer connection.Close(websocket.StatusNormalClosure, "")
			_, err = NewWebSocketConn(connection).ReadFrame(request.Context())
			result <- err
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(websocket.StatusNormalClosure, "")
		if err := connection.Write(ctx, websocket.MessageBinary, make([]byte, WebSocketReadLimit+1)); err != nil {
			t.Fatal(err)
		}
		if err := <-result; !errors.Is(err, ErrInvalidWebSocketMessage) {
			t.Fatalf("oversized WebSocket message error = %v, want %v", err, ErrInvalidWebSocketMessage)
		}
	})

	t.Run("OversizedWrite", func(t *testing.T) {
		release := make(chan struct{})
		accepted := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				return
			}
			defer connection.CloseNow()
			close(accepted)
			<-release
		}))
		defer server.Close()
		defer close(release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		stream := NewWebSocketConn(connection)
		defer stream.Close()
		<-accepted
		packets := make([]protocol.Data, 3)
		for index := range packets {
			packets[index] = protocol.Data{
				PacketID: uint64(index + 1), DeadlineMicros: 100,
				Payload: make([]byte, protocol.MaxPacketSize),
			}
		}
		if err := stream.WriteDataBatch(ctx, packets); !errors.Is(err, ErrWebSocketMessageTooLarge) {
			t.Fatalf("WriteDataBatch() error = %v, want %v", err, ErrWebSocketMessageTooLarge)
		}
	})
}

func TestWebSocketConnMessageBoundaries(t *testing.T) {
	frames := []protocol.Frame{
		{Type: protocol.FramePing, Payload: make([]byte, 16)},
		{Type: protocol.FrameProbe, Payload: make([]byte, 8)},
	}
	encoded := make([]byte, 0)
	for _, frame := range frames {
		value, err := protocol.MarshalFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, value...)
	}

	t.Run("MultipleFrames", func(t *testing.T) {
		result := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				result <- err
				return
			}
			defer connection.CloseNow()
			stream := NewWebSocketConn(connection)
			for _, expected := range frames {
				frame, err := stream.ReadFrame(request.Context())
				if err != nil {
					result <- err
					return
				}
				if !reflect.DeepEqual(frame, expected) {
					result <- errors.New("decoded WebSocket frame mismatch")
					return
				}
			}
			result <- nil
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.CloseNow()
		if err := connection.Write(ctx, websocket.MessageBinary, encoded); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("PendingFrameHonorsCancellation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				return
			}
			defer connection.CloseNow()
			connection.Write(request.Context(), websocket.MessageBinary, encoded)
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		stream := NewWebSocketConn(connection)
		defer stream.Close()
		if _, err := stream.ReadFrame(ctx); err != nil {
			t.Fatal(err)
		}
		canceled, stop := context.WithCancel(context.Background())
		stop()
		if _, err := stream.ReadFrame(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadFrame() error = %v, want %v", err, context.Canceled)
		}
	})

	for _, tt := range []struct {
		name        string
		messageType websocket.MessageType
		messages    [][]byte
		want        error
	}{
		{name: "SplitFrame", messageType: websocket.MessageBinary, messages: [][]byte{encoded[:3], encoded[3:]},
			want: protocol.ErrTrailingFrameData},
		{name: "EmptyMessage", messageType: websocket.MessageBinary, messages: [][]byte{{}},
			want: ErrInvalidWebSocketMessage},
		{name: "TextMessage", messageType: websocket.MessageText, messages: [][]byte{encoded},
			want: ErrInvalidWebSocketMessage},
		{name: "TrailingPartialFrame", messageType: websocket.MessageBinary,
			messages: [][]byte{append(append([]byte(nil), encoded...), byte(protocol.FramePing))},
			want:     protocol.ErrTrailingFrameData},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				connection, err := websocket.Accept(writer, request, nil)
				if err != nil {
					result <- err
					return
				}
				defer connection.CloseNow()
				_, err = NewWebSocketConn(connection).ReadFrame(request.Context())
				result <- err
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.CloseNow()
			for _, message := range tt.messages {
				if err := connection.Write(ctx, tt.messageType, message); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-result; !errors.Is(err, tt.want) {
				t.Fatalf("ReadFrame() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestWebSocketConnReleasesLargeReadBuffer(t *testing.T) {
	large, err := protocol.MarshalFrame(protocol.Frame{
		Type: protocol.FrameData, Payload: make([]byte, maximumRetainedBufferCapacity+1),
	})
	if err != nil {
		t.Fatal(err)
	}
	small, err := protocol.MarshalFrame(protocol.Frame{Type: protocol.FramePing, Payload: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err == nil {
			defer connection.CloseNow()
			err = connection.Write(request.Context(), websocket.MessageBinary, large)
		}
		if err == nil {
			err = connection.Write(request.Context(), websocket.MessageBinary, small)
		}
		result <- err
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := NewWebSocketConn(connection)
	defer stream.Close()
	frame, err := stream.ReadFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.Payload) != maximumRetainedBufferCapacity+1 ||
		stream.readBuffer.Cap() <= maximumRetainedBufferCapacity {
		t.Fatalf("large frame payload length = %d, read buffer capacity = %d",
			len(frame.Payload), stream.readBuffer.Cap())
	}
	frame, err = stream.ReadFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frame.Payload, []byte{1}) || stream.readBuffer.Cap() > maximumRetainedBufferCapacity {
		t.Fatalf("small frame = %#v, read buffer capacity = %d", frame, stream.readBuffer.Cap())
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestWebSocketConnReleasesLargeReadBufferBeforeReadFailure(t *testing.T) {
	large, err := protocol.MarshalFrame(protocol.Frame{
		Type: protocol.FrameData, Payload: make([]byte, maximumRetainedBufferCapacity+1),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			result <- err
			return
		}
		err = connection.Write(request.Context(), websocket.MessageBinary, large)
		connection.CloseNow()
		result <- err
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := NewWebSocketConn(connection)
	defer stream.Close()
	if _, err := stream.ReadFrame(ctx); err != nil {
		t.Fatal(err)
	}
	if stream.readBuffer.Cap() <= maximumRetainedBufferCapacity {
		t.Fatalf("large read buffer capacity = %d", stream.readBuffer.Cap())
	}
	if _, err := stream.ReadFrame(ctx); err == nil {
		t.Fatal("ReadFrame() succeeded after peer close")
	}
	if stream.readBuffer.Cap() > maximumRetainedBufferCapacity {
		t.Fatalf("read buffer capacity after failure = %d", stream.readBuffer.Cap())
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestReusableBuffer(t *testing.T) {
	for _, tt := range []struct {
		name     string
		capacity int
		wantNil  bool
	}{
		{name: "Ordinary", capacity: maximumRetainedBufferCapacity},
		{name: "Large", capacity: maximumRetainedBufferCapacity + 1, wantNil: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buffer := reusableBuffer(make([]byte, tt.capacity))
			if (buffer == nil) != tt.wantNil || (!tt.wantNil && (len(buffer) != 0 || cap(buffer) != tt.capacity)) {
				t.Fatalf("reusableBuffer() length = %d, capacity = %d, nil = %t",
					len(buffer), cap(buffer), buffer == nil)
			}
		})
	}
}

func TestWebSocketConnWriteBoundary(t *testing.T) {
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			result <- err
			return
		}
		defer connection.CloseNow()
		messageType, message, err := connection.Read(request.Context())
		if err == nil && messageType != websocket.MessageBinary {
			err = ErrInvalidWebSocketMessage
		}
		if err == nil {
			var frames []protocol.Frame
			frames, err = protocol.ParseFrames(message)
			if err == nil && len(frames) != 2 {
				err = errors.New("WebSocket write did not preserve one two-frame message")
			}
		}
		result <- err
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := NewWebSocketConn(connection)
	defer stream.Close()
	if err := stream.WriteFrames(ctx, []protocol.Frame{
		{Type: protocol.FramePing, Payload: make([]byte, 16)},
		{Type: protocol.FrameProbe, Payload: make([]byte, 8)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
