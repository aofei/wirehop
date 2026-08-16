// Package carrier provides a common framed connection contract for WireHop lanes.
package carrier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/coder/websocket"
)

const (
	// WebSocketReadLimit bounds one carrier message while allowing a full data batch plus framing.
	WebSocketReadLimit = 2 * protocol.MaxEncodedFrameSize
	// maximumRetainedBufferCapacity preserves ordinary batches without retaining exceptional carrier high-water marks.
	maximumRetainedBufferCapacity = 32 * 1024
)

var (
	// ErrInvalidWebSocketMessage indicates a non-binary, empty, or structurally incomplete carrier message.
	ErrInvalidWebSocketMessage = errors.New("invalid WebSocket carrier message")
	// ErrWebSocketMessageTooLarge indicates an outbound carrier batch above the peer's exact read limit.
	ErrWebSocketMessageTooLarge = errors.New("WebSocket carrier message too large")
)

// Conn is one full-duplex framed carrier connection for exactly one lane. ReadFrame calls must not overlap. A frame
// payload remains valid until the next ReadFrame call on the same connection.
type Conn interface {
	ReadFrame(context.Context) (protocol.Frame, error)
	WriteFrames(context.Context, []protocol.Frame) error
	WriteDataBatch(context.Context, []protocol.Data) error
	Close() error
}

// StreamConn carries WireHop frames over one ordered byte stream.
type StreamConn struct {
	conn        net.Conn
	frameReader protocol.FrameReader
	writeBuffer []byte
	writeMu     sync.Mutex
	closeOnce   sync.Once
	closeErr    error
}

// TCPOptionsListener applies WireHop TCP options before returning accepted sockets.
type TCPOptionsListener struct {
	net.Listener
}

// NewTCPOptionsListener wraps a TCP listener for pre-handshake socket configuration.
func NewTCPOptionsListener(listener net.Listener) *TCPOptionsListener {
	return &TCPOptionsListener{Listener: listener}
}

// Accept accepts and configures one TCP connection.
func (l *TCPOptionsListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tcp, ok := connection.(*net.TCPConn)
	if !ok {
		connection.Close()
		return nil, fmt.Errorf("accepted non-TCP connection %T", connection)
	}
	if err := ConfigureTCP(tcp); err != nil {
		connection.Close()
		return nil, err
	}
	return connection, nil
}

// NewStreamConn wraps one connected TCP or TLS stream.
func NewStreamConn(conn net.Conn) *StreamConn {
	return &StreamConn{conn: conn}
}

// WebSocketConn carries complete WireHop frames in bounded binary WebSocket messages.
type WebSocketConn struct {
	conn            *websocket.Conn
	abortConnection net.Conn
	readBuffer      bytes.Buffer
	readSequence    protocol.FrameSequence
	writeBuffer     []byte
	writeMu         sync.Mutex
	closeOnce       sync.Once
	closeErr        error
}

// NewWebSocketConn wraps one WebSocket while preserving its message boundaries.
func NewWebSocketConn(connection *websocket.Conn) *WebSocketConn {
	connection.SetReadLimit(WebSocketReadLimit)
	return &WebSocketConn{conn: connection}
}

// SetAbortConnection retains the underlying TCP connection for a later abortive close.
func (c *WebSocketConn) SetAbortConnection(connection net.Conn) {
	c.abortConnection = connection
}

// ReadFrame returns the next frame from one complete binary WebSocket message.
func (c *WebSocketConn) ReadFrame(ctx context.Context) (protocol.Frame, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, err
	}
	if frame, ok := c.readSequence.Next(); ok {
		return frame, nil
	}
	c.readSequence = protocol.FrameSequence{}
	c.resetReadBuffer()
	messageType, reader, err := c.conn.Reader(ctx)
	if err != nil {
		if errors.Is(err, websocket.ErrMessageTooBig) {
			return protocol.Frame{}, fmt.Errorf("%w: %v", ErrInvalidWebSocketMessage, err)
		}
		return protocol.Frame{}, fmt.Errorf("read WebSocket carrier message: %w", err)
	}
	if messageType != websocket.MessageBinary {
		return protocol.Frame{}, ErrInvalidWebSocketMessage
	}
	if _, err := c.readBuffer.ReadFrom(reader); err != nil {
		c.resetReadBuffer()
		if errors.Is(err, websocket.ErrMessageTooBig) {
			return protocol.Frame{}, fmt.Errorf("%w: %v", ErrInvalidWebSocketMessage, err)
		}
		return protocol.Frame{}, fmt.Errorf("read WebSocket carrier message: %w", err)
	}
	message := c.readBuffer.Bytes()
	if len(message) == 0 {
		c.resetReadBuffer()
		return protocol.Frame{}, ErrInvalidWebSocketMessage
	}
	c.readSequence, err = protocol.ParseFrameSequence(message)
	if err != nil {
		c.resetReadBuffer()
		return protocol.Frame{}, fmt.Errorf("parse WebSocket carrier message: %w", err)
	}
	frame, _ := c.readSequence.Next()
	return frame, nil
}

// WriteFrames writes one nonempty sequence as one binary WebSocket message.
func (c *WebSocketConn) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(frames) == 0 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	encoded := c.writeBuffer[:0]
	defer func() { c.writeBuffer = reusableBuffer(encoded) }()
	for _, frame := range frames {
		var err error
		encoded, err = protocol.AppendFrame(encoded, frame)
		if err != nil {
			return err
		}
	}
	return c.writeLocked(ctx, encoded)
}

// WriteDataBatch writes one nonempty data batch as one binary WebSocket message.
func (c *WebSocketConn) WriteDataBatch(ctx context.Context, data []protocol.Data) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	encoded := c.writeBuffer[:0]
	defer func() { c.writeBuffer = reusableBuffer(encoded) }()
	for _, packet := range data {
		var err error
		encoded, err = protocol.AppendDataFrame(encoded, packet)
		if err != nil {
			return err
		}
	}
	return c.writeLocked(ctx, encoded)
}

// writeLocked writes one complete binary WebSocket message while writeMu is held.
func (c *WebSocketConn) writeLocked(ctx context.Context, encoded []byte) error {
	if len(encoded) > WebSocketReadLimit {
		return ErrWebSocketMessageTooLarge
	}
	if err := c.conn.Write(ctx, websocket.MessageBinary, encoded); err != nil {
		return fmt.Errorf("write WebSocket carrier message: %w", err)
	}
	return nil
}

// Close closes the underlying connection without a WebSocket close handshake.
func (c *WebSocketConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.CloseNow()
	})
	return c.closeErr
}

// Abort discards unacknowledged carrier bytes and closes the WebSocket without a close handshake.
func (c *WebSocketConn) Abort() error {
	c.closeOnce.Do(func() {
		lingerErr := setAbortiveClose(c.abortConnection)
		c.closeErr = errors.Join(lingerErr, c.conn.CloseNow())
	})
	return c.closeErr
}

// ReadFrame reads one complete frame with the context deadline applied to the stream.
func (c *StreamConn) ReadFrame(ctx context.Context) (protocol.Frame, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, err
	}
	if err := c.conn.SetReadDeadline(contextDeadline(ctx)); err != nil {
		return protocol.Frame{}, fmt.Errorf("set carrier read deadline: %w", err)
	}
	frame, err := c.frameReader.Read(c.conn)
	if err != nil {
		return protocol.Frame{}, fmt.Errorf("read carrier frame: %w", err)
	}
	return frame, nil
}

// WriteFrames writes one nonempty frame batch without interleaving another writer.
func (c *StreamConn) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(frames) == 0 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	encoded := c.writeBuffer[:0]
	defer func() { c.writeBuffer = reusableBuffer(encoded) }()
	for _, frame := range frames {
		var err error
		encoded, err = protocol.AppendFrame(encoded, frame)
		if err != nil {
			return err
		}
	}

	return c.writeLocked(ctx, encoded)
}

// WriteDataBatch writes a nonempty data-frame batch using connection-local reusable storage.
func (c *StreamConn) WriteDataBatch(ctx context.Context, data []protocol.Data) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	encoded := c.writeBuffer[:0]
	defer func() { c.writeBuffer = reusableBuffer(encoded) }()
	for _, packet := range data {
		var err error
		encoded, err = protocol.AppendDataFrame(encoded, packet)
		if err != nil {
			return err
		}
	}
	return c.writeLocked(ctx, encoded)
}

// writeLocked writes one complete encoded frame sequence while writeMu is held.
func (c *StreamConn) writeLocked(ctx context.Context, encoded []byte) error {
	if err := c.conn.SetWriteDeadline(contextDeadline(ctx)); err != nil {
		return fmt.Errorf("set carrier write deadline: %w", err)
	}
	for len(encoded) > 0 {
		written, err := c.conn.Write(encoded)
		if err != nil {
			return fmt.Errorf("write carrier frames: %w", err)
		}
		if written <= 0 || written > len(encoded) {
			return fmt.Errorf("write carrier frames: %w", io.ErrShortWrite)
		}
		encoded = encoded[written:]
	}
	return nil
}

// Close closes the underlying stream exactly once.
func (c *StreamConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// Abort discards unacknowledged carrier bytes before closing the stream.
func (c *StreamConn) Abort() error {
	c.closeOnce.Do(func() {
		lingerErr := setAbortiveClose(c.conn)
		c.closeErr = errors.Join(lingerErr, c.conn.Close())
	})
	return c.closeErr
}

// reusableBuffer retains ordinary carrier storage and releases exceptional historical capacity.
func reusableBuffer(buffer []byte) []byte {
	if cap(buffer) > maximumRetainedBufferCapacity {
		return nil
	}
	return buffer[:0]
}

// resetReadBuffer prepares WebSocket read storage while releasing exceptional historical capacity.
func (c *WebSocketConn) resetReadBuffer() {
	if c.readBuffer.Cap() > maximumRetainedBufferCapacity {
		c.readBuffer = bytes.Buffer{}
		return
	}
	c.readBuffer.Reset()
}

// setAbortiveClose configures the TCP socket under known connection wrappers to discard pending bytes on close.
func setAbortiveClose(connection net.Conn) error {
	for connection != nil {
		if tcp, ok := connection.(*net.TCPConn); ok {
			if err := tcp.SetLinger(0); err != nil {
				return fmt.Errorf("configure abortive carrier close: %w", err)
			}
			return nil
		}
		provider, ok := connection.(interface{ NetConn() net.Conn })
		if !ok {
			return fmt.Errorf("configure abortive carrier close: unsupported connection %T", connection)
		}
		connection = provider.NetConn()
	}
	return errors.New("configure abortive carrier close: missing connection")
}

// ConfigureTCP enables latency-sensitive TCP behavior on conn.
func ConfigureTCP(conn *net.TCPConn) error {
	if err := conn.SetNoDelay(true); err != nil {
		return fmt.Errorf("enable TCP_NODELAY: %w", err)
	}
	return nil
}

// contextDeadline returns the context deadline or a zero deadline when none exists.
func contextDeadline(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}
