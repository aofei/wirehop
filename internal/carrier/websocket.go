package carrier

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// webSocketNetworkConn preserves transport EOF separately from WebSocket message boundaries.
type webSocketNetworkConn struct {
	net.Conn
}

// Read prevents the WebSocket reader from normalizing a transport EOF into a message boundary.
func (c webSocketNetworkConn) Read(buffer []byte) (int, error) {
	return readWebSocketTransport(c.Conn, buffer)
}

// webSocketResponseBody preserves transport EOF on the client's upgraded HTTP response.
type webSocketResponseBody struct {
	io.ReadWriteCloser
}

// Read prevents the WebSocket reader from normalizing a transport EOF into a message boundary.
func (b webSocketResponseBody) Read(buffer []byte) (int, error) {
	return readWebSocketTransport(b.ReadWriteCloser, buffer)
}

// readWebSocketTransport reports connection EOF as truncation, leaving EOF exclusively for message boundaries.
func readWebSocketTransport(reader io.Reader, buffer []byte) (int, error) {
	count, err := reader.Read(buffer)
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return count, err
}

// webSocketResponseWriter clears handshake deadlines and preserves transport errors after HTTP hijacking.
type webSocketResponseWriter struct {
	http.ResponseWriter
	connection *net.Conn
}

// Hijack transfers the socket without retaining HTTP deadlines or losing already buffered bytes.
func (w webSocketResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	connection, readWriter, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		connection.Close()
		return nil, nil, fmt.Errorf("clear WebSocket handshake deadline: %w", err)
	}
	*w.connection = connection
	return webSocketNetworkConn{Conn: connection}, readWriter, nil
}

// webSocketRoundTripper preserves transport errors on successful client upgrades.
type webSocketRoundTripper struct {
	http.RoundTripper
}

// RoundTrip wraps only upgraded response bodies, after HTTP parsing and any proxy or TLS negotiation.
func (t webSocketRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.RoundTripper.RoundTrip(request)
	if err == nil && response.StatusCode == http.StatusSwitchingProtocols {
		if body, ok := response.Body.(io.ReadWriteCloser); ok {
			response.Body = webSocketResponseBody{ReadWriteCloser: body}
		}
	}
	return response, err
}

// AcceptWebSocket upgrades one request and preserves both message boundaries and transport failures.
func AcceptWebSocket(writer http.ResponseWriter, request *http.Request, options *websocket.AcceptOptions) (*WebSocketConn, error) {
	var networkConnection net.Conn
	connection, err := websocket.Accept(webSocketResponseWriter{
		ResponseWriter: writer, connection: &networkConnection,
	}, request, options)
	if err != nil {
		return nil, err
	}
	return newWebSocketConn(connection, networkConnection), nil
}

// DialWebSocket upgrades one HTTP connection and preserves transport failures across direct and proxied TLS routes.
func DialWebSocket(ctx context.Context, url string, options *websocket.DialOptions) (*WebSocketConn, *http.Response, error) {
	var dialOptions websocket.DialOptions
	if options != nil {
		dialOptions = *options
	}
	client := http.DefaultClient
	if dialOptions.HTTPClient != nil {
		client = dialOptions.HTTPClient
	}
	httpClient := *client
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient.Transport = webSocketRoundTripper{RoundTripper: transport}
	dialOptions.HTTPClient = &httpClient
	connection, response, err := websocket.Dial(ctx, url, &dialOptions)
	if err != nil {
		return nil, response, err
	}
	return newWebSocketConn(connection, nil), response, nil
}
