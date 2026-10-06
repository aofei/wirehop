package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/coder/websocket"
)

type heldDeliveryEndpoint struct {
	*testEndpoint
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *heldDeliveryEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	e.once.Do(func() { close(e.entered) })
	select {
	case <-e.release:
		return e.testEndpoint.Write(ctx, payload, deadline)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestLaneReadPreservesPayloadDuringCarrierTeardown(t *testing.T) {
	for _, scheme := range []string{"TCP", "TLS", "WS", "WSS"} {
		t.Run(scheme, func(t *testing.T) {
			for _, tt := range []struct {
				name  string
				size  int
				abort bool
			}{
				{name: "Ordinary", size: 1452},
				{name: "BufferBoundary", size: 32 * 1024},
				{name: "MaximumPacket", size: protocol.MaxPacketSize},
				{name: "AbortOrdinary", size: 1452, abort: true},
				{name: "AbortBufferBoundary", size: 32 * 1024, abort: true},
				{name: "AbortMaximumPacket", size: protocol.MaxPacketSize, abort: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					var encoded []byte
					var expected [][]byte
					for index := range 3 {
						size := 32
						if index == 0 {
							size = tt.size
						}
						payload := bytes.Repeat([]byte{byte(index + 1)}, size)
						copy(payload, []byte{4, 0, 0, 0})
						expected = append(expected, payload)
						var err error
						encoded, err = protocol.AppendDataFrame(encoded, protocol.Data{
							PacketID: 1_000_000_000 + uint64(index), DeadlineMicros: 1_000_000, Payload: payload,
						})
						if err != nil {
							t.Fatal(err)
						}
					}
					encoded = append(encoded, 0, 0)
					connection := deliveryTestCarrier(t, ctx, scheme, encoded)
					endpoint := &heldDeliveryEndpoint{
						testEndpoint: newTestEndpoint(), entered: make(chan struct{}), release: make(chan struct{}),
					}
					var release sync.Once
					unblock := func() { release.Do(func() { close(endpoint.release) }) }
					t.Cleanup(unblock)
					receiver, err := NewReceiver(ReceiverConfig{
						Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64,
					})
					if err != nil {
						t.Fatal(err)
					}
					lane := &Lane{carrier: connection, receiver: receiver}
					result := make(chan error, 1)
					go func() { result <- lane.read(ctx) }()
					select {
					case <-endpoint.entered:
					case err := <-result:
						t.Fatalf("carrier failed before UDP delivery: %v", err)
					case <-ctx.Done():
						t.Fatal("carrier did not reach UDP delivery")
					}
					closeCarrier := connection.Close
					if tt.abort {
						closeCarrier = connection.(carrierAborter).Abort
					}
					if err := closeCarrier(); err != nil {
						t.Fatalf("carrier teardown failed: %v", err)
					}
					unblock()
					select {
					case err := <-result:
						if err == nil {
							t.Fatal("lane read completed without the malformed tail or teardown error")
						}
					case <-ctx.Done():
						t.Fatal("lane read remained blocked after carrier teardown")
					}
					count := len(endpoint.writes)
					if count == 0 || count > len(expected) || lane.progress.dataPackets != uint64(count) {
						t.Fatalf("delivered %d packets with %d parsing acknowledgements", count, lane.progress.dataPackets)
					}
					for index := range count {
						if payload := <-endpoint.writes; !bytes.Equal(payload, expected[index]) {
							t.Fatalf("carrier teardown changed payload %d", index)
						}
					}
				})
			}
		})
	}
}

func deliveryTestCarrier(t *testing.T, ctx context.Context, scheme string, encoded []byte) carrier.Conn {
	t.Helper()
	if scheme == "TCP" || scheme == "TLS" {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		destination, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { destination.Close() })
		source, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { source.Close() })
		if scheme == "TLS" {
			server := httptest.NewTLSServer(http.NotFoundHandler())
			t.Cleanup(server.Close)
			source = tls.Server(source, server.TLS.Clone())
			config := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
			config.ServerName = server.Certificate().DNSNames[0]
			destination = tls.Client(destination, config)
		}
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			source.Write(encoded)
		}()
		t.Cleanup(func() {
			destination.Close()
			source.Close()
			<-finished
		})
		return carrier.NewStreamConn(destination)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		connection.Write(r.Context(), websocket.MessageBinary, encoded)
		connection.Read(r.Context())
	}))
	if scheme == "WSS" {
		server.StartTLS()
	} else {
		server.Start()
	}
	t.Cleanup(server.Close)
	rawConnections := make(chan net.Conn, 1)
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			rawConnections <- connection
		}
		return connection, err
	}
	t.Cleanup(transport.CloseIdleConnections)
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	stream := carrier.NewWebSocketConn(connection)
	stream.SetNetworkConnection(<-rawConnections)
	t.Cleanup(func() { stream.Close() })
	return stream
}
