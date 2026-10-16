package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
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

func TestLaneWriteDataBatchTLS(t *testing.T) {
	fixture := httptest.NewTLSServer(http.NotFoundHandler())
	fixture.Close()
	for _, transport := range []struct {
		name         string
		version      uint16
		serverWriter bool
		adaptive     bool
	}{
		{name: "TLS12Client", version: tls.VersionTLS12},
		{name: "TLS12Server", version: tls.VersionTLS12, serverWriter: true},
		{name: "TLS13Client", version: tls.VersionTLS13},
		{name: "TLS13Server", version: tls.VersionTLS13, serverWriter: true},
		{name: "TLS12ClientStartup", version: tls.VersionTLS12, adaptive: true},
		{name: "TLS12ServerStartup", version: tls.VersionTLS12, serverWriter: true, adaptive: true},
		{name: "TLS13ClientStartup", version: tls.VersionTLS13, adaptive: true},
		{name: "TLS13ServerStartup", version: tls.VersionTLS13, serverWriter: true, adaptive: true},
	} {
		t.Run(transport.name, func(t *testing.T) {
			for _, tt := range []struct {
				name   string
				sizes  []int
				want   []int
				probes bool
			}{
				{name: "MTU", sizes: slices.Repeat([]int{1452}, 16), want: []int{11, 5}},
				{name: "Small", sizes: slices.Repeat([]int{32}, 16), want: []int{16}},
				// The first two frames encode to 16384 or 16385 bytes with the IDs and deadlines below.
				{name: "ExactRecord", sizes: []int{16331, 32, 32}, want: []int{2, 1}},
				{name: "RecordOverflow", sizes: []int{16332, 32, 32}, want: []int{1, 2}},
				{name: "Mixed", sizes: []int{9000, 4000, 3500}, want: []int{2, 1}},
				{name: "Oversized", sizes: []int{32, 32768, 32}, want: []int{3}},
				{name: "MaximumDatagram", sizes: []int{protocol.MaxPacketSize, 32}, want: []int{1, 1}},
				{name: "Probes", sizes: slices.Repeat([]int{protocol.ProbePayloadSize}, 4), want: []int{3, 1}, probes: true},
				{name: "PoorRecordFill", sizes: slices.Repeat([]int{3000}, 16), want: []int{16}},
				{name: "JumboFrames", sizes: slices.Repeat([]int{9000}, 16), want: []int{8, 8}},
				{name: "OversizedFrames", sizes: slices.Repeat([]int{32768}, 4), want: []int{2, 2}},
				{name: "PacketSizeChange", sizes: slices.Concat(slices.Repeat([]int{9000}, 8), slices.Repeat([]int{1452}, 16)), want: []int{8, 11, 5}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					left, right := net.Pipe()
					serverConfig := fixture.TLS.Clone()
					serverConfig.MinVersion, serverConfig.MaxVersion = transport.version, transport.version
					clientConfig := fixture.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
					clientConfig.MinVersion, clientConfig.MaxVersion = transport.version, transport.version
					clientConfig.ServerName = fixture.Certificate().DNSNames[0]
					// Steady-state cases use full-sized records to expose exact batch boundaries.
					serverConfig.DynamicRecordSizingDisabled = !transport.adaptive
					clientConfig.DynamicRecordSizingDisabled = !transport.adaptive
					var writer, reader *tls.Conn
					if transport.serverWriter {
						writer, reader = tls.Server(left, serverConfig), tls.Client(right, clientConfig)
					} else {
						writer, reader = tls.Client(left, clientConfig), tls.Server(right, serverConfig)
					}
					handshake := make(chan error, 1)
					go func() {
						defer close(handshake)
						handshake <- reader.HandshakeContext(ctx)
					}()
					t.Cleanup(func() {
						left.Close()
						right.Close()
						<-handshake
					})
					if err := writer.HandshakeContext(ctx); err != nil {
						t.Fatal(err)
					}
					if err := <-handshake; err != nil {
						t.Fatal(err)
					}
					now := time.Unix(100, 0)
					store := schedulerStore(t, packetqueue.Limits{Packets: 32, Bytes: 256 * 1024})
					store.now = func() time.Time { return now }
					store.setDeliveryWindows(256*1024, 256*1024)
					store.writeBudget.Store(64 * 1024)
					t.Cleanup(func() { releaseTransmissions(store.drain()) })
					clock := &testClock{now: uint64(now.UnixMicro())}
					receiver, err := NewReceiver(ReceiverConfig{
						Endpoint: newTestEndpoint(), Clock: clock, DeduplicationSize: 64,
					})
					if err != nil {
						t.Fatal(err)
					}
					lane, err := newObservedLane(LaneConfig{
						Carrier: carrier.NewStreamConn(writer), Receiver: receiver, Store: store, Clock: clock,
						LaneID: protocol.LaneID(1), Generation: 1,
					})
					if err != nil {
						t.Fatal(err)
					}
					var expected []protocol.Data
					for index, size := range tt.sizes {
						transmission := schedulerTransmission(uint64(index)+1_000_000_000, wgpacket.TransportData, now.Add(time.Second))
						transmission.packet.Payload = bytes.Repeat([]byte{byte(index + 1)}, size)
						copy(transmission.packet.Payload, []byte{4, 0, 0, 0})
						if tt.probes {
							transmission.packetID = 0
							transmission.packet.Kind = wgpacket.NonWireGuard
						}
						expected = append(expected, transmission.data())
						if err := store.push(transmission); err != nil {
							t.Fatal(err)
						}
					}
					results := make(chan error, len(tt.want))
					finished := make(chan struct{})
					go func() {
						defer close(finished)
						for range tt.want {
							err := lane.writeDataBatch(ctx)
							results <- err
							if err != nil {
								return
							}
						}
					}()
					t.Cleanup(func() {
						left.Close()
						right.Close()
						<-finished
					})
					peer := carrier.NewStreamConn(reader)
					var frames [maximumDataBatchFrames]protocol.Frame
					next := 0
					for _, want := range tt.want {
						encodedBytes := 0
						for _, frame := range expected[next : next+want] {
							encodedBytes += dataFrameSize(frame)
						}
						for received := 0; received < want; {
							count, err := peer.ReadFrames(ctx, frames[:])
							if err != nil || count == 0 || count > want-received || !transport.adaptive && encodedBytes <= 16*1024 && count != want {
								t.Fatalf("TLS read batch = %d frames, %v, want %d remaining", count, err, want-received)
							}
							for _, frame := range frames[:count] {
								data, err := protocol.ParseData(frame)
								want := expected[next]
								if err != nil || data.PacketID != want.PacketID || data.DeadlineMicros != want.DeadlineMicros ||
									!bytes.Equal(data.Payload, want.Payload) {
									t.Fatalf("TLS changed frame %d: %+v, %v", next, data, err)
								}
								next++
							}
							received += count
						}
						if err := <-results; err != nil {
							t.Fatal(err)
						}
					}
					if next != len(expected) {
						t.Fatalf("TLS received %d frames, want %d", next, len(expected))
					}
				})
			}
		})
	}
}

func TestLaneReadPreservesPayloadDuringCarrierTeardown(t *testing.T) {
	for _, scheme := range []string{"TCP", "TLS", "WS", "WSS"} {
		t.Run(scheme, func(t *testing.T) {
			for _, tt := range []struct {
				name          string
				size          int
				abort, probes bool
			}{
				{name: "Ordinary", size: 1452},
				{name: "BufferBoundary", size: 32 * 1024},
				{name: "MaximumPacket", size: protocol.MaxPacketSize},
				{name: "AbortOrdinary", size: 1452, abort: true},
				{name: "AbortBufferBoundary", size: 32 * 1024, abort: true},
				{name: "AbortMaximumPacket", size: protocol.MaxPacketSize, abort: true},
				{name: "ProbeOrdinary", size: 1452, probes: true},
				{name: "ProbeBufferBoundary", size: 32 * 1024, probes: true},
				{name: "ProbeMaximumPacket", size: protocol.MaxPacketSize, probes: true},
				{name: "ProbeAbortOrdinary", size: 1452, abort: true, probes: true},
				{name: "ProbeAbortBufferBoundary", size: 32 * 1024, abort: true, probes: true},
				{name: "ProbeAbortMaximumPacket", size: protocol.MaxPacketSize, abort: true, probes: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					var encoded []byte
					var expected [][]byte
					prefixBytes := []uint64{0}
					prefixDatagrams := []int{0}
					for index := range 3 {
						size := 32
						if index == 0 {
							size = tt.size
						}
						payload := bytes.Repeat([]byte{byte(index + 1)}, size)
						copy(payload, []byte{4, 0, 0, 0})
						expected = append(expected, payload)
						var err error
						if tt.probes {
							padding := bytes.Repeat([]byte{4}, protocol.ProbePayloadSize)
							encoded, err = protocol.AppendDataFrame(encoded, protocol.Data{Payload: padding})
							if err != nil {
								t.Fatal(err)
							}
							prefixBytes = append(prefixBytes, uint64(len(encoded)))
							prefixDatagrams = append(prefixDatagrams, index)
						}
						encoded, err = protocol.AppendDataFrame(encoded, protocol.Data{
							PacketID: 1_000_000_000 + uint64(index), DeadlineMicros: 1_000_000, Payload: payload,
						})
						if err != nil {
							t.Fatal(err)
						}
						prefixBytes = append(prefixBytes, uint64(len(encoded)))
						prefixDatagrams = append(prefixDatagrams, index+1)
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
					parsed := lane.progress.dataPackets
					if count == 0 || count > len(expected) || parsed >= uint64(len(prefixDatagrams)) ||
						prefixDatagrams[parsed] != count || prefixBytes[parsed] != lane.progress.dataBytes {
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
	stream, _, err := carrier.DialWebSocket(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	stream.SetNetworkConnection(<-rawConnections)
	t.Cleanup(func() { stream.Close() })
	return stream
}
