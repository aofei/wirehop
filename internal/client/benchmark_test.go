package client_test

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/server"
)

const relayBenchmarkWindowSize = 32

// BenchmarkRelayPipeline measures verified echo throughput with a fixed in-flight window.
// Draining all remaining requests after timing makes missing or duplicate responses invalidate the run.
func BenchmarkRelayPipeline(b *testing.B) {
	for _, test := range []struct {
		name   string
		scheme laneurl.Scheme
	}{
		{name: "TCP", scheme: laneurl.TCP},
		{name: "TLS", scheme: laneurl.TLS},
		{name: "WebSocket", scheme: laneurl.WS},
		{name: "SecureWebSocket", scheme: laneurl.WSS},
	} {
		b.Run(test.name, func(b *testing.B) {
			target, stopTarget := startEchoTarget(b)
			defer stopTarget()
			token := []byte("a-sufficiently-long-benchmark-authentication-token")
			relayServerConfig := serverConfig(b, token, []netip.AddrPort{target})
			relayServerConfig.IngressLimits = packetqueue.Limits{Packets: 1024, Bytes: 2 * 1024 * 1024}
			relayServerConfig.LaneLimits = packetqueue.Limits{Packets: 1024, Bytes: 2 * 1024 * 1024}
			// The active request window must coexist with a full sent-unreported lane prefix.
			relayServerConfig.RetentionLimits.Packets = relayServerConfig.IngressLimits.Packets + relayServerConfig.LaneLimits.Packets
			relayServerConfig.RetentionLimits.Bytes = relayServerConfig.IngressLimits.Bytes + relayServerConfig.LaneLimits.Bytes
			serverInstance, err := server.New(relayServerConfig)
			if err != nil {
				b.Fatal(err)
			}
			var laneURL string
			var clientTLS *tls.Config
			var stopServer func()
			if test.scheme.WebSocket() {
				laneURL, clientTLS, stopServer = startWebSocketServerInstance(b, serverInstance, test.scheme.Secure())
			} else {
				var serverTLS *tls.Config
				if test.scheme.Secure() {
					serverTLS, clientTLS = testTLSConfigs(b)
				}
				address, stop := startRawServerInstance(b, serverInstance, serverTLS)
				laneURL = string(test.scheme) + "://" + address
				stopServer = stop
			}
			defer stopServer()
			config := clientConfig(b, laneURL, target, token, clientTLS)
			config.IngressLimits = packetqueue.Limits{Packets: 1024, Bytes: 2 * 1024 * 1024}
			config.LaneLimits = packetqueue.Limits{Packets: 1024, Bytes: 2 * 1024 * 1024}
			instance, err := client.Start(context.Background(), config)
			if err != nil {
				b.Fatal(err)
			}
			defer instance.Close()
			peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
			if err != nil {
				b.Fatal(err)
			}
			defer peer.Close()
			if err := peer.SetReadBuffer(4 * 1024 * 1024); err != nil {
				b.Fatal(err)
			}
			if err := peer.SetWriteBuffer(4 * 1024 * 1024); err != nil {
				b.Fatal(err)
			}
			payload := transportPacket()
			buffer := make([]byte, len(payload)+1)
			if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
				b.Fatal(err)
			}
			if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
				b.Fatal(err)
			}
			length, _, err := peer.ReadFromUDPAddrPort(buffer)
			if err != nil {
				b.Fatal(err)
			}
			if length != len(payload) {
				b.Fatalf("warmup response length = %d, want %d", length, len(payload))
			}
			if err := peer.SetReadDeadline(time.Time{}); err != nil {
				b.Fatal(err)
			}
			var pending [relayBenchmarkWindowSize]uint64
			var sent, received uint64
			send := func(slot int) {
				sent++
				binary.LittleEndian.PutUint64(payload[16:24], sent)
				binary.LittleEndian.PutUint32(payload[24:28], uint32(slot))
				if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
					b.Fatal(err)
				}
				pending[slot] = sent
			}
			receive := func() int {
				// Refresh from recent progress so long benchmark runs retain a bounded silence timeout.
				if received%relayBenchmarkWindowSize == 0 {
					if err := peer.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
						b.Fatal(err)
					}
				}
				length, _, err := peer.ReadFromUDPAddrPort(buffer)
				if err != nil {
					b.Fatalf("receive after %d of %d sent datagrams: %v", received, sent, err)
				}
				if length != len(payload) {
					b.Fatalf("relay response length = %d, want %d", length, len(payload))
				}
				identifier := binary.LittleEndian.Uint64(buffer[16:24])
				slot := binary.LittleEndian.Uint32(buffer[24:28])
				if identifier == 0 || slot >= uint32(len(pending)) || pending[slot] != identifier {
					b.Fatalf("unexpected response identifier %d for slot %d", identifier, slot)
				}
				pending[slot] = 0
				received++
				return int(slot)
			}
			for slot := range pending {
				send(slot)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				send(receive())
			}
			// Account for the complete offered window after the measurement timer stops.
			for received < sent {
				receive()
			}
			b.ReportMetric(float64(sent), "sent-datagrams")
			b.ReportMetric(float64(received), "received-datagrams")
		})
	}
}
