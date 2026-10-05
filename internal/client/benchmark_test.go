package client_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/server"
)

const relayBenchmarkWindowSize = 128

// BenchmarkRelayPipeline measures sustained packet throughput with a fixed in-flight window.
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
			buffer := make([]byte, len(payload))
			if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
				b.Fatal(err)
			}
			if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
				b.Fatal(err)
			}
			if _, _, err := peer.ReadFromUDPAddrPort(buffer); err != nil {
				b.Fatal(err)
			}
			if err := peer.SetReadDeadline(time.Time{}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for range relayBenchmarkWindowSize {
				if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
					b.Fatal(err)
				}
			}
			if err := peer.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				length, _, err := peer.ReadFromUDPAddrPort(buffer)
				if err != nil {
					b.Fatal(err)
				}
				if length != len(payload) {
					b.Fatalf("relay response length = %d, want %d", length, len(payload))
				}
				if _, err := peer.WriteToUDPAddrPort(payload, instance.LocalAddr()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
