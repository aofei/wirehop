package client_test

import (
	"context"
	"net"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/server"
)

const relayBenchmarkWindowSize = 128

// BenchmarkRelayPipeline measures sustained packet throughput with a fixed in-flight window.
func BenchmarkRelayPipeline(b *testing.B) {
	for _, carrierName := range []string{"TCP", "WebSocket"} {
		b.Run(carrierName, func(b *testing.B) {
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
			var stopServer func()
			if carrierName == "TCP" {
				address, stop := startRawServerInstance(b, serverInstance, nil)
				laneURL = "tcp://" + address
				stopServer = stop
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				httpServer := httptest.NewServer(serverInstance.WebSocketHandler(ctx))
				laneURL = "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/_wirehop"
				stopServer = func() {
					cancel()
					httpServer.Close()
				}
			}
			defer stopServer()
			config := clientConfig(b, laneURL, target, token, nil)
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
			b.ResetTimer()
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
