package datagram

import (
	"context"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var benchmarkPacketSink Packet
var benchmarkAddressSink netip.AddrPort

func BenchmarkCopyAcceptedPacket(b *testing.B) {
	buffer := wireGuardPacket(4, 1420)
	b.ReportAllocs()
	b.SetBytes(int64(len(buffer)))
	for b.Loop() {
		packet, ok := copyAcceptedPacket(buffer, len(buffer))
		if !ok {
			b.Fatal("copyAcceptedPacket() rejected a transport packet")
		}
		benchmarkPacketSink = packet
		benchmarkPacketSink.Release()
	}
}

func BenchmarkLocalRead(b *testing.B) {
	listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		b.Fatal(err)
	}
	local := NewLocal(listener)
	b.Cleanup(func() { local.Close() })
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { peer.Close() })
	payload := wireGuardPacket(4, 1420)
	target := local.LocalAddr()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := peer.WriteToUDPAddrPort(payload, target); err != nil {
			b.Fatal(err)
		}
		packet, err := local.Read(ctx)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkPacketSink = packet
		benchmarkPacketSink.Release()
	}
}

func BenchmarkLocalReadBatch(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("Linux recvmmsg benchmark")
	}
	listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		b.Fatal(err)
	}
	local := NewLocal(listener)
	b.Cleanup(func() { local.Close() })
	peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { peer.Close() })
	payload := wireGuardPacket(4, 1420)
	target := local.LocalAddr()
	var packets [MaximumBatchSize]Packet
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload) * len(packets)))
	b.ResetTimer()
	for b.Loop() {
		for range packets {
			if _, err := peer.WriteToUDPAddrPort(payload, target); err != nil {
				b.Fatal(err)
			}
		}
		count, err := local.ReadBatch(ctx, packets[:])
		if err != nil {
			b.Fatal(err)
		}
		if count != len(packets) {
			b.Fatalf("ReadBatch() = %d packets, want %d", count, len(packets))
		}
		for index := range count {
			packets[index].Release()
		}
	}
}

func BenchmarkLocalWriteBatch(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("Linux sendmmsg benchmark")
	}
	for _, tt := range []struct {
		name  string
		batch bool
	}{
		{name: "Scalar"},
		{name: "LinuxBatch", batch: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
			if err != nil {
				b.Fatal(err)
			}
			local := NewLocal(listener)
			if !tt.batch {
				local.batch = nil
			}
			b.Cleanup(func() { local.Close() })
			peer, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { peer.Close() })
			payload := wireGuardPacket(4, 1420)
			if _, err := peer.WriteToUDPAddrPort(payload, local.LocalAddr()); err != nil {
				b.Fatal(err)
			}
			packet, err := local.Read(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			packet.Release()
			payloads := make([][]byte, MaximumBatchSize)
			for index := range payloads {
				payloads[index] = payload
			}
			drainDone := make(chan struct{})
			go func() {
				defer close(drainDone)
				buffer := make([]byte, protocol.MaxPacketSize)
				for {
					if _, _, err := peer.ReadFromUDPAddrPort(buffer); err != nil {
						return
					}
				}
			}()
			b.Cleanup(func() {
				peer.Close()
				<-drainDone
			})
			b.ReportAllocs()
			b.SetBytes(int64(len(payload) * len(payloads)))
			b.ResetTimer()
			for b.Loop() {
				written, err := local.WriteBatch(context.Background(), payloads, time.Time{})
				if err != nil {
					b.Fatal(err)
				}
				if written != len(payloads) {
					b.Fatalf("WriteBatch() = %d packets, want %d", written, len(payloads))
				}
			}
		})
	}
}

func BenchmarkRemoteDestination(b *testing.B) {
	address := netip.MustParseAddrPort("192.0.2.1:51820")
	remote := &Remote{
		current: address,
		transportRoutes: map[uint32]targetRoute{
			1: {address: address, expires: time.Now().Add(targetRouteLifetime)},
		},
	}
	header := wgpacket.Header{Kind: wgpacket.TransportData, ReceiverIndex: 1}
	var buffer [target.MaxCandidates]netip.AddrPort
	b.ReportAllocs()
	for b.Loop() {
		destinations := remote.destinations(header, buffer[:0], time.Now())
		benchmarkAddressSink = destinations[0]
	}
}
