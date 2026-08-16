package datagram

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

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
		destinations := remote.destinations(header, buffer[:0])
		benchmarkAddressSink = destinations[0]
	}
}
