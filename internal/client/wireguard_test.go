package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/tuntest"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/forward"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/server"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv6"
)

const (
	wireGuardClientPrivateKey = "087ec6e14bbed210e7215cdc73468dfa23f080a1bfb8665b2fd809bd99d28379"
	wireGuardClientPublicKey  = "f928d4f6c1b86c12f2562c10b07c555c5c57fd00f59e90c8d8d88767271cbf7c"
	wireGuardServerPrivateKey = "003ed5d73b55806c30de3f8a7bdab38af13539220533055e635690b8b87ad641"
	wireGuardServerPublicKey  = "c4c8e984c5322c8184c72265b92b250fdb63688705f504ba003c88f03393cf28"
)

func TestRealWireGuardRelay(t *testing.T) {
	for _, tt := range []struct {
		name     string
		schemes  []laneurl.Scheme
		ipv6     bool
		reserved wgpacket.Reserved
	}{
		{name: "TCP", schemes: []laneurl.Scheme{laneurl.TCP}},
		{name: "TLS", schemes: []laneurl.Scheme{laneurl.TLS}},
		{name: "WebSocket", schemes: []laneurl.Scheme{laneurl.WS}},
		{name: "SecureWebSocket", schemes: []laneurl.Scheme{laneurl.WSS}},
		{name: "RepeatedLane", schemes: []laneurl.Scheme{laneurl.WSS, laneurl.WSS}},
		{name: "MixedCarriers", schemes: []laneurl.Scheme{laneurl.TCP, laneurl.TLS, laneurl.WS, laneurl.WSS}},
		{name: "IPv6Target", schemes: []laneurl.Scheme{laneurl.WSS}, ipv6: true},
		{name: "ReservedTranslation", schemes: []laneurl.Scheme{laneurl.WSS}, reserved: wgpacket.Reserved{1, 2, 3}},
		{name: "Forward"},
		{name: "ForwardIPv6Target", ipv6: true},
		{name: "ForwardReservedTranslation", reserved: wgpacket.Reserved{1, 2, 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testRealWireGuardRelay(t, tt.schemes, tt.ipv6, tt.reserved)
		})
	}
}

func testRealWireGuardRelay(t *testing.T, schemes []laneurl.Scheme, ipv6Target bool, reserved wgpacket.Reserved) {
	t.Helper()
	clientAddress := netip.MustParseAddr("10.0.0.1")
	serverAddress := netip.MustParseAddr("10.0.0.2")
	clientAddress6 := netip.MustParseAddr("fd00::1")
	serverAddress6 := netip.MustParseAddr("fd00::2")
	serverTUN := tuntest.NewChannelTUN()
	serverBind := conn.NewDefaultBind()
	if reserved.Enabled() {
		serverBind = &wireGuardReservedBind{Bind: serverBind, reserved: reserved}
	}
	serverDevice := device.NewDevice(
		serverTUN.TUN(), serverBind, device.NewLogger(device.LogLevelSilent, "wireguard-server: "),
	)
	t.Cleanup(serverDevice.Close)
	if err := serverDevice.IpcSet(fmt.Sprintf(
		"private_key=%s\nlisten_port=0\npublic_key=%s\nallowed_ip=%s/32\nallowed_ip=%s/128\n",
		wireGuardServerPrivateKey, wireGuardClientPublicKey, clientAddress, clientAddress6,
	)); err != nil {
		t.Fatal(err)
	}
	if err := serverDevice.Up(); err != nil {
		t.Fatal(err)
	}
	serverPort := wireGuardListenPort(t, serverDevice)
	targetIP := netip.MustParseAddr("127.0.0.1")
	if ipv6Target {
		targetIP = netip.IPv6Loopback()
	}
	remote := netip.AddrPortFrom(targetIP, serverPort)

	token := []byte("a-sufficiently-long-real-WireGuard-test-token")
	var laneURLs []string
	var clientTLS *tls.Config
	var wireHopServer *server.Server
	if len(schemes) > 0 {
		config := serverConfig(t, token, []netip.AddrPort{remote})
		config.MaxPendingAdmissions = len(schemes)
		var err error
		wireHopServer, err = server.New(config)
		if err != nil {
			t.Fatal(err)
		}
		urls := make(map[laneurl.Scheme]string)
		for _, scheme := range schemes {
			if url, ok := urls[scheme]; ok {
				laneURLs = append(laneURLs, url)
				continue
			}
			var url string
			var stop func()
			if scheme.WebSocket() {
				var secureClient *tls.Config
				url, secureClient, stop = startWebSocketServerInstance(t, wireHopServer, scheme.Secure())
				if secureClient != nil {
					clientTLS = secureClient
				}
			} else {
				var serverTLS *tls.Config
				if scheme.Secure() {
					serverTLS, clientTLS = testTLSConfigs(t)
				}
				var address string
				address, stop = startRawServerInstance(t, wireHopServer, serverTLS)
				url = string(scheme) + "://" + address
			}
			t.Cleanup(stop)
			urls[scheme] = url
			laneURLs = append(laneURLs, url)
		}
	}
	start := func(listen netip.AddrPort) (netip.AddrPort, func()) {
		if len(laneURLs) == 0 {
			instance, err := forward.Start(t.Context(), forward.Config{
				Listen: listen, Target: target.MustParse(remote.String()), Reserved: reserved,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { instance.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := instance.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			return instance.LocalAddr(), func() { instance.Close() }
		}
		config := clientConfig(t, laneURLs[0], remote, token, clientTLS)
		config.Listen = listen
		config.Lanes = parseLaneSpecs(t, laneURLs...)
		config.Reserved = reserved
		instance, err := client.Start(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { instance.Close() })
		waitForServer(t, func(snapshot server.Snapshot) bool {
			return snapshot.Sessions == 1 && snapshot.AttachedLanes == len(laneURLs)
		}, wireHopServer)
		return instance.LocalAddr(), func() { instance.Close() }
	}
	local, stopRelay := start(netip.MustParseAddrPort("127.0.0.1:0"))

	clientTUN := tuntest.NewChannelTUN()
	clientDevice := device.NewDevice(
		clientTUN.TUN(), conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, "wireguard-client: "),
	)
	t.Cleanup(clientDevice.Close)
	if err := clientDevice.IpcSet(fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nendpoint=%s\nallowed_ip=%s/32\nallowed_ip=%s/128\n",
		wireGuardClientPrivateKey, wireGuardServerPublicKey, local, serverAddress, serverAddress6,
	)); err != nil {
		t.Fatal(err)
	}
	if err := clientDevice.Up(); err != nil {
		t.Fatal(err)
	}

	for pass := range 2 {
		if pass > 0 {
			stopRelay()
			start(local)
		}
		for _, size := range []int{1280, 1340, tuntest.DefaultMTU} {
			for _, addresses := range [][2]netip.Addr{{clientAddress, serverAddress}, {clientAddress6, serverAddress6}} {
				var clientPacket, serverPacket []byte
				if addresses[0].Is4() {
					clientPacket = sizedIPv4Packet(tuntest.Ping(addresses[1], addresses[0]), size)
					serverPacket = sizedIPv4Packet(tuntest.Ping(addresses[0], addresses[1]), size)
				} else {
					clientPacket = sizedIPv6Packet(t, addresses[1], addresses[0], size)
					serverPacket = sizedIPv6Packet(t, addresses[0], addresses[1], size)
				}
				writeTUNPacket(t, clientTUN.Outbound, clientPacket)
				readTUNPacket(t, serverTUN.Inbound, clientPacket)
				writeTUNPacket(t, serverTUN.Outbound, serverPacket)
				readTUNPacket(t, clientTUN.Inbound, serverPacket)
			}
		}
	}
}

type wireGuardReservedBind struct {
	conn.Bind
	reserved wgpacket.Reserved
}

func (b *wireGuardReservedBind) Open(port uint16) ([]conn.ReceiveFunc, uint16, error) {
	receivers, actualPort, err := b.Bind.Open(port)
	if err != nil {
		return nil, 0, err
	}
	for index, receive := range receivers {
		receivers[index] = func(packets [][]byte, sizes []int, endpoints []conn.Endpoint) (int, error) {
			count, err := receive(packets, sizes, endpoints)
			for index := range count {
				if sizes[index] < 4 || wgpacket.Reserved(packets[index][1:4]) != b.reserved {
					sizes[index] = 0
					continue
				}
				clear(packets[index][1:4])
			}
			return count, err
		}
	}
	return receivers, actualPort, nil
}

func (b *wireGuardReservedBind) Send(packets [][]byte, endpoint conn.Endpoint) error {
	for _, packet := range packets {
		copy(packet[1:4], b.reserved[:])
	}
	err := b.Bind.Send(packets, endpoint)
	for _, packet := range packets {
		clear(packet[1:4])
	}
	return err
}

func sizedIPv4Packet(packet []byte, size int) []byte {
	const ipv4HeaderSize = 20
	resized := make([]byte, size)
	copy(resized, packet)
	binary.BigEndian.PutUint16(resized[2:4], uint16(size))
	clear(resized[10:12])
	binary.BigEndian.PutUint16(resized[10:12], internetChecksum(resized[:ipv4HeaderSize]))
	clear(resized[ipv4HeaderSize+2 : ipv4HeaderSize+4])
	binary.BigEndian.PutUint16(
		resized[ipv4HeaderSize+2:ipv4HeaderSize+4],
		internetChecksum(resized[ipv4HeaderSize:]),
	)
	return resized
}

func sizedIPv6Packet(t *testing.T, destination, source netip.Addr, size int) []byte {
	t.Helper()
	message := icmp.Message{
		Type: ipv6.ICMPTypeEchoRequest,
		Body: &icmp.Echo{ID: 1337, Data: make([]byte, size-ipv6.HeaderLen-8)},
	}
	payload, err := message.Marshal(icmp.IPv6PseudoHeader(source.AsSlice(), destination.AsSlice()))
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, ipv6.HeaderLen, size)
	packet[0] = 0x60
	binary.BigEndian.PutUint16(packet[4:6], uint16(len(payload)))
	packet[6] = 58
	packet[7] = 64
	copy(packet[8:24], source.AsSlice())
	copy(packet[24:40], destination.AsSlice())
	return append(packet, payload...)
}

func internetChecksum(value []byte) uint16 {
	var sum uint32
	for len(value) >= 2 {
		sum += uint32(binary.BigEndian.Uint16(value[:2]))
		value = value[2:]
	}
	if len(value) == 1 {
		sum += uint32(value[0]) << 8
	}
	for sum > 0xffff {
		sum = sum>>16 + sum&0xffff
	}
	return ^uint16(sum)
}

func wireGuardListenPort(t *testing.T, instance *device.Device) uint16 {
	t.Helper()
	configuration, err := instance.IpcGet()
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(configuration, "\n") {
		value, found := strings.CutPrefix(line, "listen_port=")
		if !found {
			continue
		}
		port, err := strconv.ParseUint(value, 10, 16)
		if err != nil || port == 0 {
			t.Fatalf("WireGuard listen_port = %q, error = %v", value, err)
		}
		return uint16(port)
	}
	t.Fatalf("WireGuard configuration has no listen_port: %q", configuration)
	return 0
}

func writeTUNPacket(t *testing.T, output chan<- []byte, packet []byte) {
	t.Helper()
	select {
	case output <- packet:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out injecting a WireGuard TUN packet")
	}
}

func readTUNPacket(t *testing.T, input <-chan []byte, want []byte) {
	t.Helper()
	select {
	case got := <-input:
		if !bytes.Equal(got, want) {
			t.Fatalf("decrypted TUN packet = %x, want %x", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a decrypted WireGuard TUN packet")
	}
}
