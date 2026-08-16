package client_test

import (
	"bytes"
	"context"
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
)

const (
	wireGuardClientPrivateKey = "087ec6e14bbed210e7215cdc73468dfa23f080a1bfb8665b2fd809bd99d28379"
	wireGuardClientPublicKey  = "f928d4f6c1b86c12f2562c10b07c555c5c57fd00f59e90c8d8d88767271cbf7c"
	wireGuardServerPrivateKey = "003ed5d73b55806c30de3f8a7bdab38af13539220533055e635690b8b87ad641"
	wireGuardServerPublicKey  = "c4c8e984c5322c8184c72265b92b250fdb63688705f504ba003c88f03393cf28"
)

func TestRealWireGuardRelay(t *testing.T) {
	clientAddress := netip.MustParseAddr("10.0.0.1")
	serverAddress := netip.MustParseAddr("10.0.0.2")
	serverTUN := tuntest.NewChannelTUN()
	serverDevice := device.NewDevice(
		serverTUN.TUN(), conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, "wireguard-server: "),
	)
	t.Cleanup(serverDevice.Close)
	if err := serverDevice.IpcSet(fmt.Sprintf(
		"private_key=%s\nlisten_port=0\npublic_key=%s\nallowed_ip=%s/32\n",
		wireGuardServerPrivateKey, wireGuardClientPublicKey, clientAddress,
	)); err != nil {
		t.Fatal(err)
	}
	if err := serverDevice.Up(); err != nil {
		t.Fatal(err)
	}
	serverPort := wireGuardListenPort(t, serverDevice)
	target := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), serverPort)

	token := []byte("a-sufficiently-long-real-WireGuard-test-token")
	wireHopServer := newServer(t, token, []netip.AddrPort{target})
	serverCarrierAddress, stopServer := startRawServerInstance(t, wireHopServer, nil)
	t.Cleanup(stopServer)
	wireHopClient, err := client.Start(
		context.Background(), clientConfig(t, "tcp://"+serverCarrierAddress, target, token, nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wireHopClient.Close() })

	clientTUN := tuntest.NewChannelTUN()
	clientDevice := device.NewDevice(
		clientTUN.TUN(), conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, "wireguard-client: "),
	)
	t.Cleanup(clientDevice.Close)
	if err := clientDevice.IpcSet(fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nendpoint=%s\nallowed_ip=%s/32\n",
		wireGuardClientPrivateKey, wireGuardServerPublicKey, wireHopClient.LocalAddr(), serverAddress,
	)); err != nil {
		t.Fatal(err)
	}
	if err := clientDevice.Up(); err != nil {
		t.Fatal(err)
	}

	clientPacket := sizedIPv4Packet(tuntest.Ping(serverAddress, clientAddress), tuntest.DefaultMTU)
	writeTUNPacket(t, clientTUN.Outbound, clientPacket)
	readTUNPacket(t, serverTUN.Inbound, clientPacket)
	serverPacket := sizedIPv4Packet(tuntest.Ping(clientAddress, serverAddress), tuntest.DefaultMTU)
	writeTUNPacket(t, serverTUN.Outbound, serverPacket)
	readTUNPacket(t, clientTUN.Inbound, serverPacket)
}

// sizedIPv4Packet extends packet with zero payload and updates its IPv4 and ICMP checksums.
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

// internetChecksum returns the RFC 1071 checksum of value.
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
