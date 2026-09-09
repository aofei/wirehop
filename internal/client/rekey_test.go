package client_test

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/forward"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/server"
	"github.com/aofei/wirehop/internal/target"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/tuntest"
)

// rekeyResolver returns two fixed addresses for the same configured WireGuard identity.
type rekeyResolver struct{}

func (rekeyResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1")}, nil
}

func TestRealWireGuardRekeyPreservesBackend(t *testing.T) {
	for _, mode := range []string{"Forward", "TCP"} {
		t.Run(mode, func(t *testing.T) { testRealWireGuardRekey(t, mode) })
	}
}

func testRealWireGuardRekey(t *testing.T, mode string) {
	t.Helper()
	firstTUN, firstDevice := rekeyServer(t)
	secondTUN, secondDevice := rekeyServer(t)
	var favorSecond atomic.Bool
	var dropFirst atomic.Bool
	firstPort, firstTransport := rekeyProxy(t, "127.0.0.1", 0, wireGuardListenPort(t, firstDevice), false, &favorSecond, &dropFirst)
	_, secondTransport := rekeyProxy(t, "::1", firstPort, wireGuardListenPort(t, secondDevice), true, &favorSecond, &dropFirst)
	endpoint := target.MustParse(fmt.Sprintf("same-identity.test:%d", firstPort))
	var local netip.AddrPort
	if mode == "Forward" {
		instance, err := forward.Start(t.Context(), forward.Config{
			Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: endpoint, Resolver: rekeyResolver{},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { instance.Close() })
		if err := instance.WaitReady(t.Context()); err != nil {
			t.Fatal(err)
		}
		local = instance.LocalAddr()
	} else {
		token := []byte("a-sufficiently-long-rekey-test-token")
		placeholder := netip.MustParseAddrPort("127.0.0.1:51820")
		config := serverConfig(t, token, []netip.AddrPort{placeholder})
		var err error
		config.Targets, err = policy.NewTargetSet([]target.Endpoint{endpoint})
		if err != nil {
			t.Fatal(err)
		}
		config.Resolver = rekeyResolver{}
		instance, err := server.New(config)
		if err != nil {
			t.Fatal(err)
		}
		address, stop := startRawServerInstance(t, instance, nil)
		t.Cleanup(stop)
		clientOptions := clientConfig(t, "tcp://"+address, placeholder, token, nil)
		clientOptions.Target = endpoint
		relayClient, err := client.Start(t.Context(), clientOptions)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { relayClient.Close() })
		local = relayClient.LocalAddr()
	}
	clientTUN := tuntest.NewChannelTUN()
	clientDevice := device.NewDevice(clientTUN.TUN(), conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, "rekey-client: "))
	t.Cleanup(clientDevice.Close)
	if err := clientDevice.IpcSet(fmt.Sprintf("private_key=%s\npublic_key=%s\nendpoint=%s\nallowed_ip=10.0.0.2/32\n",
		wireGuardClientPrivateKey, wireGuardServerPublicKey, local)); err != nil {
		t.Fatal(err)
	}
	if err := clientDevice.Up(); err != nil {
		t.Fatal(err)
	}
	payload := tuntest.Ping(netip.MustParseAddr("10.0.0.2"), netip.MustParseAddr("10.0.0.1"))
	writeTUNPacket(t, clientTUN.Outbound, payload)
	readTUNPacket(t, firstTUN.Inbound, payload)
	t.Log("the first healthy backend decrypted the initial inner packet")
	// Respect WireGuard's minimum initiation interval, then perform an ordinary authenticated rekey.
	time.Sleep(6 * time.Second)
	favorSecond.Store(true)
	for len(firstTransport) > 0 {
		<-firstTransport
	}
	var publicKey device.NoisePublicKey
	if err := publicKey.FromHex(wireGuardServerPublicKey); err != nil {
		t.Fatal(err)
	}
	if err := clientDevice.LookupPeer(publicKey).SendHandshakeInitiation(false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstTransport:
	case <-time.After(5 * time.Second):
		t.Fatal("no new authenticated key confirmation reached the expected backend")
	}
	writeTUNPacket(t, clientTUN.Outbound, payload)
	readTUNPacket(t, firstTUN.Inbound, payload)
	select {
	case <-secondTransport:
		t.Fatal("routine authenticated rekey moved transport to another healthy backend")
	default:
	}
	// A target failure must still permit an authenticated retry to discover another candidate.
	time.Sleep(6 * time.Second)
	dropFirst.Store(true)
	if err := clientDevice.LookupPeer(publicKey).SendHandshakeInitiation(false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondTransport:
	case <-time.After(10 * time.Second):
		t.Fatal("unanswered handshake did not fail over to the available candidate")
	}
	writeTUNPacket(t, clientTUN.Outbound, payload)
	readTUNPacket(t, secondTUN.Inbound, payload)
}

// rekeyServer creates an independent WireGuard peer with the shared test identity.
func rekeyServer(t *testing.T) (*tuntest.ChannelTUN, *device.Device) {
	t.Helper()
	tun := tuntest.NewChannelTUN()
	peer := device.NewDevice(tun.TUN(), conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, "rekey-server: "))
	t.Cleanup(peer.Close)
	if err := peer.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=0\npublic_key=%s\nallowed_ip=10.0.0.1/32\n",
		wireGuardServerPrivateKey, wireGuardClientPublicKey)); err != nil {
		t.Fatal(err)
	}
	if err := peer.Up(); err != nil {
		t.Fatal(err)
	}
	return tun, peer
}

// rekeyProxy adds a controlled response delay while preserving candidate source addresses.
func rekeyProxy(t *testing.T, host string, port, targetPort uint16, second bool, favorSecond, dropFirst *atomic.Bool) (uint16, <-chan struct{}) {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(host), Port: int(port)})
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(targetPort)})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	var mu sync.Mutex
	var source netip.AddrPort
	transport := make(chan struct{}, 16)
	var workers sync.WaitGroup
	workers.Go(func() {
		var buffer [65535]byte
		for {
			n, peer, err := listener.ReadFromUDPAddrPort(buffer[:])
			if err != nil {
				return
			}
			if !second && dropFirst.Load() {
				continue
			}
			mu.Lock()
			source = peer
			mu.Unlock()
			if n > 0 && buffer[0] == 4 {
				select {
				case transport <- struct{}{}:
				default:
				}
			}
			upstream.Write(buffer[:n])
		}
	})
	workers.Go(func() {
		var buffer [65535]byte
		for {
			n, err := upstream.Read(buffer[:])
			if err != nil {
				return
			}
			if n > 0 && buffer[0] == 2 && second != favorSecond.Load() {
				time.Sleep(150 * time.Millisecond)
			}
			mu.Lock()
			peer := source
			mu.Unlock()
			listener.WriteToUDPAddrPort(buffer[:n], peer)
		}
	})
	t.Cleanup(func() {
		listener.Close()
		upstream.Close()
		workers.Wait()
	})
	return listener.LocalAddr().(*net.UDPAddr).AddrPort().Port(), transport
}
