//go:build linux

package datagram

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/target"
)

func TestUDPRouteRecovery(t *testing.T) {
	if os.Getenv("WIREHOP_TEST_ROUTES") != "1" {
		t.Skip("requires an isolated Linux network namespace with NET_ADMIN and iproute2")
	}
	route := func(arguments ...string) {
		t.Helper()
		if output, err := exec.Command("ip", arguments...).CombinedOutput(); err != nil {
			t.Fatalf("ip %v: %v: %s", arguments, err, output)
		}
	}
	route("route", "add", "local", "192.0.2.1/32", "dev", "lo", "table", "100")
	t.Cleanup(func() { route("route", "flush", "table", "100") })
	route("rule", "add", "pref", "100", "to", "192.0.2.1/32", "lookup", "100")
	t.Cleanup(func() { route("rule", "del", "pref", "100", "to", "192.0.2.1/32", "lookup", "100") })
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	destination := netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), listener.LocalAddr().(*net.UDPAddr).AddrPort().Port())
	for _, name := range []string{"Remote", "Local"} {
		t.Run(name, func(t *testing.T) {
			var endpoint Endpoint
			if name == "Remote" {
				remote, err := OpenRemote(t.Context(), target.MustParse(destination.String()), RemoteConfig{})
				if err != nil {
					t.Fatal(err)
				}
				endpoint = remote
			} else {
				local, err := ListenLocal(netip.MustParseAddrPort("127.0.0.1:0"), nil)
				if err != nil {
					t.Fatal(err)
				}
				local.rememberPeer(destination)
				endpoint = local
			}
			t.Cleanup(func() { endpoint.Close() })
			payloads := [][]byte{wireGuardPacket(4, 32), wireGuardPacket(4, 32)}
			write := func() (int, error) {
				return WriteBatch(context.Background(), endpoint, payloads, time.Now().Add(time.Second))
			}
			checkDelivery := func() {
				t.Helper()
				if count, err := write(); err != nil || count != len(payloads) {
					t.Fatalf("healthy write = %d, %v", count, err)
				}
				for range payloads {
					readUDP(t, listener, 32)
				}
			}
			checkDelivery()
			for _, routeType := range []string{"prohibit", "blackhole", "unreachable"} {
				route("route", "replace", routeType, "192.0.2.1/32", "table", "100")
				count, err := write()
				if count != 0 || !errors.Is(err, ErrDatagramDropped) {
					t.Errorf("%s route write = %d, %v, want reusable datagram failure", routeType, count, err)
				}
				route("route", "replace", "local", "192.0.2.1/32", "dev", "lo", "table", "100")
				checkDelivery()
			}
		})
	}
}
