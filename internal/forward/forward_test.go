package forward

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var errTestEndpoint = errors.New("test endpoint failure")

type staticResolver struct {
	addresses []netip.Addr
	err       error
	calls     atomic.Int32
}

func (r *staticResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.calls.Add(1)
	return append([]netip.Addr(nil), r.addresses...), r.err
}

type stubEndpoint struct {
	read  func(context.Context) (datagram.Packet, error)
	write func(context.Context, []byte, time.Time) error
}

func (e stubEndpoint) Read(ctx context.Context) (datagram.Packet, error) {
	return e.read(ctx)
}

func (e stubEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	return e.write(ctx, payload, deadline)
}

func (stubEndpoint) Close() error {
	return nil
}

func TestStart(t *testing.T) {
	for _, test := range []struct {
		name           string
		reserved       wgpacket.Reserved
		localReserved  wgpacket.Reserved
		targetReserved wgpacket.Reserved
		domain         bool
	}{
		{
			name: "Transparent", localReserved: wgpacket.Reserved{7, 8, 9},
			targetReserved: wgpacket.Reserved{7, 8, 9}, domain: true,
		},
		{
			name: "ReservedTranslation", reserved: wgpacket.Reserved{1, 2, 3},
			targetReserved: wgpacket.Reserved{1, 2, 3},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			targetConnection := listenUDP(t)
			targetAddress := targetConnection.LocalAddr().(*net.UDPAddr).AddrPort()
			endpoint, err := target.FromAddrPort(targetAddress)
			if err != nil {
				t.Fatal(err)
			}
			var resolver *staticResolver
			if test.domain {
				endpoint = target.MustParse("wg.example.com:" + strconv.Itoa(int(targetAddress.Port())))
				resolver = &staticResolver{addresses: []netip.Addr{targetAddress.Addr()}}
			}
			instance, err := Start(context.Background(), Config{
				Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: endpoint,
				Reserved: test.reserved, Resolver: resolver,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { instance.Close() })

			localPeer := listenUDP(t)
			initiation := wireGuardPacket(1, 148, 11, 0, test.localReserved)
			writeUDP(t, localPeer, instance.LocalAddr(), initiation)
			forwardedInitiation, targetSource := readUDP(t, targetConnection, len(initiation))
			if !bytes.Equal(forwardedInitiation, wireGuardPacket(1, 148, 11, 0, test.targetReserved)) {
				t.Fatalf("forwarded initiation reserved = %v, want %v", forwardedInitiation[1:4], test.targetReserved)
			}

			response := wireGuardPacket(2, 92, 21, 11, test.targetReserved)
			writeUDP(t, targetConnection, targetSource, response)
			forwardedResponse, _ := readUDP(t, localPeer, len(response))
			if !bytes.Equal(forwardedResponse, wireGuardPacket(2, 92, 21, 11, test.localReserved)) {
				t.Fatalf("forwarded response reserved = %v, want %v", forwardedResponse[1:4], test.localReserved)
			}
			if resolver != nil && resolver.calls.Load() == 0 {
				t.Fatal("forward target was not resolved")
			}
			if err := instance.Close(); err != nil {
				t.Fatal(err)
			}
			if err := instance.Close(); err != nil {
				t.Fatal(err)
			}
			if err := instance.Wait(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
			}
		})
	}
}

func TestStartDropsReservedMismatch(t *testing.T) {
	targetConnection := listenUDP(t)
	targetEndpoint, err := target.FromAddrPort(targetConnection.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	reserved := wgpacket.Reserved{1, 2, 3}
	instance, err := Start(context.Background(), Config{
		Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: targetEndpoint, Reserved: reserved,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close() })

	localPeer := listenUDP(t)
	initiation := wireGuardPacket(1, 148, 11, 0, wgpacket.Reserved{})
	writeUDP(t, localPeer, instance.LocalAddr(), initiation)
	_, targetSource := readUDP(t, targetConnection, len(initiation))
	response := wireGuardPacket(2, 92, 21, 11, wgpacket.Reserved{4, 5, 6})
	writeUDP(t, targetConnection, targetSource, response)
	assertNoUDP(t, localPeer)

	response = wireGuardPacket(2, 92, 21, 11, reserved)
	writeUDP(t, targetConnection, targetSource, response)
	forwarded, _ := readUDP(t, localPeer, len(response))
	if got := wgpacket.Reserved(forwarded[1:4]); got != (wgpacket.Reserved{}) {
		t.Fatalf("forwarded response reserved = %v, want zero", got)
	}
}

func TestStartFailureReleasesListener(t *testing.T) {
	listener := listenUDP(t)
	listenAddress := listener.LocalAddr().(*net.UDPAddr).AddrPort()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	resolver := &staticResolver{err: errTestEndpoint}
	instance, err := Start(context.Background(), Config{
		Listen: listenAddress, Target: target.MustParse("wg.example.com:51820"), Resolver: resolver,
	})
	if instance != nil || !errors.Is(err, errTestEndpoint) {
		t.Fatalf("Start() = %v, %v, want nil and %v", instance, err, errTestEndpoint)
	}
	rebound, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(listenAddress))
	if err != nil {
		t.Fatalf("rebind released listener: %v", err)
	}
	rebound.Close()
}

func TestForwarderWaitPreservesEndpointFailure(t *testing.T) {
	targetConnection := listenUDP(t)
	targetEndpoint, err := target.FromAddrPort(targetConnection.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	instance, err := Start(context.Background(), Config{
		Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: targetEndpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close() })
	if err := instance.local.Close(); err != nil {
		t.Fatal(err)
	}
	err = instance.Wait()
	if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "read local UDP endpoint") {
		t.Fatalf("Wait() error = %v, want local endpoint failure", err)
	}
}

func TestStartRejectsInvalidConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		config Config
	}{
		{name: "MissingListen", config: Config{Target: target.MustParse("127.0.0.1:51820")}},
		{name: "MissingTarget", config: Config{Listen: netip.MustParseAddrPort("127.0.0.1:51820")}},
		{
			name: "MulticastListen",
			config: Config{
				Listen: netip.MustParseAddrPort("224.0.0.1:51820"),
				Target: target.MustParse("127.0.0.1:51820"),
			},
		},
		{
			name: "MappedListen",
			config: Config{
				Listen: netip.MustParseAddrPort("[::ffff:127.0.0.1]:51820"),
				Target: target.MustParse("127.0.0.1:51820"),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			instance, err := Start(context.Background(), test.config)
			if instance != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Start(%+v) = %v, %v, want nil and %v", test.config, instance, err, ErrInvalidConfig)
			}
		})
	}
}

func TestCopyPackets(t *testing.T) {
	t.Run("PacketLocalErrors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		packet := wireGuardPacket(4, 32, 0, 0, wgpacket.Reserved{})
		var reads atomic.Int32
		source := stubEndpoint{
			read: func(ctx context.Context) (datagram.Packet, error) {
				if reads.Add(1) <= 3 {
					return datagram.Packet{Kind: wgpacket.TransportData, Payload: packet}, nil
				}
				<-ctx.Done()
				return datagram.Packet{}, ctx.Err()
			},
			write: func(context.Context, []byte, time.Time) error { return nil },
		}
		var writes atomic.Int32
		delivered := make(chan struct{})
		destination := stubEndpoint{
			read: func(context.Context) (datagram.Packet, error) { return datagram.Packet{}, errTestEndpoint },
			write: func(context.Context, []byte, time.Time) error {
				switch writes.Add(1) {
				case 1:
					return datagram.ErrNoLocalPeer
				case 2:
					return datagram.ErrDatagramDropped
				default:
					close(delivered)
					return nil
				}
			},
		}
		result := make(chan error, 1)
		go func() { result <- copyPackets(ctx, "local", source, destination) }()
		<-delivered
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) || writes.Load() != 3 {
			t.Fatalf("copyPackets() = %v after %d writes", err, writes.Load())
		}
	})

	t.Run("ReadFailure", func(t *testing.T) {
		source := stubEndpoint{
			read:  func(context.Context) (datagram.Packet, error) { return datagram.Packet{}, errTestEndpoint },
			write: func(context.Context, []byte, time.Time) error { return nil },
		}
		err := copyPackets(context.Background(), "local", source, source)
		if !errors.Is(err, errTestEndpoint) || !strings.Contains(err.Error(), "read local UDP endpoint") {
			t.Fatalf("copyPackets() error = %v", err)
		}
	})

	t.Run("WriteFailure", func(t *testing.T) {
		packet := wireGuardPacket(4, 32, 0, 0, wgpacket.Reserved{})
		source := stubEndpoint{
			read: func(context.Context) (datagram.Packet, error) {
				return datagram.Packet{Kind: wgpacket.TransportData, Payload: packet}, nil
			},
			write: func(context.Context, []byte, time.Time) error { return nil },
		}
		destination := stubEndpoint{
			read:  func(context.Context) (datagram.Packet, error) { return datagram.Packet{}, nil },
			write: func(context.Context, []byte, time.Time) error { return errTestEndpoint },
		}
		err := copyPackets(context.Background(), "target", source, destination)
		if !errors.Is(err, errTestEndpoint) || !strings.Contains(err.Error(), "forward target UDP packet") {
			t.Fatalf("copyPackets() error = %v", err)
		}
	})
}

func listenUDP(t *testing.T) *net.UDPConn {
	t.Helper()
	connection, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return connection
}

func writeUDP(t *testing.T, connection *net.UDPConn, destination netip.AddrPort, payload []byte) {
	t.Helper()
	if _, err := connection.WriteToUDPAddrPort(payload, destination); err != nil {
		t.Fatal(err)
	}
}

func readUDP(t *testing.T, connection *net.UDPConn, size int) ([]byte, netip.AddrPort) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, size)
	length, source, err := connection.ReadFromUDPAddrPort(payload)
	if err != nil || length != size {
		t.Fatalf("UDP read = %d, %v, want %d", length, err, size)
	}
	return payload, source
}

func assertNoUDP(t *testing.T, connection *net.UDPConn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Read(make([]byte, 2048)); err == nil {
		t.Fatal("unexpected UDP datagram")
	} else if networkError, ok := errors.AsType[net.Error](err); !ok || !networkError.Timeout() {
		t.Fatalf("UDP read error = %v, want timeout", err)
	}
}

func wireGuardPacket(typeID byte, size int, sender, receiver uint32, reserved wgpacket.Reserved) []byte {
	packet := make([]byte, size)
	packet[0] = typeID
	copy(packet[1:4], reserved[:])
	if typeID == 1 || typeID == 2 {
		binary.LittleEndian.PutUint32(packet[4:8], sender)
	} else {
		binary.LittleEndian.PutUint32(packet[4:8], receiver)
	}
	if typeID == 2 {
		binary.LittleEndian.PutUint32(packet[8:12], receiver)
	}
	return packet
}
