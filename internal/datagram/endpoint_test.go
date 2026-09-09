package datagram

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	targetpkg "github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestLocal(t *testing.T) {
	listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	local := NewLocal(listener)
	t.Cleanup(func() { local.Close() })
	if err := local.Write(context.Background(), []byte{1}, time.Time{}); !errors.Is(err, ErrNoLocalPeer) {
		t.Fatalf("Write() error = %v", err)
	}

	first := listenUDP(t)
	second := listenUDP(t)
	writeUDP(t, first, local.LocalAddr(), []byte{9})
	writeUDP(t, first, local.LocalAddr(), wireGuardPacket(1, 148))
	packet, err := local.Read(context.Background())
	if err != nil || packet.Kind.String() != "handshake_initiation" {
		t.Fatalf("Read() = %#v, %v", packet, err)
	}
	packet.Release()
	if err := local.Write(context.Background(), wireGuardPacket(2, 92), time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, 92)

	writeUDP(t, second, local.LocalAddr(), wireGuardPacket(4, 32))
	readAndRelease(t, local)
	if err := local.Write(context.Background(), wireGuardPacket(3, 64), time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, 64)
}

func TestLocalReadBatchIgnoresSoftDrainError(t *testing.T) {
	listener := listenUDP(t)
	local := NewLocal(listener)
	local.batch = stubUDPBatchConn{readErr: syscall.ENOBUFS}
	t.Cleanup(func() { local.Close() })
	peer := listenUDP(t)
	writeUDP(t, peer, local.LocalAddr(), wireGuardPacket(4, 32))

	var packets [MaximumBatchSize]Packet
	count, err := local.ReadBatch(context.Background(), packets[:])
	if err != nil || count != 1 {
		t.Fatalf("ReadBatch() = %d, %v, want 1, nil", count, err)
	}
	packets[0].Release()
}

func TestRemote(t *testing.T) {
	peer := listenUDP(t)
	endpoint, err := targetpkg.FromAddrPort(peer.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := OpenRemote(context.Background(), endpoint, RemoteConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	if err := remote.Write(context.Background(), wireGuardPacket(1, 148), time.Time{}); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 148)
	length, source, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil || length != 148 {
		t.Fatalf("target read = %d, %v", length, err)
	}
	writeUDP(t, peer, source, []byte{9})
	writeUDP(t, peer, source, wireGuardPacket(2, 92))
	packet, err := remote.Read(context.Background())
	if err != nil || len(packet.Payload) != 92 {
		t.Fatalf("Read() = %#v, %v", packet, err)
	}
	packet.Release()
}

func TestRemotePreservesReserved(t *testing.T) {
	peer := listenUDP(t)
	endpoint, err := targetpkg.FromAddrPort(peer.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := OpenRemote(context.Background(), endpoint, RemoteConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	initiation := indexedWireGuardPacket(1, 148, 11, 0)
	copy(initiation[1:4], []byte{1, 2, 3})
	if err := remote.Write(context.Background(), initiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(initiation))
	length, source, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil || length != len(initiation) || string(buffer[:length]) != string(initiation) {
		t.Fatalf("target read length = %d, error = %v", length, err)
	}

	response := indexedWireGuardPacket(2, 92, 21, 11)
	copy(response[1:4], initiation[1:4])
	writeUDP(t, peer, source, response)
	packet, err := remote.Read(context.Background())
	if err != nil || string(packet.Payload) != string(response) {
		t.Fatalf("Read() = %#v, %v", packet, err)
	}
	packet.Release()
}

func TestOpenRemoteRejectsCanceledParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := OpenRemote(ctx, targetpkg.MustParse("127.0.0.1:51820"), RemoteConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenRemote() error = %v, want %v", err, context.Canceled)
	}
}

func TestOpenRemotePreparationContextDoesNotOwnSocket(t *testing.T) {
	peer := listenUDP(t)
	ctx, cancel := context.WithCancel(t.Context())
	remote, err := OpenRemote(ctx, targetpkg.MustParse(peer.LocalAddr().String()), RemoteConfig{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	payload := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(t.Context(), payload, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("Write() after preparation cancellation = %v", err)
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, len(payload))
	_, source, err := peer.ReadFromUDPAddrPort(buffer)
	if err != nil {
		t.Fatal(err)
	}
	response := indexedWireGuardPacket(2, 92, 21, 11)
	writeUDP(t, peer, source, response)
	readContext, cancelRead := context.WithTimeout(t.Context(), time.Second)
	defer cancelRead()
	packet, err := remote.Read(readContext)
	if err != nil {
		t.Fatalf("Read() after preparation cancellation = %v", err)
	}
	packet.Release()
}

func TestOpenRemoteUsesListenConfig(t *testing.T) {
	peer := listenUDP(t)
	endpoint, err := targetpkg.FromAddrPort(peer.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	var observedNetwork string
	remote, err := OpenRemote(context.Background(), endpoint, RemoteConfig{ListenConfig: net.ListenConfig{
		Control: func(network, _ string, _ syscall.RawConn) error {
			observedNetwork = network
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })
	if observedNetwork != "udp4" {
		t.Fatalf("target socket network = %q, want udp4", observedNetwork)
	}
}

func TestRemoteRejectsUnknownSource(t *testing.T) {
	peer := listenUDP(t)
	endpoint, err := targetpkg.FromAddrPort(peer.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := OpenRemote(context.Background(), endpoint, RemoteConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	initiation := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(context.Background(), initiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	remoteAddress := readUDPFrom(t, peer, len(initiation))
	unknown := listenUDP(t)
	writeUDP(t, unknown, remoteAddress, indexedWireGuardPacket(2, 92, 21, 11))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := remote.Read(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read() error = %v, want %v", err, context.DeadlineExceeded)
	}

	writeUDP(t, peer, remoteAddress, indexedWireGuardPacket(2, 92, 21, 11))
	readAndRelease(t, remote)
}

func TestRemoteDropsResponseWithoutIndexRoute(t *testing.T) {
	peer := listenUDP(t)
	endpoint, err := targetpkg.FromAddrPort(peer.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := OpenRemote(context.Background(), endpoint, RemoteConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	response := indexedWireGuardPacket(2, 92, 21, 11)
	if err := remote.Write(context.Background(), response, time.Time{}); !errors.Is(err, ErrDatagramDropped) {
		t.Fatalf("Write() error = %v, want %v", err, ErrDatagramDropped)
	}
	assertNoUDP(t, peer)
}

func TestRemoteRoutesWireGuardIndexesAcrossCandidates(t *testing.T) {
	first, second := listenCandidatePair(t)
	resolver := &mutableResolver{addresses: []netip.Addr{
		first.LocalAddr().(*net.UDPAddr).AddrPort().Addr(),
		second.LocalAddr().(*net.UDPAddr).AddrPort().Addr(),
	}}
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(first.LocalAddr().(*net.UDPAddr).Port)), RemoteConfig{Resolver: resolver}, time.Second,
		time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	initiation := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(context.Background(), initiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstSource := readUDPFrom(t, first, len(initiation))
	secondSource := readUDPFrom(t, second, len(initiation))
	writeUDP(t, first, firstSource, indexedWireGuardPacket(2, 92, 21, 11))
	writeUDP(t, second, secondSource, indexedWireGuardPacket(2, 92, 22, 11))
	for range 2 {
		packet, err := remote.Read(context.Background())
		if err != nil || packet.Kind.String() != "handshake_response" {
			t.Fatalf("Read() = %#v, %v", packet, err)
		}
		packet.Release()
	}

	transport := indexedWireGuardPacket(4, 32, 0, 22)
	if err := remote.Write(context.Background(), transport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(transport))
	assertNoUDP(t, first)

	writeUDP(t, second, secondSource, indexedWireGuardPacket(1, 148, 31, 0))
	readAndRelease(t, remote)
	response := indexedWireGuardPacket(2, 92, 41, 31)
	if err := remote.Write(context.Background(), response, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(response))
	assertNoUDP(t, first)
}

func TestRemoteRoutesResponderTransportAcrossCandidates(t *testing.T) {
	first, second := listenCandidatePair(t)
	resolver := &mutableResolver{addresses: []netip.Addr{
		first.LocalAddr().(*net.UDPAddr).AddrPort().Addr(),
		second.LocalAddr().(*net.UDPAddr).AddrPort().Addr(),
	}}
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(first.LocalAddr().(*net.UDPAddr).Port)), RemoteConfig{Resolver: resolver}, time.Second,
		time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	fanout := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(context.Background(), fanout, time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstSource := readUDPFrom(t, first, len(fanout))
	secondSource := readUDPFrom(t, second, len(fanout))

	firstInitiation := indexedWireGuardPacket(1, 148, 31, 0)
	writeUDP(t, first, firstSource, firstInitiation)
	readAndRelease(t, remote)
	firstResponse := indexedWireGuardPacket(2, 92, 41, 31)
	if err := remote.Write(context.Background(), firstResponse, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(firstResponse))

	secondInitiation := indexedWireGuardPacket(1, 148, 32, 0)
	writeUDP(t, second, secondSource, secondInitiation)
	readAndRelease(t, remote)
	secondResponse := indexedWireGuardPacket(2, 92, 42, 32)
	if err := remote.Write(context.Background(), secondResponse, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(secondResponse))

	transport := indexedWireGuardPacket(4, 32, 0, 31)
	if err := remote.Write(context.Background(), transport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(transport))
	assertNoUDP(t, second)
}

func TestRemoteRefreshesDomainCandidates(t *testing.T) {
	first, second := listenCandidatePair(t)
	resolver := &mutableResolver{addresses: []netip.Addr{first.LocalAddr().(*net.UDPAddr).AddrPort().Addr()}}
	observedNetworks := make(chan string, 2)
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(first.LocalAddr().(*net.UDPAddr).Port)), RemoteConfig{
		Resolver: resolver,
		ListenConfig: net.ListenConfig{Control: func(network, _ string, _ syscall.RawConn) error {
			observedNetworks <- network
			return nil
		}},
	}, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })
	if network := <-observedNetworks; network != "udp4" {
		t.Fatalf("initial target socket network = %q, want udp4", network)
	}

	resolver.set([]netip.Addr{second.LocalAddr().(*net.UDPAddr).AddrPort().Addr()}, nil)
	time.Sleep(2 * time.Millisecond)
	remote.triggerRefresh()
	waitRemoteRefresh(t, remote)
	if network := <-observedNetworks; network != "udp6" {
		t.Fatalf("refreshed target socket network = %q, want udp6", network)
	}
	initiation := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(context.Background(), initiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(initiation))
	assertNoUDP(t, first)

	resolver.set(nil, errors.New("test resolver failure"))
	time.Sleep(2 * time.Millisecond)
	remote.triggerRefresh()
	waitRemoteRefresh(t, remote)
	if err := remote.Write(context.Background(), initiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(initiation))
}

func TestRemoteInitiationTriggersRefresh(t *testing.T) {
	peer := listenUDP(t)
	address := peer.LocalAddr().(*net.UDPAddr).AddrPort()
	resolver := &mutableResolver{addresses: []netip.Addr{address.Addr()}}
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(int(address.Port()))), RemoteConfig{Resolver: resolver}, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	time.Sleep(2 * time.Millisecond)
	if err := remote.Write(context.Background(), indexedWireGuardPacket(1, 148, 11, 0), time.Time{}); err != nil {
		t.Fatal(err)
	}
	waitRemoteRefresh(t, remote)
	if calls := resolver.callCount(); calls != 2 {
		t.Fatalf("resolver calls = %d, want 2", calls)
	}
}

func TestRemoteBatchDrainErrorTriggersRefresh(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "SoftError", err: syscall.ENOBUFS},
		{name: "HardError", err: syscall.EBADF},
	} {
		t.Run(tt.name, func(t *testing.T) {
			peer := listenUDP(t)
			peerAddress := peer.LocalAddr().(*net.UDPAddr).AddrPort()
			connection, fallback := listenCandidatePair(t)
			resolver := &mutableResolver{addresses: []netip.Addr{peerAddress.Addr()}}
			ctx, cancel := context.WithCancel(context.Background())
			remote := &Remote{
				target:   targetpkg.MustParse("wg.example.com:" + strconv.Itoa(int(peerAddress.Port()))),
				resolver: resolver, resolveTimeout: time.Second, refreshInterval: time.Nanosecond,
				ctx: ctx, cancel: cancel, reads: make(chan remoteRead, 1),
				recent: make(map[netip.AddrPort]time.Time), retained: make(map[netip.AddrPort]int),
				handshakeRoutes: make(map[uint32]targetRoute), transportRoutes: make(map[uint32]targetRoute),
				candidates: []netip.AddrPort{peerAddress},
			}
			remote.sockets[0] = connection
			remote.sockets[1] = fallback
			remote.workers.Add(1)
			go remote.readSocket(connection, stubUDPBatchConn{readErr: tt.err})
			t.Cleanup(func() { remote.Close() })

			writeUDP(t, peer, connection.LocalAddr().(*net.UDPAddr).AddrPort(), indexedWireGuardPacket(1, 148, 11, 0))
			select {
			case result := <-remote.reads:
				result.release()
			case <-time.After(time.Second):
				t.Fatal("target packet was not delivered")
			}
			deadline := time.Now().Add(time.Second)
			for resolver.callCount() == 0 {
				if time.Now().After(deadline) {
					t.Fatal("batch drain error did not trigger target refresh")
				}
				time.Sleep(time.Millisecond)
			}
			waitRemoteRefresh(t, remote)
			current := remote.socket(peerAddress.Addr())
			if current == nil {
				t.Fatal("target refresh left the failed socket family unavailable")
			}
			if replaced := current != connection; replaced != (tt.err == syscall.EBADF) {
				t.Fatalf("target socket replaced = %v after %v", replaced, tt.err)
			}
			if remote.socket(netip.IPv6Loopback()) != fallback {
				t.Fatal("target refresh replaced the healthy fallback socket")
			}
		})
	}
}

func TestRemoteMovesTransportAfterCandidateFailover(t *testing.T) {
	first, second := listenCandidatePair(t)
	resolver := &mutableResolver{addresses: []netip.Addr{first.LocalAddr().(*net.UDPAddr).AddrPort().Addr()}}
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(first.LocalAddr().(*net.UDPAddr).Port)), RemoteConfig{Resolver: resolver}, time.Second,
		time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	firstInitiation := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(context.Background(), firstInitiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstSource := readUDPFrom(t, first, len(firstInitiation))
	writeUDP(t, first, firstSource, indexedWireGuardPacket(2, 92, 21, 11))
	readAndRelease(t, remote)
	firstTransport := indexedWireGuardPacket(4, 32, 0, 21)
	if err := remote.Write(context.Background(), firstTransport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(firstTransport))

	resolver.set([]netip.Addr{second.LocalAddr().(*net.UDPAddr).AddrPort().Addr()}, nil)
	time.Sleep(2 * time.Millisecond)
	remote.triggerRefresh()
	waitRemoteRefresh(t, remote)
	if err := remote.Write(context.Background(), firstTransport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(firstTransport))
	assertNoUDP(t, second)

	secondInitiation := indexedWireGuardPacket(1, 148, 12, 0)
	if err := remote.Write(context.Background(), secondInitiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(secondInitiation))
	assertNoUDP(t, second)
	remote.mu.Lock()
	remote.handshakeStarted = time.Now().Add(-targetHandshakeRetryInterval)
	remote.mu.Unlock()
	secondInitiation = indexedWireGuardPacket(1, 148, 13, 0)
	if err := remote.Write(context.Background(), secondInitiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	secondSource := readUDPFrom(t, second, len(secondInitiation))
	assertNoUDP(t, first)
	writeUDP(t, second, secondSource, indexedWireGuardPacket(2, 92, 22, 13))
	readAndRelease(t, remote)
	secondTransport := indexedWireGuardPacket(4, 32, 0, 22)
	if err := remote.Write(context.Background(), secondTransport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(secondTransport))
	assertNoUDP(t, first)
	if err := remote.Write(context.Background(), firstTransport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(firstTransport))
	assertNoUDP(t, second)
	if err := remote.Write(context.Background(), indexedWireGuardPacket(1, 148, 14, 0), time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, 148)
	assertNoUDP(t, first)

	writeUDP(t, first, firstSource, indexedWireGuardPacket(4, 32, 0, 31))
	readAndRelease(t, remote)
}

func TestRemoteAcceptsResponseFromRecentlyReplacedCandidate(t *testing.T) {
	first, second := listenCandidatePair(t)
	resolver := &mutableResolver{addresses: []netip.Addr{
		first.LocalAddr().(*net.UDPAddr).AddrPort().Addr(),
		second.LocalAddr().(*net.UDPAddr).AddrPort().Addr(),
	}}
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(first.LocalAddr().(*net.UDPAddr).Port)), RemoteConfig{Resolver: resolver}, time.Second,
		time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	initiation := indexedWireGuardPacket(1, 148, 11, 0)
	if err := remote.Write(context.Background(), initiation, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, first, len(initiation))
	secondSource := readUDPFrom(t, second, len(initiation))
	resolver.set([]netip.Addr{first.LocalAddr().(*net.UDPAddr).AddrPort().Addr()}, nil)
	time.Sleep(2 * time.Millisecond)
	remote.triggerRefresh()
	waitRemoteRefresh(t, remote)

	writeUDP(t, second, secondSource, indexedWireGuardPacket(2, 92, 22, 11))
	readAndRelease(t, remote)
	transport := indexedWireGuardPacket(4, 32, 0, 22)
	if err := remote.Write(context.Background(), transport, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(transport))
	assertNoUDP(t, first)
}

func TestRemoteDestinationsPreservesConfirmedTarget(t *testing.T) {
	first := netip.MustParseAddrPort("192.0.2.1:51820")
	second := netip.MustParseAddrPort("192.0.2.2:51820")
	remote := &Remote{
		current: first, candidates: []netip.AddrPort{first, second}, retained: make(map[netip.AddrPort]int),
		handshakeRoutes: make(map[uint32]targetRoute), transportRoutes: make(map[uint32]targetRoute),
	}
	now := time.Now()
	remote.observeSource(first, wgpacket.Header{Kind: wgpacket.HandshakeResponse, SenderIndex: 11}, now)
	remote.destinations(wgpacket.Header{Kind: wgpacket.TransportData, ReceiverIndex: 11}, nil, now)
	initiation := wgpacket.Header{Kind: wgpacket.HandshakeInitiation, SenderIndex: 21}
	for _, elapsed := range []time.Duration{time.Second, 2 * time.Second} {
		got := remote.destinations(initiation, nil, now.Add(elapsed))
		if len(got) != 1 || got[0] != first {
			t.Fatalf("routine rekey destinations = %v, want only %s", got, first)
		}
	}
	remote.destinations(wgpacket.Header{Kind: wgpacket.TransportData, ReceiverIndex: 11}, nil, now.Add(3*time.Second))
	if got := remote.destinations(initiation, nil, now.Add(6*time.Second)); len(got) != 2 {
		t.Fatalf("unanswered handshake retry destinations = %v, want both candidates", got)
	}
	remote.observeSource(second, wgpacket.Header{Kind: wgpacket.HandshakeResponse, SenderIndex: 12}, now.Add(7*time.Second))
	remote.destinations(wgpacket.Header{Kind: wgpacket.TransportData, ReceiverIndex: 12}, nil, now.Add(7*time.Second))
	remote.destinations(wgpacket.Header{Kind: wgpacket.TransportData, ReceiverIndex: 11}, nil, now.Add(8*time.Second))
	remote.observeSource(first, wgpacket.Header{Kind: wgpacket.HandshakeResponse, SenderIndex: 11}, now.Add(9*time.Second))
	remote.destinations(wgpacket.Header{Kind: wgpacket.TransportData, ReceiverIndex: 11}, nil, now.Add(9*time.Second))
	if got := remote.destinations(initiation, nil, now.Add(10*time.Second)); len(got) != 1 || got[0] != second {
		t.Fatalf("rekey after old-key traffic routes to %v, want selected target %s", got, second)
	}
	unsolicited := wgpacket.Header{Kind: wgpacket.HandshakeInitiation, SenderIndex: 31}
	if remote.observeSource(first, unsolicited, now.Add(11*time.Second)) {
		t.Fatal("an alternate backend's unsolicited rekey displaced a healthy selection")
	}
	if !remote.observeSource(first, unsolicited, now.Add(15*time.Second)) {
		t.Fatal("handshake retry did not permit an alternate backend to recover the peer")
	}
}

func TestRemoteDestinationsRoutesCookies(t *testing.T) {
	for _, kind := range []wgpacket.Kind{wgpacket.HandshakeInitiation, wgpacket.HandshakeResponse} {
		name := "Initiation"
		if kind == wgpacket.HandshakeResponse {
			name = "Response"
		}
		t.Run(name, func(t *testing.T) {
			peer := netip.MustParseAddrPort("192.0.2.1:51820")
			remote := &Remote{
				candidates: []netip.AddrPort{peer}, retained: make(map[netip.AddrPort]int),
				handshakeRoutes: make(map[uint32]targetRoute), transportRoutes: make(map[uint32]targetRoute),
			}
			now := time.Now()
			remote.observeSource(peer, wgpacket.Header{Kind: kind, SenderIndex: 42}, now)
			got := remote.destinations(wgpacket.Header{Kind: wgpacket.CookieReply, ReceiverIndex: 42}, nil, now)
			if len(got) != 1 || got[0] != peer {
				t.Fatalf("cookie reply destinations = %v, want %s", got, peer)
			}
			if !remote.currentRouteAt.IsZero() {
				t.Fatal("an unauthenticated cookie challenge confirmed a target")
			}
		})
	}
}

func TestRemoteRouteRetention(t *testing.T) {
	first := netip.MustParseAddrPort("192.0.2.1:51820")
	second := netip.MustParseAddrPort("192.0.2.2:51820")
	remote := &Remote{
		retained: make(map[netip.AddrPort]int), handshakeRoutes: make(map[uint32]targetRoute),
		transportRoutes: make(map[uint32]targetRoute),
	}
	now := time.Now()
	remote.rememberRouteLocked(remote.handshakeRoutes, 1, first, now)
	remote.rememberRouteLocked(remote.transportRoutes, 2, first, now)
	remote.rememberRouteLocked(remote.handshakeRoutes, 1, second, now)
	if remote.retained[first] != 1 || remote.retained[second] != 1 {
		t.Fatalf("retained routes = %v", remote.retained)
	}
	for index, route := range remote.handshakeRoutes {
		route.expires = now
		remote.handshakeRoutes[index] = route
	}
	for index, route := range remote.transportRoutes {
		route.expires = now
		remote.transportRoutes[index] = route
	}
	remote.nextRoutePrune = now
	remote.pruneRoutesLocked(now)
	if len(remote.handshakeRoutes) != 0 || len(remote.transportRoutes) != 0 || len(remote.retained) != 0 {
		t.Fatalf("expired route state = %v, %v, %v", remote.handshakeRoutes, remote.transportRoutes, remote.retained)
	}
}

func TestRemoteCloseCancelsRefresh(t *testing.T) {
	peer := listenUDP(t)
	address := peer.LocalAddr().(*net.UDPAddr).AddrPort()
	resolver := &blockingRefreshResolver{address: address.Addr(), started: make(chan struct{})}
	remote, err := openRemote(context.Background(), targetpkg.MustParse("wg.example.com:"+
		strconv.Itoa(int(address.Port()))), RemoteConfig{Resolver: resolver}, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })

	time.Sleep(2 * time.Millisecond)
	remote.triggerRefresh()
	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("target refresh did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- remote.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel target refresh")
	}
}

func TestReadCancellation(t *testing.T) {
	local := NewLocal(listenUDP(t))
	t.Cleanup(func() { local.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := local.Read(ctx); err == nil {
		t.Fatal("Read() succeeded after context deadline")
	}
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := local.Read(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Read() error = %v, want %v", err, context.Canceled)
		}
	}
	peer := listenUDP(t)
	ctx, cancel = context.WithCancel(context.Background())
	for range 2 {
		writeUDP(t, peer, local.LocalAddr(), wireGuardPacket(4, 32))
		packet, err := local.Read(ctx)
		if err != nil {
			t.Fatalf("Read() with reused context = %v", err)
		}
		packet.Release()
	}
	result := make(chan error, 1)
	go func() {
		_, err := local.Read(ctx)
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Read() reused-context cancellation error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Read() did not observe reused-context cancellation")
	}
	writeUDP(t, peer, local.LocalAddr(), wireGuardPacket(4, 32))
	readAndRelease(t, local)
}

func TestReadInterruptReusesContextRegistration(t *testing.T) {
	first := &afterFuncContext{Context: context.Background(), done: make(chan struct{}), stopResult: false}
	second := &afterFuncContext{Context: context.Background(), done: make(chan struct{}), stopResult: true}
	conn := listenUDP(t)
	var interrupt readInterrupt
	if err := interrupt.prepare(first, conn); err != nil {
		t.Fatal(err)
	}
	if err := interrupt.prepare(first, conn); err != nil {
		t.Fatal(err)
	}
	if first.registrations != 1 || first.stops != 0 {
		t.Fatalf("first registration state = %d, %d", first.registrations, first.stops)
	}
	if err := interrupt.prepare(second, conn); err != nil {
		t.Fatal(err)
	}
	if first.stops != 1 || second.registrations != 1 {
		t.Fatalf("replacement registration state = %d, %d", first.stops, second.registrations)
	}
	interrupt.interrupt(conn, first.done)
	interrupt.mu.Lock()
	deadlineSet := interrupt.deadlineSet
	interrupt.mu.Unlock()
	if !deadlineSet {
		t.Fatal("stale cancellation callback changed the replacement deadline")
	}
	interrupt.interrupt(conn, second.done)
	interrupt.mu.Lock()
	deadlineSet = interrupt.deadlineSet
	interrupt.mu.Unlock()
	if deadlineSet {
		t.Fatal("current cancellation callback did not invalidate the deadline")
	}
	interrupt.close()
	if err := interrupt.prepare(first, conn); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("prepare() after close error = %v, want %v", err, net.ErrClosed)
	}
	if second.stops != 1 || first.registrations != 1 {
		t.Fatalf("closed registration state = %d, %d", second.stops, first.registrations)
	}
}

func TestEarlierDeadline(t *testing.T) {
	maximum := time.Unix(100, 0)
	for _, tt := range []struct {
		name     string
		context  context.Context
		maximum  time.Time
		expected time.Time
	}{
		{name: "MaximumOnly", context: context.Background(), maximum: maximum, expected: maximum},
		{name: "NoDeadlines", context: context.Background()},
		{
			name: "EarlierContext", context: deadlineContext{deadline: maximum.Add(-time.Second)},
			maximum: maximum, expected: maximum.Add(-time.Second),
		},
		{
			name: "LaterContext", context: deadlineContext{deadline: maximum.Add(time.Second)},
			maximum: maximum, expected: maximum,
		},
		{
			name: "ContextOnly", context: deadlineContext{deadline: maximum}, expected: maximum,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := earlierDeadline(tt.context, tt.maximum); !got.Equal(tt.expected) {
				t.Fatalf("earlierDeadline() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestCopyAcceptedPacket(t *testing.T) {
	valid := wireGuardPacket(4, 32)
	packet, ok := copyAcceptedPacket(valid, len(valid))
	if !ok || len(packet.Payload) != len(valid) {
		t.Fatalf("copyAcceptedPacket() = %#v, %t", packet, ok)
	}
	packet.Payload[4] = 1
	if valid[4] != 0 {
		t.Fatal("accepted packet did not own its payload")
	}
	packet.Release()

	oversized := wireGuardPacket(4, protocol.MaxPacketSize+1)
	if packet, ok := copyAcceptedPacket(oversized, len(oversized)); ok {
		t.Fatalf("oversized copyAcceptedPacket() = %#v, true", packet)
	}
}

func TestPacketOwnership(t *testing.T) {
	packet := ownPacket(4, wireGuardPacket(4, 1420))
	buffer := packet.buffer
	if cap(packet.Payload) != 1536 {
		t.Fatalf("packet capacity = %d, want 1536", cap(packet.Payload))
	}
	retained := packet.Retain()
	if refs := buffer.refs.Load(); refs != 2 {
		t.Fatalf("retained references = %d, want 2", refs)
	}
	packet.Release()
	if refs := buffer.refs.Load(); refs != 1 {
		t.Fatalf("references after first release = %d, want 1", refs)
	}
	if len(retained.Payload) != 1420 || retained.Payload[0] != 4 {
		t.Fatal("retained packet payload changed before final release")
	}
	retained.Release()
	if refs := buffer.refs.Load(); refs != 0 {
		t.Fatalf("references after final release = %d, want 0", refs)
	}
}

func TestPacketBufferClass(t *testing.T) {
	for _, tt := range []struct {
		name string
		size int
		want int
	}{
		{name: "FirstClassMaximum", size: 64, want: 0},
		{name: "SecondClassMinimum", size: 65, want: 1},
		{name: "SecondClassMaximum", size: 96, want: 1},
		{name: "HandshakeInitiation", size: 148, want: 2},
		{name: "MTUClassMaximum", size: 1536, want: 6},
		{name: "NextClassMinimum", size: 1537, want: 7},
		{name: "PooledMaximum", size: 32768, want: 11},
		{name: "UnpooledMinimum", size: 32769, want: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := packetBufferClass(tt.size); got != tt.want {
				t.Fatalf("packetBufferClass(%d) = %d, want %d", tt.size, got, tt.want)
			}
		})
	}
}

func TestSoftNetworkError(t *testing.T) {
	for _, err := range []error{
		syscall.ECONNREFUSED,
		&net.OpError{Op: "read", Net: "udp", Err: syscall.ECONNRESET},
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
		syscall.EMSGSIZE,
		syscall.ENOBUFS,
		syscall.ENOMEM,
		syscall.EACCES,
		syscall.EPERM,
		syscall.ENETDOWN,
		syscall.EHOSTDOWN,
		syscall.ETIMEDOUT,
		&net.DNSError{IsTimeout: true},
	} {
		if !isSoftNetworkError(err) {
			t.Fatalf("isSoftNetworkError(%v) = false", err)
		}
	}
	if isSoftNetworkError(syscall.EINVAL) != (runtime.GOOS == "linux") {
		t.Fatal("isSoftNetworkError(EINVAL) did not account for Linux blackhole routes")
	}
	for _, err := range []error{net.ErrClosed, syscall.EBADF, syscall.ENOTSOCK} {
		if isSoftNetworkError(err) {
			t.Fatalf("isSoftNetworkError(%v) = true", err)
		}
	}
}

func listenUDP(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readAndRelease(t *testing.T, endpoint Endpoint) {
	t.Helper()
	packet, err := endpoint.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	packet.Release()
}

func listenCandidatePair(t *testing.T) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	first, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	port := first.LocalAddr().(*net.UDPAddr).Port
	second, err := net.ListenUDP("udp6", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.MustParseAddr("::1"), uint16(port)),
	))
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		first.Close()
		second.Close()
	})
	return first, second
}

func writeUDP(t *testing.T, conn *net.UDPConn, target netip.AddrPort, payload []byte) {
	t.Helper()
	if _, err := conn.WriteToUDPAddrPort(payload, target); err != nil {
		t.Fatal(err)
	}
}

func readUDP(t *testing.T, conn *net.UDPConn, size int) {
	t.Helper()
	readUDPFrom(t, conn, size)
}

func readUDPFrom(t *testing.T, conn *net.UDPConn, size int) netip.AddrPort {
	t.Helper()
	buffer := make([]byte, size)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	length, peer, err := conn.ReadFromUDPAddrPort(buffer)
	if err != nil || length != size {
		t.Fatalf("UDP read = %d, %v", length, err)
	}
	return peer
}

func assertNoUDP(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, protocol.MaxPacketSize)); err == nil {
		t.Fatal("unexpected UDP datagram")
	} else if networkError, ok := errors.AsType[net.Error](err); !ok || !networkError.Timeout() {
		t.Fatalf("UDP read error = %v, want timeout", err)
	}
}

func wireGuardPacket(typeID byte, size int) []byte {
	packet := make([]byte, size)
	packet[0] = typeID
	return packet
}

func indexedWireGuardPacket(typeID byte, size int, sender, receiver uint32) []byte {
	packet := wireGuardPacket(typeID, size)
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

type mutableResolver struct {
	mu        sync.Mutex
	addresses []netip.Addr
	err       error
	calls     int
}

type blockingRefreshResolver struct {
	mu      sync.Mutex
	address netip.Addr
	started chan struct{}
	calls   int
}

type stubUDPBatchConn struct {
	readErr error
}

func (c stubUDPBatchConn) readAvailable(int) (udpReadBatch, error) {
	return nil, c.readErr
}

func (stubUDPBatchConn) write(messages []udpMessage) (int, error) {
	return len(messages), nil
}

func (r *blockingRefreshResolver) LookupNetIP(ctx context.Context, _, _ string) ([]netip.Addr, error) {
	r.mu.Lock()
	r.calls++
	initial := r.calls == 1
	if !initial {
		close(r.started)
	}
	r.mu.Unlock()
	if initial {
		return []netip.Addr{r.address}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *mutableResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return append([]netip.Addr(nil), r.addresses...), r.err
}

func (r *mutableResolver) set(addresses []netip.Addr, err error) {
	r.mu.Lock()
	r.addresses = append(r.addresses[:0], addresses...)
	r.err = err
	r.mu.Unlock()
}

func (r *mutableResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func waitRemoteRefresh(t *testing.T, remote *Remote) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		remote.mu.Lock()
		refreshing := remote.refreshing
		remote.mu.Unlock()
		if !refreshing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for target refresh")
		}
		time.Sleep(time.Millisecond)
	}
}

type deadlineContext struct {
	context.Context
	deadline time.Time
}

func (c deadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

type afterFuncContext struct {
	context.Context
	done          chan struct{}
	stopResult    bool
	registrations int
	stops         int
}

func (c *afterFuncContext) Done() <-chan struct{} {
	return c.done
}

func (c *afterFuncContext) AfterFunc(func()) func() bool {
	c.registrations++
	stopped := false
	return func() bool {
		if stopped {
			return false
		}
		stopped = true
		c.stops++
		return c.stopResult
	}
}
