//go:build linux

package datagram

import (
	"bytes"
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLinuxUDPBatch(t *testing.T) {
	for _, tt := range []struct {
		name    string
		network string
		address netip.Addr
	}{
		{name: "IPv4", network: "udp4", address: netip.MustParseAddr("127.0.0.1")},
		{name: "IPv6", network: "udp6", address: netip.MustParseAddr("::1")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			receiver, err := net.ListenUDP(tt.network, net.UDPAddrFromAddrPort(netip.AddrPortFrom(tt.address, 0)))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { receiver.Close() })
			sender, err := net.ListenUDP(tt.network, net.UDPAddrFromAddrPort(netip.AddrPortFrom(tt.address, 0)))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { sender.Close() })

			receiverBatch := newUDPBatchConn(receiver)
			if batch, err := receiverBatch.readAvailable(3); err != nil || batch != nil {
				t.Fatalf("empty readAvailable() = %v, %v", batch, err)
			}
			payloads := [3][]byte{{1, 0, 0}, {2, 0, 0}, {3, 0, 0}}
			messages := [3]udpMessage{
				{payload: payloads[0], peer: receiver.LocalAddr().(*net.UDPAddr).AddrPort()},
				{payload: payloads[1], peer: receiver.LocalAddr().(*net.UDPAddr).AddrPort()},
				{payload: payloads[2], peer: receiver.LocalAddr().(*net.UDPAddr).AddrPort()},
			}
			written, err := newUDPBatchConn(sender).write(messages[:])
			if err != nil || written != len(messages) {
				t.Fatalf("write() = %d, %v, want %d, nil", written, err, len(messages))
			}
			batch, err := receiverBatch.readAvailable(len(messages))
			if err != nil {
				t.Fatal(err)
			}
			if batch == nil {
				t.Fatal("readAvailable() returned no queued datagrams")
			}
			defer batch.release()
			values := batch.messages()
			if len(values) != len(messages) {
				t.Fatalf("readAvailable() = %d datagrams, want %d", len(values), len(messages))
			}
			wantPeer := sender.LocalAddr().(*net.UDPAddr).AddrPort()
			for index, value := range values {
				if string(value.payload) != string(payloads[index]) || value.peer != wantPeer {
					t.Fatalf("message %d = %v from %v, want %v from %v", index, value.payload, value.peer,
						payloads[index], wantPeer)
				}
			}
		})
	}
}

func TestLinuxUDPBatchEqualTailWorkaround(t *testing.T) {
	address := netip.MustParseAddr("127.0.0.1")
	receiver, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(address, 0)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { receiver.Close() })
	sender, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(address, 0)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sender.Close() })

	senderBatch := newUDPBatchConn(sender).(*linuxUDPBatchConn)
	if !senderBatch.gso {
		t.Skip("UDP_SEGMENT is unavailable")
	}
	senderBatch.equalGSOTailWorkaround = true
	peer := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
	payloads := [minimumEqualTailWorkaroundBatchSize][32]byte{}
	messages := make([]udpMessage, len(payloads))
	for index := range messages {
		payloads[index][0] = byte(index + 1)
		messages[index] = udpMessage{payload: payloads[index][:], peer: peer}
	}
	written, err := senderBatch.write(messages)
	if err != nil || written != len(messages) {
		t.Fatalf("write() = %d, %v, want %d, nil", written, err, len(messages))
	}
	if !senderBatch.gso {
		t.Skip("UDP_SEGMENT failed and fell back to scalar writes")
	}

	received, err := newUDPBatchConn(receiver).readAvailable(len(messages) + 1)
	if err != nil {
		t.Fatal(err)
	}
	if received == nil {
		t.Fatal("readAvailable() returned no queued datagrams")
	}
	defer received.release()
	values := received.messages()
	if len(values) != len(messages)+1 {
		t.Fatalf("readAvailable() = %d datagrams, want %d", len(values), len(messages)+1)
	}
	for index, value := range values[:len(messages)] {
		if len(value.payload) != len(payloads[index]) || value.payload[0] != byte(index+1) {
			t.Fatalf("message %d = %v", index, value.payload)
		}
	}
	if len(values[len(messages)].payload) != len(invalidWireGuardDatagram) {
		t.Fatalf("sentinel length = %d, want %d", len(values[len(messages)].payload), len(invalidWireGuardDatagram))
	}
}

func TestLinuxUDPBatchMTUFallback(t *testing.T) {
	for _, tt := range []struct {
		name  string
		sizes []int
	}{
		{name: "CompleteBatch", sizes: []int{1400, 1400}},
		{name: "AfterSuccessfulPrefix", sizes: []int{32, 1400, 1400, 32}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			receiver, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { receiver.Close() })
			sender, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { sender.Close() })
			batch := newUDPBatchConn(sender).(*linuxUDPBatchConn)
			if !batch.gso {
				t.Skip("UDP_SEGMENT is unavailable")
			}
			batch.equalGSOTailWorkaround = false
			var optionErr error
			if err := batch.raw.Control(func(descriptor uintptr) {
				optionErr = unix.SetsockoptInt(int(descriptor), unix.IPPROTO_IPV6, unix.IPV6_MTU, 1280)
			}); err != nil {
				t.Fatal(err)
			}
			if optionErr != nil {
				t.Fatal(optionErr)
			}
			peer := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
			messages := make([]udpMessage, len(tt.sizes))
			for index, size := range tt.sizes {
				messages[index] = udpMessage{payload: bytes.Repeat([]byte{byte(index + 1)}, size), peer: peer}
			}
			written, err := batch.write(messages)
			if err != nil || written != len(messages) {
				t.Fatalf("write() = %d, %v, want %d, nil", written, err, len(messages))
			}
			if !batch.gso {
				t.Fatal("MTU fallback disabled GSO for subsequent writes")
			}
			followup := []udpMessage{
				{payload: bytes.Repeat([]byte{5}, 32), peer: peer},
				{payload: bytes.Repeat([]byte{6}, 32), peer: peer},
			}
			if written, err := batch.write(followup); err != nil || written != len(followup) {
				t.Fatalf("follow-up write() = %d, %v, want %d, nil", written, err, len(followup))
			}
			if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, 2048)
			for index, message := range append(messages, followup...) {
				length, _, err := receiver.ReadFromUDP(buffer)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(buffer[:length], message.payload) {
					t.Fatalf("received datagram %d differs from the submitted payload", index)
				}
			}
		})
	}
}

func TestLinuxRawSockaddrRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name    string
		address netip.AddrPort
	}{
		{name: "IPv4", address: netip.MustParseAddrPort("192.0.2.1:51820")},
		{name: "IPv6", address: netip.MustParseAddrPort("[2001:db8::1]:51820")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var storage unix.RawSockaddrAny
			length, ok := putRawSockaddr(&storage, tt.address)
			if !ok || length == 0 {
				t.Fatalf("putRawSockaddr(%v) = %d, %t", tt.address, length, ok)
			}
			got, ok := rawSockaddrAddrPort(&storage)
			if !ok || got != tt.address {
				t.Fatalf("rawSockaddrAddrPort(%v) = %v, %t", tt.address, got, ok)
			}
		})
	}
}

func TestLinuxWriteBatchPreparation(t *testing.T) {
	first := netip.MustParseAddrPort("192.0.2.1:51820")
	second := netip.MustParseAddrPort("192.0.2.2:51820")
	for _, tt := range []struct {
		name         string
		messages     []udpMessage
		gso          bool
		workaround   bool
		wantHeaders  int
		wantVectors  int
		wantSegments []int
	}{
		{
			name: "SegmentedRun",
			messages: []udpMessage{
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1000), peer: first},
			},
			gso: true, wantHeaders: 1, wantVectors: 3, wantSegments: []int{3},
		},
		{
			name: "GrowingPayload",
			messages: []udpMessage{
				{payload: make([]byte, 1000), peer: first},
				{payload: make([]byte, 1420), peer: first},
			},
			gso: true, wantHeaders: 2, wantVectors: 2, wantSegments: []int{1, 1},
		},
		{
			name: "DistinctPeers",
			messages: []udpMessage{
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: second},
			},
			gso: true, wantHeaders: 2, wantVectors: 2, wantSegments: []int{1, 1},
		},
		{
			name: "Disabled",
			messages: []udpMessage{
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
			},
			wantHeaders: 2, wantVectors: 2, wantSegments: []int{1, 1},
		},
		{
			name: "AffectedKernelSmallBatch",
			messages: []udpMessage{
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
			},
			gso: true, workaround: true, wantHeaders: 2, wantVectors: 2, wantSegments: []int{1, 1},
		},
		{
			name: "AffectedKernelEqualTail",
			messages: []udpMessage{
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
			},
			gso: true, workaround: true, wantHeaders: 1, wantVectors: 9, wantSegments: []int{8},
		},
		{
			name: "AffectedKernelShortTail",
			messages: []udpMessage{
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1420), peer: first},
				{payload: make([]byte, 1000), peer: first},
			},
			gso: true, workaround: true, wantHeaders: 1, wantVectors: 8, wantSegments: []int{8},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			batch := newLinuxWriteBatch()
			if err := batch.prepare(tt.messages, tt.gso, tt.workaround); err != nil {
				t.Fatal(err)
			}
			if batch.headersCount != tt.wantHeaders ||
				!slices.Equal(batch.segments[:batch.headersCount], tt.wantSegments) {
				t.Fatalf("prepared batch = %d headers with %v segments, want %d with %v", batch.headersCount,
					batch.segments[:batch.headersCount], tt.wantHeaders, tt.wantSegments)
			}
			if batch.vectorsCount != tt.wantVectors {
				t.Fatalf("prepared batch = %d vectors, want %d", batch.vectorsCount, tt.wantVectors)
			}
			wantGSO := tt.gso && tt.wantSegments[0] > 1 && (!tt.workaround || len(tt.messages) >= 8)
			if (batch.headers[0].header.Control != nil) != wantGSO {
				t.Fatalf("first header control enabled = %t", batch.headers[0].header.Control != nil)
			}
			if tt.name == "AffectedKernelEqualTail" && int(batch.vectors[batch.vectorsCount-1].Len) != 1 {
				t.Fatalf("sentinel vector length = %d, want 1", batch.vectors[batch.vectorsCount-1].Len)
			}
		})
	}
}

func TestLinuxKernelHasEqualGSOTailBug(t *testing.T) {
	for _, tt := range []struct {
		name    string
		release string
		want    bool
	}{
		{name: "BeforeRegression", release: "6.18.7"},
		{name: "ShortAffectedSeries", release: "7.0", want: true},
		{name: "FirstAffectedSeries", release: "7.0.0", want: true},
		{name: "AffectedSeriesReleaseCandidate", release: "7.0-rc4", want: true},
		{name: "LastAffectedStablePatch", release: "7.0.10-1-pve", want: true},
		{name: "FirstFixedStablePatch", release: "7.0.11"},
		{name: "LaterFixedStablePatch", release: "7.0.12-1-pve"},
		{name: "AffectedReleaseCandidate", release: "7.1.0-rc4-generic", want: true},
		{name: "AffectedShortReleaseCandidate", release: "7.1-rc4", want: true},
		{name: "FirstFixedReleaseCandidate", release: "7.1.0-rc5"},
		{name: "StableFixedSeries", release: "7.1.0"},
		{name: "LaterSeries", release: "7.2.0"},
		{name: "Malformed", release: "unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := linuxKernelHasEqualGSOTailBug(tt.release); got != tt.want {
				t.Fatalf("linuxKernelHasEqualGSOTailBug(%q) = %t, want %t", tt.release, got, tt.want)
			}
		})
	}
}
