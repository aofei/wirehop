//go:build linux

package datagram

import (
	"bytes"
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type observedUDPRead struct {
	syscall.RawConn
	operations int
	callbacks  int
	after      func(bool)
}

func (r *observedUDPRead) Read(receive func(uintptr) bool) error {
	r.operations++
	return r.RawConn.Read(func(descriptor uintptr) bool {
		r.callbacks++
		done := receive(descriptor)
		if r.after != nil {
			r.after(done)
		}
		return done
	})
}

func TestLinuxUDPBatchReadQueued(t *testing.T) {
	for _, tt := range []struct {
		name  string
		limit int
	}{
		{name: "Scalar", limit: 1},
		{name: "FullVector", limit: MaximumBatchSize},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sender, receiver := listenUDP(t), listenUDP(t)
			connection := newUDPBatchConn(receiver).(*linuxUDPBatchConn)
			observed := &observedUDPRead{RawConn: connection.raw}
			connection.raw = observed
			peer := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
			for index := range tt.limit {
				writeUDP(t, sender, peer, bytes.Repeat([]byte{byte(index + 1)}, 32))
			}
			if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			batch, err := connection.read(tt.limit)
			if err != nil || batch == nil {
				t.Fatalf("read() = %v, %v", batch, err)
			}
			defer batch.release()
			if len(batch.messages()) != tt.limit {
				t.Fatalf("received %d datagrams, want %d", len(batch.messages()), tt.limit)
			}
			for index, message := range batch.messages() {
				if !bytes.Equal(message.payload, bytes.Repeat([]byte{byte(index + 1)}, 32)) {
					t.Fatalf("datagram %d was changed or reordered", index)
				}
			}
			if observed.operations != 1 || observed.callbacks != 1 {
				t.Fatalf("queued receive used %d poll operations and %d callbacks, want 1 each",
					observed.operations, observed.callbacks)
			}
		})
	}
}

func TestLinuxUDPBatchReadPoolExhaustion(t *testing.T) {
	previous := linuxReadBatches
	linuxReadBatches = newLinuxReadBatchPool()
	t.Cleanup(func() { linuxReadBatches = previous })
	var held []*linuxReadBatch
	for batch := linuxReadBatches.acquire(); batch != nil; batch = linuxReadBatches.acquire() {
		held = append(held, batch)
	}
	t.Cleanup(func() {
		for _, batch := range held {
			batch.release()
		}
	})
	sender, receiver := listenUDP(t), listenUDP(t)
	writeUDP(t, sender, receiver.LocalAddr().(*net.UDPAddr).AddrPort(), []byte("pending"))
	connection := newUDPBatchConn(receiver).(*linuxUDPBatchConn)
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	batch, err := connection.read(MaximumBatchSize)
	if err != nil || batch == nil {
		t.Fatalf("read() with busy staging pool = %v, %v", batch, err)
	}
	if messages := batch.messages(); len(messages) != 1 || string(messages[0].payload) != "pending" {
		t.Fatalf("scalar fallback received %v, want pending datagram", messages)
	}
	batch.release()
	if connection.scalar.storage != nil || connection.scalar.vector.Base != nil {
		t.Fatal("scalar fallback retained a released receive buffer")
	}
	if len(linuxReadBatches.permits) != cap(linuxReadBatches.permits) {
		t.Fatal("scalar fallback changed the bounded vector reservations")
	}
}

func TestLinuxUDPBatchReadReleasesStagingBeforePoll(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		name := "Vector"
		if exhausted {
			name = "ScalarFallback"
		}
		t.Run(name, func(t *testing.T) {
			previous := linuxReadBatches
			linuxReadBatches = newLinuxReadBatchPool()
			t.Cleanup(func() { linuxReadBatches = previous })
			var held []*linuxReadBatch
			if exhausted {
				for batch := linuxReadBatches.acquire(); batch != nil; batch = linuxReadBatches.acquire() {
					held = append(held, batch)
				}
			}
			t.Cleanup(func() {
				for _, batch := range held {
					batch.release()
				}
			})
			local := NewLocal(listenUDP(t))
			t.Cleanup(func() { local.Close() })
			connection := local.batch.(*linuxUDPBatchConn)
			type observation struct {
				permits        int
				scalarRetained bool
			}
			polling := make(chan observation, 1)
			connection.raw = &observedUDPRead{RawConn: connection.raw, after: func(done bool) {
				if !done {
					select {
					case polling <- observation{len(linuxReadBatches.permits),
						connection.scalar.storage != nil || connection.scalar.vector.Base != nil}:
					default:
					}
				}
			}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				var packets [MaximumBatchSize]Packet
				_, err := local.ReadBatch(ctx, packets[:])
				finished <- err
			}()
			select {
			case observed := <-polling:
				if observed.permits != len(held) || observed.scalarRetained {
					t.Fatalf("idle read retained staging: %+v with %d reservations held by test", observed, len(held))
				}
			case <-time.After(time.Second):
				t.Fatal("receive did not enter the socket poller")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled poll returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("context cancellation did not interrupt the socket poller")
			}
		})
	}
}

func TestLinuxUDPBatchReadRejectsTruncationAndUnknownFamily(t *testing.T) {
	previous := linuxReadBatches
	linuxReadBatches = newLinuxReadBatchPool()
	t.Cleanup(func() { linuxReadBatches = previous })
	injected := newLinuxReadBatch()
	injected.receive = func(descriptor uintptr) {
		injected.receiveFrom(descriptor)
		if injected.count == 3 {
			injected.headers[0].header.Flags |= unix.MSG_TRUNC
			injected.addresses[1].Addr.Family = unix.AF_UNSPEC
		}
	}
	linuxReadBatches.idle <- injected
	sender, receiver := listenUDP(t), listenUDP(t)
	peer := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
	for _, payload := range []string{"truncated", "unknown", "accepted"} {
		writeUDP(t, sender, peer, []byte(payload))
	}
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	batch, err := newUDPBatchConn(receiver).read(MaximumBatchSize)
	if err != nil || batch == nil {
		t.Fatalf("read() with rejected prefix = %v, %v", batch, err)
	}
	defer batch.release()
	if messages := batch.messages(); len(messages) != 1 || string(messages[0].payload) != "accepted" {
		t.Fatalf("received %v, want only the accepted suffix", messages)
	}
}

func TestLocalReadBatchPreservesPeerAndPacketOwnership(t *testing.T) {
	previous := linuxReadBatches
	linuxReadBatches = newLinuxReadBatchPool()
	t.Cleanup(func() { linuxReadBatches = previous })
	local := NewLocal(listenUDP(t))
	t.Cleanup(func() { local.Close() })
	first, second := listenUDP(t), listenUDP(t)
	firstPayload, secondPayload := wireGuardPacket(4, 32), wireGuardPacket(4, 32)
	firstPayload[4], secondPayload[4] = 1, 2
	writeUDP(t, second, local.LocalAddr(), []byte{9})
	writeUDP(t, first, local.LocalAddr(), firstPayload)
	writeUDP(t, second, local.LocalAddr(), secondPayload)
	writeUDP(t, first, local.LocalAddr(), []byte{9})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var packets [MaximumBatchSize]Packet
	count, err := local.ReadBatch(ctx, packets[:])
	if err != nil || count != 2 {
		t.Fatalf("mixed ReadBatch() = %d packets, %v, want 2, nil", count, err)
	}
	defer func() {
		for index := range count {
			packets[index].Release()
		}
	}()
	if err := local.Write(ctx, firstPayload, time.Time{}); err != nil {
		t.Fatal(err)
	}
	readUDP(t, second, len(firstPayload))
	for range 3 {
		writeUDP(t, first, local.LocalAddr(), wireGuardPacket(4, 32))
	}
	var next [MaximumBatchSize]Packet
	nextCount, err := local.ReadBatch(ctx, next[:])
	if err != nil || nextCount != 3 {
		t.Fatalf("next ReadBatch() = %d packets, %v, want 3, nil", nextCount, err)
	}
	defer func() {
		for index := range nextCount {
			next[index].Release()
		}
	}()
	if !bytes.Equal(packets[0].Payload, firstPayload) || !bytes.Equal(packets[1].Payload, secondPayload) {
		t.Fatal("a subsequent receive changed already owned datagrams")
	}
}
