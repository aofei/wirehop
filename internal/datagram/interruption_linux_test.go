//go:build linux

package datagram

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestLinuxUDPBatchWriteInterrupted(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		name := "BeforeProgress"
		if prefix {
			name = "AfterPartialWrite"
		}
		t.Run(name, func(t *testing.T) {
			sender, receiver := listenUDP(t), listenUDP(t)
			connection := newUDPBatchConn(sender).(*linuxUDPBatchConn)
			connection.gso = false
			calls := 0
			previous := linuxWriteBatches
			linuxWriteBatches = &sync.Pool{New: func() any {
				batch := newLinuxWriteBatch()
				batch.send = func(descriptor uintptr) bool {
					calls++
					if prefix && calls == 1 {
						batch.limit = 1
						return batch.sendTo(descriptor)
					}
					if !prefix && calls == 1 || prefix && calls == 2 {
						batch.count = 0
						batch.err = syscall.EINTR
						return true
					}
					return batch.sendTo(descriptor)
				}
				return batch
			}}
			t.Cleanup(func() { linuxWriteBatches = previous })
			messages := []udpMessage{
				{payload: []byte("first"), peer: receiver.LocalAddr().(*net.UDPAddr).AddrPort()},
				{payload: []byte("second"), peer: receiver.LocalAddr().(*net.UDPAddr).AddrPort()},
			}
			if err := sender.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if count, err := connection.write(messages); count != len(messages) || err != nil {
				t.Fatalf("interrupted write = %d, %v, want %d, nil", count, err, len(messages))
			}
			for _, message := range messages {
				buffer := make([]byte, 32)
				receiver.SetReadDeadline(time.Now().Add(time.Second))
				count, _, err := receiver.ReadFromUDPAddrPort(buffer)
				if err != nil || !bytes.Equal(buffer[:count], message.payload) {
					t.Fatalf("received %q, %v, want %q", buffer[:count], err, message.payload)
				}
			}
			assertNoUDP(t, receiver)
		})
	}
}

func TestLinuxUDPBatchReadInterrupted(t *testing.T) {
	sender, receiver := listenUDP(t), listenUDP(t)
	writeUDP(t, sender, receiver.LocalAddr().(*net.UDPAddr).AddrPort(), []byte("pending"))
	previous := linuxReadBatches
	linuxReadBatches = newLinuxReadBatchPool()
	t.Cleanup(func() { linuxReadBatches = previous })
	batch := newLinuxReadBatch()
	calls := 0
	batch.receive = func(descriptor uintptr) {
		calls++
		if calls == 1 {
			batch.count = 0
			batch.err = syscall.EINTR
			return
		}
		batch.receiveFrom(descriptor)
	}
	linuxReadBatches.idle <- batch
	result, err := newUDPBatchConn(receiver).read(2)
	if result != nil {
		defer result.release()
	}
	if err != nil || result == nil {
		t.Fatalf("interrupted read = %v, %v", result, err)
	}
	if messages := result.messages(); len(messages) != 1 || string(messages[0].payload) != "pending" {
		t.Fatalf("received %v, want pending datagram", messages)
	}
}

func TestLinuxUDPBatchReadInterruptedRechecksCancellation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		limit    int
		deadline bool
	}{
		{name: "VectorContext", limit: MaximumBatchSize},
		{name: "ScalarContext", limit: 1},
		{name: "VectorDeadline", limit: MaximumBatchSize, deadline: true},
		{name: "ScalarDeadline", limit: 1, deadline: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			previous := linuxReadBatches
			linuxReadBatches = newLinuxReadBatchPool()
			t.Cleanup(func() { linuxReadBatches = previous })
			local := NewLocal(listenUDP(t))
			t.Cleanup(func() { local.Close() })
			connection := local.batch.(*linuxUDPBatchConn)
			observed := &observedUDPRead{RawConn: connection.raw}
			connection.raw = observed
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			calls := 0
			interrupt := func() {
				calls++
				if !tt.deadline {
					cancel()
				}
				if err := local.conn.SetReadDeadline(time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			if tt.limit == 1 {
				connection.scalar.receive = func(descriptor uintptr) {
					if calls != 0 {
						calls++
						connection.scalar.receiveFrom(descriptor)
						return
					}
					connection.scalar.err = syscall.EINTR
					interrupt()
				}
			} else {
				batch := newLinuxReadBatch()
				batch.receive = func(descriptor uintptr) {
					if calls != 0 {
						calls++
						batch.receiveFrom(descriptor)
						return
					}
					batch.err = syscall.EINTR
					interrupt()
				}
				linuxReadBatches.idle <- batch
			}
			var err error
			if tt.deadline {
				if err := local.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				_, err = connection.read(tt.limit)
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("interrupted read returned %v, want expired deadline", err)
				}
			} else {
				var packets [MaximumBatchSize]Packet
				_, err = local.ReadBatch(ctx, packets[:tt.limit])
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("interrupted read returned %v, want context cancellation", err)
				}
			}
			if calls != 1 || observed.operations != 2 {
				t.Fatalf("interruption used %d receive attempts and %d poll operations, want 1 and 2",
					calls, observed.operations)
			}
			if len(linuxReadBatches.permits) != 0 || connection.scalar.storage != nil || connection.scalar.vector.Base != nil {
				t.Fatal("interrupted read retained vector or scalar staging")
			}
		})
	}
}
