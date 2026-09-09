//go:build linux

package datagram

import (
	"bytes"
	"net"
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
	batch.receive = func(descriptor uintptr) bool {
		calls++
		if calls == 1 {
			batch.count = 0
			batch.err = syscall.EINTR
			return true
		}
		return batch.receiveFrom(descriptor)
	}
	linuxReadBatches.idle <- batch
	result, err := newUDPBatchConn(receiver).readAvailable(1)
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
