package relay

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
)

// Ingress reads accepted WireGuard datagrams into a bounded session queue.
type Ingress struct {
	endpoint  datagram.Endpoint
	queue     *packetqueue.Queue[Packet]
	clock     Clock
	deadlines DeadlinePolicy
}

// NewIngress returns a UDP ingress using clock for both local and wire deadlines. The queue must use the same clock
// through [monotime.Time].
func NewIngress(endpoint datagram.Endpoint, queue *packetqueue.Queue[Packet], clock Clock,
	deadlines DeadlinePolicy) (*Ingress, error) {
	if endpoint == nil || queue == nil || clock == nil {
		return nil, ErrInvalidPacket
	}
	if err := deadlines.Validate(); err != nil {
		return nil, err
	}
	return &Ingress{endpoint: endpoint, queue: queue, clock: clock, deadlines: deadlines}, nil
}

// Run reads UDP packets until the context, endpoint, or queue is closed.
func (i *Ingress) Run(ctx context.Context) error {
	var packets [datagram.MaximumBatchSize]datagram.Packet
	var items [datagram.MaximumBatchSize]packetqueue.Item[Packet]
	for {
		count, readErr := datagram.ReadBatch(ctx, i.endpoint, packets[:])
		if count == 0 && readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: read relay ingress: %w", ErrEndpointFailure, readErr)
		}
		nowMicros := i.clock.NowMicros()
		for index := range count {
			packet := packets[index]
			packets[index] = datagram.Packet{}
			lifetime := i.deadlines.Lifetime(packet.Kind)
			lifetimeMicros := durationMicros(lifetime)
			if lifetimeMicros > math.MaxUint64-nowMicros {
				packet.Release()
				releaseItems(items[:index])
				releaseDatagrams(packets[index+1 : count])
				return fmt.Errorf("compute relay ingress deadline: %w", ErrCounterExhausted)
			}
			items[index] = packetqueue.Item[Packet]{
				Value:    newPacket(packet, nowMicros+lifetimeMicros),
				Size:     len(packet.Payload),
				Priority: packetPriority(packet.Kind.Control()),
				Deadline: monotime.Time(nowMicros + lifetimeMicros),
			}
		}
		for offset := 0; offset < count; {
			written, err := i.queue.PushBatch(items[offset:count])
			clear(items[offset : offset+written])
			offset += written
			if errors.Is(err, packetqueue.ErrFull) || errors.Is(err, packetqueue.ErrExpired) {
				items[offset].Release()
				offset++
				continue
			}
			if err != nil {
				releaseItems(items[offset:count])
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("enqueue relay ingress: %w", err)
			}
		}
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: read relay ingress: %w", ErrEndpointFailure, readErr)
		}
	}
}

// releaseDatagrams relinquishes every packet in packets.
func releaseDatagrams(packets []datagram.Packet) {
	for index := range packets {
		packets[index].Release()
	}
}

// releaseItems relinquishes every queue item still owned by the caller.
func releaseItems(items []packetqueue.Item[Packet]) {
	for index := range items {
		items[index].Release()
	}
}

// durationMicros returns duration rounded up to whole protocol microseconds.
func durationMicros(duration time.Duration) uint64 {
	return uint64((duration + time.Microsecond - 1) / time.Microsecond)
}

// packetPriority maps a WireGuard class to the queue priority contract.
func packetPriority(control bool) packetqueue.Priority {
	if control {
		return packetqueue.PriorityControl
	}
	return packetqueue.PriorityNormal
}
