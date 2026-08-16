package relay

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
)

// Ingress reads accepted WireGuard datagrams into a bounded session queue.
type Ingress struct {
	endpoint  datagram.Endpoint
	queue     *packetqueue.Queue[Packet]
	clock     Clock
	deadlines DeadlinePolicy
	now       func() time.Time
}

// NewIngress returns a UDP ingress using the process wall clock for local queue deadlines.
func NewIngress(endpoint datagram.Endpoint, queue *packetqueue.Queue[Packet], clock Clock,
	deadlines DeadlinePolicy) (*Ingress, error) {
	return newIngress(endpoint, queue, clock, deadlines, time.Now)
}

// newIngress returns a UDP ingress with an injectable local deadline clock.
func newIngress(endpoint datagram.Endpoint, queue *packetqueue.Queue[Packet], clock Clock,
	deadlines DeadlinePolicy, now func() time.Time) (*Ingress, error) {
	if endpoint == nil || queue == nil || clock == nil || now == nil {
		return nil, ErrInvalidPacket
	}
	if err := deadlines.Validate(); err != nil {
		return nil, err
	}
	return &Ingress{endpoint: endpoint, queue: queue, clock: clock, deadlines: deadlines, now: now}, nil
}

// Run reads UDP packets until the context, endpoint, or queue is closed.
func (i *Ingress) Run(ctx context.Context) error {
	for {
		packet, err := i.endpoint.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: read relay ingress: %w", ErrEndpointFailure, err)
		}
		lifetime := i.deadlines.Lifetime(packet.Kind)
		nowMicros := i.clock.NowMicros()
		lifetimeMicros := durationMicros(lifetime)
		if lifetimeMicros > math.MaxUint64-nowMicros {
			return fmt.Errorf("compute relay ingress deadline: %w", ErrCounterExhausted)
		}
		item := packetqueue.Item[Packet]{
			Value: Packet{
				Kind:           packet.Kind,
				Payload:        packet.Payload,
				DeadlineMicros: nowMicros + lifetimeMicros,
			},
			Size:     len(packet.Payload),
			Priority: packetPriority(packet.Kind.Control()),
			// Strip the monotonic reading so local expiry includes time spent suspended.
			Deadline: i.now().Add(lifetime).Round(0),
		}
		err = i.queue.Push(item)
		if errors.Is(err, packetqueue.ErrFull) || errors.Is(err, packetqueue.ErrExpired) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("enqueue relay ingress: %w", err)
		}
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
