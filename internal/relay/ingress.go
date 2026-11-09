package relay

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
)

// Ingress reads accepted WireGuard datagrams into a bounded session queue.
type Ingress struct {
	admissionMu sync.Mutex
	pauseOwner  *measurementDrain
	resumed     chan struct{}
	readCancel  context.CancelFunc
	reading     bool
	endpoint    datagram.Endpoint
	queue       *packetqueue.Queue[Packet]
	clock       Clock
	deadlines   DeadlinePolicy
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
	readCtx, cancelRead := context.WithCancel(ctx)
	defer func() {
		cancelRead()
		i.admissionMu.Lock()
		i.readCancel = nil
		i.admissionMu.Unlock()
	}()
	for {
		if err := i.lockAdmission(ctx); err != nil {
			return err
		}
		if readCtx.Err() != nil {
			readCtx, cancelRead = context.WithCancel(ctx)
		}
		i.readCancel = cancelRead
		i.reading = true
		i.admissionMu.Unlock()
		count, readErr := datagram.ReadBatch(readCtx, i.endpoint, packets[:])
		if err := i.admitBatch(ctx, packets[:count], items[:count]); err != nil {
			return err
		}
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if readCtx.Err() != nil && errors.Is(readErr, context.Canceled) {
				continue
			}
			return fmt.Errorf("%w: read relay ingress: %w", ErrEndpointFailure, readErr)
		}
	}
}

// admitBatch includes a completed pre-cut read before publishing that the reader is quiet.
func (i *Ingress) admitBatch(ctx context.Context, packets []datagram.Packet, items []packetqueue.Item[Packet]) error {
	nowMicros := i.clock.NowMicros()
	i.admissionMu.Lock()
	defer func() {
		i.reading = false
		owner := i.pauseOwner
		i.admissionMu.Unlock()
		if owner != nil && owner.changed != nil {
			owner.changed()
		}
	}()
	if err := ctx.Err(); err != nil {
		releaseDatagrams(packets)
		return err
	}
	for index := range packets {
		packet := newPacket(packets[index])
		packets[index] = datagram.Packet{}
		packet.queuedMicros = nowMicros
		lifetimeMicros := durationMicros(i.deadlines.Lifetime(packet.Kind))
		if nowMicros > math.MaxUint64-math.MaxUint64%protocol.DeadlineResolutionMicros-lifetimeMicros {
			packet.Release()
			releaseItems(items[:index])
			releaseDatagrams(packets[index+1:])
			return fmt.Errorf("compute relay ingress deadline: %w", ErrCounterExhausted)
		}
		items[index] = packetqueue.Item[Packet]{Value: packet, Size: len(packet.Payload),
			Priority: packetPriority(packet.Kind.Control()), Deadline: monotime.Time(nowMicros + lifetimeMicros)}
	}
	for offset := 0; offset < len(items); {
		written, err := i.queue.PushBatch(items[offset:])
		clear(items[offset : offset+written])
		offset += written
		if errors.Is(err, packetqueue.ErrFull) || errors.Is(err, packetqueue.ErrExpired) {
			items[offset].Release()
			offset++
			continue
		}
		if err != nil {
			releaseItems(items[offset:])
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("enqueue relay ingress: %w", err)
		}
	}
	return nil
}

// lockAdmission returns with admissionMu held, or returns the caller's cancellation while admission is paused.
func (i *Ingress) lockAdmission(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		i.admissionMu.Lock()
		if i.pauseOwner == nil {
			return nil
		}
		resumed := i.resumed
		i.admissionMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resumed:
		}
	}
}

// pauseForMeasurement establishes a cut after every batch already accepted into the relay.
func (i *Ingress) pauseForMeasurement(owner *measurementDrain) bool {
	i.admissionMu.Lock()
	defer i.admissionMu.Unlock()
	if i.pauseOwner != nil && i.pauseOwner != owner {
		return false
	}
	if i.pauseOwner == nil {
		i.pauseOwner = owner
		i.resumed = make(chan struct{})
		if i.readCancel != nil {
			i.readCancel()
		}
	}
	return true
}

// measurementReadComplete requires an interrupted read's entire returned prefix to join the accepted cut.
func (i *Ingress) measurementReadComplete(owner *measurementDrain) bool {
	i.admissionMu.Lock()
	defer i.admissionMu.Unlock()
	return i.pauseOwner == owner && !i.reading
}

// resumeAfterMeasurement releases only the matching owner's admission cut.
func (i *Ingress) resumeAfterMeasurement(owner *measurementDrain) {
	i.admissionMu.Lock()
	defer i.admissionMu.Unlock()
	if i.pauseOwner == owner {
		i.pauseOwner = nil
		close(i.resumed)
		i.resumed = nil
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
