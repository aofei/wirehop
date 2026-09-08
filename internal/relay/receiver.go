package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/dedup"
	"github.com/aofei/wirehop/internal/protocol"
)

var (
	// ErrInvalidReceiver indicates missing endpoint, clock, or deduplication configuration.
	ErrInvalidReceiver = errors.New("invalid relay receiver")
	// ErrInvalidPacketDeadline indicates a deadline that cannot describe bounded in-flight work.
	ErrInvalidPacketDeadline = errors.New("invalid packet deadline")
	// ErrEndpointFailure indicates that a relay UDP endpoint could not be created, read, or written.
	ErrEndpointFailure = errors.New("relay UDP endpoint failure")
)

// ReceiverConfig defines session-shared inbound delivery state.
type ReceiverConfig struct {
	Endpoint          datagram.Endpoint
	Clock             Clock
	ClockMapping      clockmap.Mapping
	DeduplicationSize int
	UDPWriteTimeout   time.Duration
}

// Receiver deduplicates and delivers one session direction across all lanes.
type Receiver struct {
	endpoint        datagram.Endpoint
	clock           Clock
	udpWriteTimeout time.Duration
	writeSlot       chan struct{}
	mu              sync.Mutex
	mapping         clockmap.Mapping
	deduplication   *dedup.Window
	payloads        [datagram.MaximumBatchSize][]byte
	packetIDs       [datagram.MaximumBatchSize]uint64
}

// NewReceiver validates config and returns session-shared inbound state.
func NewReceiver(config ReceiverConfig) (*Receiver, error) {
	if config.Endpoint == nil || config.Clock == nil || config.DeduplicationSize <= 0 {
		return nil, ErrInvalidReceiver
	}
	if config.UDPWriteTimeout == 0 {
		config.UDPWriteTimeout = defaultUDPWriteTimeout
	}
	if config.UDPWriteTimeout <= 0 {
		return nil, ErrInvalidReceiver
	}
	window, err := dedup.NewWindow(config.DeduplicationSize)
	if err != nil {
		return nil, err
	}
	return &Receiver{
		endpoint: config.Endpoint, clock: config.Clock, udpWriteTimeout: config.UDPWriteTimeout,
		writeSlot: make(chan struct{}, 1), mapping: config.ClockMapping,
		deduplication: window,
	}, nil
}

// UpdateClock replaces the session mapping with one authenticated lane sample.
func (r *Receiver) UpdateClock(mapping clockmap.Mapping) {
	r.mu.Lock()
	r.mapping = mapping
	r.mu.Unlock()
}

// ValidateDeadline verifies that deadline maps into the receiver clock and respects the protocol lifetime bound.
func (r *Receiver) ValidateDeadline(deadlineMicros uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := deadlineStatus(r.mapping, r.clock.NowMicros(), deadlineMicros)
	return err
}

// ValidateDeadlines verifies one sender deadline vector against a shared receiver clock sample.
func (r *Receiver) ValidateDeadlines(deadlines []uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	nowMicros := r.clock.NowMicros()
	for _, deadlineMicros := range deadlines {
		if _, err := deadlineStatus(r.mapping, nowMicros, deadlineMicros); err != nil {
			return err
		}
	}
	return nil
}

// Deliver deduplicates, deadline-checks, and writes one validated packet to UDP.
func (r *Receiver) Deliver(ctx context.Context, data protocol.Data) error {
	if err := r.ValidateDeadline(data.DeadlineMicros); err != nil {
		return err
	}
	return r.deliver(ctx, data)
}

// deliver deduplicates and writes data whose deadline was already validated before parse acknowledgement.
func (r *Receiver) deliver(ctx context.Context, data protocol.Data) error {
	var batch [1]protocol.Data
	batch[0] = data
	return r.deliverBatch(ctx, batch[:])
}

// deliverBatch deduplicates and writes data whose deadlines were validated before parse acknowledgement.
func (r *Receiver) deliverBatch(ctx context.Context, data []protocol.Data) error {
	err := r.writeBatch(ctx, data)
	if errors.Is(err, datagram.ErrNoLocalPeer) || errors.Is(err, datagram.ErrDatagramDropped) {
		return nil
	}
	return err
}

// writeBatch serializes and opportunistically batches one lane's UDP delivery vector.
func (r *Receiver) writeBatch(ctx context.Context, data []protocol.Data) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) > datagram.MaximumBatchSize {
		return ErrInvalidReceiver
	}
	select {
	case r.writeSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.writeSlot }()
	r.mu.Lock()
	mapping := r.mapping
	nowMicros := r.clock.NowMicros()
	r.mu.Unlock()
	writeDeadline := time.Now().Add(r.udpWriteTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(writeDeadline) {
		writeDeadline = parentDeadline
	}
	for dataOffset := 0; dataOffset < len(data); {
		accepted := 0
		r.mu.Lock()
		for dataOffset < len(data) {
			packet := data[dataOffset]
			expired, err := deadlineStatus(mapping, nowMicros, packet.DeadlineMicros)
			if err != nil {
				r.mu.Unlock()
				return err
			}
			if r.deduplication.Classify(packet.PacketID) != dedup.New || expired {
				dataOffset++
				continue
			}
			if accepted > 0 && packet.PacketID <= r.packetIDs[accepted-1] {
				break
			}
			r.payloads[accepted] = packet.Payload
			r.packetIDs[accepted] = packet.PacketID
			accepted++
			dataOffset++
		}
		r.mu.Unlock()
		if accepted == 0 {
			continue
		}
		err := r.writeAcceptedBatch(ctx, r.payloads[:accepted], r.packetIDs[:accepted], writeDeadline)
		clear(r.payloads[:accepted])
		clear(r.packetIDs[:accepted])
		if err != nil {
			return err
		}
	}
	return nil
}

// writeAcceptedBatch writes one increasing PacketID run and records only its successful packets.
func (r *Receiver) writeAcceptedBatch(ctx context.Context, payloads [][]byte, packetIDs []uint64,
	writeDeadline time.Time) error {
	offset := 0
	for offset < len(payloads) {
		written, err := datagram.WriteBatch(ctx, r.endpoint, payloads[offset:], writeDeadline)
		if written > 0 {
			r.mu.Lock()
			for _, packetID := range packetIDs[offset : offset+written] {
				r.deduplication.Observe(packetID)
			}
			r.mu.Unlock()
			offset += written
		}
		if err == nil {
			if written == 0 {
				return fmt.Errorf("%w: %w", ErrEndpointFailure, io.ErrNoProgress)
			}
			continue
		}
		if errors.Is(err, datagram.ErrNoLocalPeer) {
			return err
		}
		if errors.Is(err, datagram.ErrDatagramDropped) {
			offset++
			continue
		}
		return fmt.Errorf("%w: %w", ErrEndpointFailure, err)
	}
	return nil
}

// deadlineStatus validates one sender deadline and reports conservative receiver-side expiry.
func deadlineStatus(mapping clockmap.Mapping, receiverNowMicros, senderDeadlineMicros uint64) (bool, error) {
	earliestDeadline, latestDeadline, err := mapping.DeadlineBounds(senderDeadlineMicros)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrInvalidPacketDeadline, err)
	}
	if earliestDeadline > receiverNowMicros &&
		earliestDeadline-receiverNowMicros > protocol.MaxPacketLifetimeMicros {
		return false, ErrInvalidPacketDeadline
	}
	return receiverNowMicros >= latestDeadline, nil
}
