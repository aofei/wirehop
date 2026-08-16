package relay

import (
	"context"
	"errors"
	"fmt"
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

// Deliver deduplicates, deadline-checks, and writes one validated packet to UDP.
func (r *Receiver) Deliver(ctx context.Context, data protocol.Data) error {
	if err := r.ValidateDeadline(data.DeadlineMicros); err != nil {
		return err
	}
	return r.deliver(ctx, data)
}

// deliver deduplicates and writes data whose deadline was already validated before parse acknowledgement.
func (r *Receiver) deliver(ctx context.Context, data protocol.Data) error {
	err := r.write(ctx, data)
	if errors.Is(err, datagram.ErrNoLocalPeer) || errors.Is(err, datagram.ErrDatagramDropped) {
		return nil
	}
	return err
}

// write serializes UDP delivery so one operation cannot overwrite another operation's socket deadline.
func (r *Receiver) write(ctx context.Context, data protocol.Data) error {
	select {
	case r.writeSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.writeSlot }()
	r.mu.Lock()
	duplicate := r.deduplication.Classify(data.PacketID) != dedup.New
	expired, err := deadlineStatus(r.mapping, r.clock.NowMicros(), data.DeadlineMicros)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if duplicate || expired {
		return nil
	}
	writeDeadline := time.Now().Add(r.udpWriteTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(writeDeadline) {
		writeDeadline = parentDeadline
	}
	if err := r.endpoint.Write(ctx, data.Payload, writeDeadline); err != nil {
		if errors.Is(err, datagram.ErrDatagramDropped) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrEndpointFailure, err)
	}
	r.mu.Lock()
	r.deduplication.Observe(data.PacketID)
	r.mu.Unlock()
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
