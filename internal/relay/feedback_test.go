package relay

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

// linkedCarrier is one endpoint of a deterministic delayed full-duplex carrier pair.
type linkedCarrier struct {
	peer     *linkedCarrier
	incoming chan protocol.Frame
	delay    time.Duration
	done     chan struct{}
	once     sync.Once
}

// newLinkedCarrierPair returns two connected carrier endpoints.
func newLinkedCarrierPair(delay time.Duration) (*linkedCarrier, *linkedCarrier) {
	first := &linkedCarrier{incoming: make(chan protocol.Frame, 4096), delay: delay, done: make(chan struct{})}
	second := &linkedCarrier{incoming: make(chan protocol.Frame, 4096), delay: delay, done: make(chan struct{})}
	first.peer = second
	second.peer = first
	return first, second
}

// ReadFrame reads one frame written by the peer.
func (c *linkedCarrier) ReadFrame(ctx context.Context) (protocol.Frame, error) {
	select {
	case frame := <-c.incoming:
		return frame, nil
	case <-c.done:
		return protocol.Frame{}, net.ErrClosed
	case <-ctx.Done():
		return protocol.Frame{}, ctx.Err()
	}
}

// WriteFrames writes one copied frame batch to the peer after the configured delay.
func (c *linkedCarrier) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	if c.delay > 0 {
		timer := time.NewTimer(c.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-c.done:
			return net.ErrClosed
		case <-c.peer.done:
			return net.ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, frame := range frames {
		copied := protocol.Frame{Type: frame.Type, Payload: append([]byte(nil), frame.Payload...)}
		select {
		case c.peer.incoming <- copied:
		case <-c.done:
			return net.ErrClosed
		case <-c.peer.done:
			return net.ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// WriteDataBatch encodes and writes one data batch to the peer.
func (c *linkedCarrier) WriteDataBatch(ctx context.Context, data []protocol.Data) error {
	frames := make([]protocol.Frame, 0, len(data))
	for _, packet := range data {
		frame, err := protocol.MarshalData(packet)
		if err != nil {
			return err
		}
		frames = append(frames, frame)
	}
	return c.WriteFrames(ctx, frames)
}

// Close closes this carrier endpoint.
func (c *linkedCarrier) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

// Abort closes this carrier endpoint without draining queued frames.
func (c *linkedCarrier) Abort() error {
	return c.Close()
}

func TestLaneDeliveryThresholdReleasesHighBandwidthWindow(t *testing.T) {
	const packetCount = 1024
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstCarrier, secondCarrier := newLinkedCarrierPair(time.Millisecond)
	firstEndpoint := newTestEndpoint()
	secondEndpoint := newTestEndpoint()
	secondEndpoint.writes = make(chan []byte, packetCount)
	firstIngress, firstScheduler := feedbackScheduler(t)
	_, secondScheduler := feedbackScheduler(t)
	firstStore := feedbackStore(t)
	secondStore := feedbackStore(t)
	firstLane := feedbackLane(t, firstCarrier, firstEndpoint, firstStore, firstScheduler)
	secondLane := feedbackLane(t, secondCarrier, secondEndpoint, secondStore, secondScheduler)

	results := make(chan error, 4)
	go func() { results <- firstScheduler.Run(ctx) }()
	go func() { results <- secondScheduler.Run(ctx) }()
	registerFeedbackLane(t, ctx, firstScheduler, firstLane, firstStore)
	registerFeedbackLane(t, ctx, secondScheduler, secondLane, secondStore)
	go func() { results <- firstLane.Run(ctx) }()
	go func() { results <- secondLane.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for index := range packetCount {
		payload := relayWireGuardPacket(wgpacket.TransportData)
		payload[4] = byte(index)
		err := firstIngress.Push(packetqueue.Item[Packet]{
			Value: Packet{
				Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 1_000_000,
			},
			Size: len(payload), Deadline: deadline,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for range packetCount {
		select {
		case <-secondEndpoint.writes:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for relayed packets")
		}
	}
	for firstStore.backlogByteCount() != 0 {
		select {
		case err := <-results:
			t.Fatalf("relay worker ended before feedback drained the store: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("transmission backlog = %d bytes after all packets arrived", firstStore.backlogByteCount())
		}
	}

	cancel()
	for range 4 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("relay worker error = %v, want %v", err, context.Canceled)
		}
	}
}

// feedbackScheduler returns a scheduler with an ingress queue sized above the delivery-report packet threshold.
func feedbackScheduler(t *testing.T) (*packetqueue.Queue[Packet], *Scheduler) {
	t.Helper()
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 2048, Bytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	return ingress, scheduler
}

// feedbackStore returns one transmission store whose packet capacity matches the delivery-report threshold.
func feedbackStore(t *testing.T) *TransmissionStore {
	t.Helper()
	store, err := NewTransmissionStore(packetqueue.Limits{
		Packets: reportPacketThreshold,
		Bytes:   8 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// feedbackLane returns one lane whose reports can be emitted only by a progress threshold during the test.
func feedbackLane(t *testing.T, connection *linkedCarrier, endpoint *testEndpoint, store *TransmissionStore,
	scheduler *Scheduler) *Lane {
	t.Helper()
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := NewLane(LaneConfig{
		Carrier: connection, Receiver: receiver, Store: store, Clock: &testClock{now: 1000},
		Observer: scheduler, LaneID: protocol.LaneID{1}, Generation: 1,
		ReportInterval: time.Hour, PingInterval: time.Hour, PingTimeout: 2 * time.Hour,
		ProbeInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return lane
}

// registerFeedbackLane makes one test lane eligible before carrier workers start.
func registerFeedbackLane(t *testing.T, ctx context.Context, scheduler *Scheduler, lane *Lane,
	store *TransmissionStore) {
	t.Helper()
	err := scheduler.Register(ctx, LaneRegistration{
		LaneID: protocol.LaneID{1}, Generation: 1, PathGroupID: protocol.PathGroupID{1}, Store: store,
		Abandon: func() {}, SendControl: lane.SendControl, ValidateProbeProgress: lane.ValidateProbeProgress,
	})
	if err != nil {
		t.Fatal(err)
	}
}
