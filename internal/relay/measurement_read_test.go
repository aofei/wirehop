package relay

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestIngressMeasurementReadCut(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prefix  bool
		err     error
		replace bool
		cancel  bool
	}{
		{name: "IdleRead", err: context.Canceled},
		{name: "CompletedRead", prefix: true},
		{name: "CanceledReadWithPrefix", prefix: true, err: context.Canceled},
		{name: "FailedReadDuringPause", err: io.ErrUnexpectedEOF},
		{name: "FailedPrefixDuringPause", prefix: true, err: io.ErrUnexpectedEOF},
		{name: "ReplacementBeforeReadCompletes", prefix: true, err: context.Canceled, replace: true},
		{name: "CanceledDuringPause", prefix: true, err: context.Canceled, cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clock := &testClock{now: 1000}
				queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192},
					func() time.Time { return monotime.Time(clock.now) })
				if err != nil {
					t.Fatal(err)
				}
				defer queue.Close()
				endpoint := &measurementReadEndpoint{
					testEndpoint: newTestEndpoint(), started: make(chan context.Context, 8),
					complete: make(chan struct{}), prefix: tt.prefix, err: tt.err,
				}
				defer func() {
					select {
					case <-endpoint.complete:
					default:
						close(endpoint.complete)
					}
				}()
				input, err := NewIngress(endpoint, queue, clock, DeadlinePolicy{Control: 5 * time.Second, Transport: 5 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- input.Run(ctx) }()
				reading := <-endpoint.started
				owner, stale := &measurementDrain{}, &measurementDrain{}
				if !input.pauseForMeasurement(owner) || input.pauseForMeasurement(stale) {
					t.Fatal("the ingress cut accepted overlapping measurement owners")
				}
				if reading.Err() != context.Canceled || ctx.Err() != nil {
					t.Fatal("the measurement pause did not interrupt only the pending read")
				}
				if tt.replace {
					input.resumeAfterMeasurement(owner)
					stale, owner = owner, &measurementDrain{}
					if !input.pauseForMeasurement(owner) {
						t.Fatal("the next session could not acquire admission while the old read finished")
					}
				}
				close(endpoint.complete)
				synctest.Wait()
				if tt.prefix {
					var item packetqueue.Item[Packet]
					if err := queue.TryPop(&item, queue.Now()); err != nil {
						t.Fatalf("completed pre-pause data was not included in the cut: %v", err)
					}
					if item.Value.queuedMicros != 1000 || item.Deadline != monotime.Time(1000).Add(5*time.Second) {
						t.Fatal("the measurement cut changed a completed read's lifetime")
					}
					item.Release()
				}
				if queue.Len() != 0 {
					t.Fatal("unexpected data remained in the accepted cut")
				}
				if tt.err != nil && !errors.Is(tt.err, context.Canceled) {
					if err := <-done; !errors.Is(err, tt.err) || !errors.Is(err, ErrEndpointFailure) {
						t.Fatalf("Run() = %v, want the endpoint failure", err)
					}
					if input.readCancel != nil || !input.measurementReadComplete(owner) {
						t.Fatal("the failed reader retained its cancellation hook or unfinished cut")
					}
					input.resumeAfterMeasurement(owner)
					return
				}
				clock.now += 6_000_000
				input.resumeAfterMeasurement(stale)
				synctest.Wait()
				select {
				case <-endpoint.started:
					t.Fatal("another read started during the measurement pause")
				default:
				}
				if tt.cancel {
					cancel()
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Fatalf("Run() = %v", err)
					}
					input.resumeAfterMeasurement(owner)
					return
				}
				input.resumeAfterMeasurement(owner)
				resumed := <-endpoint.started
				if resumed == reading || resumed.Err() != nil {
					t.Fatal("resumed ingress reused its interrupted read context")
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() = %v, want cancellation", err)
				}
				if queue.Len() != 0 {
					t.Fatal("parent cancellation admitted a returned packet")
				}
			})
		})
	}
}

func TestSchedulerMeasurementWaitsForReadCut(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix bool
	}{
		{name: "IdleRead"},
		{name: "CompletedPrefix", prefix: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, scheduler := feedbackScheduler(t)
				defer queue.Close()
				endpoint := &measurementReadEndpoint{
					testEndpoint: newTestEndpoint(), started: make(chan context.Context, 8),
					complete: make(chan struct{}), prefix: tt.prefix, err: context.Canceled,
				}
				defer func() {
					select {
					case <-endpoint.complete:
					default:
						close(endpoint.complete)
					}
				}()
				input, err := NewIngress(endpoint, queue, &testClock{now: 1000},
					DeadlinePolicy{Control: 5 * time.Second, Transport: 5 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				scheduler.input = input
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- scheduler.input.Run(ctx) }()
				<-endpoint.started
				lane := schedulerLane(t, 1, 1, 10_000, 1_000_000)
				defer func() { releaseTransmissions(lane.registration.Store.drain()) }()
				lanes := map[protocol.LaneID]*scheduledLane{1: lane}
				preferred := protocol.LaneID(1)
				ready, _ := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, false, nil)
				scheduler.applyEvent(lanes, &preferred, <-scheduler.events, queue.Now(), false)
				if scheduler.measurementQuiet || lane.registration.Store.measurementDrain.Load() {
					t.Fatal("the data gate closed before the interrupted read returned")
				}
				close(endpoint.complete)
				synctest.Wait()
				select {
				case <-scheduler.measurementChanged:
				default:
					t.Fatal("finishing the interrupted read did not wake the scheduler")
				}
				if tt.prefix {
					scheduler.updateMeasurementDrain(lanes, false)
					if scheduler.measurementQuiet {
						t.Fatal("the returned read prefix escaped accepted queue drainage")
					}
					var item packetqueue.Item[Packet]
					if err := queue.TryPop(&item, queue.Now()); err != nil {
						t.Fatal(err)
					}
					item.Release()
				}
				scheduler.updateMeasurementDrain(lanes, false)
				select {
				case <-ready:
				default:
					t.Fatal("the empty accepted cut did not become ready")
				}
				cancel()
				scheduler.updateMeasurementDrain(lanes, false)
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() = %v", err)
				}
				if scheduler.input.pauseOwner != nil || scheduler.input.readCancel != nil {
					t.Fatal("shutdown retained admission ownership or a read cancellation hook")
				}
			})
		})
	}
}

type measurementReadEndpoint struct {
	*testEndpoint
	started  chan context.Context
	complete chan struct{}
	prefix   bool
	err      error
}

func (e *measurementReadEndpoint) ReadBatch(ctx context.Context, packets []datagram.Packet) (int, error) {
	e.started <- ctx
	<-ctx.Done()
	<-e.complete
	if e.prefix {
		packets[0] = datagram.Packet{Kind: wgpacket.TransportData, Payload: relayWireGuardPacket(wgpacket.TransportData)}
		return 1, e.err
	}
	return 0, e.err
}
