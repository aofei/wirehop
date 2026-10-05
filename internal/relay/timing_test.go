package relay

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

type timingClock struct {
	now atomic.Uint64
}

func (c *timingClock) NowMicros() uint64 {
	return c.now.Load()
}

func TestReceiverUpdateClock(t *testing.T) {
	clock := &timingClock{}
	clock.now.Store(1000)
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: newTestEndpoint(), Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	precise := clockmap.Mapping{OffsetMicros: 100, UncertaintyMicros: 100}
	receiver.UpdateClock(precise)
	for _, tt := range []struct {
		name    string
		mapping clockmap.Mapping
		want    clockmap.Mapping
	}{
		{name: "LateSample", mapping: clockmap.Mapping{UncertaintyMicros: 60_000_000}, want: precise},
		{name: "LessPreciseFreshSample", mapping: clockmap.Mapping{UncertaintyMicros: 200},
			want: clockmap.Mapping{UncertaintyMicros: 200}},
		{name: "MaximumUncertainty", mapping: clockmap.Mapping{UncertaintyMicros: 5_000_000},
			want: clockmap.Mapping{UncertaintyMicros: 5_000_000}},
		{name: "ExcessUncertainty", mapping: clockmap.Mapping{UncertaintyMicros: 5_000_001}, want: precise},
		{name: "MorePreciseSample", mapping: clockmap.Mapping{UncertaintyMicros: 50},
			want: clockmap.Mapping{UncertaintyMicros: 50}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			receiver.UpdateClock(precise)
			receiver.UpdateClock(tt.mapping)
			if receiver.mapping != tt.want {
				t.Fatalf("mapping = %+v, want %+v", receiver.mapping, tt.want)
			}
		})
	}
}

type expiringBatchEndpoint struct {
	*testEndpoint
	clock   *timingClock
	dropped bool
	calls   int
}

func (e *expiringBatchEndpoint) WriteBatch(_ context.Context, payloads [][]byte, _ time.Time) (int, error) {
	e.calls++
	e.clock.now.Store(2_000_000)
	if e.dropped {
		return 1, datagram.ErrDatagramDropped
	}
	return len(payloads), nil
}

func TestReceiverWriteBatchExpiry(t *testing.T) {
	for _, tt := range []struct {
		name    string
		ids     []uint64
		dropped bool
	}{
		{name: "NewRun", ids: []uint64{5, 3}},
		{name: "RetryAfterPartialDrop", ids: []uint64{1, 2, 3}, dropped: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := &timingClock{}
			clock.now.Store(1000)
			endpoint := &expiringBatchEndpoint{testEndpoint: newTestEndpoint(), clock: clock, dropped: tt.dropped}
			receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
			if err != nil {
				t.Fatal(err)
			}
			var data []protocol.Data
			for _, id := range tt.ids {
				data = append(data, protocol.Data{PacketID: id, DeadlineMicros: 1_000_000, Payload: []byte{1}})
			}
			if err := receiver.deliverBatch(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			if endpoint.calls != 1 {
				t.Fatalf("UDP attempts = %d, want 1 before remaining packets expired", endpoint.calls)
			}
		})
	}
}

func TestReceiverWriteBatchInvalidatedDeadlineReleasesBuffers(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ids    []uint64
		writes int
	}{
		{name: "SingleRun", ids: []uint64{1, 2}},
		{name: "AfterLargerRun", ids: []uint64{4, 5, 6, 1, 2}, writes: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := &timingClock{}
			clock.now.Store(1000)
			endpoint := newTestEndpoint()
			receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
			if err != nil {
				t.Fatal(err)
			}
			var data []protocol.Data
			var deadlines []uint64
			for index, id := range tt.ids {
				deadline := uint64(2000)
				if index == len(tt.ids)-1 {
					deadline = 1000 + protocol.MaxPacketLifetimeMicros
				}
				data = append(data, protocol.Data{PacketID: id, DeadlineMicros: deadline, Payload: []byte{byte(id)}})
				deadlines = append(deadlines, deadline)
			}
			if err := receiver.ValidateDeadlines(deadlines); err != nil {
				t.Fatal(err)
			}
			// Another lane can update the clock mapping between validation and acquisition of the UDP write slot.
			receiver.UpdateClock(clockmap.Mapping{OffsetMicros: 1})
			if err := receiver.writeBatch(t.Context(), data); !errors.Is(err, ErrInvalidPacketDeadline) {
				t.Fatalf("invalidated deadline = %v, want deadline rejection", err)
			}
			if len(endpoint.writes) != tt.writes || len(receiver.writeSlot) != 0 {
				t.Fatalf("rejected batch wrote %d packets and retained %d write slots", len(endpoint.writes), len(receiver.writeSlot))
			}
			for index := range receiver.payloads {
				if receiver.payloads[index] != nil || receiver.packetIDs[index] != 0 || receiver.deadlines[index] != 0 {
					t.Fatalf("rejected batch retained buffer state at index %d", index)
				}
			}
		})
	}
}

func TestSchedulerRunIdleAndDetachedExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clockReads atomic.Uint64
		origin := time.Now()
		now := func() time.Time {
			clockReads.Add(1)
			return time.Unix(100, 0).Add(time.Since(origin))
		}
		budget, err := retention.NewBudget(retention.Limits{Packets: 4, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		limits := packetqueue.Limits{Packets: 4, Bytes: 4096}
		queue, err := packetqueue.NewWithBudget[Packet](limits, budget, now)
		if err != nil {
			t.Fatal(err)
		}
		defer queue.Close()
		scheduler, err := NewScheduler(queue)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- scheduler.Run(ctx) }()
		synctest.Wait()
		before := clockReads.Load()
		synctest.Sleep(time.Minute)
		synctest.Wait()
		if clockReads.Load() != before {
			t.Fatal("empty detached scheduler continued polling its clock")
		}
		for _, lifetime := range []time.Duration{2 * time.Second, time.Second} {
			if err := queue.Push(packetqueue.Item[Packet]{Size: 1024, Deadline: now().Add(lifetime)}); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		synctest.Sleep(time.Second)
		synctest.Wait()
		if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: 1024}) {
			t.Fatalf("first expiry retained %+v", got)
		}
		synctest.Sleep(time.Second)
		synctest.Wait()
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Fatalf("idle expired payloads retained %+v", got)
		}
		before = clockReads.Load()
		synctest.Sleep(time.Minute)
		synctest.Wait()
		if clockReads.Load() != before {
			t.Fatal("empty reclaimed scheduler continued polling")
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("scheduler exit = %v", err)
		}
	})
}

func TestLanePingResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
		clock := &timingClock{}
		clock.now.Store(1000)
		lane.clock = clock
		result := make(chan error, 1)
		go func() { result <- lane.ping(t.Context()) }()
		synctest.Wait()
		clock.now.Store(60_001_000)
		synctest.Sleep(time.Second)
		if err := <-result; !errors.Is(err, monotime.ErrResumed) {
			t.Fatalf("resume = %v, want resumed error", err)
		}
	})
}

type cancellationClosingCarrier struct {
	*testCarrier
	reading chan context.Context
	entered chan struct{}
}

func (c *cancellationClosingCarrier) ReadFrame(ctx context.Context) (protocol.Frame, error) {
	c.reading <- ctx
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	close(c.entered)
	// A WebSocket's underlying reader stops when the cancellation callback closes the carrier.
	frame, err := c.testCarrier.ReadFrame(context.WithoutCancel(ctx))
	if err != nil && ctx.Err() != nil {
		return protocol.Frame{}, ctx.Err()
	}
	return frame, err
}

func (c *cancellationClosingCarrier) Abort() error {
	ctx := <-c.reading
	if ctx.Err() != nil {
		// Exercise the ordering where read cancellation closes the socket before the lane owner aborts it.
		<-c.done
	}
	c.once.Do(func() {
		c.aborts <- struct{}{}
		close(c.done)
	})
	return nil
}

func TestLaneRunClockSyncTeardown(t *testing.T) {
	for _, tt := range []struct {
		name      string
		cause     error
		wantAbort bool
	}{
		{name: "SessionCancellation", cause: context.Canceled},
		{name: "LaneAbandonment", cause: ErrLaneAbandoned, wantAbort: true},
		{name: "SystemResume", cause: monotime.ErrResumed, wantAbort: true},
		{name: "ClockSyncTimeout", cause: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				connection := &cancellationClosingCarrier{
					testCarrier: newTestCarrier(), reading: make(chan context.Context, 1), entered: make(chan struct{}),
				}
				lane := newTestLane(t, connection.testCarrier, newTestEndpoint())
				lane.carrier = connection
				lane.clockSyncTimeout = 10 * time.Second
				lane.pingTimeout = time.Minute
				clock := &timingClock{}
				lane.clock = clock
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(context.Canceled)
				result := make(chan error, 1)
				go func() { result <- lane.Run(ctx) }()
				<-connection.entered
				synctest.Wait()
				switch tt.cause {
				case monotime.ErrResumed:
					clock.now.Store(60_000_000)
					synctest.Sleep(time.Second)
				case context.DeadlineExceeded:
					synctest.Sleep(lane.clockSyncTimeout)
				default:
					cancel(tt.cause)
				}
				if err := <-result; !errors.Is(err, tt.cause) {
					t.Fatalf("Run() error = %v, want %v", err, tt.cause)
				}
				select {
				case <-connection.aborts:
					if !tt.wantAbort {
						t.Fatal("ordinary teardown discarded queued carrier bytes")
					}
				default:
					if tt.wantAbort {
						t.Fatal("read cancellation closed the carrier before abortive teardown")
					}
				}
				synctest.Wait()
			})
		})
	}
}

type timingReportObserver struct {
	*testLaneObserver
	reports atomic.Int64
	accept  atomic.Bool
}

func (o *timingReportObserver) RouteDeliveryReport(_ protocol.DeliveryReport, complete func(bool)) bool {
	o.reports.Add(1)
	accepted := o.accept.Load()
	if accepted {
		complete(true)
	}
	return accepted
}

func testLaneReportIdleAndRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
		observer := &timingReportObserver{testLaneObserver: &testLaneObserver{}}
		lane.observer = observer
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- lane.report(ctx) }()
		synctest.Wait()
		synctest.Sleep(time.Minute)
		if observer.reports.Load() != 0 {
			t.Fatal("idle lane generated a report")
		}
		if err := lane.progress.addProbe(1203); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		synctest.Sleep(lane.reportInterval)
		synctest.Wait()
		if observer.reports.Load() != 1 {
			t.Fatalf("small progress reports = %d, want 1", observer.reports.Load())
		}
		synctest.Sleep(4 * lane.reportInterval)
		synctest.Wait()
		if observer.reports.Load() != 2 {
			t.Fatalf("unsent report retries = %d, want 2", observer.reports.Load())
		}
		observer.accept.Store(true)
		synctest.Sleep(4 * lane.reportInterval)
		synctest.Wait()
		synctest.Sleep(time.Minute)
		if observer.reports.Load() != 3 {
			t.Fatalf("completed report continued retrying: %d", observer.reports.Load())
		}
		if err := lane.progress.addData(1, reportByteThreshold); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if observer.reports.Load() != 4 {
			t.Fatalf("threshold report was delayed: %d", observer.reports.Load())
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("report exit = %v", err)
		}
	})
}

type prefixErrorCarrier struct {
	*testCarrier
	frames []protocol.Frame
}

func (c *prefixErrorCarrier) ReadFrames(_ context.Context, frames []protocol.Frame) (int, error) {
	return copy(frames, c.frames), io.ErrUnexpectedEOF
}

func TestLaneReadValidPrefixBeforeError(t *testing.T) {
	for _, test := range []struct {
		name        string
		dataPackets int
		probe       bool
		clockSync   bool
		invalidTail bool
		err         error
	}{
		{name: "Scalar", dataPackets: 1, err: io.ErrUnexpectedEOF},
		{name: "MaximumBatch", dataPackets: datagram.MaximumBatchSize, err: io.ErrUnexpectedEOF},
		{name: "InterleavedProbe", dataPackets: 15, probe: true, err: io.ErrUnexpectedEOF},
		{name: "InitialClockSync", dataPackets: 15, clockSync: true, err: io.ErrUnexpectedEOF},
		{name: "InvalidControlTail", dataPackets: 15, invalidTail: true, err: protocol.ErrInvalidControlFrame},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &prefixErrorCarrier{testCarrier: newTestCarrier()}
			endpoint := newTestEndpoint()
			endpoint.writes = make(chan []byte, datagram.MaximumBatchSize)
			lane := newTestLane(t, connection.testCarrier, endpoint)
			lane.carrier = connection
			observer := &timingSampleObserver{testLaneObserver: &testLaneObserver{}}
			lane.observer = observer
			confirmed := false
			if test.clockSync {
				lane.clockSyncTimeout = time.Second
				lane.clockSynced = func() { confirmed = true }
				frame, err := protocol.MarshalClockSync(protocol.ClockSync{
					ClientSendMicros: 1, ServerReceiveMicros: 2, ServerSendMicros: 2, ClientReceiveMicros: 3,
				})
				if err != nil {
					t.Fatal(err)
				}
				connection.frames = append(connection.frames, frame)
			}
			var dataBytes uint64
			for index := 0; index < test.dataPackets; index++ {
				payload := relayWireGuardPacket(wgpacket.TransportData)
				payload[4] = byte(index)
				frame, err := protocol.MarshalData(protocol.Data{
					PacketID: uint64(index + 1), DeadlineMicros: lane.clock.NowMicros() + 1_000_000, Payload: payload,
				})
				if err != nil {
					t.Fatal(err)
				}
				connection.frames = append(connection.frames, frame)
				encoded, err := protocol.MarshalFrame(frame)
				if err != nil {
					t.Fatal(err)
				}
				dataBytes += uint64(len(encoded))
				if test.probe && index == 6 {
					probe, err := protocol.MarshalProbe(protocol.Probe{Payload: make([]byte, 32)})
					if err != nil {
						t.Fatal(err)
					}
					connection.frames = append(connection.frames, probe)
				}
			}
			if test.invalidTail {
				connection.frames = append(connection.frames, protocol.Frame{Type: protocol.FramePong})
			}
			if err := lane.read(t.Context()); !errors.Is(err, test.err) {
				t.Fatalf("read() error = %v, want %v", err, test.err)
			}
			if lane.progress.dataPackets != uint64(test.dataPackets) || lane.progress.dataBytes != dataBytes ||
				len(endpoint.writes) != test.dataPackets {
				t.Fatal("trailing carrier or control error discarded a complete valid prefix")
			}
			if test.probe && (lane.progress.probePackets != 1 || lane.progress.probeBytes != 34) {
				t.Fatal("interleaved probe was omitted or counted as data")
			}
			if test.clockSync && (!confirmed || observer.samples != 1) {
				t.Fatal("initial clock synchronization did not confirm and observe the generation")
			}
			for index := 0; index < test.dataPackets; index++ {
				if payload := <-endpoint.writes; payload[4] != byte(index) {
					t.Fatalf("UDP delivery marker = %d, want %d", payload[4], index)
				}
			}
		})
	}
}

type timingSampleObserver struct {
	*testLaneObserver
	samples int
	sample  clockmap.Sample
}

func (o *timingSampleObserver) ObserveTiming(_ protocol.LaneID, _ uint64, sample clockmap.Sample) {
	o.samples++
	o.sample = sample
}

func TestLaneReadClockSync(t *testing.T) {
	for _, test := range []struct {
		name      string
		sync      protocol.ClockSync
		malformed bool
		err       error
	}{
		{name: "AdmissionSample", sync: protocol.ClockSync{
			ClientSendMicros: 1000, ServerReceiveMicros: 9000, ServerSendMicros: 11_000, ClientReceiveMicros: 7000,
		}},
		{name: "MalformedSample", malformed: true, err: protocol.ErrInvalidControlFrame},
		{name: "MaximumUncertainty", sync: protocol.ClockSync{
			ClientReceiveMicros: 2 * uint64(maximumClockUncertainty/time.Microsecond),
		}},
		{name: "TimestampOverflow", sync: protocol.ClockSync{
			ClientSendMicros: 1 << 63, ClientReceiveMicros: 1 << 63,
		}, err: clockmap.ErrTimestampOverflow},
		{name: "StaleSample", sync: protocol.ClockSync{
			ClientReceiveMicros: 2*uint64(maximumClockUncertainty/time.Microsecond) + 1,
		}, err: ErrStaleClockSample},
	} {
		t.Run(test.name, func(t *testing.T) {
			lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
			observer := &timingSampleObserver{testLaneObserver: &testLaneObserver{}}
			lane.observer = observer
			frame, err := protocol.MarshalClockSync(test.sync)
			if err != nil {
				t.Fatal(err)
			}
			if test.malformed {
				frame.Payload = nil
			}
			err = lane.readClockSync(frame)
			if !errors.Is(err, test.err) {
				t.Fatalf("readClockSync() error = %v, want %v", err, test.err)
			}
			if test.err != nil {
				if observer.samples != 0 {
					t.Fatal("rejected admission timing reached the scheduler")
				}
				return
			}
			want := clockmap.Sample{
				LocalSendMicros: test.sync.ClientSendMicros, RemoteReceiveMicros: test.sync.ServerReceiveMicros,
				RemoteSendMicros: test.sync.ServerSendMicros, LocalReceiveMicros: test.sync.ClientReceiveMicros,
			}
			if observer.samples != 1 || observer.sample != want {
				t.Fatalf("observed admission timing = %+v, want %+v", observer.sample, want)
			}
		})
	}
}

func TestLaneReadControlPongUncertainty(t *testing.T) {
	for _, tt := range []struct {
		name    string
		receive uint64
		samples int
	}{
		{name: "UsableSample", receive: 8_000_000, samples: 1},
		{name: "ExcessUncertainty", receive: 10_000_002},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
			clock := &timingClock{}
			clock.now.Store(tt.receive)
			lane.clock = clock
			observer := &timingSampleObserver{testLaneObserver: &testLaneObserver{}}
			lane.observer = observer
			precise := clockmap.Mapping{OffsetMicros: 100, UncertaintyMicros: 100}
			lane.receiver.UpdateClock(precise)
			lane.startPing(1)
			lane.recordPingSend(1, 0)
			lane.recordPingWritten(1, time.Now())
			frame, err := protocol.MarshalTimingPong(protocol.TimingPong{ID: 1})
			if err != nil {
				t.Fatal(err)
			}
			pendingSync := false
			if err := lane.readControl(t.Context(), frame, &pendingSync); err != nil {
				t.Fatal(err)
			}
			if pending, _ := lane.pingState(time.Now()); pending || observer.samples != tt.samples {
				t.Fatalf("Pong liveness or sample filtering failed: pending %t, samples %d", pending, observer.samples)
			}
			if tt.samples == 0 && lane.receiver.mapping != precise {
				t.Fatal("late Pong replaced the usable session clock mapping")
			}
		})
	}
}
