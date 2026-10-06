package relay

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

type testClock struct {
	now uint64
}

func (c *testClock) NowMicros() uint64 {
	return c.now
}

type testLaneObserver struct {
	lane          *Lane
	source        protocol.LaneGeneration
	report        protocol.DeliveryReport
	receiveMicros uint64
}

func (o *testLaneObserver) ObserveDeliveryReport(_ context.Context, source protocol.LaneGeneration,
	report protocol.DeliveryReport, receiveMicros uint64) error {
	o.source = source
	o.report = report
	o.receiveMicros = receiveMicros
	return nil
}

func TestLaneReadControlDeliveryReportSource(t *testing.T) {
	observer := &testLaneObserver{}
	lane := &Lane{
		laneID: protocol.LaneID(2), generation: 7, clock: &testClock{now: 12345}, observer: observer,
	}
	report := protocol.DeliveryReport{
		LaneID: protocol.LaneID(3), Generation: 9, DataPackets: 1,
	}
	frame, err := protocol.MarshalDeliveryReport(report)
	if err != nil {
		t.Fatal(err)
	}
	clockSyncPending := false
	if err := lane.readControl(context.Background(), frame, &clockSyncPending); err != nil {
		t.Fatal(err)
	}
	wantSource := protocol.LaneGeneration{LaneID: lane.laneID, Generation: lane.generation}
	if observer.source != wantSource || observer.report != report || observer.receiveMicros != 12345 {
		t.Fatalf("observed report = %+v, want source %+v, report %+v, time 12345", observer, wantSource, report)
	}
}

func (*testLaneObserver) ObserveTiming(protocol.LaneID, uint64, clockmap.Sample) {}

func (*testLaneObserver) ObserveLaneAbandon(context.Context, protocol.LaneGeneration) error {
	return nil
}

func (o *testLaneObserver) RouteDeliveryReport(report protocol.DeliveryReport, _ time.Time, complete func(bool)) bool {
	frame, err := protocol.MarshalDeliveryReport(report)
	if err != nil {
		return false
	}
	return o.lane.SendControl(frame, func() { complete(true) })
}

func newObservedLane(config LaneConfig) (*Lane, error) {
	observer := &testLaneObserver{}
	config.Observer = observer
	lane, err := NewLane(config)
	observer.lane = lane
	return lane, err
}

type testEndpoint struct {
	reads  chan datagram.Packet
	writes chan []byte
	done   chan struct{}
	once   sync.Once
}

type failOnceEndpoint struct {
	*testEndpoint
	mu     sync.Mutex
	failed bool
}

type dropOnceEndpoint struct {
	*testEndpoint
	mu      sync.Mutex
	dropped bool
}

type blockingWriteEndpoint struct {
	*testEndpoint
	entered chan byte
	release chan struct{}
}

type deadlineEndpoint struct {
	*testEndpoint
	deadlines chan time.Time
}

type recordingBatchEndpoint struct {
	*testEndpoint
	mu        sync.Mutex
	calls     []int
	dropFirst bool
}

type partialWriteEndpoint struct {
	*testEndpoint
	failureAt    int
	failure      error
	afterFailure func()
	calls        int
}

func (e *partialWriteEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	e.calls++
	if e.calls == e.failureAt {
		if e.afterFailure != nil {
			e.afterFailure()
		}
		return e.failure
	}
	return e.testEndpoint.Write(ctx, payload, deadline)
}

type partialBatchWriteEndpoint struct {
	*partialWriteEndpoint
}

func (e *partialBatchWriteEndpoint) WriteBatch(ctx context.Context, payloads [][]byte, deadline time.Time) (int, error) {
	for index, payload := range payloads {
		if err := e.Write(ctx, payload, deadline); err != nil {
			return index, err
		}
	}
	return len(payloads), nil
}

func (e *recordingBatchEndpoint) WriteBatch(ctx context.Context, payloads [][]byte, deadline time.Time) (int, error) {
	e.mu.Lock()
	e.calls = append(e.calls, len(payloads))
	drop := e.dropFirst
	e.dropFirst = false
	e.mu.Unlock()
	if drop {
		return 0, datagram.ErrDatagramDropped
	}
	for index, payload := range payloads {
		if err := e.testEndpoint.Write(ctx, payload, deadline); err != nil {
			return index, err
		}
	}
	return len(payloads), nil
}

func (e *deadlineEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	e.deadlines <- deadline
	return e.testEndpoint.Write(ctx, payload, deadline)
}

func (e *blockingWriteEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	select {
	case e.entered <- payload[4]:
	case <-ctx.Done():
		return ctx.Err()
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-e.release:
		return nil
	case <-timer.C:
		return context.DeadlineExceeded
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *failOnceEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	e.mu.Lock()
	if !e.failed {
		e.failed = true
		e.mu.Unlock()
		return net.ErrClosed
	}
	e.mu.Unlock()
	return e.testEndpoint.Write(ctx, payload, deadline)
}

func (e *dropOnceEndpoint) Write(ctx context.Context, payload []byte, deadline time.Time) error {
	e.mu.Lock()
	if !e.dropped {
		e.dropped = true
		e.mu.Unlock()
		return datagram.ErrDatagramDropped
	}
	e.mu.Unlock()
	return e.testEndpoint.Write(ctx, payload, deadline)
}

func newTestEndpoint() *testEndpoint {
	return &testEndpoint{reads: make(chan datagram.Packet, 8), writes: make(chan []byte, 8), done: make(chan struct{})}
}

func (e *testEndpoint) Read(ctx context.Context) (datagram.Packet, error) {
	select {
	case packet := <-e.reads:
		return packet, nil
	case <-e.done:
		return datagram.Packet{}, net.ErrClosed
	case <-ctx.Done():
		return datagram.Packet{}, ctx.Err()
	}
}

func (e *testEndpoint) Write(ctx context.Context, payload []byte, _ time.Time) error {
	copyPayload := append([]byte(nil), payload...)
	select {
	case e.writes <- copyPayload:
		return nil
	case <-e.done:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *testEndpoint) Close() error {
	e.once.Do(func() { close(e.done) })
	return nil
}

type testCarrier struct {
	reads        chan protocol.Frame
	writes       chan []protocol.Frame
	aborts       chan struct{}
	done         chan struct{}
	dataWriteErr error
	once         sync.Once
}

type blockingFrameCarrier struct {
	*testCarrier
	writeEntered chan struct{}
}

func newTestCarrier() *testCarrier {
	return &testCarrier{
		reads: make(chan protocol.Frame, 8), writes: make(chan []protocol.Frame, 8), aborts: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

func (c *testCarrier) ReadFrame(ctx context.Context) (protocol.Frame, error) {
	select {
	case frame := <-c.reads:
		return frame, nil
	case <-c.done:
		return protocol.Frame{}, net.ErrClosed
	case <-ctx.Done():
		return protocol.Frame{}, ctx.Err()
	}
}

func (c *testCarrier) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	batch := make([]protocol.Frame, len(frames))
	for index, frame := range frames {
		batch[index] = protocol.Frame{Type: frame.Type, Payload: append([]byte(nil), frame.Payload...)}
	}
	select {
	case c.writes <- batch:
		return nil
	case <-c.done:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *blockingFrameCarrier) WriteFrames(ctx context.Context, _ []protocol.Frame) error {
	close(c.writeEntered)
	<-ctx.Done()
	return ctx.Err()
}

func (c *testCarrier) WriteDataBatch(ctx context.Context, data []protocol.Data) error {
	if c.dataWriteErr != nil {
		return c.dataWriteErr
	}
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

func (c *testCarrier) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

func (c *testCarrier) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
}

func (c *testCarrier) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *testCarrier) Abort() error {
	select {
	case c.aborts <- struct{}{}:
	default:
	}
	return c.Close()
}

func TestDeadlinePolicy(t *testing.T) {
	policy := DeadlinePolicy{Control: 200 * time.Millisecond, Transport: 800 * time.Millisecond}
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got := policy.Lifetime(wgpacket.HandshakeInitiation); got != policy.Control {
		t.Fatalf("control lifetime = %v, want %v", got, policy.Control)
	}
	if got := policy.Lifetime(wgpacket.TransportData); got != policy.Transport {
		t.Fatalf("transport lifetime = %v, want %v", got, policy.Transport)
	}
	for _, invalid := range []DeadlinePolicy{
		{},
		{
			Control:   time.Millisecond,
			Transport: time.Duration(protocol.MaxPacketLifetimeMicros+1) * time.Microsecond,
		},
	} {
		if !errors.Is(invalid.Validate(), ErrInvalidDeadlinePolicy) {
			t.Fatalf("Validate() error = %v, want %v", invalid.Validate(), ErrInvalidDeadlinePolicy)
		}
	}
}

func TestPacketValidation(t *testing.T) {
	valid := Packet{
		Kind: wgpacket.TransportData, Payload: relayWireGuardPacket(wgpacket.TransportData), DeadlineMicros: 1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	oversized := valid
	oversized.Payload = make([]byte, protocol.MaxPacketSize+1)
	oversized.Payload[0] = 4
	if wgpacket.Classify(oversized.Payload) != wgpacket.TransportData {
		t.Fatal("oversized test packet is not structurally valid WireGuard transport data")
	}
	if err := oversized.Validate(); !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("oversized packet error = %v, want %v", err, ErrInvalidPacket)
	}
}

func TestLanePhase(t *testing.T) {
	spread := time.Second
	first := lanePhase(protocol.LaneID(1), 1, spread)
	if first < 0 || first >= spread || first != lanePhase(protocol.LaneID(1), 1, spread) {
		t.Fatalf("lanePhase() = %v", first)
	}
	if lanePhase(protocol.LaneID(1), 1, 0) != 0 {
		t.Fatal("lanePhase() returned a phase without spread")
	}
}

func TestDeliveryProgressTracksCarrierOrder(t *testing.T) {
	progress := deliveryProgress{}
	if err := progress.addData(1, 10); err != nil {
		t.Fatal(err)
	}
	if err := progress.addData(16, 20); err != nil {
		t.Fatal(err)
	}
	snapshot, changed := progress.claim(1, 2, time.Now(), time.Second)
	if snapshot.dataBytes != 30 || snapshot.report.DataPackets != 17 || snapshot.revision != 2 || !changed {
		t.Fatalf("claim() = %+v, changed %t", snapshot, changed)
	}
	progress.complete(snapshot, true)
	if _, changed := progress.claim(1, 2, time.Now(), time.Second); changed {
		t.Fatal("reported progress remained changed")
	}
}

func TestDeliveryProgressRetriesUnsentClaim(t *testing.T) {
	for _, test := range []struct {
		name              string
		additionalPackets int
	}{
		{name: "UnchangedProgress"},
		{name: "ChangedProgress", additionalPackets: reportPacketThreshold},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := deliveryProgress{notify: make(chan struct{}, 1)}
			if err := progress.addData(1, 10); err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			snapshot, changed := progress.claim(protocol.LaneID(1), 1, now, time.Second)
			if !changed {
				t.Fatal("initial progress was not claimed")
			}
			select {
			case <-progress.notify:
			default:
				t.Fatal("initial progress did not publish a notification")
			}
			progress.complete(snapshot, false)
			select {
			case <-progress.notify:
			default:
				t.Fatal("failed completion did not publish a notification")
			}
			for range test.additionalPackets {
				if err := progress.addData(1, 1); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-progress.notify:
				t.Fatal("pending failed claim published an immediate retry notification")
			default:
			}
			if _, changed := progress.claim(protocol.LaneID(1), 1, now.Add(time.Second-time.Nanosecond), time.Second); changed {
				t.Fatal("pending progress was claimed before its retry interval")
			}
			retry, changed := progress.claim(protocol.LaneID(1), 1, now.Add(time.Second), time.Second)
			if !changed || retry.report.DataPackets != uint64(1+test.additionalPackets) {
				t.Fatalf("retry = %+v, changed %t", retry, changed)
			}
			progress.complete(retry, true)
		})
	}
}

func TestDeliveryProgressThresholdNotification(t *testing.T) {
	for _, test := range []struct {
		name    string
		packets int
		bytes   int
	}{
		{name: "Packets", packets: reportPacketThreshold, bytes: 1},
		{name: "Bytes", packets: 4, bytes: reportByteThreshold / 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := deliveryProgress{notify: make(chan struct{}, 1)}
			for index := range test.packets {
				if err := progress.addData(1, uint64(test.bytes)); err != nil {
					t.Fatal(err)
				}
				if index == 0 {
					select {
					case <-progress.notify:
					default:
						t.Fatal("initial progress did not publish a notification")
					}
				}
				if index+1 == test.packets {
					continue
				}
				select {
				case <-progress.notify:
					t.Fatalf("notification arrived after %d packets", index+1)
				default:
				}
			}
			select {
			case <-progress.notify:
			default:
				t.Fatal("threshold did not publish a notification")
			}
		})
	}
}

func TestDeliveryProgressRejectsCounterOverflow(t *testing.T) {
	for _, test := range []struct {
		name        string
		dataBytes   uint64
		dataPackets uint64
		revision    uint64
	}{
		{name: "DataBytes", dataBytes: ^uint64(0) - 15},
		{name: "DataPackets", dataPackets: ^uint64(0) - 15},
		{name: "Revision", revision: ^uint64(0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := deliveryProgress{
				dataBytes: test.dataBytes, dataPackets: test.dataPackets, revision: test.revision,
				notify: make(chan struct{}, 1),
			}
			if err := progress.addData(16, 16); !errors.Is(err, ErrCounterExhausted) {
				t.Fatalf("addData() error = %v, want %v", err, ErrCounterExhausted)
			}
			if progress.dataBytes != test.dataBytes || progress.dataPackets != test.dataPackets ||
				progress.revision != test.revision || len(progress.notify) != 0 {
				t.Fatal("rejected batch changed counters or published progress")
			}
		})
	}

	t.Run("ExactDataLimit", func(t *testing.T) {
		progress := deliveryProgress{
			dataBytes: ^uint64(0) - 16, dataPackets: ^uint64(0) - 16, revision: ^uint64(0) - 1,
		}
		if err := progress.addData(16, 16); err != nil {
			t.Fatal(err)
		}
		if progress.dataBytes != ^uint64(0) || progress.dataPackets != ^uint64(0) || progress.revision != ^uint64(0) {
			t.Fatal("last representable batch was not recorded exactly")
		}
	})

	t.Run("PingRevision", func(t *testing.T) {
		progress := deliveryProgress{revision: ^uint64(0)}
		if err := progress.addPing(1); !errors.Is(err, ErrCounterExhausted) {
			t.Fatal(err)
		}
		if progress.pingID != 0 {
			t.Fatal("rejected ping changed progress")
		}
	})
}

func TestLaneReadDataBatch(t *testing.T) {
	for _, test := range []struct {
		name          string
		kind          string
		largePacket   bool
		widePacketID  bool
		priorProgress bool
		err           error
	}{
		{name: "ValidBatch"},
		{name: "MaximumPacket", largePacket: true},
		{name: "FullWidthPacketID", widePacketID: true},
		{name: "MalformedLastFrame", kind: "frame", err: protocol.ErrInvalidDataFrame},
		{name: "NoncanonicalLastID", kind: "integer", err: protocol.ErrInvalidDataFrame},
		{name: "InvalidLastPacket", kind: "packet", err: ErrInvalidWireGuardPacket},
		{name: "InvalidLastDeadline", kind: "deadline", err: ErrInvalidPacketDeadline},
		{name: "CounterOverflow", kind: "counter", err: ErrCounterExhausted},
		{name: "MalformedAfterReportedPrefix", kind: "frame", priorProgress: true, err: protocol.ErrInvalidDataFrame},
		{name: "NoncanonicalAfterReportedPrefix", kind: "integer", priorProgress: true, err: protocol.ErrInvalidDataFrame},
		{name: "InvalidPacketAfterReportedPrefix", kind: "packet", priorProgress: true, err: ErrInvalidWireGuardPacket},
		{name: "InvalidDeadlineAfterReportedPrefix", kind: "deadline", priorProgress: true, err: ErrInvalidPacketDeadline},
		{name: "CounterOverflowAfterReportedPrefix", kind: "counter", priorProgress: true, err: ErrCounterExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := newTestEndpoint()
			endpoint.writes = make(chan []byte, datagram.MaximumBatchSize)
			lane := newTestLane(t, newTestCarrier(), endpoint)
			if test.priorProgress {
				prefix, err := protocol.MarshalData(protocol.Data{
					PacketID: 1, DeadlineMicros: 1_000_000, Payload: relayWireGuardPacket(wgpacket.TransportData),
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := lane.readDataBatch(t.Context(), []protocol.Frame{prefix}); err != nil {
					t.Fatal(err)
				}
				snapshot, changed := lane.progress.claim(lane.laneID, lane.generation, time.Now(), time.Second)
				if !changed || snapshot.report.DataPackets != 1 {
					t.Fatal("accepted prefix was not available for feedback")
				}
				lane.progress.complete(snapshot, true)
				if len(endpoint.writes) != 1 || len(lane.progress.notify) != 1 {
					t.Fatal("accepted prefix did not deliver data and notify the report worker")
				}
				<-endpoint.writes
				<-lane.progress.notify
			}
			var frames [datagram.MaximumBatchSize]protocol.Frame
			var bytes uint64
			for index := range frames {
				packet := protocol.Data{
					PacketID: uint64(index + 1), DeadlineMicros: 1_000_000,
					Payload: relayWireGuardPacket(wgpacket.TransportData),
				}
				if test.priorProgress {
					packet.PacketID++
				}
				if test.largePacket {
					packet.Payload = make([]byte, protocol.MaxPacketSize)
					packet.Payload[0] = 4
				}
				if test.widePacketID {
					packet.PacketID += 1 << 63
				}
				if index == len(frames)-1 {
					switch test.kind {
					case "packet":
						packet.Payload = []byte{0}
					case "deadline":
						packet.DeadlineMicros = protocol.MaxPacketLifetimeMicros + 2_000_000
					}
				}
				frame, err := protocol.MarshalData(packet)
				if err != nil {
					t.Fatal(err)
				}
				frames[index] = frame
				encoded, err := protocol.MarshalDataFrame(packet)
				if err != nil {
					t.Fatal(err)
				}
				bytes += uint64(len(encoded))
			}
			if test.kind == "frame" {
				frames[len(frames)-1].Payload = nil
			}
			if test.kind == "integer" {
				last := &frames[len(frames)-1]
				last.Payload = append([]byte{0x90, 0}, last.Payload[1:]...)
			}
			if test.kind == "counter" {
				lane.progress.dataPackets = ^uint64(0) - uint64(len(frames)) + 1
			}
			beforePackets := lane.progress.dataPackets
			beforeBytes := lane.progress.dataBytes
			beforeRevision := lane.progress.revision
			err := lane.readDataBatch(t.Context(), frames[:])
			if !errors.Is(err, test.err) {
				t.Fatalf("readDataBatch() error = %v, want %v", err, test.err)
			}
			if test.err == nil {
				if lane.progress.dataPackets != uint64(len(frames)) || lane.progress.dataBytes != bytes ||
					lane.progress.revision != 1 || len(endpoint.writes) != len(frames) {
					t.Fatal("validated batch did not record and deliver its exact prefix")
				}
			} else {
				if lane.progress.dataPackets != beforePackets || lane.progress.dataBytes != beforeBytes ||
					lane.progress.revision != beforeRevision || len(endpoint.writes) != 0 || len(lane.progress.notify) != 0 {
					t.Fatal("invalid batch changed parse progress or delivered UDP data")
				}
				if test.priorProgress {
					if _, changed := lane.progress.reportDelay(time.Now(), time.Second); changed {
						t.Fatal("rejected batch made the completed prefix reportable again")
					}
					packet, err := protocol.ParseData(frames[0])
					if err != nil {
						t.Fatal(err)
					}
					if err := lane.receiver.Deliver(t.Context(), packet); err != nil {
						t.Fatal(err)
					}
					if len(endpoint.writes) != 1 {
						t.Fatal("rejected batch changed deduplication state")
					}
				}
			}
		})
	}
}

func TestLaneReadDataBatchAcknowledgesPrefix(t *testing.T) {
	for _, test := range []struct {
		name      string
		batchSize int
		firstID   uint64
	}{
		{name: "SingleFrame", batchSize: 1, firstID: 1},
		{name: "UnevenBatches", batchSize: 7, firstID: 120},
		{name: "MaximumBatch", batchSize: datagram.MaximumBatchSize, firstID: 1 << 63},
	} {
		t.Run(test.name, func(t *testing.T) {
			const packetCount = 32
			endpoint := newTestEndpoint()
			endpoint.writes = make(chan []byte, datagram.MaximumBatchSize)
			lane := newTestLane(t, newTestCarrier(), endpoint)
			sender := schedulerLaneWithLimits(t, 1, 1, 1000, 1_000_000,
				packetqueue.Limits{Packets: packetCount, Bytes: 1024 * 1024})
			store := sender.registration.Store
			now := time.UnixMicro(1000)
			store.now = func() time.Time { return now }
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			var frames [packetCount]protocol.Frame
			var encodedBytes [packetCount + 1]uint64
			payloadSizes := [...]int{32, 123, 124, 16_379, 16_380, protocol.MaxPacketSize}
			for index := range frames {
				transmission := schedulerTransmission(test.firstID+uint64(index), wgpacket.TransportData, now.Add(time.Second))
				transmission.wireDeadline = 1_000_000
				transmission.packet.Payload = make([]byte, payloadSizes[index%len(payloadSizes)])
				transmission.packet.Payload[0] = 4
				transmission.packet.Payload[4] = byte(index)
				if err := store.push(transmission); err != nil {
					t.Fatal(err)
				}
				packet := takeOneTransmission(t, store)
				frame, err := protocol.MarshalData(packet)
				if err != nil {
					t.Fatal(err)
				}
				frames[index] = frame
				encoded, err := protocol.MarshalDataFrame(packet)
				if err != nil {
					t.Fatal(err)
				}
				encodedBytes[index+1] = encodedBytes[index] + uint64(len(encoded))
			}
			for offset := 0; offset < len(frames); {
				end := min(offset+test.batchSize, len(frames))
				if err := lane.readDataBatch(t.Context(), frames[offset:end]); err != nil {
					t.Fatal(err)
				}
				snapshot, changed := lane.progress.claim(lane.laneID, lane.generation, now, time.Second)
				if !changed || snapshot.report.DataPackets != uint64(end) || snapshot.dataBytes != encodedBytes[end] {
					t.Fatalf("received prefix report = %+v, changed %t", snapshot, changed)
				}
				if progressed, err := sender.applyReport(snapshot.report, uint64(now.UnixMicro()), now); err != nil || !progressed {
					t.Fatalf("prefix acknowledgement = %t, %v", progressed, err)
				}
				if packets, bytes := store.backlog(); packets != packetCount-end ||
					bytes != encodedBytes[packetCount]-encodedBytes[end] {
					t.Fatalf("retained suffix = %d packets, %d bytes", packets, bytes)
				}
				lane.progress.complete(snapshot, true)
				for index := offset; index < end; index++ {
					if payload := <-endpoint.writes; payload[4] != byte(index) {
						t.Fatalf("UDP delivery marker = %d, want %d", payload[4], index)
					}
				}
				offset = end
				now = now.Add(time.Millisecond)
			}
		})
	}
}

func TestLaneReadDataBatchPartialDelivery(t *testing.T) {
	for _, batched := range []bool{false, true} {
		name := "Scalar"
		if batched {
			name = "Vector"
		}
		t.Run(name, func(t *testing.T) {
			for _, test := range []struct {
				name      string
				failureAt int
				failure   error
				cancel    bool
				expire    bool
				wantError error
				first     []byte
				retry     []byte
			}{
				{name: "DropFirst", failureAt: 1, failure: datagram.ErrDatagramDropped,
					first: []byte{2, 3, 4, 5}, retry: []byte{1}},
				{name: "DropMiddle", failureAt: 3, failure: datagram.ErrDatagramDropped,
					first: []byte{1, 2, 4, 5}, retry: []byte{3}},
				{name: "DropLast", failureAt: 5, failure: datagram.ErrDatagramDropped,
					first: []byte{1, 2, 3, 4}, retry: []byte{5}},
				{name: "MissingLocalPeer", failureAt: 3, failure: datagram.ErrNoLocalPeer,
					first: []byte{1, 2}, retry: []byte{3, 4, 5}},
				{name: "EndpointFailure", failureAt: 3, failure: net.ErrClosed, wantError: ErrEndpointFailure,
					first: []byte{1, 2}, retry: []byte{3, 4, 5}},
				{name: "CanceledAfterPrefix", failureAt: 3, failure: context.Canceled, cancel: true,
					wantError: ErrEndpointFailure, first: []byte{1, 2}, retry: []byte{3, 4, 5}},
				{name: "ExpiryAfterDrop", failureAt: 3, failure: datagram.ErrDatagramDropped, expire: true,
					first: []byte{1, 2, 5}, retry: []byte{3}},
			} {
				t.Run(test.name, func(t *testing.T) {
					endpoint := &partialWriteEndpoint{
						testEndpoint: newTestEndpoint(), failureAt: test.failureAt, failure: test.failure,
					}
					lane := newTestLane(t, newTestCarrier(), endpoint.testEndpoint)
					lane.receiver.endpoint = endpoint
					if batched {
						lane.receiver.endpoint = &partialBatchWriteEndpoint{partialWriteEndpoint: endpoint}
					}
					clock := lane.receiver.clock.(*testClock)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if test.cancel {
						endpoint.afterFailure = cancel
					}
					if test.expire {
						endpoint.afterFailure = func() { clock.now = 200_000 }
					}
					var frames [5]protocol.Frame
					var encodedBytes uint64
					for index := range frames {
						packet := protocol.Data{
							PacketID: uint64(index + 1), DeadlineMicros: 1_000_000,
							Payload: relayWireGuardPacket(wgpacket.TransportData),
						}
						packet.Payload[4] = byte(index + 1)
						if test.expire && index == 3 {
							packet.DeadlineMicros = 200_000
						}
						frame, err := protocol.MarshalData(packet)
						if err != nil {
							t.Fatal(err)
						}
						frames[index] = frame
						encodedBytes += uint64(protocol.FrameSize(len(frame.Payload)))
					}
					if err := lane.readDataBatch(ctx, frames[:]); !errors.Is(err, test.wantError) ||
						test.wantError != nil && !errors.Is(err, test.failure) {
						t.Fatalf("partial delivery error = %v, want %v and %v", err, test.wantError, test.failure)
					}
					if lane.progress.dataPackets != uint64(len(frames)) || lane.progress.dataBytes != encodedBytes {
						t.Fatal("partial UDP delivery changed the cumulative parsed prefix")
					}
					if len(endpoint.writes) != len(test.first) {
						t.Fatalf("first delivery wrote %d packets, want %d", len(endpoint.writes), len(test.first))
					}
					for _, marker := range test.first {
						if payload := <-endpoint.writes; payload[4] != marker {
							t.Fatalf("first delivery marker = %d, want %d", payload[4], marker)
						}
					}
					if err := lane.readDataBatch(t.Context(), frames[:]); err != nil {
						t.Fatal(err)
					}
					if len(endpoint.writes) != len(test.retry) {
						t.Fatalf("retry wrote %d packets, want %d", len(endpoint.writes), len(test.retry))
					}
					for _, marker := range test.retry {
						if payload := <-endpoint.writes; payload[4] != marker {
							t.Fatalf("retry delivery marker = %d, want %d", payload[4], marker)
						}
					}
					if err := lane.readDataBatch(t.Context(), frames[:]); err != nil {
						t.Fatal(err)
					}
					if len(endpoint.writes) != 0 || lane.progress.dataPackets != 3*uint64(len(frames)) ||
						lane.progress.dataBytes != 3*encodedBytes {
						t.Fatal("repeated carrier parsing changed exactly-once UDP delivery")
					}
				})
			}
		})
	}
}

func TestLaneReadDataBatchDelivery(t *testing.T) {
	for _, test := range []struct {
		name       string
		duplicate  bool
		expired    bool
		writeFails bool
		canceled   bool
		wantWrites int
		err        error
	}{
		{name: "Delivered", wantWrites: 3},
		{name: "Duplicate", duplicate: true, wantWrites: 2},
		{name: "Expired", expired: true},
		{name: "EndpointFailure", writeFails: true, err: ErrEndpointFailure},
		{name: "CanceledDelivery", canceled: true, err: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := newTestEndpoint()
			lane := newTestLane(t, newTestCarrier(), endpoint)
			if test.writeFails {
				lane.receiver.endpoint = &failOnceEndpoint{testEndpoint: endpoint}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.canceled {
				lane.receiver.writeSlot <- struct{}{}
				cancel()
			}
			var frames [3]protocol.Frame
			var encodedBytes uint64
			for index := range frames {
				packet := protocol.Data{
					PacketID: uint64(index + 1), DeadlineMicros: 1_000_000,
					Payload: relayWireGuardPacket(wgpacket.TransportData),
				}
				if test.duplicate && index == 1 {
					packet.PacketID = 1
				}
				if test.expired {
					packet.DeadlineMicros = 999
				}
				frame, err := protocol.MarshalData(packet)
				if err != nil {
					t.Fatal(err)
				}
				frames[index] = frame
				encoded, err := protocol.MarshalDataFrame(packet)
				if err != nil {
					t.Fatal(err)
				}
				encodedBytes += uint64(len(encoded))
			}
			if err := lane.readDataBatch(ctx, frames[:]); !errors.Is(err, test.err) {
				t.Fatalf("readDataBatch() error = %v, want %v", err, test.err)
			}
			if lane.progress.dataPackets != uint64(len(frames)) || lane.progress.dataBytes != encodedBytes ||
				lane.progress.revision != 1 || len(endpoint.writes) != test.wantWrites {
				t.Fatal("UDP delivery outcome changed the fully parsed carrier prefix")
			}
			if test.canceled {
				<-lane.receiver.writeSlot
			}
			if test.expired || test.writeFails || test.canceled {
				packet, err := protocol.ParseData(frames[0])
				if err != nil {
					t.Fatal(err)
				}
				packet.DeadlineMicros = 1_000_000
				if err := lane.receiver.Deliver(t.Context(), packet); err != nil {
					t.Fatal(err)
				}
				if len(endpoint.writes) != test.wantWrites+1 {
					t.Fatal("undelivered packet was committed to deduplication")
				}
			}
		})
	}
}

func TestDeliveryProgressAddPing(t *testing.T) {
	progress := deliveryProgress{}
	if err := progress.addPing(2); err != nil {
		t.Fatal(err)
	}
	for _, identifier := range []uint64{0, 1, 2} {
		if err := progress.addPing(identifier); !errors.Is(err, protocol.ErrInvalidControlFrame) {
			t.Fatal(err)
		}
	}
	if progress.pingID != 2 || progress.revision != 1 {
		t.Fatal("invalid ping changed progress")
	}
}

func TestLaneValidatePingProgress(t *testing.T) {
	lane := &Lane{}
	if !lane.ValidatePingProgress(0) || lane.ValidatePingProgress(1) {
		t.Fatal("unexposed ping accepted")
	}
	lane.exposedPingID.Store(2)
	if !lane.ValidatePingProgress(1) || !lane.ValidatePingProgress(2) || lane.ValidatePingProgress(3) {
		t.Fatal("ping progress does not match the exposed request bound")
	}
}

func TestNextIdleInterval(t *testing.T) {
	for _, test := range []struct {
		current time.Duration
		maximum time.Duration
		want    time.Duration
	}{
		{current: time.Second, maximum: 15 * time.Second, want: 2 * time.Second},
		{current: 8 * time.Second, maximum: 15 * time.Second, want: 15 * time.Second},
		{current: 15 * time.Second, maximum: 15 * time.Second, want: 15 * time.Second},
	} {
		if got := nextIdleInterval(test.current, test.maximum); got != test.want {
			t.Fatalf("nextIdleInterval(%v, %v) = %v, want %v", test.current, test.maximum, got, test.want)
		}
	}
}

func TestLanePingState(t *testing.T) {
	lane := &Lane{
		pingInterval: time.Second, pingTimeout: 3 * time.Second, pingChanged: make(chan struct{}, 1),
	}
	now := time.Now()
	if pending, remaining := lane.pingState(now); pending || remaining != 0 {
		t.Fatalf("empty ping state = %t, %v", pending, remaining)
	}
	lane.startPing(1)
	if lane.completePing(1, 0) {
		t.Fatal("queued ping accepted a response before the writer generated its timestamp")
	}
	if pending, remaining := lane.pingState(now); !pending || remaining != time.Second {
		t.Fatalf("queued ping state = %t, %v", pending, remaining)
	}
	lane.recordPingSend(1, 0)
	lane.recordPingWritten(1, now)
	if pending, remaining := lane.pingState(now.Add(2 * time.Second)); !pending || remaining != time.Second {
		t.Fatalf("written ping state = %t, %v", pending, remaining)
	}
	if pending, remaining := lane.pingState(now.Add(3 * time.Second)); !pending || remaining != 0 {
		t.Fatalf("expired ping state = %t, %v", pending, remaining)
	}
	if !lane.completePing(1, 0) {
		t.Fatal("matching ping did not complete")
	}
	if pending, remaining := lane.pingState(now); pending || remaining != 0 {
		t.Fatalf("completed ping state = %t, %v", pending, remaining)
	}
	lane.startPing(2)
	lane.recordPingSend(2, 42)
	lane.cancelPing(1)
	if pending, _ := lane.pingState(now); !pending {
		t.Fatal("mismatched cancellation removed the pending ping")
	}
	lane.cancelPing(2)
	if pending, remaining := lane.pingState(now); pending || remaining != 0 {
		t.Fatalf("canceled ping state = %t, %v", pending, remaining)
	}
	if lane.completePing(2, 42) {
		t.Fatal("canceled ping accepted a later response")
	}
	t.Run("PongValidation", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			built bool
		}{
			{name: "Queued"},
			{name: "Built", built: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
				lane.startPing(1)
				want := ErrUnexpectedPong
				if tt.built {
					lane.recordPingSend(1, 0)
					want = nil
				}
				frame, err := protocol.MarshalTimingPong(protocol.TimingPong{
					ID: 1, PingSendMicros: 0, ReceiveMicros: 1000, SendMicros: 1000,
				})
				if err != nil {
					t.Fatal(err)
				}
				pending := false
				if err := lane.readControl(context.Background(), frame, &pending); !errors.Is(err, want) {
					t.Fatalf("readControl() error = %v, want %v", err, want)
				}
				if !tt.built && (lane.pendingPingID != 1 || lane.receiver.mapping != (clockmap.Mapping{})) {
					t.Fatal("rejected pong changed the pending request or session clock mapping")
				}
				if tt.built && lane.pendingPingID != 0 {
					t.Fatal("matching pong did not complete the writer-built request")
				}
			})
		}
	})
	t.Run("ReplyBeforeWriteCompletion", func(t *testing.T) {
		for _, sendMicros := range []uint64{0, 128} {
			lane := &Lane{pingInterval: time.Second, pingTimeout: 3 * time.Second,
				pingChanged: make(chan struct{}, 1)}
			lane.startPing(1)
			lane.recordPingSend(1, sendMicros)
			if !lane.completePing(1, sendMicros) {
				t.Fatal("writer-built ping rejected a response before write completion")
			}
			lane.startPing(2)
			lane.recordPingWritten(1, now)
			if lane.pendingPingID != 2 || !lane.pendingPingAt.IsZero() || lane.pendingPingBuilt {
				t.Fatal("late write completion changed the newer queued request")
			}
			if lane.completePing(2, 0) {
				t.Fatal("new queued request inherited the previous request's exposure")
			}
			lane.cancelPing(2)
			lane.recordPingSend(2, sendMicros)
			if lane.pendingPingBuilt || lane.completePing(2, sendMicros) {
				t.Fatal("canceled request was restored by a late builder")
			}
		}
	})
}

func TestLanePingResumesActiveInterval(t *testing.T) {
	const (
		pingInterval = 10 * time.Millisecond
		pingTimeout  = 50 * time.Millisecond
	)
	lane := &Lane{
		clock: &testClock{now: 1000}, laneID: protocol.LaneID(1), generation: 1,
		control: make(chan controlWrite, 1), pingInterval: pingInterval, pingTimeout: pingTimeout,
		pingChanged: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- lane.ping(ctx) }()

	for range 4 {
		request := awaitControlWrite(t, lane.control)
		frame, err := request.build(lane.clock.NowMicros())
		if err != nil {
			t.Fatal(err)
		}
		request.sent()
		ping, err := protocol.ParseTimingPing(frame)
		if err != nil {
			t.Fatal(err)
		}
		if !lane.completePing(ping.ID, ping.SendMicros) {
			t.Fatal("matching ping did not complete")
		}
	}
	resumedAt := time.Now()
	lane.dataWrites.Add(1)
	lane.signalPingChanged()
	awaitControlWrite(t, lane.control)
	if elapsed := time.Since(resumedAt); elapsed > 5*pingInterval {
		t.Fatalf("active ping resumed after %v, want at most %v", elapsed, 5*pingInterval)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("ping() error = %v, want %v", err, context.Canceled)
	}
}

func TestIngress(t *testing.T) {
	endpoint := newTestEndpoint()
	clock := &testClock{now: 1234}
	queuedAt := monotime.Time(clock.now)
	queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 2, Bytes: 1024}, func() time.Time { return queuedAt })
	if err != nil {
		t.Fatal(err)
	}
	policy := DeadlinePolicy{Control: 1500 * time.Microsecond, Transport: 3 * time.Millisecond}
	ingress, err := NewIngress(endpoint, queue, clock, policy)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- ingress.Run(ctx) }()
	payload := relayWireGuardPacket(wgpacket.HandshakeInitiation)
	endpoint.reads <- datagram.Packet{Kind: wgpacket.HandshakeInitiation, Payload: payload}

	item, err := queue.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if item.Value.Kind != wgpacket.HandshakeInitiation || item.Value.DeadlineMicros != 2734 ||
		item.Priority != packetqueue.PriorityControl {
		t.Fatalf("ingress item = %+v", item)
	}
	if want := queuedAt.Add(policy.Control); item.Deadline != want {
		t.Fatalf("ingress deadline = %v, want protocol deadline %v", item.Deadline, want)
	}
	if len(item.Value.Payload) == 0 || &item.Value.Payload[0] != &payload[0] {
		t.Fatal("ingress did not transfer packet ownership")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
}

func TestIngressDeadlineFollowsProtocolClock(t *testing.T) {
	endpoint := newTestEndpoint()
	clock := &testClock{now: 1000}
	queue, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 2, Bytes: 1024}, func() time.Time {
		return time.Unix(0, int64(clock.now)*int64(time.Microsecond))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	ingress, err := NewIngress(endpoint, queue, clock, DeadlinePolicy{Control: time.Second, Transport: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- ingress.Run(ctx) }()
	endpoint.reads <- datagram.Packet{
		Kind: wgpacket.TransportData, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	select {
	case <-queue.Ready():
	case <-time.After(time.Second):
		t.Fatal("ingress did not enqueue the packet")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want cancellation", err)
	}
	clock.now += 2_000_000
	var item packetqueue.Item[Packet]
	if err := queue.TryPop(&item); !errors.Is(err, packetqueue.ErrEmpty) {
		item.Release()
		t.Fatalf("TryPop() = %v, want expiry after the protocol clock advances", err)
	}
}

func TestIngressMarksEndpointFailure(t *testing.T) {
	endpoint := newTestEndpoint()
	queue, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := NewIngress(endpoint, queue, &testClock{}, DeadlinePolicy{
		Control:   time.Second,
		Transport: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	err = ingress.Run(context.Background())
	if !errors.Is(err, ErrEndpointFailure) || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Run() error = %v, want endpoint and underlying closure errors", err)
	}
}

func TestReceiverRetriesDuplicateAfterWriteFailure(t *testing.T) {
	endpoint := &failOnceEndpoint{testEndpoint: newTestEndpoint()}
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := protocol.Data{
		PacketID: 1, DeadlineMicros: 100_900, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- receiver.Deliver(context.Background(), data) }()
	}
	failures := 0
	for range 2 {
		if err := <-results; err != nil {
			failures++
		}
	}
	if failures != 1 || len(endpoint.writes) != 1 {
		t.Fatalf("delivery failures = %d, successful writes = %d", failures, len(endpoint.writes))
	}
}

func TestReceiverRetriesDuplicateAfterDatagramDrop(t *testing.T) {
	endpoint := &dropOnceEndpoint{testEndpoint: newTestEndpoint()}
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := protocol.Data{
		PacketID: 1, DeadlineMicros: 100_900, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- receiver.Deliver(context.Background(), data) }()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("Deliver() error = %v", err)
		}
	}
	if len(endpoint.writes) != 1 {
		t.Fatalf("successful writes = %d, want 1", len(endpoint.writes))
	}
}

func TestReceiverFailedHighPacketIDDoesNotAdvanceWindow(t *testing.T) {
	endpoint := &failOnceEndpoint{testEndpoint: newTestEndpoint()}
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := protocol.Data{
		PacketID: 100, DeadlineMicros: 100_900, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	if err := receiver.Deliver(context.Background(), data); !errors.Is(err, ErrEndpointFailure) {
		t.Fatalf("Deliver() error = %v, want %v", err, ErrEndpointFailure)
	}
	data.PacketID = 1
	data.Payload[4] = 1
	if err := receiver.Deliver(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-endpoint.writes:
		if payload[4] != 1 {
			t.Fatalf("delivered payload marker = %d, want 1", payload[4])
		}
	default:
		t.Fatal("older valid PacketID was not delivered")
	}
}

func TestReceiverDeliverBatch(t *testing.T) {
	t.Run("PreservesPacketIDOrder", func(t *testing.T) {
		endpoint := &recordingBatchEndpoint{testEndpoint: newTestEndpoint()}
		receiver, err := NewReceiver(ReceiverConfig{
			Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64,
		})
		if err != nil {
			t.Fatal(err)
		}
		packetIDs := []uint64{2, 1, 3}
		data := make([]protocol.Data, len(packetIDs))
		for index, packetID := range packetIDs {
			payload := relayWireGuardPacket(wgpacket.TransportData)
			payload[4] = byte(packetID)
			data[index] = protocol.Data{PacketID: packetID, DeadlineMicros: 100_900, Payload: payload}
		}
		if err := receiver.deliverBatch(context.Background(), data); err != nil {
			t.Fatal(err)
		}
		if len(endpoint.calls) != 2 || endpoint.calls[0] != 1 || endpoint.calls[1] != 2 {
			t.Fatalf("UDP batch sizes = %v, want [1 2]", endpoint.calls)
		}
		for _, packetID := range packetIDs {
			payload := <-endpoint.writes
			if payload[4] != byte(packetID) {
				t.Fatalf("delivered payload marker = %d, want %d", payload[4], packetID)
			}
		}
	})

	t.Run("ReclassifiesAfterDatagramDrop", func(t *testing.T) {
		endpoint := &recordingBatchEndpoint{testEndpoint: newTestEndpoint(), dropFirst: true}
		receiver, err := NewReceiver(ReceiverConfig{
			Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		high := relayWireGuardPacket(wgpacket.TransportData)
		high[4] = 100
		low := relayWireGuardPacket(wgpacket.TransportData)
		low[4] = 1
		if err := receiver.deliverBatch(context.Background(), []protocol.Data{
			{PacketID: 100, DeadlineMicros: 100_900, Payload: high},
			{PacketID: 1, DeadlineMicros: 100_900, Payload: low},
		}); err != nil {
			t.Fatal(err)
		}
		if len(endpoint.calls) != 2 || endpoint.calls[0] != 1 || endpoint.calls[1] != 1 {
			t.Fatalf("UDP batch sizes = %v, want [1 1]", endpoint.calls)
		}
		select {
		case payload := <-endpoint.writes:
			if payload[4] != 1 {
				t.Fatalf("delivered payload marker = %d, want 1", payload[4])
			}
		default:
			t.Fatal("lower PacketID was not delivered after the higher PacketID dropped")
		}
	})
}

func TestReceiverSerializesUDPWrites(t *testing.T) {
	endpoint := &blockingWriteEndpoint{
		testEndpoint: newTestEndpoint(), entered: make(chan byte), release: make(chan struct{}),
	}
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := protocol.Data{
		PacketID: 1, DeadlineMicros: 100_900, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	second := first
	second.PacketID = 2
	first.Payload[4] = 1
	second.Payload = append([]byte(nil), first.Payload...)
	second.Payload[4] = 2
	firstResult := make(chan error, 1)
	go func() { firstResult <- receiver.Deliver(context.Background(), first) }()
	if marker := <-endpoint.entered; marker != 1 {
		t.Fatalf("first UDP write marker = %d, want 1", marker)
	}
	secondContext, cancelSecond := context.WithCancel(context.Background())
	secondResult := make(chan error, 1)
	go func() { secondResult <- receiver.Deliver(secondContext, second) }()
	select {
	case marker := <-endpoint.entered:
		t.Fatalf("concurrent UDP write entered with marker %d", marker)
	case <-time.After(20 * time.Millisecond):
	}
	cancelSecond()
	if err := <-secondResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting Deliver() error = %v, want %v", err, context.Canceled)
	}
	endpoint.release <- struct{}{}
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	retryResult := make(chan error, 1)
	go func() { retryResult <- receiver.Deliver(context.Background(), second) }()
	if marker := <-endpoint.entered; marker != 2 {
		t.Fatalf("retried UDP write marker = %d, want 2", marker)
	}
	endpoint.release <- struct{}{}
	if err := <-retryResult; err != nil {
		t.Fatal(err)
	}
}

func TestReceiverWriteDeadline(t *testing.T) {
	endpoint := &deadlineEndpoint{testEndpoint: newTestEndpoint(), deadlines: make(chan time.Time, 2)}
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: &testClock{now: 1000}, DeduplicationSize: 64, UDPWriteTimeout: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := protocol.Data{
		PacketID: 1, DeadlineMicros: 100_900, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	before := time.Now()
	if err := receiver.Deliver(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	deadline := <-endpoint.deadlines
	if deadline.Before(before.Add(time.Hour)) || deadline.After(time.Now().Add(time.Hour)) {
		t.Fatalf("write deadline = %v, want current time plus one hour", deadline)
	}

	data.PacketID++
	parentDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()
	if err := receiver.Deliver(ctx, data); err != nil {
		t.Fatal(err)
	}
	if deadline := <-endpoint.deadlines; !deadline.Equal(parentDeadline) {
		t.Fatalf("write deadline = %v, want parent deadline %v", deadline, parentDeadline)
	}
}

func TestReceiverDeadlineValidation(t *testing.T) {
	clock := &testClock{now: 1000}
	endpoint := newTestEndpoint()
	receiver, err := NewReceiver(ReceiverConfig{
		Endpoint: endpoint, Clock: clock, DeduplicationSize: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.ValidateDeadline(clock.now + protocol.MaxPacketLifetimeMicros); err != nil {
		t.Fatalf("maximum deadline error = %v", err)
	}
	if err := receiver.ValidateDeadline(clock.now + protocol.MaxPacketLifetimeMicros + protocol.DeadlineResolutionMicros); !errors.Is(
		err, ErrInvalidPacketDeadline,
	) {
		t.Fatalf("future deadline error = %v, want %v", err, ErrInvalidPacketDeadline)
	}

	receiver.UpdateClock(clockmap.Mapping{UncertaintyMicros: 100})
	clock.now = 1100
	data := protocol.Data{
		PacketID: 1, DeadlineMicros: 1000, Payload: relayWireGuardPacket(wgpacket.TransportData),
	}
	if err := receiver.Deliver(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if len(endpoint.writes) != 0 {
		t.Fatal("provably expired packet reached UDP")
	}

	receiver.UpdateClock(clockmap.Mapping{OffsetMicros: -10})
	if err := receiver.ValidateDeadline(9); !errors.Is(err, ErrInvalidPacketDeadline) ||
		!errors.Is(err, clockmap.ErrTimestampOverflow) {
		t.Fatalf("overflowing deadline error = %v", err)
	}
}

func TestNewLaneRequiresObserver(t *testing.T) {
	carrier := newTestCarrier()
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock,
		LaneID: protocol.LaneID(1), Generation: 1,
	}); !errors.Is(err, ErrInvalidLane) {
		t.Fatalf("NewLane() error = %v, want %v", err, ErrInvalidLane)
	}
}

func TestLane(t *testing.T) {
	carrier := newTestCarrier()
	endpoint := newTestEndpoint()
	lane := newTestLane(t, carrier, endpoint)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- lane.Run(ctx) }()

	payload := relayWireGuardPacket(wgpacket.TransportData)
	for range 2 {
		frame, err := protocol.MarshalData(protocol.Data{
			PacketID: 1, DeadlineMicros: 1900, Payload: payload,
		})
		if err != nil {
			t.Fatal(err)
		}
		carrier.reads <- frame
	}
	select {
	case got := <-endpoint.writes:
		if string(got) != string(payload) {
			t.Fatalf("delivered payload differs")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for UDP delivery")
	}
	select {
	case <-endpoint.writes:
		t.Fatal("duplicate packet reached UDP endpoint")
	case <-time.After(20 * time.Millisecond):
	}

	ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 7, SendMicros: 800})
	if err != nil {
		t.Fatal(err)
	}
	carrier.reads <- ping
	var sawPong bool
	var sawReport bool
	deadline := time.After(time.Second)
	for !sawPong || !sawReport {
		select {
		case batch := <-carrier.writes:
			for _, frame := range batch {
				switch frame.Type {
				case protocol.FramePong:
					pong, err := protocol.ParseTimingPong(frame)
					if err != nil || pong.ID != 7 || pong.ReceiveMicros != 1000 || pong.SendMicros != 1000 {
						t.Fatalf("pong = %+v, %v", pong, err)
					}
					sawPong = true
				case protocol.FrameDeliveryReport:
					report, err := protocol.ParseDeliveryReport(frame)
					if err != nil || report.DataPackets > 2 {
						t.Fatalf("report = %+v, %v", report, err)
					}
					sawReport = report.DataPackets == 2 && report.PingID == 7
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for pong and delivery report")
		}
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
}

func TestLaneProtocolErrorIsPreserved(t *testing.T) {
	carrier := newTestCarrier()
	lane := newTestLane(t, carrier, newTestEndpoint())
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	frame, err := protocol.MarshalData(protocol.Data{
		PacketID: 1, DeadlineMicros: 1900, Payload: []byte{0, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier.reads <- frame
	if err := <-result; !errors.Is(err, ErrInvalidWireGuardPacket) {
		t.Fatalf("Run() error = %v, want %v", err, ErrInvalidWireGuardPacket)
	}
	select {
	case batch := <-carrier.writes:
		if len(batch) != 1 {
			t.Fatalf("protocol error batch length = %d", len(batch))
		}
		remote, err := protocol.ParseErrorFrame(batch[0])
		if err != nil {
			t.Fatal(err)
		}
		if remote.Code != protocol.ErrorProtocolViolation || remote.Class != protocol.ErrorLaneRejected ||
			remote.Scope != protocol.ErrorScopeLane || remote.LaneID != (protocol.LaneID(1)) || remote.Generation != 1 {
			t.Fatalf("protocol error frame = %+v", remote)
		}
	case <-time.After(time.Second):
		t.Fatal("protocol violation did not produce an error frame")
	}
}

func TestLaneAbandonUsesAbortiveClose(t *testing.T) {
	for _, test := range []struct {
		name      string
		cause     error
		wantAbort bool
	}{
		{name: "SessionCancellation", cause: context.Canceled},
		{name: "LaneAbandonment", cause: ErrLaneAbandoned, wantAbort: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := newTestCarrier()
			lane := newTestLane(t, carrier, newTestEndpoint())
			ctx, cancel := context.WithCancelCause(context.Background())
			result := make(chan error, 1)
			go func() { result <- lane.Run(ctx) }()
			cancel(test.cause)
			if err := <-result; !errors.Is(err, test.cause) {
				t.Fatalf("Run() error = %v, want %v", err, test.cause)
			}
			select {
			case <-carrier.aborts:
				if !test.wantAbort {
					t.Fatal("ordinary session cancellation used an abortive close")
				}
			default:
				if test.wantAbort {
					t.Fatal("lane abandonment did not use an abortive close")
				}
			}
		})
	}
}

func TestLaneRequiresInitialClockSync(t *testing.T) {
	carrier := newTestCarrier()
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, ClockSyncTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	frame, err := protocol.MarshalData(protocol.Data{
		PacketID: 1, DeadlineMicros: 1900, Payload: relayWireGuardPacket(wgpacket.TransportData),
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier.reads <- frame
	if err := <-result; !errors.Is(err, ErrClockSyncRequired) {
		t.Fatalf("Run() error = %v, want %v", err, ErrClockSyncRequired)
	}
}

func TestLaneRejectsUnexpectedClockSync(t *testing.T) {
	frame, err := protocol.MarshalClockSync(protocol.ClockSync{
		ClientSendMicros: 1, ServerReceiveMicros: 1, ServerSendMicros: 1, ClientReceiveMicros: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name             string
		clockSyncTimeout time.Duration
		frames           int
	}{
		{name: "ClientDirection", frames: 1},
		{name: "RepeatedServerSync", clockSyncTimeout: time.Second, frames: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := newTestCarrier()
			lane := newTestLane(t, carrier, newTestEndpoint())
			lane.clockSyncTimeout = test.clockSyncTimeout
			result := make(chan error, 1)
			go func() { result <- lane.Run(context.Background()) }()
			for range test.frames {
				carrier.reads <- frame
			}
			if err := <-result; !errors.Is(err, ErrUnexpectedFrame) {
				t.Fatalf("Run() error = %v, want %v", err, ErrUnexpectedFrame)
			}
		})
	}
}

func TestLaneAcceptsSessionCloseOnlyWhenConfigured(t *testing.T) {
	frame, err := protocol.MarshalSessionClose(protocol.CloseClientShutdown)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		configure bool
		want      error
	}{
		{name: "ClientDirection", want: ErrUnexpectedFrame},
		{name: "ServerDirection", configure: true, want: ErrRemoteClosed},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := newTestCarrier()
			lane := newTestLane(t, carrier, newTestEndpoint())
			closed := make(chan protocol.CloseReason, 1)
			if test.configure {
				lane.sessionClose = func(reason protocol.CloseReason) { closed <- reason }
			}
			result := make(chan error, 1)
			go func() { result <- lane.Run(context.Background()) }()
			carrier.reads <- frame
			if err := <-result; !errors.Is(err, test.want) {
				t.Fatalf("Run() error = %v, want %v", err, test.want)
			}
			select {
			case reason := <-closed:
				if !test.configure || reason != protocol.CloseClientShutdown {
					t.Fatalf("session close reason = %d", reason)
				}
			default:
				if test.configure {
					t.Fatal("configured session close was not reported")
				}
			}
		})
	}
}

func TestLaneRejectsMisdirectedError(t *testing.T) {
	frame, err := protocol.MarshalErrorFrame(protocol.ErrorFrame{
		Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected, Scope: protocol.ErrorScopeLane,
		LaneID: protocol.LaneID(2), Generation: 1, Diagnostic: "misdirected",
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier := newTestCarrier()
	lane := newTestLane(t, carrier, newTestEndpoint())
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	carrier.reads <- frame
	if err := <-result; !errors.Is(err, ErrUnexpectedFrame) {
		t.Fatalf("Run() error = %v, want %v", err, ErrUnexpectedFrame)
	}
}

func TestLaneAppliesRemoteErrorScope(t *testing.T) {
	for _, test := range []struct {
		name      string
		value     protocol.ErrorFrame
		wantClose bool
	}{
		{name: "Lane", value: protocol.ErrorFrame{
			Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected,
			Scope: protocol.ErrorScopeLane, LaneID: protocol.LaneID(1), Generation: 1,
		}},
		{name: "Session", value: protocol.ErrorFrame{
			Code: protocol.ErrorUnavailable, Class: protocol.ErrorRetryable, Scope: protocol.ErrorScopeSession,
		}, wantClose: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			frame, err := protocol.MarshalErrorFrame(test.value)
			if err != nil {
				t.Fatal(err)
			}
			carrier := newTestCarrier()
			endpoint := newTestEndpoint()
			lane := newTestLane(t, carrier, endpoint)
			closed := make(chan struct{}, 1)
			lane.sessionFailure = func() { closed <- struct{}{} }
			result := make(chan error, 1)
			go func() { result <- lane.Run(context.Background()) }()
			carrier.reads <- frame
			err = <-result
			remote, ok := errors.AsType[*RemoteError](err)
			if !ok || remote.Value != test.value {
				t.Fatalf("Run() error = %v, want remote error %+v", err, test.value)
			}
			select {
			case <-closed:
				if !test.wantClose {
					t.Fatal("lane-scoped error notified the session owner")
				}
			default:
				if test.wantClose {
					t.Fatal("session-scoped error did not notify the session owner")
				}
			}
		})
	}
}

func TestLanePingTimeout(t *testing.T) {
	carrier := newTestCarrier()
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, PingInterval: 5 * time.Millisecond, PingTimeout: 15 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrPingTimeout) {
			t.Fatalf("Run() error = %v, want %v", err, ErrPingTimeout)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ping failure")
	}
}

func TestLanePingTimeoutPreemptsIdleBackoff(t *testing.T) {
	const (
		pingInterval = 10 * time.Millisecond
		pingTimeout  = 50 * time.Millisecond
	)
	carrier := newTestCarrier()
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, PingInterval: pingInterval, PingTimeout: pingTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()

	for range 4 {
		ping := awaitLanePing(t, carrier)
		pong, err := protocol.MarshalTimingPong(protocol.TimingPong{
			ID: ping.ID, PingSendMicros: ping.SendMicros, ReceiveMicros: clock.now, SendMicros: clock.now,
		})
		if err != nil {
			t.Fatal(err)
		}
		carrier.reads <- pong
	}
	awaitLanePing(t, carrier)
	lostAt := time.Now()
	select {
	case err := <-result:
		if !errors.Is(err, ErrPingTimeout) {
			t.Fatalf("Run() error = %v, want %v", err, ErrPingTimeout)
		}
		if elapsed := time.Since(lostAt); elapsed > 3*pingTimeout {
			t.Fatalf("ping timeout after idle backoff took %v, want at most %v", elapsed, 3*pingTimeout)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ping failure")
	}
}

func TestLanePingTimeoutStartsAfterCarrierWrite(t *testing.T) {
	carrier := newTestCarrier()
	carrier.writes = make(chan []protocol.Frame)
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := protocol.MarshalClockSync(protocol.ClockSync{})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, InitialFrames: []protocol.Frame{initial}, PingInterval: 5 * time.Millisecond,
		PingTimeout: 15 * time.Millisecond, WriteTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- lane.Run(ctx) }()
	select {
	case err := <-result:
		t.Fatalf("Run() returned before the queued ping was written: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case batch := <-carrier.writes:
		if len(batch) != 1 || batch[0].Type != protocol.FrameClockSync {
			t.Fatalf("initial carrier batch = %+v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out releasing the initial carrier write")
	}
	for {
		select {
		case batch := <-carrier.writes:
			if len(batch) == 1 && batch[0].Type == protocol.FramePing {
				cancel()
				if err := <-result; !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
				}
				return
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for the queued ping write")
		}
	}
}

func TestLaneWriterBoundsInternalControlBurst(t *testing.T) {
	carrier := newTestCarrier()
	carrier.writes = make(chan []protocol.Frame, 2*maximumConsecutiveControlFrames)
	lane := newTestLane(t, carrier, newTestEndpoint())
	control, err := protocol.MarshalTimingPong(protocol.TimingPong{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range maximumConsecutiveControlFrames + 1 {
		if !lane.SendControl(control, nil) {
			t.Fatal("SendControl() rejected a bounded test write")
		}
	}
	transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	transmission.wireDeadline = 1_000_000
	if err := lane.store.push(transmission); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- lane.write(ctx) }()
	controlWrites := 0
	for {
		select {
		case batch := <-carrier.writes:
			if len(batch) != 1 {
				t.Fatalf("carrier batch has %d frames, want 1", len(batch))
			}
			if batch[0].Type == protocol.FrameData {
				if controlWrites != maximumConsecutiveControlFrames {
					t.Fatalf("control writes before data = %d, want %d", controlWrites,
						maximumConsecutiveControlFrames)
				}
				cancel()
				if err := <-result; !errors.Is(err, context.Canceled) {
					t.Fatalf("write() error = %v, want %v", err, context.Canceled)
				}
				return
			}
			controlWrites++
		case <-time.After(time.Second):
			cancel()
			t.Fatal("timed out waiting for a data write")
		}
	}
}

func TestLaneWriteTimeout(t *testing.T) {
	carrier := newTestCarrier()
	carrier.writes = make(chan []protocol.Frame)
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, WriteTimeout: -time.Second,
	}); !errors.Is(err, ErrInvalidLane) {
		t.Fatalf("NewLane() error = %v, want %v", err, ErrInvalidLane)
	}
	initialFrame, err := protocol.MarshalClockSync(protocol.ClockSync{})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, InitialFrames: []protocol.Frame{initialFrame}, WriteTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run() error = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for carrier write failure")
	}
}

func TestLaneDataWriteFailureRetainsSentPrefix(t *testing.T) {
	writeErr := errors.New("scripted data write failure")
	carrier := newTestCarrier()
	carrier.dataWriteErr = writeErr
	lane := newTestLane(t, carrier, newTestEndpoint())
	transmission := schedulerTransmission(7, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := lane.store.push(transmission); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	select {
	case err := <-result:
		if !errors.Is(err, writeErr) {
			t.Fatalf("Run() error = %v, want %v", err, writeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for data write failure")
	}

	if lane.store.sent.len() != 1 || lane.store.normal.len() != 0 {
		t.Fatalf("store order = %d sent and %d queued, want 1 sent and 0 queued",
			lane.store.sent.len(), lane.store.normal.len())
	}
	if packets, bytes := lane.store.backlog(); packets != 1 || bytes != uint64(transmission.size) {
		t.Fatalf("retained backlog = %d packets and %d bytes", packets, bytes)
	}
	drained := lane.store.drain()
	if len(drained) != 1 || drained[0].packetID != transmission.packetID {
		t.Fatalf("drained transmissions = %+v", drained)
	}
}

func TestLaneDefaultWriteTimeout(t *testing.T) {
	lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
	if lane.writeTimeout != 10*time.Second {
		t.Fatalf("write timeout = %v, want 10s", lane.writeTimeout)
	}
}

func TestLaneRejectsMismatchedPong(t *testing.T) {
	carrier := newTestCarrier()
	endpoint := newTestEndpoint()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID(1),
		Generation: 1, PingInterval: 5 * time.Millisecond, PingTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- lane.Run(context.Background()) }()
	var ping protocol.TimingPing
	for ping.ID == 0 {
		select {
		case batch := <-carrier.writes:
			for _, frame := range batch {
				if frame.Type == protocol.FramePing {
					ping, err = protocol.ParseTimingPing(frame)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ping")
		}
	}
	pong, err := protocol.MarshalTimingPong(protocol.TimingPong{
		ID: ping.ID, PingSendMicros: ping.SendMicros + 1, ReceiveMicros: 1000, SendMicros: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier.reads <- pong
	if err := <-result; !errors.Is(err, ErrUnexpectedPong) {
		t.Fatalf("Run() error = %v, want %v", err, ErrUnexpectedPong)
	}
}

func awaitLanePing(t *testing.T, carrier *testCarrier) protocol.TimingPing {
	t.Helper()
	for {
		select {
		case batch := <-carrier.writes:
			for _, frame := range batch {
				if frame.Type != protocol.FramePing {
					continue
				}
				ping, err := protocol.ParseTimingPing(frame)
				if err != nil {
					t.Fatal(err)
				}
				return ping
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for ping")
		}
	}
}

func awaitControlWrite(t *testing.T, control <-chan controlWrite) controlWrite {
	t.Helper()
	select {
	case request := <-control:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for control write")
		return controlWrite{}
	}
}

func newTestLane(t *testing.T, carrier *testCarrier, endpoint *testEndpoint) *Lane {
	t.Helper()
	store, err := NewTransmissionStore(packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	laneID := protocol.LaneID(1)
	clock := &testClock{now: 1000}
	receiver, err := NewReceiver(ReceiverConfig{Endpoint: endpoint, Clock: clock, DeduplicationSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: laneID,
		Generation: 1, ReportInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return lane
}

func relayWireGuardPacket(kind wgpacket.Kind) []byte {
	var length int
	var messageType byte
	switch kind {
	case wgpacket.HandshakeInitiation:
		length, messageType = 148, 1
	case wgpacket.HandshakeResponse:
		length, messageType = 92, 2
	case wgpacket.CookieReply:
		length, messageType = 64, 3
	case wgpacket.TransportData:
		length, messageType = 32, 4
	}
	packet := make([]byte, length)
	packet[0] = messageType
	return packet
}
