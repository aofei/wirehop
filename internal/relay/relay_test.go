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
	lane *Lane
}

func (*testLaneObserver) ObserveDeliveryReport(context.Context, protocol.DeliveryReport, uint64) error {
	return nil
}

func (*testLaneObserver) ObserveTiming(protocol.LaneID, uint64, protocol.TimingPong, uint64) {}

func (*testLaneObserver) ObserveLaneAbandon(context.Context, protocol.LaneGeneration) error {
	return nil
}

func (o *testLaneObserver) RouteDeliveryReport(report protocol.DeliveryReport, complete func(bool)) bool {
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
	first := lanePhase(protocol.LaneID{1}, 1, spread)
	if first < 0 || first >= spread || first != lanePhase(protocol.LaneID{1}, 1, spread) {
		t.Fatalf("lanePhase() = %v", first)
	}
	if lanePhase(protocol.LaneID{1}, 1, 0) != 0 {
		t.Fatal("lanePhase() returned a phase without spread")
	}
}

func TestDeliveryProgressTracksCarrierOrder(t *testing.T) {
	progress := deliveryProgress{}
	if err := progress.addData(10); err != nil {
		t.Fatal(err)
	}
	if err := progress.addData(20); err != nil {
		t.Fatal(err)
	}
	report, revision, changed := progress.claim(protocol.LaneID{1}, 2, time.Now(), time.Second)
	if report.DataBytes != 30 || report.DataPackets != 2 || revision != 2 || !changed {
		t.Fatalf("claim() = %+v, revision %d, changed %t", report, revision, changed)
	}
	progress.complete(report, revision, true)
	if _, _, changed := progress.claim(protocol.LaneID{1}, 2, time.Now(), time.Second); changed {
		t.Fatal("reported progress remained changed")
	}
}

func TestDeliveryProgressRetriesUnsentClaim(t *testing.T) {
	progress := deliveryProgress{notify: make(chan struct{}, 1)}
	if err := progress.addData(10); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	report, revision, changed := progress.claim(protocol.LaneID{1}, 1, now, time.Second)
	if !changed {
		t.Fatal("initial progress was not claimed")
	}
	progress.complete(report, revision, false)
	for range reportPacketThreshold {
		if err := progress.addData(1); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-progress.notify:
		t.Fatal("pending failed claim published an immediate retry notification")
	default:
	}
	if _, _, changed := progress.claim(protocol.LaneID{1}, 1, now.Add(time.Second-time.Nanosecond), time.Second); changed {
		t.Fatal("pending progress was claimed before its retry interval")
	}
	retryReport, retryRevision, changed := progress.claim(
		protocol.LaneID{1}, 1, now.Add(time.Second), time.Second,
	)
	if !changed {
		t.Fatal("pending progress was not reclaimed after its retry interval")
	}
	progress.complete(retryReport, retryRevision, true)
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
				if err := progress.addData(test.bytes); err != nil {
					t.Fatal(err)
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
	t.Run("DataBytes", func(t *testing.T) {
		progress := deliveryProgress{dataBytes: ^uint64(0)}
		if err := progress.addData(1); !errors.Is(err, ErrCounterExhausted) {
			t.Fatalf("addData() error = %v, want %v", err, ErrCounterExhausted)
		}
		if progress.dataPackets != 0 || progress.revision != 0 {
			t.Fatalf("progress changed after rejected data: packets %d, revision %d",
				progress.dataPackets, progress.revision)
		}
	})

	t.Run("ProbePackets", func(t *testing.T) {
		progress := deliveryProgress{probePackets: ^uint64(0)}
		if err := progress.addProbe(1); !errors.Is(err, ErrCounterExhausted) {
			t.Fatalf("addProbe() error = %v, want %v", err, ErrCounterExhausted)
		}
		if progress.probeBytes != 0 || progress.revision != 0 {
			t.Fatalf("progress changed after rejected probe: bytes %d, revision %d",
				progress.probeBytes, progress.revision)
		}
	})
}

func TestLaneProbeNeeded(t *testing.T) {
	lane := &Lane{}
	observed := uint64(0)
	if !lane.probeNeeded(&observed) {
		t.Fatal("idle lane did not request a probe")
	}
	lane.dataWrites.Add(1)
	needed := lane.probeNeeded(&observed)
	if needed || observed != 1 {
		t.Fatalf("data write did not suppress probe or update observation: needed %t, observed %d",
			needed, observed)
	}
	if !lane.probeNeeded(&observed) {
		t.Fatal("lane did not resume probing after one idle interval")
	}
}

func TestLaneProbeProgressValidation(t *testing.T) {
	const probeSize = 32
	carrier := &blockingFrameCarrier{
		testCarrier: newTestCarrier(), writeEntered: make(chan struct{}),
	}
	lane := &Lane{
		carrier: carrier, clock: &testClock{now: 1}, writeTimeout: time.Second, probeSize: probeSize,
	}
	frame, err := protocol.MarshalProbe(protocol.Probe{ID: 1, Payload: make([]byte, probeSize)})
	if err != nil {
		t.Fatal(err)
	}
	frameBytes := uint64(protocol.ProbeFrameOverhead + probeSize)
	if lane.ValidateProbeProgress(1, frameBytes) {
		t.Fatal("probe progress was accepted before the probe reached the carrier writer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- lane.writeControl(ctx, controlWrite{
			build: func(uint64) (protocol.Frame, error) { return frame, nil },
		})
	}()
	select {
	case <-carrier.writeEntered:
	case <-time.After(time.Second):
		t.Fatal("probe did not reach the carrier writer")
	}
	if !lane.ValidateProbeProgress(1, frameBytes) {
		t.Fatal("exact probe progress was rejected while the carrier write was in progress")
	}
	if lane.ValidateProbeProgress(1, frameBytes-1) {
		t.Fatal("probe progress with an invalid byte count was accepted")
	}
	if lane.ValidateProbeProgress(2, 2*frameBytes) {
		t.Fatal("progress for an unwritten probe was accepted")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked probe write error = %v, want %v", err, context.Canceled)
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
	if pending, remaining := lane.pingState(now); !pending || remaining != time.Second {
		t.Fatalf("queued ping state = %t, %v", pending, remaining)
	}
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
}

func TestLanePingResumesActiveInterval(t *testing.T) {
	const (
		pingInterval = 10 * time.Millisecond
		pingTimeout  = 50 * time.Millisecond
	)
	lane := &Lane{
		clock: &testClock{now: 1000}, laneID: protocol.LaneID{1}, generation: 1,
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
	queue, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 2, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: 1234}
	policy := DeadlinePolicy{Control: 1500 * time.Microsecond, Transport: 3 * time.Millisecond}
	queuedAt := time.Now()
	ingress, err := newIngress(endpoint, queue, clock, policy, func() time.Time { return queuedAt })
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
	if want := queuedAt.Add(policy.Control).Round(0); item.Deadline != want {
		t.Fatalf("ingress deadline = %v, want suspend-aware wall deadline %v", item.Deadline, want)
	}
	if len(item.Value.Payload) == 0 || &item.Value.Payload[0] != &payload[0] {
		t.Fatal("ingress did not transfer packet ownership")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
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
	if err := receiver.ValidateDeadline(clock.now + protocol.MaxPacketLifetimeMicros + 1); !errors.Is(
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
		LaneID: protocol.LaneID{1}, Generation: 1,
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
					if err != nil || report.DataPackets != 2 {
						t.Fatalf("report = %+v, %v", report, err)
					}
					sawReport = true
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
			remote.Scope != protocol.ErrorScopeLane || remote.LaneID != (protocol.LaneID{1}) || remote.Generation != 1 {
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
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
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
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
		Generation: 1, RequireClockSync: true,
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
		requireClockSync bool
		frames           int
	}{
		{name: "ClientDirection", frames: 1},
		{name: "RepeatedServerSync", requireClockSync: true, frames: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := newTestCarrier()
			lane := newTestLane(t, carrier, newTestEndpoint())
			lane.requireClockSync = test.requireClockSync
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
		LaneID: protocol.LaneID{2}, Generation: 1, Diagnostic: "misdirected",
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
			Scope: protocol.ErrorScopeLane, LaneID: protocol.LaneID{1}, Generation: 1,
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
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
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
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
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
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
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
	carrier.writes = make(chan []protocol.Frame, 2*maximumConsecutiveControlWrites)
	lane := newTestLane(t, carrier, newTestEndpoint())
	control, err := protocol.MarshalTimingPong(protocol.TimingPong{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range maximumConsecutiveControlWrites + 1 {
		if !lane.SendControl(control, nil) {
			t.Fatal("SendControl() rejected a bounded test write")
		}
	}
	transmission := retainedTransmission{
		data: protocol.Data{
			PacketID: 1, DeadlineMicros: 1_000_000,
			Payload: relayWireGuardPacket(wgpacket.TransportData),
		},
		kind: wgpacket.TransportData, priority: packetqueue.PriorityNormal, deadline: time.Now().Add(time.Second),
	}
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
				if controlWrites != maximumConsecutiveControlWrites {
					t.Fatalf("control writes before data = %d, want %d", controlWrites,
						maximumConsecutiveControlWrites)
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
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
		Generation: 1, WriteTimeout: -time.Second,
	}); !errors.Is(err, ErrInvalidLane) {
		t.Fatalf("NewLane() error = %v, want %v", err, ErrInvalidLane)
	}
	initialFrame, err := protocol.MarshalClockSync(protocol.ClockSync{})
	if err != nil {
		t.Fatal(err)
	}
	lane, err := newObservedLane(LaneConfig{
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
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
	if len(drained) != 1 || drained[0].data.PacketID != transmission.data.PacketID {
		t.Fatalf("drained transmissions = %+v", drained)
	}
}

func TestLaneDefaultWriteTimeout(t *testing.T) {
	lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
	if lane.writeTimeout != 3*time.Second {
		t.Fatalf("write timeout = %v, want 3s", lane.writeTimeout)
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
		Carrier: carrier, Receiver: receiver, Store: store, Clock: clock, LaneID: protocol.LaneID{1},
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
	laneID := protocol.LaneID{1}
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
