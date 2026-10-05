package relay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var (
	// ErrInvalidLane indicates missing resources or invalid lane generation metadata.
	ErrInvalidLane = errors.New("invalid relay lane")
	// ErrInvalidWireGuardPacket indicates a data frame without a structurally valid WireGuard packet.
	ErrInvalidWireGuardPacket = errors.New("invalid WireGuard packet")
	// ErrRemoteClosed indicates an intentional close received from the relay peer.
	ErrRemoteClosed = errors.New("relay session closed by peer")
	// ErrPingTimeout indicates that a written timing request remained pending while the lane stopped receiving frames.
	ErrPingTimeout = errors.New("lane ping timed out without receive progress")
	// ErrUnexpectedPong indicates a response that does not match the sole outstanding timing request.
	ErrUnexpectedPong = errors.New("unexpected lane pong")
	// ErrClockSyncRequired indicates data arrived before the generation's initial clock mapping.
	ErrClockSyncRequired = errors.New("initial clock sync required")
	// ErrUnexpectedFrame indicates a valid frame type used outside its protocol phase.
	ErrUnexpectedFrame = errors.New("unexpected in-session frame")
	// ErrLaneAbandoned marks scheduler cancellation that must discard the generation's buffered carrier bytes.
	ErrLaneAbandoned = errors.New("relay lane abandoned")
)

// carrierAborter performs an abortive close for stale carrier generations.
type carrierAborter interface {
	Abort() error
}

// RemoteError preserves one machine-readable in-session error received from the peer.
type RemoteError struct {
	Value protocol.ErrorFrame
}

// Error returns stable remote metadata without exposing the peer's diagnostic text.
func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote relay error %d (class=%d, scope=%d)", e.Value.Code, e.Value.Class, e.Value.Scope)
}

const (
	// defaultControlCapacity bounds pending internally generated control writes.
	defaultControlCapacity = 64
	// defaultReportInterval bounds changed cumulative delivery-report delay.
	defaultReportInterval = 25 * time.Millisecond
	// reportPacketThreshold triggers feedback before the maximum delay under packet-dense load.
	reportPacketThreshold = 256
	// reportByteThreshold triggers feedback before the maximum delay under byte-dense load.
	reportByteThreshold = 256 * 1024
	// defaultUDPWriteTimeout bounds one local UDP delivery operation.
	defaultUDPWriteTimeout = time.Second
	// defaultPingInterval refreshes lane-local RTT without material background traffic.
	defaultPingInterval = time.Second
	// maximumIdlePingInterval bounds timing-request backoff without real data writes.
	maximumIdlePingInterval = 15 * time.Second
	// defaultPingTimeout bounds idle black-hole detection without reacting to one delayed response.
	defaultPingTimeout = 10 * time.Second
	// defaultCarrierWriteTimeout bounds one blocked stream or WebSocket write operation.
	defaultCarrierWriteTimeout = 10 * time.Second
	// defaultProbeInterval validates an otherwise idle lane and its feedback path.
	defaultProbeInterval = 2 * time.Second
	// maximumIdleProbeInterval bounds representative probe backoff without real data writes.
	maximumIdleProbeInterval = time.Minute
	// defaultProbeSize provides representative bounded idle carrier traffic.
	defaultProbeSize = 1200
	// maximumDataBatchFrames bounds nonblocking carrier write coalescing.
	maximumDataBatchFrames = 16
	// targetDataBatchBytes stops nonblocking coalescing after a compact write budget.
	targetDataBatchBytes = 64 * 1024
	// maximumConsecutiveControlFrames bounds internal-control priority by frame count while data is ready.
	maximumConsecutiveControlFrames = 8
	// protocolErrorWriteTimeout bounds best-effort rejection before a violating lane closes.
	protocolErrorWriteTimeout = 200 * time.Millisecond
)

// LaneConfig contains the resources and immutable identity for one connection generation.
type LaneConfig struct {
	Carrier  carrier.Conn
	Receiver *Receiver
	Store    *TransmissionStore
	Clock    Clock
	Observer LaneObserver
	// SessionClose handles client shutdown frames. A nil callback rejects them in this lane direction.
	SessionClose     func(protocol.CloseReason)
	SessionFailure   func()
	LaneID           protocol.LaneID
	Generation       uint64
	InitialFrames    []protocol.Frame
	ClockSyncTimeout time.Duration
	ClockSynced      func()
	ControlCapacity  int
	ReportInterval   time.Duration
	PingInterval     time.Duration
	PingTimeout      time.Duration
	WriteTimeout     time.Duration
	ProbeInterval    time.Duration
	ProbeSize        int
}

// Lane runs one full-duplex carrier generation with one carrier writer.
type Lane struct {
	carrier           carrier.Conn
	receiver          *Receiver
	store             *TransmissionStore
	clock             Clock
	observer          LaneObserver
	sessionClose      func(protocol.CloseReason)
	sessionFailure    func()
	laneID            protocol.LaneID
	generation        uint64
	initialFrames     []protocol.Frame
	clockSyncTimeout  time.Duration
	clockSynced       func()
	control           chan controlWrite
	reportInterval    time.Duration
	pingInterval      time.Duration
	pingTimeout       time.Duration
	writeTimeout      time.Duration
	probeInterval     time.Duration
	probeSize         int
	dataWrites        atomic.Uint64
	probeWrites       atomic.Uint64
	controlBatch      [maximumConsecutiveControlFrames]protocol.Frame
	dataBatch         [maximumDataBatchFrames]protocol.Data
	dataOwnership     [maximumDataBatchFrames]Packet
	progress          deliveryProgress
	pingMu            sync.Mutex
	pendingPingID     uint64
	pendingPingAt     time.Time
	pendingPingMicros uint64
	pendingPingBuilt  bool
	lastReceivedAt    time.Time
	pingChanged       chan struct{}
}

// controlWrite builds a control frame immediately before the carrier writer sends it.
type controlWrite struct {
	build func(uint64) (protocol.Frame, error)
	sent  func()
}

// deliveryProgress stores cumulative parse progress for one incoming lane generation.
type deliveryProgress struct {
	mu                  sync.Mutex
	dataBytes           uint64
	dataPackets         uint64
	probeBytes          uint64
	probePackets        uint64
	revision            uint64
	reported            uint64
	reportedDataBytes   uint64
	reportedDataPackets uint64
	pendingRevision     uint64
	pendingAt           time.Time
	notify              chan struct{}
}

// NewLane validates config and returns one relay lane generation.
func NewLane(config LaneConfig) (*Lane, error) {
	if config.Carrier == nil || config.Receiver == nil || config.Store == nil || config.Clock == nil ||
		config.Observer == nil || config.LaneID.IsZero() || config.Generation == 0 {
		return nil, ErrInvalidLane
	}
	if config.ControlCapacity == 0 {
		config.ControlCapacity = defaultControlCapacity
	}
	if config.ReportInterval == 0 {
		config.ReportInterval = defaultReportInterval
	}
	if config.PingInterval == 0 {
		config.PingInterval = defaultPingInterval
	}
	if config.PingTimeout == 0 {
		config.PingTimeout = defaultPingTimeout
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaultCarrierWriteTimeout
	}
	if config.ProbeInterval == 0 {
		config.ProbeInterval = defaultProbeInterval
	}
	if config.ProbeSize == 0 {
		config.ProbeSize = defaultProbeSize
	}
	if config.ControlCapacity < 1 || config.ReportInterval <= 0 || config.PingInterval <= 0 ||
		config.PingTimeout <= config.PingInterval || config.WriteTimeout <= 0 || config.ClockSyncTimeout < 0 ||
		config.ProbeInterval <= 0 || config.ProbeSize < 0 || config.ProbeSize > protocol.MaxProbePayloadSize {
		return nil, ErrInvalidLane
	}
	initialFrames := append([]protocol.Frame(nil), config.InitialFrames...)
	return &Lane{
		carrier: config.Carrier, receiver: config.Receiver, store: config.Store, clock: config.Clock,
		observer: config.Observer, sessionClose: config.SessionClose, sessionFailure: config.SessionFailure,
		laneID: config.LaneID, generation: config.Generation, initialFrames: initialFrames,
		clockSyncTimeout: config.ClockSyncTimeout, clockSynced: config.ClockSynced,
		control: make(chan controlWrite, config.ControlCapacity), reportInterval: config.ReportInterval,
		pingInterval: config.PingInterval, pingTimeout: config.PingTimeout, writeTimeout: config.WriteTimeout,
		probeInterval: config.ProbeInterval, probeSize: config.ProbeSize,
		progress: deliveryProgress{notify: make(chan struct{}, 1)}, pingChanged: make(chan struct{}, 1),
	}, nil
}

// Run serves the lane until cancellation, carrier failure, or a protocol error.
func (l *Lane) Run(ctx context.Context) error {
	parent := ctx
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	errorsChannel := make(chan error, 5)
	var workers sync.WaitGroup
	start := func(run func() error) {
		workers.Go(func() {
			l.runPart(errorsChannel, run)
		})
	}
	start(func() error { return l.write(ctx) })
	start(func() error { return l.read(ctx) })
	start(func() error { return l.report(ctx) })
	start(func() error { return l.ping(ctx) })
	start(func() error { return l.probe(ctx) })
	err := <-errorsChannel
	if IsProtocolViolation(err) {
		l.reportProtocolViolation(parent, err)
	}
	cancel()
	if errors.Is(context.Cause(parent), ErrLaneAbandoned) || errors.Is(err, monotime.ErrResumed) {
		if aborter, ok := l.carrier.(carrierAborter); ok {
			aborter.Abort()
		} else {
			l.carrier.Close()
		}
	} else {
		l.carrier.Close()
	}
	workers.Wait()
	if parent.Err() != nil {
		return context.Cause(parent)
	}
	return err
}

// reportProtocolViolation sends one bounded lane-scoped error before carrier teardown.
func (l *Lane) reportProtocolViolation(parent context.Context, violation error) {
	frame, err := protocol.MarshalErrorFrame(protocol.ErrorFrame{
		Code: protocol.ErrorProtocolViolation, Class: protocol.ErrorLaneRejected, Scope: protocol.ErrorScopeLane,
		LaneID: l.laneID, Generation: l.generation, Diagnostic: violation.Error(),
	})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, protocolErrorWriteTimeout)
	defer cancel()
	written := make(chan struct{})
	if !l.SendControl(frame, func() { close(written) }) {
		return
	}
	select {
	case <-written:
	case <-ctx.Done():
	}
}

// runPart reports the result of one lane worker.
func (l *Lane) runPart(results chan<- error, run func() error) {
	err := run()
	if err == nil {
		err = ErrRemoteClosed
	}
	results <- err
}

// ping periodically queues one lane-local timing request.
func (l *Lane) ping(ctx context.Context) error {
	resume := monotime.NewResumeDetector(l.clock.NowMicros)
	phase := lanePhase(l.laneID, l.generation, l.pingInterval/4)
	nextPingAt := time.Now().Add(phase)
	timer := time.NewTimer(phase)
	defer timer.Stop()
	identifier := uint64(0)
	interval := l.pingInterval
	maximumInterval := max(l.pingInterval, maximumIdlePingInterval)
	observedDataWrites := l.dataWrites.Load()
	dataActive := false
	for {
		select {
		case <-timer.C:
		case <-l.pingChanged:
		case <-ctx.Done():
			return ctx.Err()
		}

		now := time.Now()
		if resume.Resumed() {
			return monotime.ErrResumed
		}
		if currentDataWrites := l.dataWrites.Load(); currentDataWrites != observedDataWrites {
			observedDataWrites = currentDataWrites
			dataActive = true
			interval = l.pingInterval
			activePingAt := now.Add(l.pingInterval)
			if activePingAt.Before(nextPingAt) {
				nextPingAt = activePingAt
			}
		}
		if pending, remaining := l.pingState(now); pending {
			if remaining <= 0 {
				return ErrPingTimeout
			}
			timer.Reset(remaining)
			continue
		}
		if now.Before(nextPingAt) {
			timer.Reset(nextPingAt.Sub(now))
			continue
		}

		identifier++
		if identifier == 0 {
			return ErrCounterExhausted
		}
		id := identifier
		request := controlWrite{
			build: func(sendMicros uint64) (protocol.Frame, error) {
				l.recordPingSend(id, sendMicros)
				return protocol.MarshalTimingPing(protocol.TimingPing{ID: id, SendMicros: sendMicros})
			},
			sent: func() { l.recordPingWritten(id, time.Now()) },
		}
		if dataActive {
			interval = l.pingInterval
			dataActive = false
		} else {
			interval = nextIdleInterval(interval, maximumInterval)
		}
		l.startPing(id)
		select {
		case l.control <- request:
		default:
			l.cancelPing(id)
			interval = l.pingInterval
		}
		nextPingAt = now.Add(interval)
		timer.Reset(l.pingInterval)
	}
}

// pingState reports whether a request is pending and how long remains in its receive inactivity budget.
func (l *Lane) pingState(now time.Time) (bool, time.Duration) {
	l.pingMu.Lock()
	defer l.pingMu.Unlock()
	if l.pendingPingID == 0 {
		return false, 0
	}
	if l.pendingPingAt.IsZero() {
		return true, l.pingInterval
	}
	since := l.pendingPingAt
	if l.lastReceivedAt.After(since) {
		since = l.lastReceivedAt
	}
	return true, l.pingTimeout - now.Sub(since)
}

// recordReceive preserves a progressing stream while a timing response waits behind TCP data or retransmissions.
func (l *Lane) recordReceive() {
	l.pingMu.Lock()
	l.lastReceivedAt = time.Now()
	l.pingMu.Unlock()
}

// startPing records a timing request before exposing it to the carrier writer.
func (l *Lane) startPing(identifier uint64) {
	l.pingMu.Lock()
	l.pendingPingID = identifier
	l.pendingPingAt = time.Time{}
	l.pendingPingMicros = 0
	l.pendingPingBuilt = false
	l.pingMu.Unlock()
}

// recordPingSend binds the request to its writer-sampled timestamp before carrier exposure.
func (l *Lane) recordPingSend(identifier, sendMicros uint64) {
	l.pingMu.Lock()
	if l.pendingPingID == identifier {
		l.pendingPingMicros = sendMicros
		l.pendingPingBuilt = true
	}
	l.pingMu.Unlock()
}

// recordPingWritten starts the inactivity timeout after the carrier accepts the complete request.
func (l *Lane) recordPingWritten(identifier uint64, now time.Time) {
	l.pingMu.Lock()
	written := l.pendingPingID == identifier
	if written {
		l.pendingPingAt = now
	}
	l.pingMu.Unlock()
	if written {
		l.signalPingChanged()
	}
}

// cancelPing removes a request that could not enter the bounded control queue.
func (l *Lane) cancelPing(identifier uint64) {
	l.pingMu.Lock()
	if l.pendingPingID == identifier {
		l.pendingPingID = 0
		l.pendingPingAt = time.Time{}
		l.pendingPingMicros = 0
		l.pendingPingBuilt = false
	}
	l.pingMu.Unlock()
}

// completePing accepts only a matching response for a request built by the carrier writer.
func (l *Lane) completePing(identifier, sendMicros uint64) bool {
	l.pingMu.Lock()
	if !l.pendingPingBuilt || l.pendingPingID != identifier || l.pendingPingMicros != sendMicros {
		l.pingMu.Unlock()
		return false
	}
	l.pendingPingID = 0
	l.pendingPingAt = time.Time{}
	l.pendingPingMicros = 0
	l.pendingPingBuilt = false
	l.pingMu.Unlock()
	l.signalPingChanged()
	return true
}

// signalPingChanged wakes the ping state machine after timing-state or real-data progress.
func (l *Lane) signalPingChanged() {
	select {
	case l.pingChanged <- struct{}{}:
	default:
	}
}

// probe periodically queues bounded opaque traffic for idle carrier and feedback-path validation.
func (l *Lane) probe(ctx context.Context) error {
	timer := time.NewTimer(l.probeInterval + lanePhase(l.laneID, l.generation, l.probeInterval/4))
	defer timer.Stop()
	interval := l.probeInterval
	maximumInterval := max(l.probeInterval, maximumIdleProbeInterval)
	observedDataWrites := l.dataWrites.Load()
	frame := protocol.Frame{Type: protocol.FrameProbe, Payload: make([]byte, l.probeSize)}
	for {
		select {
		case <-timer.C:
			if !l.probeNeeded(&observedDataWrites) {
				interval = l.probeInterval
				timer.Reset(l.probeInterval)
				continue
			}
			request := controlWrite{build: func(uint64) (protocol.Frame, error) { return frame, nil }}
			select {
			case l.control <- request:
				interval = nextIdleInterval(interval, maximumInterval)
			default:
				interval = l.probeInterval
			}
			timer.Reset(interval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// nextIdleInterval doubles current without exceeding maximum.
func nextIdleInterval(current, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}

// probeNeeded reports whether one complete probe interval passed without a successful data write.
func (l *Lane) probeNeeded(observed *uint64) bool {
	current := l.dataWrites.Load()
	if current == *observed {
		return true
	}
	*observed = current
	return false
}

// lanePhase returns a stable sub-interval offset that spreads simultaneous lane startup work.
func lanePhase(laneID protocol.LaneID, generation uint64, spread time.Duration) time.Duration {
	if spread <= 0 {
		return 0
	}
	hash := generation
	for _, value := range laneID {
		hash = hash*1099511628211 ^ uint64(value)
	}
	return time.Duration(hash % uint64(spread))
}

// write serializes initial, control, and data frames onto the carrier.
func (l *Lane) write(ctx context.Context) error {
	carrierContext := context.WithoutCancel(ctx)
	if len(l.initialFrames) > 0 {
		if err := l.writeFrames(carrierContext, l.initialFrames); err != nil {
			return err
		}
	}
	controlFrames := 0
	for {
		if controlFrames == maximumConsecutiveControlFrames {
			select {
			case <-l.store.Ready():
				if err := l.writeReadyData(carrierContext); err != nil {
					return err
				}
				controlFrames = 0
				continue
			default:
				controlFrames = 0
			}
		}
		select {
		case request := <-l.control:
			count, err := l.writeControlBatch(carrierContext, request, maximumConsecutiveControlFrames-controlFrames)
			if err != nil {
				return err
			}
			controlFrames += count
		default:
			select {
			case request := <-l.control:
				count, err := l.writeControlBatch(carrierContext, request, maximumConsecutiveControlFrames-controlFrames)
				if err != nil {
					return err
				}
				controlFrames += count
			case <-l.store.Ready():
				if err := l.writeReadyData(carrierContext); err != nil {
					return err
				}
				controlFrames = 0
			case <-l.store.Done():
				return ErrLaneAbandoned
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// writeReadyData writes one notified data batch and normalizes store lifecycle results.
func (l *Lane) writeReadyData(ctx context.Context) error {
	err := l.writeDataBatch(ctx)
	if errors.Is(err, packetqueue.ErrClosed) {
		return ErrLaneAbandoned
	}
	if errors.Is(err, packetqueue.ErrEmpty) {
		return nil
	}
	return err
}

// writeDataBatch coalesces only data already available without introducing a batching delay.
func (l *Lane) writeDataBatch(ctx context.Context) error {
	count, err := l.store.takeBatch(l.dataBatch[:], l.dataOwnership[:], targetDataBatchBytes)
	if err != nil {
		return err
	}
	err = l.writeDataFrames(ctx, l.dataBatch[:count])
	clear(l.dataBatch[:count])
	for index := range count {
		l.dataOwnership[index].Release()
	}
	return err
}

// writeControlBatch drains ready controls within the remaining fairness budget. Timing and lifecycle frames end
// the batch. Success callbacks run in queue order after the complete carrier write succeeds.
func (l *Lane) writeControlBatch(ctx context.Context, first controlWrite, limit int) (int, error) {
	frames := l.controlBatch[:]
	defer clear(frames)
	var callbacks [maximumConsecutiveControlFrames]func()
	count := 0
	request := first
collect:
	for {
		frame, err := request.build(l.clock.NowMicros())
		if err != nil {
			return count, err
		}
		frames[count] = frame
		callbacks[count] = request.sent
		count++
		switch frame.Type {
		case protocol.FramePing, protocol.FramePong, protocol.FrameClockSync,
			protocol.FrameSessionClose, protocol.FrameLaneAbandon, protocol.FrameError:
			break collect
		}
		if count == limit {
			break
		}
		select {
		case request = <-l.control:
		default:
			break collect
		}
	}
	if err := l.writeFrames(ctx, frames[:count]); err != nil {
		return count, err
	}
	for _, callback := range callbacks[:count] {
		if callback != nil {
			callback()
		}
	}
	return count, nil
}

// writeFrames writes one control batch within the carrier stall budget.
func (l *Lane) writeFrames(ctx context.Context, frames []protocol.Frame) error {
	var probes uint64
	for _, frame := range frames {
		if frame.Type == protocol.FrameProbe {
			probes++
		}
	}
	if probes != 0 {
		if probes > ^uint64(0)-l.probeWrites.Load() {
			return ErrCounterExhausted
		}
		// Publish before writing because peer feedback can return on another lane before this call completes.
		l.probeWrites.Add(probes)
	}
	return carrier.WriteFramesWithin(ctx, l.carrier, frames, l.writeTimeout)
}

// writeDataFrames writes one data batch within the carrier stall budget.
func (l *Lane) writeDataFrames(ctx context.Context, data []protocol.Data) error {
	if err := carrier.WriteDataBatchWithin(ctx, l.carrier, data, l.writeTimeout); err != nil {
		return err
	}
	l.dataWrites.Add(1)
	l.signalPingChanged()
	return nil
}

// SendControl queues one fixed control frame and invokes onSent after carrier write completion.
func (l *Lane) SendControl(frame protocol.Frame, onSent func()) bool {
	if !frame.Type.Valid() || frame.Type == protocol.FrameData {
		return false
	}
	request := controlWrite{build: func(uint64) (protocol.Frame, error) { return frame, nil }, sent: onSent}
	select {
	case l.control <- request:
		return true
	default:
		return false
	}
}

// ValidateProbeProgress reports whether cumulative feedback matches probes exposed to this generation's carrier writer.
func (l *Lane) ValidateProbeProgress(packets, bytes uint64) bool {
	frameBytes := uint64(protocol.FrameSize(l.probeSize))
	return packets <= l.probeWrites.Load() && packets <= ^uint64(0)/frameBytes && bytes == packets*frameBytes
}

// read parses incoming frames and delivers accepted data to the UDP endpoint.
func (l *Lane) read(ctx context.Context) error {
	carrierContext := context.WithoutCancel(ctx)
	clockSyncPending := l.clockSyncTimeout > 0
	readContext := carrierContext
	var cancelSync context.CancelFunc
	if clockSyncPending {
		// Run owns teardown on lane cancellation. Only this phase deadline may cancel the carrier read directly.
		readContext, cancelSync = context.WithTimeout(carrierContext, l.clockSyncTimeout)
		defer cancelSync()
	}
	var frames [datagram.MaximumBatchSize]protocol.Frame
	for {
		count, err := carrier.ReadFrames(readContext, l.carrier, frames[:])
		if err != nil && count == 0 {
			return err
		}
		for index := 0; index < count; {
			frame := frames[index]
			if clockSyncPending && frame.Type != protocol.FrameClockSync {
				return ErrClockSyncRequired
			}
			if frame.Type == protocol.FrameData {
				end := index + 1
				for end < count && frames[end].Type == protocol.FrameData {
					end++
				}
				if err := l.readDataBatch(ctx, frames[index:end]); err != nil {
					return err
				}
				index = end
				continue
			}
			if err := l.readControl(ctx, frame, &clockSyncPending); err != nil {
				return err
			}
			l.recordReceive()
			if readContext != carrierContext && !clockSyncPending {
				cancelSync()
				readContext = carrierContext
				if l.clockSynced != nil {
					l.clockSynced()
				}
			}
			index++
		}
		clear(frames[:count])
		if err != nil {
			return err
		}
	}
}

// readDataBatch validates, acknowledges, and delivers one consecutive data-frame vector.
func (l *Lane) readDataBatch(ctx context.Context, frames []protocol.Frame) error {
	var data [datagram.MaximumBatchSize]protocol.Data
	var deadlines [datagram.MaximumBatchSize]uint64
	var sizes [datagram.MaximumBatchSize]int
	for index, frame := range frames {
		packet, err := protocol.ParseData(frame)
		if err != nil {
			return err
		}
		if !wgpacket.Classify(packet.Payload).Accepted() {
			return ErrInvalidWireGuardPacket
		}
		frameSize, err := protocol.DataFrameSize(packet)
		if err != nil {
			return err
		}
		data[index] = packet
		deadlines[index] = packet.DeadlineMicros
		sizes[index] = frameSize
	}
	if err := l.receiver.ValidateDeadlines(deadlines[:len(frames)]); err != nil {
		return err
	}
	for _, frameSize := range sizes[:len(frames)] {
		if err := l.progress.addData(frameSize); err != nil {
			return err
		}
	}
	l.recordReceive()
	return l.receiver.deliverBatch(ctx, data[:len(frames)])
}

// readControl processes one non-data frame and updates clock-sync admission state.
func (l *Lane) readControl(ctx context.Context, frame protocol.Frame, clockSyncPending *bool) error {
	switch frame.Type {
	case protocol.FramePing:
		return l.readPing(frame)
	case protocol.FramePong:
		pong, err := protocol.ParseTimingPong(frame)
		if err != nil {
			return err
		}
		if !l.completePing(pong.ID, pong.PingSendMicros) {
			return ErrUnexpectedPong
		}
		receiveMicros := l.clock.NowMicros()
		mapping, err := clockmap.Estimate(clockmap.Sample{
			LocalSendMicros: pong.PingSendMicros, RemoteReceiveMicros: pong.ReceiveMicros,
			RemoteSendMicros: pong.SendMicros, LocalReceiveMicros: receiveMicros,
		})
		if err != nil {
			return err
		}
		if ClockMappingUsable(mapping) {
			l.receiver.UpdateClock(mapping.Inverse())
			l.observer.ObserveTiming(l.laneID, l.generation, pong, receiveMicros)
		}
		return nil
	case protocol.FrameClockSync:
		if !*clockSyncPending {
			return ErrUnexpectedFrame
		}
		if err := l.readClockSync(frame); err != nil {
			return err
		}
		*clockSyncPending = false
		return nil
	case protocol.FrameProbe:
		probe, err := protocol.ParseProbe(frame)
		if err != nil {
			return err
		}
		return l.progress.addProbe(protocol.FrameSize(len(probe.Payload)))
	case protocol.FrameDeliveryReport:
		report, err := protocol.ParseDeliveryReport(frame)
		if err != nil {
			return err
		}
		return l.observer.ObserveDeliveryReport(ctx, protocol.LaneGeneration{
			LaneID: l.laneID, Generation: l.generation,
		}, report, l.clock.NowMicros())
	case protocol.FrameSessionClose:
		reason, err := protocol.ParseSessionClose(frame)
		if err != nil {
			return err
		}
		if l.sessionClose == nil {
			return ErrUnexpectedFrame
		}
		l.sessionClose(reason)
		return ErrRemoteClosed
	case protocol.FrameLaneAbandon:
		generation, err := protocol.ParseLaneAbandon(frame)
		if err != nil {
			return err
		}
		return l.observer.ObserveLaneAbandon(ctx, generation)
	case protocol.FrameError:
		value, err := protocol.ParseErrorFrame(frame)
		if err != nil {
			return err
		}
		if value.Scope == protocol.ErrorScopeLane &&
			(value.LaneID != l.laneID || value.Generation != l.generation) {
			return ErrUnexpectedFrame
		}
		if value.Scope == protocol.ErrorScopeSession && l.sessionFailure != nil {
			l.sessionFailure()
		}
		return &RemoteError{Value: value}
	default:
		return fmt.Errorf("%w: type %d", ErrUnexpectedFrame, frame.Type)
	}
}

// readClockSync updates the sender-to-receiver monotonic clock mapping.
func (l *Lane) readClockSync(frame protocol.Frame) error {
	syncFrame, err := protocol.ParseClockSync(frame)
	if err != nil {
		return err
	}
	mapping, err := clockmap.Estimate(clockmap.Sample{
		LocalSendMicros: syncFrame.ClientSendMicros, RemoteReceiveMicros: syncFrame.ServerReceiveMicros,
		RemoteSendMicros: syncFrame.ServerSendMicros, LocalReceiveMicros: syncFrame.ClientReceiveMicros,
	})
	if err != nil {
		return err
	}
	if !ClockMappingUsable(mapping) {
		return ErrStaleClockSample
	}
	l.receiver.UpdateClock(mapping)
	return nil
}

// readPing queues a timing response without introducing another carrier writer.
func (l *Lane) readPing(frame protocol.Frame) error {
	ping, err := protocol.ParseTimingPing(frame)
	if err != nil {
		return err
	}
	receiveMicros := l.clock.NowMicros()
	request := controlWrite{build: func(sendMicros uint64) (protocol.Frame, error) {
		return protocol.MarshalTimingPong(protocol.TimingPong{
			ID: ping.ID, PingSendMicros: ping.SendMicros, ReceiveMicros: receiveMicros, SendMicros: sendMicros,
		})
	}}
	select {
	case l.control <- request:
	default:
	}
	return nil
}

// report arms feedback deadlines only while unreported progress exists.
func (l *Lane) report(ctx context.Context) error {
	timer := time.NewTimer(l.reportInterval)
	timer.Stop()
	defer timer.Stop()
	var ready <-chan time.Time
	var due time.Time
	for {
		select {
		case <-ready:
			due = time.Time{}
			l.queueDeliveryReport()
		case <-l.progress.notify:
		case <-ctx.Done():
			return ctx.Err()
		}
		now := time.Now()
		delay, changed := l.progress.reportDelay(now, l.reportInterval)
		if !changed {
			timer.Stop()
			ready = nil
			due = time.Time{}
			continue
		}
		next := now.Add(delay)
		if due.IsZero() || next.Before(due) {
			due = next
			timer.Reset(max(0, due.Sub(now)))
			ready = timer.C
		}
	}
}

// reportDelay describes the next feedback deadline without polling an idle progress accumulator.
func (p *deliveryProgress) reportDelay(now time.Time, interval time.Duration) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.revision == p.reported {
		return 0, false
	}
	if p.pendingRevision != 0 {
		return max(0, 4*interval-now.Sub(p.pendingAt)), true
	}
	if p.thresholdReachedLocked() {
		return 0, true
	}
	return interval, true
}

// queueDeliveryReport claims and queues the newest cumulative delivery progress.
func (l *Lane) queueDeliveryReport() {
	report, revision, changed := l.progress.claim(
		l.laneID, l.generation, time.Now(), 4*l.reportInterval,
	)
	if !changed {
		return
	}
	complete := func(sent bool) { l.progress.complete(report, revision, sent) }
	if !l.observer.RouteDeliveryReport(report, complete) {
		complete(false)
	}
}

// addData records one parsed data frame in carrier order.
func (p *deliveryProgress) addData(bytes int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if uint64(bytes) > ^uint64(0)-p.dataBytes || p.dataPackets == ^uint64(0) || p.revision == ^uint64(0) {
		return ErrCounterExhausted
	}
	p.dataBytes += uint64(bytes)
	p.dataPackets++
	p.revision++
	p.notifyProgressLocked()
	return nil
}

// addProbe records one parsed probe frame.
func (p *deliveryProgress) addProbe(bytes int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if uint64(bytes) > ^uint64(0)-p.probeBytes || p.probePackets == ^uint64(0) || p.revision == ^uint64(0) {
		return ErrCounterExhausted
	}
	p.probeBytes += uint64(bytes)
	p.probePackets++
	p.revision++
	p.notifyProgressLocked()
	return nil
}

// claim returns changed cumulative progress and marks it pending for bounded duplicate suppression.
func (p *deliveryProgress) claim(laneID protocol.LaneID, generation uint64, now time.Time,
	retryAfter time.Duration) (protocol.DeliveryReport, uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.revision == p.reported || p.pendingRevision != 0 && now.Sub(p.pendingAt) < retryAfter {
		return protocol.DeliveryReport{}, 0, false
	}
	report := protocol.DeliveryReport{
		LaneID: laneID, Generation: generation, DataBytes: p.dataBytes, DataPackets: p.dataPackets,
		ProbeBytes: p.probeBytes, ProbePackets: p.probePackets,
	}
	p.pendingRevision = p.revision
	p.pendingAt = now
	return report, p.revision, true
}

// complete records whether one claimed report completed a carrier write.
func (p *deliveryProgress) complete(report protocol.DeliveryReport, revision uint64, sent bool) {
	p.mu.Lock()
	if sent && revision > p.reported {
		p.reported = revision
		p.reportedDataBytes = report.DataBytes
		p.reportedDataPackets = report.DataPackets
	}
	if sent && p.pendingRevision == revision {
		p.pendingRevision = 0
		p.pendingAt = time.Time{}
	}
	p.mu.Unlock()
	p.signal()
}

// notifyProgressLocked signals the first unreported change and either immediate-feedback data threshold.
func (p *deliveryProgress) notifyProgressLocked() {
	if p.pendingRevision == 0 && (p.revision-p.reported == 1 || p.thresholdReachedLocked()) {
		p.signal()
	}
}

// thresholdReachedLocked reports whether unreported data warrants feedback before the maximum delay.
func (p *deliveryProgress) thresholdReachedLocked() bool {
	return p.dataPackets-p.reportedDataPackets >= reportPacketThreshold ||
		p.dataBytes-p.reportedDataBytes >= reportByteThreshold
}

// signal publishes coalesced changed-progress notification without blocking a carrier reader.
func (p *deliveryProgress) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}
