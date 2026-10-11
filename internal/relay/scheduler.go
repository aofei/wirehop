package relay

import (
	"cmp"
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var (
	// ErrInvalidScheduler indicates missing ingress state.
	ErrInvalidScheduler = errors.New("invalid relay scheduler")
	// ErrInvalidRegistration indicates incomplete lane scheduling metadata.
	ErrInvalidRegistration = errors.New("invalid lane registration")
	// ErrStaleLane indicates a non-increasing connection generation.
	ErrStaleLane = errors.New("stale lane generation")
	// ErrNoActiveLane indicates that no carrier can accept a control frame.
	ErrNoActiveLane = errors.New("no active relay lane")
)

const (
	// defaultInitialRTTMicros is the RTT estimate before timing samples arrive.
	defaultInitialRTTMicros = 100_000
	// defaultInitialRateBytesPerSecond is the delivery-rate estimate before reports arrive.
	defaultInitialRateBytesPerSecond = 1_000_000
	// preferredLaneHysteresisMicros prevents sparse traffic from oscillating between similar lanes.
	preferredLaneHysteresisMicros = 2_000
	// schedulerEventCapacity bounds queued scheduler transitions.
	schedulerEventCapacity = 256
	// abandonmentCheckInterval bounds reaction time to a stale retained packet.
	abandonmentCheckInterval = 25 * time.Millisecond
	// minimumRateSampleBytes avoids treating isolated packets as path-capacity measurements.
	minimumRateSampleBytes = 4 * 1024
	// minimumProgressStall prevents ordinary report cadence and congestion from looking like a carrier black hole.
	minimumProgressStall = 250 * time.Millisecond
	// progressStallReportMargin allows two report intervals beyond the measured round trips.
	progressStallReportMargin = 2 * defaultReportInterval
)

// LaneRegistration makes one lane generation eligible for packet scheduling.
type LaneRegistration struct {
	LaneID               protocol.LaneID
	Generation           uint64
	PathGroupID          protocol.PathGroupID
	Store                *TransmissionStore
	Abandon              context.CancelFunc
	SendControl          func(protocol.Frame, func()) bool
	ValidatePingProgress func(uint64) bool
	SendDeliveryReport   func(protocol.DeliveryReport, time.Time, func()) bool
	// InitialTiming seeds RTT from usable admission timing known before registration.
	InitialTiming *clockmap.Sample
}

// LaneObserver receives cumulative lane feedback parsed by any carrier reader.
type LaneObserver interface {
	ObserveDeliveryReport(context.Context, protocol.LaneGeneration, protocol.DeliveryReport, uint64, chan error) error
	ObserveTiming(protocol.LaneID, uint64, clockmap.Sample)
	ObserveLaneAbandon(context.Context, protocol.LaneGeneration) error
	RouteDeliveryReport(protocol.DeliveryReport, time.Time, func(bool)) bool
}

// schedulerEventKind identifies one serialized scheduler state transition.
type schedulerEventKind uint8

const (
	// schedulerRegister adds or supersedes one lane generation.
	schedulerRegister schedulerEventKind = iota + 1
	// schedulerRemove removes one exact lane generation.
	schedulerRemove
	// schedulerReport applies cumulative peer parsing progress.
	schedulerReport
	// schedulerTiming applies one RTT sample.
	schedulerTiming
	// schedulerRouteReport routes cumulative feedback over a connected outbound lane.
	schedulerRouteReport
	// schedulerPeerAbandon closes the exact generation abandoned by the peer.
	schedulerPeerAbandon
	// schedulerCloseSession writes one explicit graceful session close.
	schedulerCloseSession
)

// schedulerEvent is one state transition consumed by the scheduler goroutine.
type schedulerEvent struct {
	kind           schedulerEventKind
	registration   LaneRegistration
	laneID         protocol.LaneID
	generation     uint64
	report         protocol.DeliveryReport
	parsedAt       time.Time
	timing         clockmap.Sample
	receiveMicros  uint64
	reportComplete func(bool)
	frame          protocol.Frame
	result         chan error
}

// rateObservation records one accepted delivery rate and its local sampling time.
type rateObservation struct {
	rate           uint64
	receivedMicros uint64
}

// scheduledLane contains direction-local predictive state for one generation.
type scheduledLane struct {
	registration        LaneRegistration
	rttMicros           uint64
	minimumRTTMicros    uint64
	feedbackDelayMicros uint64
	deliveryRate        uint64
	lastDataPackets     uint64
	lastPingID          uint64
	rateHistory         [5]rateObservation
	rateHistoryCount    int
	rateHistoryNext     int
	rateObserved        bool
	probeUntil          time.Time
	nextProbeAt         time.Time
	probeBytes          uint64
	lastProgressAt      time.Time
	rttObserved         bool
	degraded            bool
	abandoning          bool
}

// laneCandidates is a fixed-capacity ordered scheduler result.
type laneCandidates struct {
	lanes     [2]*scheduledLane
	count     int
	available bool
}

// scoredLane snapshots one lane's predicted delivery score for a scheduling decision.
type scoredLane struct {
	lane  *scheduledLane
	score uint64
}

// scoredLaneBetter orders score snapshots by delivery prediction and stable lane identity.
func scoredLaneBetter(left, right scoredLane) bool {
	if right.lane == nil || left.score != right.score {
		return right.lane == nil || left.score < right.score
	}
	return left.lane.registration.LaneID < right.lane.registration.LaneID
}

// Scheduler assigns session packets to dynamically registered lanes.
type Scheduler struct {
	ingress            *packetqueue.Queue[Packet]
	events             chan schedulerEvent
	controlOrder       []scoredLane
	packetID           uint64
	transportHoldUntil time.Time
	lastTransportAt    time.Time
	lastProbeLane      protocol.LaneID
}

// NewScheduler validates resources and returns an empty multipath scheduler.
func NewScheduler(ingress *packetqueue.Queue[Packet]) (*Scheduler, error) {
	if ingress == nil {
		return nil, ErrInvalidScheduler
	}
	return &Scheduler{ingress: ingress, events: make(chan schedulerEvent, schedulerEventCapacity)}, nil
}

// Register synchronously adds a lane or replaces it with a higher generation.
func (s *Scheduler) Register(ctx context.Context, registration LaneRegistration) error {
	if registration.LaneID.IsZero() || registration.Generation == 0 || registration.PathGroupID.IsZero() ||
		registration.Store == nil || registration.Abandon == nil || registration.SendControl == nil ||
		registration.ValidatePingProgress == nil || registration.SendDeliveryReport == nil {
		return ErrInvalidRegistration
	}
	result := make(chan error, 1)
	event := schedulerEvent{kind: schedulerRegister, registration: registration, result: result}
	select {
	case s.events <- event:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Remove synchronously makes one exact lane generation ineligible and drains its retained backlog.
func (s *Scheduler) Remove(ctx context.Context, laneID protocol.LaneID, generation uint64) error {
	result := make(chan error, 1)
	event := schedulerEvent{kind: schedulerRemove, laneID: laneID, generation: generation, result: result}
	select {
	case s.events <- event:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ObserveDeliveryReport validates cumulative parsing feedback received over one exact carrier generation.
// The reader owns result, which must have capacity one and no outstanding request. A canceled request ends that
// reader's lifetime, so its channel is never reused while the scheduler may still complete the request.
func (s *Scheduler) ObserveDeliveryReport(ctx context.Context, source protocol.LaneGeneration, report protocol.DeliveryReport,
	receiveMicros uint64, result chan error) error {
	select {
	case s.events <- schedulerEvent{
		kind: schedulerReport, report: report, receiveMicros: receiveMicros, result: result,
		laneID: source.LaneID, generation: source.Generation,
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ObserveTiming applies one lane RTT observation without blocking a carrier reader.
func (s *Scheduler) ObserveTiming(laneID protocol.LaneID, generation uint64, sample clockmap.Sample) {
	select {
	case s.events <- schedulerEvent{
		kind: schedulerTiming, laneID: laneID, generation: generation, timing: sample,
	}:
	default:
	}
}

// ObserveLaneAbandon synchronously applies one generation-specific peer abandonment request.
func (s *Scheduler) ObserveLaneAbandon(ctx context.Context, lane protocol.LaneGeneration) error {
	result := make(chan error, 1)
	event := schedulerEvent{
		kind: schedulerPeerAbandon, laneID: lane.LaneID, generation: lane.Generation, result: result,
	}
	select {
	case s.events <- event:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseSession writes one graceful close over an active lane and waits for carrier write completion.
func (s *Scheduler) CloseSession(ctx context.Context, reason protocol.CloseReason) error {
	frame, err := protocol.MarshalSessionClose(reason)
	if err != nil {
		return err
	}
	result := make(chan error, 1)
	event := schedulerEvent{kind: schedulerCloseSession, frame: frame, result: result}
	select {
	case s.events <- event:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RouteDeliveryReport queues cumulative feedback and its carrier-write completion callback.
func (s *Scheduler) RouteDeliveryReport(report protocol.DeliveryReport, parsedAt time.Time, complete func(bool)) bool {
	select {
	case s.events <- schedulerEvent{kind: schedulerRouteReport, report: report, parsedAt: parsedAt, reportComplete: complete}:
		return true
	default:
		return false
	}
}

// Run schedules packets and serializes all mutable lane prediction state.
func (s *Scheduler) Run(parent context.Context) (result error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	lanes := make(map[protocol.LaneID]*scheduledLane)
	defer func() {
		for _, lane := range lanes {
			releaseTransmissions(lane.registration.Store.drain())
		}
	}()
	ticker := time.NewTicker(abandonmentCheckInterval)
	ticker.Stop()
	defer ticker.Stop()
	checking := false
	var check <-chan time.Time
	expiry := time.NewTimer(time.Hour)
	expiry.Stop()
	defer expiry.Stop()
	var expiryReady <-chan time.Time
	var expiryAt time.Time
	var preferred protocol.LaneID
	var pending packetqueue.Item[Packet]
	var preempted packetqueue.Item[Packet]
	var controlCandidate packetqueue.Item[Packet]
	hasPending := false
	hasPreempted := false
	defer func() {
		for _, state := range []struct {
			item *packetqueue.Item[Packet]
			has  bool
		}{
			{item: &preempted, has: hasPreempted},
			{item: &pending, has: hasPending},
		} {
			if !state.has {
				continue
			}
			err := s.ingress.Push(*state.item)
			switch {
			case err == nil:
			case errors.Is(err, packetqueue.ErrFull), errors.Is(err, packetqueue.ErrExpired),
				errors.Is(err, packetqueue.ErrClosed):
				state.item.Release()
			default:
				state.item.Release()
				result = errors.Join(result, err)
			}
		}
	}()
	for {
		now := s.ingress.Now()
		progressed := false
		if !hasPending {
			if hasPreempted {
				pending = preempted
				preempted = packetqueue.Item[Packet]{}
				hasPending = true
				hasPreempted = false
				progressed = true
			} else if len(lanes) > 0 {
				err := s.ingress.TryPop(&pending, now)
				switch {
				case err == nil:
					hasPending = true
					progressed = true
				case errors.Is(err, packetqueue.ErrEmpty):
				case errors.Is(err, packetqueue.ErrClosed):
					return queueResult(ctx, "dequeue scheduler ingress", err)
				default:
					return err
				}
			}
		}
		if hasPending && pending.Priority == packetqueue.PriorityNormal {
			err := s.ingress.TryPopPriority(packetqueue.PriorityControl, &controlCandidate, now)
			switch {
			case err == nil:
				preempted = pending
				hasPreempted = true
				pending = controlCandidate
				controlCandidate = packetqueue.Item[Packet]{}
				progressed = true
			case errors.Is(err, packetqueue.ErrEmpty):
			case errors.Is(err, packetqueue.ErrClosed):
				return queueResult(ctx, "dequeue scheduler control ingress", err)
			default:
				return err
			}
		}
		if hasPending {
			if len(lanes) > 0 {
				scheduled, err := s.schedule(lanes, &preferred, &pending, now)
				if err != nil {
					return err
				}
				if scheduled {
					pending = packetqueue.Item[Packet]{}
					hasPending = false
					progressed = true
				}
			} else if !now.Before(pending.Deadline) {
				pending.Release()
				pending = packetqueue.Item[Packet]{}
				hasPending = false
				progressed = true
			}
		}
		reserveBytes := uint64(0)
		if hasPending {
			reserveBytes = uint64(len(pending.Value.Payload) + protocol.MaxEncodedFrameSize - protocol.MaxPacketSize)
		}
		progressed = s.fillTransportProbe(lanes, preferred, now, reserveBytes) || progressed
		needsCheck := hasPending || hasPreempted
		if !needsCheck {
			for _, lane := range lanes {
				if packets, _ := lane.registration.Store.backlog(); packets > 0 {
					needsCheck = true
					break
				}
			}
		}
		if needsCheck != checking {
			checking = needsCheck
			if checking {
				ticker.Reset(abandonmentCheckInterval)
				check = ticker.C
			} else {
				ticker.Stop()
				check = nil
			}
		}
		if next := s.ingress.NextDeadline(now); !next.Equal(expiryAt) {
			expiryAt = next
			if next.IsZero() {
				expiry.Stop()
				expiryReady = nil
			} else {
				expiry.Reset(max(0, next.Sub(now)))
				expiryReady = expiry.C
			}
		}
		if progressed && !hasPending {
			select {
			case event := <-s.events:
				s.applyEvent(lanes, &preferred, event, s.ingress.Now())
			case <-check:
				s.checkAbandonment(lanes, s.ingress.Now())
			case <-expiryReady:
				s.ingress.Expire()
				expiryAt = time.Time{}
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			continue
		}
		select {
		case event := <-s.events:
			s.applyEvent(lanes, &preferred, event, s.ingress.Now())
		case <-s.ingress.Ready():
		case <-check:
			s.checkAbandonment(lanes, s.ingress.Now())
		case <-expiryReady:
			s.ingress.Expire()
			expiryAt = time.Time{}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyEvent mutates lane state for one serialized event.
func (s *Scheduler) applyEvent(lanes map[protocol.LaneID]*scheduledLane, preferred *protocol.LaneID,
	event schedulerEvent, now time.Time) {
	switch event.kind {
	case schedulerRegister:
		current := lanes[event.registration.LaneID]
		if current != nil && current.registration.Generation >= event.registration.Generation {
			event.result <- ErrStaleLane
			return
		}
		replacement := &scheduledLane{
			registration: event.registration, rttMicros: defaultInitialRTTMicros,
			deliveryRate: defaultInitialRateBytesPerSecond, lastProgressAt: now,
		}
		if event.registration.InitialTiming != nil {
			replacement.applyTiming(*event.registration.InitialTiming)
		}
		lanes[event.registration.LaneID] = replacement
		if current != nil {
			if *preferred == event.registration.LaneID {
				*preferred = 0
			}
			current.registration.Abandon()
			s.migrateTransmissions(lanes, current)
		}
		event.result <- nil
	case schedulerRemove:
		lane := lanes[event.laneID]
		if lane != nil && lane.registration.Generation == event.generation {
			delete(lanes, event.laneID)
			s.migrateTransmissions(lanes, lane)
			if *preferred == event.laneID {
				*preferred = protocol.LaneID(0)
			}
		}
		event.result <- nil
	case schedulerReport:
		lane := lanes[event.report.LaneID]
		if lane == nil || lane.registration.Generation != event.report.Generation {
			event.result <- nil
			return
		}
		progressed, err := lane.applyReport(event.report, event.receiveMicros, now)
		if err != nil {
			event.result <- err
			return
		}
		if progressed {
			lane.feedbackDelayMicros = event.report.DelayMicros
			if source := lanes[event.laneID]; source != nil && source.registration.Generation == event.generation {
				lane.feedbackDelayMicros = saturatingAdd(lane.feedbackDelayMicros, source.minimumRTTMicros/2)
			}
			lane.registration.Store.deliveryWindow.Store(lane.deliveryWindowBytes())
		}
		event.result <- nil
	case schedulerTiming:
		lane := lanes[event.laneID]
		if lane != nil && lane.registration.Generation == event.generation {
			lane.applyTiming(event.timing)
			lane.registration.Store.deliveryWindow.Store(lane.deliveryWindowBytes())
		}
	case schedulerRouteReport:
		s.routeReport(lanes, event.report, event.parsedAt, event.reportComplete)
	case schedulerPeerAbandon:
		lane := lanes[event.laneID]
		if lane != nil && lane.registration.Generation == event.generation && !lane.abandoning {
			lane.abandoning = true
			lane.registration.Abandon()
		}
		event.result <- nil
	case schedulerCloseSession:
		result := event.result
		if !s.routeControl(lanes, event.frame, protocol.LaneID(0), func() { result <- nil }) {
			event.result <- ErrNoActiveLane
		}
	}
}

// routeReport duplicates feedback over its own connected lane and one best alternate lane.
func (s *Scheduler) routeReport(lanes map[protocol.LaneID]*scheduledLane, report protocol.DeliveryReport,
	parsedAt time.Time, complete func(bool)) {
	candidates := s.orderedControlLanes(lanes)
	if len(candidates) == 0 {
		complete(false)
		return
	}
	var completeOnce sync.Once
	onSent := func() { completeOnce.Do(func() { complete(true) }) }
	accepted := false
	for _, scored := range candidates {
		candidate := scored.lane
		if candidate.registration.LaneID == report.LaneID {
			accepted = candidate.registration.SendDeliveryReport(report, parsedAt, onSent)
			break
		}
	}
	for _, scored := range candidates {
		candidate := scored.lane
		if candidate.registration.LaneID != report.LaneID &&
			candidate.registration.SendDeliveryReport(report, parsedAt, onSent) {
			accepted = true
			break
		}
	}
	if !accepted {
		complete(false)
	}
}

// orderedControlLanes returns every non-abandoning lane in deterministic predicted-delivery order.
// The result remains valid until this method is called again.
func (s *Scheduler) orderedControlLanes(lanes map[protocol.LaneID]*scheduledLane) []scoredLane {
	clear(s.controlOrder)
	scores := s.controlOrder[:0]
	for _, lane := range lanes {
		if !lane.abandoning {
			scores = append(scores, scoredLane{lane, lane.score(0)})
		}
	}
	for index := 1; index < len(scores); index++ {
		candidate := scores[index]
		position := index
		for position > 0 && scoredLaneBetter(candidate, scores[position-1]) {
			scores[position] = scores[position-1]
			position--
		}
		scores[position] = candidate
	}
	s.controlOrder = scores
	return scores
}

// schedule assigns one PacketID to one or two eligible lanes.
func (s *Scheduler) schedule(lanes map[protocol.LaneID]*scheduledLane, preferred *protocol.LaneID,
	item *packetqueue.Item[Packet], now time.Time) (bool, error) {
	if err := item.Value.Validate(); err != nil {
		item.Release()
		return true, nil
	}
	remaining := item.Deadline.Sub(now)
	if remaining <= 0 {
		item.Release()
		return true, nil
	}
	deadlineMicros := uint64(remaining / time.Microsecond)
	packetID := s.packetID + 1
	if packetID == 0 {
		return false, ErrCounterExhausted
	}
	size, err := protocol.DataFrameSize(protocol.Data{
		PacketID: packetID, DeadlineMicros: uint64(item.Deadline.UnixMicro()), Payload: item.Value.Payload,
	})
	if err != nil {
		item.Release()
		return true, nil
	}
	frameBytes := uint64(size)
	control := item.Value.Kind.Control()
	initialKeepalive := !control && preferred.IsZero() && len(item.Value.Payload) == wgpacket.TransportKeepaliveLength
	var candidates laneCandidates
	if control {
		candidates = selectControlCandidates(lanes, frameBytes, deadlineMicros)
	} else if initialKeepalive {
		candidates = selectCandidates(lanes, 0, frameBytes, deadlineMicros)
	} else {
		candidates = s.selectTransportCandidates(lanes, *preferred, frameBytes, deadlineMicros, now)
	}
	if candidates.count == 0 {
		if candidates.available {
			item.Release()
		}
		return candidates.available, nil
	}
	scheduled := false
	for _, lane := range candidates.lanes[:candidates.count] {
		if lane.enqueue(item, packetID, now) {
			if !control && !initialKeepalive {
				s.recordTransportAssignment(lanes, *preferred, lane, now)
				*preferred = lane.registration.LaneID
			}
			scheduled = true
		} else if !control {
			return false, nil
		}
	}
	if scheduled {
		s.packetID = packetID
		item.Release()
	}
	return scheduled, nil
}

// selectCandidates returns lanes ordered by predicted transport delivery.
func selectCandidates(lanes map[protocol.LaneID]*scheduledLane, preferred protocol.LaneID,
	frameBytes, maximumScore uint64) laneCandidates {
	var preferredCandidate scoredLane
	var preferredQueuedBytes uint64
	if lane := lanes[preferred]; lane != nil && !lane.degraded && !lane.abandoning &&
		(frameBytes == 0 || lane.canAccept(frameBytes)) {
		queuedBytes, retainedBytes := lane.registration.Store.deliveryBacklog()
		preferredQueuedBytes = queuedBytes
		preferredCandidate = scoredLane{lane: lane, score: lane.scoreBacklog(frameBytes, queuedBytes, retainedBytes)}
	}
	result := laneCandidates{}
	var first scoredLane
	preferredBetter := 0
	for _, lane := range lanes {
		if lane != preferredCandidate.lane &&
			(lane.degraded || lane.abandoning || frameBytes > 0 && !lane.canAccept(frameBytes)) {
			continue
		}
		result.available = true
		candidate := preferredCandidate
		if lane != preferredCandidate.lane {
			candidate = scoredLane{lane: lane, score: lane.score(frameBytes)}
		}
		if preferredCandidate.lane != nil && lane != preferredCandidate.lane &&
			lane.registration.PathGroupID == preferredCandidate.lane.registration.PathGroupID &&
			scoredLaneBetter(candidate, preferredCandidate) {
			preferredBetter++
		}
		if candidate.score < maximumScore && scoredLaneBetter(candidate, first) {
			first = candidate
		}
	}
	if first.lane == nil {
		return result
	}
	preferredLimit := first.score
	if preferredLimit <= math.MaxUint64-preferredLaneHysteresisMicros {
		preferredLimit += preferredLaneHysteresisMicros
	} else {
		preferredLimit = math.MaxUint64
	}
	if preferredCandidate.lane != nil && preferredCandidate.score < maximumScore {
		stable := frameBytes > 0 && preferredQueuedBytes == 0 &&
			preferredCandidate.lane.registration.PathGroupID == first.lane.registration.PathGroupID
		if stable || preferredBetter < 2 && preferredCandidate.score <= preferredLimit {
			first = preferredCandidate
		}
	}
	result.lanes[0] = first.lane
	result.count = 1
	return result
}

// selectControlCandidates keeps the fastest lane and the best alternate, preferring a distinct path group.
func selectControlCandidates(lanes map[protocol.LaneID]*scheduledLane,
	frameBytes, maximumScore uint64) laneCandidates {
	result := laneCandidates{}
	var first, second scoredLane
	for _, lane := range lanes {
		if lane.degraded || lane.abandoning || frameBytes > 0 && !lane.canAccept(frameBytes) {
			continue
		}
		result.available = true
		candidate := scoredLane{lane: lane, score: lane.score(frameBytes)}
		if candidate.score >= maximumScore {
			continue
		}
		if scoredLaneBetter(candidate, first) {
			// A new fastest group makes the previous winner the best distinct alternate. Within the same
			// group, preserve an existing distinct alternate or replace the slower same-group alternate.
			if first.lane != nil && (lane.registration.PathGroupID != first.lane.registration.PathGroupID ||
				second.lane == nil || second.lane.registration.PathGroupID == lane.registration.PathGroupID) {
				second = first
			}
			first = candidate
			continue
		}
		candidateDistinct := lane.registration.PathGroupID != first.lane.registration.PathGroupID
		secondDistinct := second.lane != nil &&
			second.lane.registration.PathGroupID != first.lane.registration.PathGroupID
		if second.lane == nil || candidateDistinct && !secondDistinct ||
			candidateDistinct == secondDistinct && scoredLaneBetter(candidate, second) {
			second = candidate
		}
	}
	if first.lane != nil {
		result.lanes[0] = first.lane
		result.count = 1
		if second.lane != nil {
			result.lanes[1] = second.lane
			result.count = 2
		}
	}
	return result
}

// canAccept reports whether current retained work can accept one more frame.
func (l *scheduledLane) canAccept(frameBytes uint64) bool {
	if !l.registration.Store.canAccept(frameBytes) {
		return false
	}

	_, bytes := l.registration.Store.backlog()
	window := l.deliveryWindowBytes()
	return bytes == 0 || frameBytes <= window && bytes <= window-frameBytes
}

// score returns predicted delivery delay in microseconds.
func (l *scheduledLane) score(frameBytes uint64) uint64 {
	queuedBytes, backlogBytes := l.registration.Store.deliveryBacklog()
	return l.scoreBacklog(frameBytes, queuedBytes, backlogBytes)
}

// scoreBacklog predicts delivery from one consistent queued and retained byte snapshot.
func (l *scheduledLane) scoreBacklog(frameBytes, queuedBytes, backlogBytes uint64) uint64 {
	if l.deliveryRate == 0 || backlogBytes > math.MaxUint64-frameBytes ||
		backlogBytes+frameBytes > math.MaxUint64/1_000_000 {
		return math.MaxUint64
	}
	queuedMicros := (queuedBytes + frameBytes) * 1_000_000 / l.deliveryRate
	retainedMicros := (backlogBytes + frameBytes) * 1_000_000 / l.deliveryRate
	base := l.rttMicros / 2
	if queuedMicros > math.MaxUint64-base {
		return math.MaxUint64
	}
	// Unsent data still needs serialization and propagation. Already committed data can have reached the peer
	// while its report returns over another lane. Discount the reported construction wait and the carrier's minimum observed return delay.
	if retainedMicros <= l.feedbackDelayMicros {
		return base + queuedMicros
	}
	return max(base+queuedMicros, retainedMicros-l.feedbackDelayMicros)
}

// retentionDelay conservatively estimates deadline risk without discounting unreported work.
func (l *scheduledLane) retentionDelay(bytes uint64) uint64 {
	if l.deliveryRate == 0 || bytes > math.MaxUint64/1_000_000 {
		return math.MaxUint64
	}
	queueMicros := bytes * 1_000_000 / l.deliveryRate
	base := l.rttMicros / 2
	if queueMicros > math.MaxUint64-base {
		return math.MaxUint64
	}
	return base + queueMicros
}

// enqueue retains one newly identified packet after successful store admission.
func (l *scheduledLane) enqueue(item *packetqueue.Item[Packet], packetID uint64, now time.Time) bool {
	packet := item.Value.Retain()
	data := protocol.Data{
		PacketID: packetID, DeadlineMicros: uint64(item.Deadline.UnixMicro()), Payload: packet.Payload,
	}
	size, err := protocol.DataFrameSize(data)
	if err != nil {
		packet.Release()
		return false
	}
	budget, ok := item.TakeRetention(size)
	if !ok {
		packet.Release()
		return false
	}
	transmission := retainedTransmission{
		packetID: packetID, deadlineMicros: uint64(item.Deadline.UnixMicro()),
		budget: budget, packet: packet.Packet,
	}
	if l.registration.Store.pushAt(transmission, now) == nil {
		return true
	}
	item.RestoreRetention(budget, size)
	transmission.releasePacket()
	return false
}

// enqueueTransmission retains one packet in the lane generation.
func (l *scheduledLane) enqueueTransmission(transmission retainedTransmission) bool {
	return l.registration.Store.push(transmission) == nil
}

// applyReport releases the reported carrier prefix and returns whether validated progress advanced.
func (l *scheduledLane) applyReport(report protocol.DeliveryReport, receiveMicros uint64,
	receiveTime time.Time) (bool, error) {
	if !l.registration.ValidatePingProgress(report.PingID) {
		return false, ErrInvalidDeliveryReport
	}
	dataDirection := cmp.Compare(report.DataPackets, l.lastDataPackets)
	pingDirection := cmp.Compare(report.PingID, l.lastPingID)
	if dataDirection < 0 || pingDirection < 0 {
		if dataDirection > 0 || pingDirection > 0 {
			return false, ErrInvalidDeliveryReport
		}
		return false, nil
	}
	if dataDirection == 0 && pingDirection == 0 {
		return false, nil
	}
	sample, stale, err := l.registration.Store.acknowledge(report.DataPackets, receiveMicros)
	if err != nil || stale {
		return false, err
	}
	l.updateDeliveryRate(sample, receiveMicros)
	quantum := uint64(1024)
	if l.rateObserved {
		quantum = targetDataBatchBytes
		interval := uint64(defaultReportInterval / time.Microsecond)
		if l.deliveryRate <= math.MaxUint64/interval {
			quantum = max(uint64(1024), min(quantum, l.deliveryRate*interval/1_000_000))
		}
	}
	l.registration.Store.writeBudget.Store(quantum)
	l.lastDataPackets = report.DataPackets
	l.lastPingID = report.PingID
	l.lastProgressAt = receiveTime
	l.degraded = false
	return true, nil
}

// updateDeliveryRate keeps a time-bounded maximum of send-bounded samples. Application-limited observations can raise
// capacity but cannot lower it or evict a useful peak. Reports may return over a different carrier.
func (l *scheduledLane) updateDeliveryRate(sample deliverySample, receiveMicros uint64) {
	if sample.bytes < minimumRateSampleBytes || sample.intervalMicros < uint64(time.Millisecond/time.Microsecond) ||
		sample.bytes > math.MaxUint64/1_000_000 {
		return
	}
	rate := sample.bytes * 1_000_000 / sample.intervalMicros
	if rate == 0 || l.rateObserved && sample.applicationLimited && rate <= l.deliveryRate {
		return
	}
	l.rateHistory[l.rateHistoryNext] = rateObservation{rate: rate, receivedMicros: receiveMicros}
	l.rateHistoryNext = (l.rateHistoryNext + 1) % len(l.rateHistory)
	l.rateHistoryCount = min(l.rateHistoryCount+1, len(l.rateHistory))
	rtt := l.minimumRTTMicros
	if rtt == 0 {
		rtt = l.rttMicros
	}
	lifetime := uint64(math.MaxUint64)
	if rtt <= math.MaxUint64/4 {
		lifetime = max(uint64(time.Second/time.Microsecond), rtt*4)
	}
	for _, previous := range l.rateHistory[:l.rateHistoryCount] {
		if receiveMicros >= previous.receivedMicros && receiveMicros-previous.receivedMicros < lifetime {
			rate = max(rate, previous.rate)
		}
	}
	l.deliveryRate = rate
	l.rateObserved = true
}

// applyTiming updates a bounded RTT exponential moving average.
func (l *scheduledLane) applyTiming(sample clockmap.Sample) {
	if sample.LocalReceiveMicros < sample.LocalSendMicros || sample.RemoteSendMicros < sample.RemoteReceiveMicros {
		return
	}
	roundTrip := sample.LocalReceiveMicros - sample.LocalSendMicros
	processing := sample.RemoteSendMicros - sample.RemoteReceiveMicros
	if processing < roundTrip {
		roundTrip -= processing
	} else {
		roundTrip = 1
	}
	if l.minimumRTTMicros == 0 || roundTrip < l.minimumRTTMicros {
		l.minimumRTTMicros = roundTrip
	}
	if !l.rttObserved {
		l.rttMicros = roundTrip
		l.rttObserved = true
		return
	}
	l.rttMicros = weightedAverage7(l.rttMicros, roundTrip)
}

// weightedAverage7 returns a seven-to-one moving average without unsigned overflow.
func weightedAverage7(previous, sample uint64) uint64 {
	return previous/8*7 + sample/8 + (previous%8*7+sample%8)/8
}

// checkAbandonment diverts deadline-risk work and migrates stalled generations while packets remain useful.
func (s *Scheduler) checkAbandonment(lanes map[protocol.LaneID]*scheduledLane, now time.Time) {
	for _, lane := range lanes {
		if lane.abandoning {
			continue
		}
		unreportedSince, firstBytes := lane.registration.Store.expire(now)
		if !laneProgressStalled(lane, now, unreportedSince, firstBytes) {
			lane.degraded = false
			continue
		}
		assessment := lane.registration.Store.assessDeadlines(now, lane.retentionDelay)
		if !assessment.retained {
			lane.degraded = true
			continue
		}
		// Stalled progress makes the previous capacity prediction optimistic. A healthy alternative that can
		// deliver useful work within another progress guard justifies recovery before the salvage window closes.
		remaining := assessment.usefulDeadline.Sub(now)
		recoveryMicros := progressGuardMicros(lane, firstBytes)
		progressSince := lane.lastProgressAt
		if unreportedSince.After(progressSince) {
			progressSince = unreportedSince
		}
		recoveryMicros = max(recoveryMicros, uint64(now.Sub(progressSince)/time.Microsecond))
		var alternative, proven bool
		if remaining > 0 {
			alternative, proven = hasTimelyAlternative(lanes, lane, now, assessment.usefulBytes,
				min(uint64(remaining/time.Microsecond), recoveryMicros))
		}
		lane.degraded = assessment.atRisk || alternative
		if proven {
			lane.abandoning = true
			s.announceAbandonment(lanes, lane)
			lane.registration.Abandon()
		}
	}
}

// laneProgressStalled reports whether outstanding work has waited beyond a path-aware progress guard.
func laneProgressStalled(lane *scheduledLane, now, unreportedSince time.Time, firstBytes uint64) bool {
	if lane.lastProgressAt.IsZero() || unreportedSince.IsZero() {
		return false
	}
	progressSince := lane.lastProgressAt
	if unreportedSince.After(progressSince) {
		progressSince = unreportedSince
	}
	thresholdMicros := progressGuardMicros(lane, firstBytes)
	if thresholdMicros > uint64(math.MaxInt64/time.Microsecond) {
		return false
	}
	threshold := time.Duration(thresholdMicros) * time.Microsecond
	return now.Sub(progressSince) >= threshold
}

// progressGuardMicros allows the longer of two feedback cycles or a startup flight, plus the first frame's serialization.
// The flight allowance keeps low-rate carriers from looking stalled between small-frame reports.
func progressGuardMicros(lane *scheduledLane, firstBytes uint64) uint64 {
	if lane.rttMicros > math.MaxUint64/2 || lane.deliveryRate == 0 || firstBytes > math.MaxUint64/1_000_000 {
		return math.MaxUint64
	}
	cycle := max(lane.rttMicros, saturatingAdd(lane.rttMicros/2, lane.feedbackDelayMicros))
	if cycle > math.MaxUint64/2 {
		return math.MaxUint64
	}
	guard := max(cycle*2, initialDeliveryWindow*1_000_000/lane.deliveryRate)
	guard = saturatingAdd(guard, uint64(progressStallReportMargin/time.Microsecond))
	guard = saturatingAdd(guard, firstBytes*1_000_000/lane.deliveryRate)
	return max(uint64(minimumProgressStall/time.Microsecond), guard)
}

// hasTimelyAlternative reports timely alternatives and whether one has delivered enough real transport to prove recovery.
// Both rates must be measured before a capacity comparison can justify abandonment. Stalled group members cannot
// disqualify a usable lower-ranked lane.
func hasTimelyAlternative(lanes map[protocol.LaneID]*scheduledLane, current *scheduledLane, now time.Time,
	frameBytes, remainingMicros uint64) (available, proven bool) {
	for _, candidate := range lanes {
		if candidate == current || !current.rateObserved || !candidate.rateObserved {
			continue
		}
		if _, eligible := recoveryCandidate(candidate, now, frameBytes, remainingMicros); eligible {
			available = true
			if candidate.registration.Store.transportReported.Load() >= minimumRateSampleBytes {
				return true, true
			}
		}
	}
	return available, false
}

// recoveryCandidate scores a live recovery lane after reclaiming queued expiry and checking current parsing progress.
func recoveryCandidate(lane *scheduledLane, now time.Time, frameBytes, remainingMicros uint64) (scoredLane, bool) {
	if lane.degraded || lane.abandoning {
		return scoredLane{}, false
	}
	since, firstBytes := lane.registration.Store.expire(now)
	if laneProgressStalled(lane, now, since, firstBytes) || !lane.canAccept(frameBytes) {
		return scoredLane{}, false
	}
	candidate := scoredLane{lane: lane, score: lane.score(frameBytes)}
	return candidate, candidate.score < remainingMicros
}

// announceAbandonment asks the peer to close the same generation over another connected lane.
func (s *Scheduler) announceAbandonment(lanes map[protocol.LaneID]*scheduledLane, abandoned *scheduledLane) {
	frame, err := protocol.MarshalLaneAbandon(protocol.LaneGeneration{
		LaneID: abandoned.registration.LaneID, Generation: abandoned.registration.Generation,
	})
	if err != nil {
		return
	}
	s.routeControl(lanes, frame, abandoned.registration.LaneID, nil)
}

// routeControl queues one fixed control frame on the best connected lane outside exclude.
func (s *Scheduler) routeControl(lanes map[protocol.LaneID]*scheduledLane, frame protocol.Frame,
	exclude protocol.LaneID, onSent func()) bool {
	for _, scored := range s.orderedControlLanes(lanes) {
		candidate := scored.lane
		if candidate.registration.LaneID == exclude {
			continue
		}
		if candidate.registration.SendControl(frame, onSent) {
			return true
		}
	}
	return false
}

// migrateTransmissions moves each still-fresh transport packet at most once after generation removal.
func (s *Scheduler) migrateTransmissions(lanes map[protocol.LaneID]*scheduledLane, removed *scheduledLane) {
	now := s.ingress.Now()
	nowMicros := uint64(now.UnixMicro())
	retained := removed.registration.Store.drain()
	sort.SliceStable(retained, func(left, right int) bool {
		return retained[left].deadlineMicros < retained[right].deadlineMicros
	})
	for index := range retained {
		transmission := &retained[index]
		if transmission.packet.Kind != wgpacket.TransportData || transmission.migrated ||
			nowMicros >= transmission.deadlineMicros {
			transmission.release()
			continue
		}
		transmission.migrated = true
		frameBytes := uint64(transmission.size)
		deadlineMicros := transmission.deadlineMicros - nowMicros
		var best scoredLane
		for _, lane := range lanes {
			candidate, eligible := recoveryCandidate(lane, now, frameBytes, deadlineMicros)
			if eligible && scoredLaneBetter(candidate, best) {
				best = candidate
			}
		}
		if best.lane != nil && best.lane.enqueueTransmission(*transmission) {
			transmission.budget = nil
			transmission.packet = datagram.Packet{}
		}
		transmission.release()
	}
}

// releaseTransmissions returns aggregate capacity for discarded drained work.
func releaseTransmissions(transmissions []retainedTransmission) {
	for index := range transmissions {
		transmissions[index].release()
	}
}

// saturatingAdd combines time estimates without wrapping an untrusted wire value.
func saturatingAdd(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}
