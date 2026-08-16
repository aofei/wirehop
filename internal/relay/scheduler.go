package relay

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

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
	// defaultInitialRTTMicros is the conservative RTT before timing samples arrive.
	defaultInitialRTTMicros = 100_000
	// defaultInitialRateBytesPerSecond is the conservative delivery rate before reports arrive.
	defaultInitialRateBytesPerSecond = 1_000_000
	// preferredLaneHysteresisMicros prevents sparse traffic from oscillating between similar lanes.
	preferredLaneHysteresisMicros = 2_000
	// schedulerEventCapacity bounds queued scheduler transitions.
	schedulerEventCapacity = 256
	// abandonmentCheckInterval bounds reaction time to a stale retained packet.
	abandonmentCheckInterval = 25 * time.Millisecond
	// minimumRateSampleBytes avoids treating isolated packets as path-capacity measurements.
	minimumRateSampleBytes = 4 * 1024
	// maximumRateSampleIntervalMicros discards sparse offered traffic from delivery-rate estimation.
	maximumRateSampleIntervalMicros = uint64((250 * time.Millisecond) / time.Microsecond)
	// minimumProgressStall prevents ordinary report cadence and congestion from looking like a carrier black hole.
	minimumProgressStall = 250 * time.Millisecond
	// progressStallReportMargin allows two report intervals beyond the measured round trips.
	progressStallReportMargin = 2 * defaultReportInterval
	// candidateGroupInlineCapacity covers the command's complete lane limit without heap allocation.
	candidateGroupInlineCapacity = 16
)

// LaneRegistration makes one lane generation eligible for packet scheduling.
type LaneRegistration struct {
	LaneID                protocol.LaneID
	Generation            uint64
	PathGroupID           protocol.PathGroupID
	Store                 *TransmissionStore
	Abandon               context.CancelFunc
	SendControl           func(protocol.Frame, func()) bool
	ValidateProbeProgress func(uint64, uint64) bool
}

// LaneObserver receives cumulative lane feedback parsed by any carrier reader.
type LaneObserver interface {
	ObserveDeliveryReport(context.Context, protocol.DeliveryReport, uint64) error
	ObserveTiming(protocol.LaneID, uint64, protocol.TimingPong, uint64)
	ObserveLaneAbandon(context.Context, protocol.LaneGeneration) error
	RouteDeliveryReport(protocol.DeliveryReport, func(bool)) bool
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
	// schedulerRouteReport routes cumulative feedback over a healthy outbound lane.
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
	pong           protocol.TimingPong
	receiveMicros  uint64
	reportComplete func(bool)
	frame          protocol.Frame
	result         chan error
}

// scheduledLane contains direction-local predictive state for one generation.
type scheduledLane struct {
	registration          LaneRegistration
	rttMicros             uint64
	deliveryRate          uint64
	lastReportMicros      uint64
	lastDataBytes         uint64
	lastDataPackets       uint64
	lastProbeBytes        uint64
	lastProbePackets      uint64
	rateSampleBytes       uint64
	rateSampleStartMicros uint64
	lastProgressAt        time.Time
	rttObserved           bool
	degraded              bool
	abandoning            bool
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

// laneCandidateGroup retains the two best currently usable lanes in one path group.
type laneCandidateGroup struct {
	id     protocol.PathGroupID
	first  scoredLane
	second scoredLane
}

// add retains candidate when it belongs to the group's best two lanes.
func (g *laneCandidateGroup) add(candidate scoredLane) {
	if scoredLaneBetter(candidate, g.first) {
		g.second = g.first
		g.first = candidate
	} else if scoredLaneBetter(candidate, g.second) {
		g.second = candidate
	}
}

// scoredLaneBetter orders score snapshots by delivery prediction and stable lane identity.
func scoredLaneBetter(left, right scoredLane) bool {
	if right.lane == nil || left.score != right.score {
		return right.lane == nil || left.score < right.score
	}
	leftID := left.lane.registration.LaneID
	rightID := right.lane.registration.LaneID
	for index := range leftID {
		if leftID[index] != rightID[index] {
			return leftID[index] < rightID[index]
		}
	}
	return false
}

// Scheduler assigns session packets to dynamically registered lanes.
type Scheduler struct {
	ingress      *packetqueue.Queue[Packet]
	events       chan schedulerEvent
	controlOrder []*scheduledLane
	packetID     uint64
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
		registration.ValidateProbeProgress == nil {
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

// ObserveDeliveryReport synchronously validates and applies cumulative parsing feedback.
func (s *Scheduler) ObserveDeliveryReport(ctx context.Context, report protocol.DeliveryReport,
	receiveMicros uint64) error {
	result := make(chan error, 1)
	select {
	case s.events <- schedulerEvent{
		kind: schedulerReport, report: report, receiveMicros: receiveMicros, result: result,
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
func (s *Scheduler) ObserveTiming(laneID protocol.LaneID, generation uint64, pong protocol.TimingPong,
	receiveMicros uint64) {
	select {
	case s.events <- schedulerEvent{
		kind: schedulerTiming, laneID: laneID, generation: generation, pong: pong, receiveMicros: receiveMicros,
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

// RouteDeliveryReport routes cumulative feedback and invokes onAccepted after the carrier write completes.
func (s *Scheduler) RouteDeliveryReport(report protocol.DeliveryReport, complete func(bool)) bool {
	select {
	case s.events <- schedulerEvent{kind: schedulerRouteReport, report: report, reportComplete: complete}:
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
	defer ticker.Stop()
	var preferred protocol.LaneID
	var pending *packetqueue.Item[Packet]
	var preempted *packetqueue.Item[Packet]
	defer func() {
		for _, item := range []*packetqueue.Item[Packet]{preempted, pending} {
			if item == nil {
				continue
			}
			err := s.ingress.Push(*item)
			switch {
			case err == nil:
			case errors.Is(err, packetqueue.ErrFull), errors.Is(err, packetqueue.ErrExpired),
				errors.Is(err, packetqueue.ErrClosed):
				item.ReleaseRetention()
			default:
				item.ReleaseRetention()
				result = errors.Join(result, err)
			}
		}
	}()
	for {
		progressed := false
		if pending == nil {
			if preempted != nil {
				pending = preempted
				preempted = nil
				progressed = true
			} else if len(lanes) > 0 {
				item, err := s.ingress.TryPop()
				switch {
				case err == nil:
					pending = &item
					progressed = true
				case errors.Is(err, packetqueue.ErrEmpty):
				case errors.Is(err, packetqueue.ErrClosed):
					return queueResult(ctx, "dequeue scheduler ingress", err)
				default:
					return err
				}
			}
		}
		if pending != nil && pending.Priority == packetqueue.PriorityNormal {
			item, err := s.ingress.TryPopPriority(packetqueue.PriorityControl)
			switch {
			case err == nil:
				preempted = pending
				pending = &item
				progressed = true
			case errors.Is(err, packetqueue.ErrEmpty):
			case errors.Is(err, packetqueue.ErrClosed):
				return queueResult(ctx, "dequeue scheduler control ingress", err)
			default:
				return err
			}
		}
		if pending != nil {
			if !time.Now().Before(pending.Deadline) {
				pending.ReleaseRetention()
				pending = nil
				progressed = true
			} else if len(lanes) > 0 {
				scheduled, err := s.schedule(lanes, &preferred, pending)
				if err != nil {
					return err
				}
				if scheduled {
					pending = nil
					progressed = true
				}
			}
		}
		if progressed && pending == nil {
			select {
			case event := <-s.events:
				s.applyEvent(lanes, &preferred, event, time.Now())
			case now := <-ticker.C:
				s.checkAbandonment(lanes, now)
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			continue
		}
		select {
		case event := <-s.events:
			s.applyEvent(lanes, &preferred, event, time.Now())
		case <-s.ingress.Ready():
		case now := <-ticker.C:
			s.checkAbandonment(lanes, now)
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
		lanes[event.registration.LaneID] = replacement
		if current != nil {
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
				*preferred = protocol.LaneID{}
			}
		}
		event.result <- nil
	case schedulerReport:
		lane := lanes[event.report.LaneID]
		if lane != nil && lane.registration.Generation == event.report.Generation {
			event.result <- lane.applyReport(event.report, event.receiveMicros, now)
		} else {
			event.result <- nil
		}
	case schedulerTiming:
		lane := lanes[event.laneID]
		if lane != nil && lane.registration.Generation == event.generation {
			lane.applyTiming(event.pong, event.receiveMicros)
		}
	case schedulerRouteReport:
		s.routeReport(lanes, event.report, event.reportComplete)
	case schedulerPeerAbandon:
		lane := lanes[event.laneID]
		if lane != nil && lane.registration.Generation == event.generation && !lane.abandoning {
			lane.abandoning = true
			lane.registration.Abandon()
		}
		event.result <- nil
	case schedulerCloseSession:
		if !s.routeControl(lanes, event.frame, protocol.LaneID{}, func() { event.result <- nil }) {
			event.result <- ErrNoActiveLane
		}
	}
}

// routeReport duplicates feedback over its own healthy lane and one best alternate lane.
func (s *Scheduler) routeReport(lanes map[protocol.LaneID]*scheduledLane, report protocol.DeliveryReport,
	complete func(bool)) {
	frame, err := protocol.MarshalDeliveryReport(report)
	if err != nil {
		complete(false)
		return
	}
	var completeOnce sync.Once
	completed := func(sent bool) { completeOnce.Do(func() { complete(sent) }) }
	onSent := func() { completed(true) }
	accepted := false
	candidates := s.orderedControlLanes(lanes)
	for _, candidate := range candidates {
		if candidate.registration.LaneID == report.LaneID {
			accepted = candidate.registration.SendControl(frame, onSent)
			break
		}
	}
	for _, candidate := range candidates {
		if candidate.registration.LaneID != report.LaneID &&
			candidate.registration.SendControl(frame, onSent) {
			accepted = true
			break
		}
	}
	if !accepted {
		completed(false)
	}
}

// orderedControlLanes returns every healthy lane in deterministic predicted-delivery order.
// The result remains valid until this method is called again.
func (s *Scheduler) orderedControlLanes(lanes map[protocol.LaneID]*scheduledLane) []*scheduledLane {
	clear(s.controlOrder)
	ordered := s.controlOrder[:0]
	for _, lane := range lanes {
		if !lane.degraded && !lane.abandoning {
			ordered = append(ordered, lane)
		}
	}
	for index := 1; index < len(ordered); index++ {
		lane := ordered[index]
		position := index
		for position > 0 && laneBetterForFrame(lane, ordered[position-1], 0) {
			ordered[position] = ordered[position-1]
			position--
		}
		ordered[position] = lane
	}
	s.controlOrder = ordered
	return ordered
}

// schedule assigns one PacketID to one or two eligible lanes.
func (s *Scheduler) schedule(lanes map[protocol.LaneID]*scheduledLane, preferred *protocol.LaneID,
	item *packetqueue.Item[Packet]) (bool, error) {
	if err := item.Value.Validate(); err != nil {
		item.ReleaseRetention()
		return true, nil
	}
	remaining := time.Until(item.Deadline)
	if remaining <= 0 {
		item.ReleaseRetention()
		return true, nil
	}
	deadlineMicros := uint64(remaining / time.Microsecond)
	frameBytes := uint64(protocol.DataFrameOverhead + len(item.Value.Payload))
	candidates := selectCandidates(lanes, *preferred, item.Value.Kind.Control(), frameBytes, deadlineMicros)
	if candidates.count == 0 {
		if candidates.available {
			item.ReleaseRetention()
		}
		return candidates.available, nil
	}
	packetID := s.packetID + 1
	if packetID == 0 {
		return false, ErrCounterExhausted
	}
	scheduled := false
	for _, lane := range candidates.lanes[:candidates.count] {
		if lane.enqueue(item, packetID) {
			scheduled = true
			if !item.Value.Kind.Control() {
				s.packetID = packetID
				*preferred = lane.registration.LaneID
				return true, nil
			}
		}
	}
	if scheduled {
		s.packetID = packetID
	}
	return scheduled, nil
}

// selectCandidates returns lanes ordered for one transport packet or duplicated control packet.
func selectCandidates(lanes map[protocol.LaneID]*scheduledLane, preferred protocol.LaneID,
	control bool, frameBytes, maximumScore uint64) laneCandidates {
	var groupStorage [candidateGroupInlineCapacity]laneCandidateGroup
	groups := groupStorage[:0]
	for _, lane := range lanes {
		if lane.degraded || lane.abandoning || frameBytes > 0 && !lane.canAccept(frameBytes) {
			continue
		}
		groupIndex := -1
		for index := range groups {
			if groups[index].id == lane.registration.PathGroupID {
				groupIndex = index
				break
			}
		}
		if groupIndex < 0 {
			groups = append(groups, laneCandidateGroup{id: lane.registration.PathGroupID})
			groupIndex = len(groups) - 1
		}
		groups[groupIndex].add(scoredLane{lane: lane, score: lane.score(frameBytes)})
	}
	result := laneCandidates{available: len(groups) > 0}
	var first scoredLane
	var preferredCandidate scoredLane
	for index := range groups {
		for _, candidate := range [...]scoredLane{groups[index].first, groups[index].second} {
			if candidate.lane == nil {
				continue
			}
			if candidate.lane.registration.LaneID == preferred {
				preferredCandidate = candidate
			}
			if candidate.score < maximumScore && scoredLaneBetter(candidate, first) {
				first = candidate
			}
		}
	}
	if first.lane == nil {
		return result
	}
	if !control {
		preferredLimit := first.score
		if preferredLimit <= math.MaxUint64-preferredLaneHysteresisMicros {
			preferredLimit += preferredLaneHysteresisMicros
		} else {
			preferredLimit = math.MaxUint64
		}
		if preferredCandidate.lane != nil && preferredCandidate.score < maximumScore &&
			preferredCandidate.score <= preferredLimit {
			first = preferredCandidate
		}
		result.lanes[0] = first.lane
		result.count = 1
		return result
	}
	var second scoredLane
	for index := range groups {
		for _, candidate := range [...]scoredLane{groups[index].first, groups[index].second} {
			if candidate.lane == nil || candidate.lane == first.lane || candidate.score >= maximumScore {
				continue
			}
			candidateDistinct := candidate.lane.registration.PathGroupID != first.lane.registration.PathGroupID
			secondDistinct := second.lane != nil &&
				second.lane.registration.PathGroupID != first.lane.registration.PathGroupID
			if second.lane == nil || candidateDistinct && !secondDistinct ||
				candidateDistinct == secondDistinct && scoredLaneBetter(candidate, second) {
				second = candidate
			}
		}
	}
	if second.lane == nil {
		result.lanes[0] = first.lane
		result.count = 1
		return result
	}
	result.lanes = [2]*scheduledLane{first.lane, second.lane}
	result.count = 2
	return result
}

// laneEligible reports whether lane is one of the two best group members for the current frame size.
func laneEligible(lanes map[protocol.LaneID]*scheduledLane, lane *scheduledLane, frameBytes uint64) bool {
	if frameBytes > 0 && !lane.canAccept(frameBytes) {
		return false
	}
	better := 0
	for _, candidate := range lanes {
		if candidate == lane || candidate.degraded || candidate.abandoning ||
			candidate.registration.PathGroupID != lane.registration.PathGroupID ||
			frameBytes > 0 && !candidate.canAccept(frameBytes) {
			continue
		}
		if laneBetterForFrame(candidate, lane, frameBytes) {
			better++
			if better == 2 {
				return false
			}
		}
	}
	return true
}

// canAccept reports whether current retained work can accept one more frame.
func (l *scheduledLane) canAccept(frameBytes uint64) bool {
	return l.registration.Store.canAccept(frameBytes)
}

// laneBetterForFrame orders candidates by predicted frame delivery and then stable lane identity.
func laneBetterForFrame(left, right *scheduledLane, frameBytes uint64) bool {
	leftScore := left.score(frameBytes)
	rightScore := right.score(frameBytes)
	if leftScore != rightScore {
		return leftScore < rightScore
	}
	for index := range left.registration.LaneID {
		if left.registration.LaneID[index] != right.registration.LaneID[index] {
			return left.registration.LaneID[index] < right.registration.LaneID[index]
		}
	}
	return false
}

// score returns predicted delivery delay in microseconds.
func (l *scheduledLane) score(frameBytes uint64) uint64 {
	backlogBytes := l.registration.Store.backlogByteCount()
	if backlogBytes > math.MaxUint64-frameBytes {
		return math.MaxUint64
	}
	return l.backlogDelay(backlogBytes + frameBytes)
}

// backlogDelay returns the predicted completion delay for an already ordered byte prefix.
func (l *scheduledLane) backlogDelay(bytes uint64) uint64 {
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
func (l *scheduledLane) enqueue(item *packetqueue.Item[Packet], packetID uint64) bool {
	data := protocol.Data{
		PacketID: packetID, DeadlineMicros: item.Value.DeadlineMicros, Payload: item.Value.Payload,
	}
	size, err := protocol.DataFrameSize(data)
	if err != nil {
		return false
	}
	budget, ok := item.TakeRetention(size)
	if !ok {
		return false
	}
	transmission := retainedTransmission{
		data: data, kind: item.Value.Kind, priority: item.Priority, deadline: item.Deadline,
		budget: budget,
	}
	if l.enqueueTransmission(transmission) {
		return true
	}
	item.RestoreRetention(budget, size)
	return false
}

// enqueueTransmission retains one packet in the lane generation.
func (l *scheduledLane) enqueueTransmission(transmission retainedTransmission) bool {
	return l.registration.Store.push(transmission) == nil
}

// applyReport releases the reported carrier prefix and updates delivery-rate estimates.
func (l *scheduledLane) applyReport(report protocol.DeliveryReport, receiveMicros uint64, receiveTime time.Time) error {
	if !l.registration.ValidateProbeProgress(report.ProbePackets, report.ProbeBytes) {
		return ErrInvalidDeliveryReport
	}
	dataDirection, err := cumulativeDirection(
		report.DataPackets, report.DataBytes, l.lastDataPackets, l.lastDataBytes,
	)
	if err != nil {
		return err
	}
	probeDirection, err := cumulativeDirection(
		report.ProbePackets, report.ProbeBytes, l.lastProbePackets, l.lastProbeBytes,
	)
	if err != nil {
		return err
	}
	if dataDirection < 0 || probeDirection < 0 {
		if dataDirection > 0 || probeDirection > 0 {
			return ErrInvalidDeliveryReport
		}
		return nil
	}
	if dataDirection == 0 && probeDirection == 0 {
		return nil
	}
	deltaDataBytes := report.DataBytes - l.lastDataBytes
	deliveryConstrained := l.registration.Store.deliveryConstrained()
	_, stale, err := l.registration.Store.acknowledge(report.DataPackets, report.DataBytes)
	if err != nil {
		return err
	}
	if stale {
		return nil
	}
	if receiveMicros > l.lastReportMicros {
		l.updateDeliveryRate(deltaDataBytes, receiveMicros, deliveryConstrained)
		l.lastReportMicros = receiveMicros
	} else if deltaDataBytes > 0 {
		l.rateSampleStartMicros = l.lastReportMicros
		l.rateSampleBytes = 0
	}
	l.lastDataBytes = report.DataBytes
	l.lastDataPackets = report.DataPackets
	l.lastProbeBytes = report.ProbeBytes
	l.lastProbePackets = report.ProbePackets
	l.lastProgressAt = receiveTime
	l.degraded = laneDeadlineAtRisk(l, receiveTime)
	return nil
}

// updateDeliveryRate measures dense real data without reducing capacity from an application-limited sample.
func (l *scheduledLane) updateDeliveryRate(dataBytes, receiveMicros uint64, deliveryConstrained bool) {
	if dataBytes == 0 {
		return
	}
	if l.lastReportMicros == 0 || receiveMicros <= l.lastReportMicros ||
		receiveMicros-l.lastReportMicros > maximumRateSampleIntervalMicros ||
		l.rateSampleStartMicros == 0 || receiveMicros-l.rateSampleStartMicros > maximumRateSampleIntervalMicros {
		l.rateSampleStartMicros = receiveMicros
		l.rateSampleBytes = 0
		return
	}
	if l.rateSampleBytes > math.MaxUint64-dataBytes {
		l.rateSampleStartMicros = receiveMicros
		l.rateSampleBytes = 0
		return
	}
	l.rateSampleBytes += dataBytes
	if l.rateSampleBytes < minimumRateSampleBytes || receiveMicros <= l.rateSampleStartMicros {
		return
	}
	deltaMicros := receiveMicros - l.rateSampleStartMicros
	var sample uint64
	if l.rateSampleBytes > math.MaxUint64/1_000_000 {
		sample = math.MaxUint64
	} else {
		sample = l.rateSampleBytes * 1_000_000 / deltaMicros
	}
	if sample > l.deliveryRate || sample > 0 && deliveryConstrained {
		l.deliveryRate = weightedAverage7(l.deliveryRate, sample)
	}
	l.rateSampleBytes = 0
	l.rateSampleStartMicros = receiveMicros
}

// applyTiming updates a bounded RTT exponential moving average.
func (l *scheduledLane) applyTiming(pong protocol.TimingPong, receiveMicros uint64) {
	if receiveMicros < pong.PingSendMicros || pong.SendMicros < pong.ReceiveMicros {
		return
	}
	roundTrip := receiveMicros - pong.PingSendMicros
	processing := pong.SendMicros - pong.ReceiveMicros
	if processing < roundTrip {
		roundTrip -= processing
	} else {
		roundTrip = 1
	}
	if roundTrip == 0 {
		roundTrip = 1
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

// checkAbandonment degrades deadline-risk lanes and cancels expired generations.
func (s *Scheduler) checkAbandonment(lanes map[protocol.LaneID]*scheduledLane, now time.Time) {
	for _, lane := range lanes {
		if lane.abandoning {
			continue
		}
		assessment := lane.registration.Store.assessDeadlines(now, lane.backlogDelay)
		if !assessment.retained {
			lane.degraded = false
			continue
		}
		remaining := assessment.deadline.Sub(now)
		if remaining <= 0 {
			lane.degraded = true
			lane.abandoning = true
			s.announceAbandonment(lanes, lane)
			lane.registration.Abandon()
			continue
		}
		lane.degraded = assessment.atRisk
		if lane.degraded && laneProgressStalled(lane, now) &&
			hasTimelyAlternative(lanes, lane, assessment.frameBytes, remaining) {
			lane.abandoning = true
			s.announceAbandonment(lanes, lane)
			lane.registration.Abandon()
		}
	}
}

// laneProgressStalled reports whether cumulative peer parsing progress has stopped beyond a path-aware guard.
func laneProgressStalled(lane *scheduledLane, now time.Time) bool {
	if lane.lastProgressAt.IsZero() || now.Before(lane.lastProgressAt) {
		return false
	}
	marginMicros := uint64(progressStallReportMargin / time.Microsecond)
	if lane.rttMicros > (math.MaxUint64-marginMicros)/2 {
		return false
	}
	thresholdMicros := lane.rttMicros*2 + marginMicros
	minimumMicros := uint64(minimumProgressStall / time.Microsecond)
	if thresholdMicros < minimumMicros {
		thresholdMicros = minimumMicros
	}
	if thresholdMicros > uint64(math.MaxInt64/time.Microsecond) {
		return false
	}
	threshold := time.Duration(thresholdMicros) * time.Microsecond
	return now.Sub(lane.lastProgressAt) >= threshold
}

// laneDeadlineAtRisk reports whether retained work is no longer predicted to meet its earliest deadline.
func laneDeadlineAtRisk(lane *scheduledLane, now time.Time) bool {
	return lane.registration.Store.atRisk(now, lane.backlogDelay)
}

// hasTimelyAlternative reports whether another eligible generation can carry equivalent work before the first deadline.
func hasTimelyAlternative(lanes map[protocol.LaneID]*scheduledLane, current *scheduledLane,
	frameBytes uint64, remaining time.Duration) bool {
	remainingMicros := uint64(remaining / time.Microsecond)
	for _, candidate := range lanes {
		if candidate != current && !candidate.degraded && !candidate.abandoning &&
			laneEligible(lanes, candidate, frameBytes) && candidate.score(frameBytes) < remainingMicros {
			return true
		}
	}
	return false
}

// announceAbandonment asks the peer to close the same generation over another healthy lane.
func (s *Scheduler) announceAbandonment(lanes map[protocol.LaneID]*scheduledLane, abandoned *scheduledLane) {
	frame, err := protocol.MarshalLaneAbandon(protocol.LaneGeneration{
		LaneID: abandoned.registration.LaneID, Generation: abandoned.registration.Generation,
	})
	if err != nil {
		return
	}
	s.routeControl(lanes, frame, abandoned.registration.LaneID, nil)
}

// routeControl queues one fixed control frame on the best healthy lane outside exclude.
func (s *Scheduler) routeControl(lanes map[protocol.LaneID]*scheduledLane, frame protocol.Frame,
	exclude protocol.LaneID, onSent func()) bool {
	for _, candidate := range s.orderedControlLanes(lanes) {
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
	now := time.Now()
	retained := removed.registration.Store.drain()
	sort.SliceStable(retained, func(left, right int) bool {
		return retained[left].deadline.Before(retained[right].deadline)
	})
	for index := range retained {
		transmission := &retained[index]
		if transmission.kind != wgpacket.TransportData || transmission.migrated ||
			!now.Before(transmission.deadline) {
			transmission.release()
			continue
		}
		transmission.migrated = true
		frameBytes := uint64(transmission.size)
		remaining := transmission.deadline.Sub(now)
		deadlineMicros := uint64(remaining / time.Microsecond)
		candidates := selectCandidates(lanes, protocol.LaneID{}, false, frameBytes, deadlineMicros)
		for _, candidate := range candidates.lanes[:candidates.count] {
			if candidate.enqueueTransmission(*transmission) {
				transmission.budget = nil
				break
			}
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
