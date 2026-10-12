package relay

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
)

const (
	// measurementPauseLimit bounds preparation and completion independently of connection failure detection.
	measurementPauseLimit = time.Duration(protocol.MaxMeasurementHoldMicros) * time.Microsecond
	// measurementRetryInterval bounds unsuccessful attempts and unsolicited peer hints.
	measurementRetryInterval = 5 * time.Second
	// measurementQuietRatio limits recurring pause cost to one part in fifty of elapsed runtime.
	measurementQuietRatio = 49
)

// MeasurementWrite samples a phase at the carrier writer. Build returns false for an expired operation.
// Sent runs only after a complete successful write. SendMeasurement must queue rather than invoke Build.
type MeasurementWrite struct {
	Build func(uint64) (protocol.Measurement, bool)
	Sent  func()
}

// measurementTarget holds one actual carrier's timing evidence within a session operation.
type measurementTarget struct {
	peer        LaneRegistration
	stage       protocol.MeasurementKind
	sample      clockmap.Sample
	probeBudget uint64
	replyBudget uint64
	done        bool
}

// measurementAttempt owns one fixed cohort, one session pause, and a non-extendable deadline.
type measurementAttempt struct {
	id         uint64
	peer       LaneRegistration
	identities []protocol.LaneGeneration
	targets    map[protocol.LaneID]*measurementTarget
	ctx        context.Context
	cancel     context.CancelFunc
	ready      <-chan struct{}
	proof      func() (time.Time, bool)
	stage      protocol.MeasurementKind
	prepared   bool
	peerReady  bool
	completed  int
}

// measurementCoordinator owns the sole operation across all session carrier readers and writers.
// The scheduler owns Data gates. This mutex never covers carrier I/O or a blocking wait.
type measurementCoordinator struct {
	mu            sync.Mutex
	scheduler     *Scheduler
	peers         map[protocol.LaneID]LaneRegistration
	active        *measurementAttempt
	nextID        uint64
	lastPeerID    uint64
	nextAt        time.Time
	nextRequestAt time.Time
	closed        bool
}

// register publishes one exact target generation after scheduler admission.
func (m *measurementCoordinator) register(peer LaneRegistration) {
	m.mu.Lock()
	m.peers[peer.LaneID] = peer
	m.mu.Unlock()
}

// remove withdraws a target without retracting any exposed quiet promise.
func (m *measurementCoordinator) remove(laneID protocol.LaneID, generation uint64) {
	m.mu.Lock()
	if m.peers[laneID].Generation == generation {
		delete(m.peers, laneID)
	}
	m.mu.Unlock()
}

// close releases the operation on session shutdown.
func (m *measurementCoordinator) close() {
	m.mu.Lock()
	m.closed = true
	if m.active != nil {
		m.active.cancel()
		m.active = nil
	}
	clear(m.peers)
	m.mu.Unlock()
}

// RequestMeasurement coalesces controller requests independently of Ping/Pong scheduling.
func (s *Scheduler) RequestMeasurement(target protocol.LaneGeneration) {
	if s.measurements == nil {
		return
	}
	m := s.measurements
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[target.LaneID]
	if !ok || peer.Generation != target.Generation || peer.SendMeasurement == nil || m.closed ||
		m.active != nil || time.Now().Before(m.nextAt) {
		return
	}
	if !peer.MeasurementInitiator {
		if time.Now().Before(m.nextRequestAt) {
			return
		}
		m.nextRequestAt = time.Now().Add(measurementRetryInterval)
		peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
			return protocol.Measurement{Kind: protocol.MeasurementRequest}, true
		}})
		return
	}
	m.startClientLocked(peer)
}

// startClientLocked snapshots all eligible generations so one pause refreshes the entire fixed cohort.
func (m *measurementCoordinator) startClientLocked(peer LaneRegistration) {
	if m.nextID == ^uint64(0) {
		return
	}
	identities := make([]protocol.LaneGeneration, 0, len(m.peers))
	for _, target := range m.peers {
		if target.SendMeasurement != nil {
			identities = append(identities, protocol.LaneGeneration{LaneID: target.LaneID, Generation: target.Generation})
		}
	}
	if len(identities) == 0 || len(identities) > protocol.MaxMeasurementTargets {
		return
	}
	slices.SortFunc(identities, func(left, right protocol.LaneGeneration) int { return cmp.Compare(left.LaneID, right.LaneID) })
	m.nextID++
	m.startLocked(peer, m.nextID, identities)
}

// startLocked reserves session ownership before asynchronously preparing both endpoints.
func (m *measurementCoordinator) startLocked(peer LaneRegistration, id uint64, identities []protocol.LaneGeneration) {
	ctx, cancel := context.WithTimeout(context.Background(), measurementPauseLimit)
	a := &measurementAttempt{id: id, peer: peer, identities: identities,
		targets: make(map[protocol.LaneID]*measurementTarget, len(identities)), ctx: ctx, cancel: cancel}
	for _, target := range identities {
		a.targets[target.LaneID] = &measurementTarget{peer: m.peers[target.LaneID]}
	}
	m.active = a
	a.ready, a.proof = m.scheduler.beginMeasurementDrain(ctx, func(cost time.Duration) { m.released(a, cost) })
	if a.ready == nil {
		m.active = nil
		m.nextAt = time.Now().Add(measurementRetryInterval)
		a.cancel()
		return
	}
	if peer.MeasurementInitiator && !peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.active != a || a.ctx.Err() != nil {
			return protocol.Measurement{}, false
		}
		a.stage = protocol.MeasurementBegin
		return protocol.Measurement{Kind: protocol.MeasurementBegin, ID: a.id, Targets: a.identities}, true
	}}) {
		m.finishLocked(a)
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-a.ready:
			m.prepared(a)
		}
		<-ctx.Done()
		m.finish(a)
	}()
}

// prepared announces server readiness or starts probes after both preparations have completed.
func (m *measurementCoordinator) prepared(a *measurementAttempt) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.usableLocked(a) {
		m.finishLocked(a)
		return
	}
	a.prepared = true
	if a.peer.MeasurementInitiator {
		if a.peerReady {
			m.queueProbesLocked(a)
		}
		return
	}
	if !a.peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !m.usableLocked(a) {
			return protocol.Measurement{}, false
		}
		a.stage = protocol.MeasurementReady
		return protocol.Measurement{Kind: protocol.MeasurementReady, ID: a.id, Valid: true}, true
	}}) {
		m.finishLocked(a)
	}
}

// queueProbesLocked measures the fixed cohort on its actual carriers without serializing their round trips.
func (m *measurementCoordinator) queueProbesLocked(a *measurementAttempt) {
	for _, identity := range a.identities {
		target := a.targets[identity.LaneID]
		if !target.peer.SendMeasurement(MeasurementWrite{Build: func(now uint64) (protocol.Measurement, bool) {
			m.mu.Lock()
			defer m.mu.Unlock()
			if !m.usableLocked(a) {
				return protocol.Measurement{}, false
			}
			budget := m.budgetLocked(a)
			if budget == 0 {
				return protocol.Measurement{}, false
			}
			target.sample.LocalSendMicros = now
			target.probeBudget = budget
			target.stage = protocol.MeasurementProbe
			return protocol.Measurement{Kind: protocol.MeasurementProbe, ID: a.id, ProbeMicros: now, HoldMicros: budget}, true
		}}) {
			// Build cannot run under this mutex, so no queued probe has been exposed yet.
			m.finishLocked(a)
			return
		}
	}
}

// usableLocked checks operation ownership, the fixed cohort, and the scheduler's unchanged prefix proof.
func (m *measurementCoordinator) usableLocked(a *measurementAttempt) bool {
	if m.active != a || a.ctx.Err() != nil || a.proof == nil {
		return false
	}
	for _, identity := range a.identities {
		if m.peers[identity.LaneID].Generation != identity.Generation {
			return false
		}
	}
	_, valid := a.proof()
	return valid
}

// budgetLocked samples the remaining promise without extending the operation or scheduler deadline.
func (m *measurementCoordinator) budgetLocked(a *measurementAttempt) uint64 {
	deadline, valid := a.proof()
	if !valid {
		return 0
	}
	if own, ok := a.ctx.Deadline(); ok && own.Before(deadline) {
		deadline = own
	}
	return uint64(max(0, time.Until(deadline)/time.Microsecond))
}

// ObserveMeasurement dispatches a parsed phase on its actual carrier generation.
// Delayed operations are harmless. A current operation in the wrong direction or phase is a protocol error.
func (s *Scheduler) ObserveMeasurement(source protocol.LaneGeneration, value protocol.Measurement, now uint64) error {
	if s.measurements == nil {
		return nil
	}
	m := s.measurements
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[source.LaneID]
	if !ok || peer.Generation != source.Generation || m.closed {
		return nil
	}
	clientPhase := value.Kind == protocol.MeasurementRequest || value.Kind == protocol.MeasurementReady || value.Kind == protocol.MeasurementReply
	if peer.MeasurementInitiator != clientPhase {
		return ErrUnexpectedFrame
	}
	if value.Kind == protocol.MeasurementRequest {
		if m.active == nil && !time.Now().Before(m.nextAt) {
			m.startClientLocked(peer)
		}
		return nil
	}
	if value.Kind == protocol.MeasurementBegin {
		if value.ID <= m.lastPeerID {
			return nil
		}
		m.lastPeerID = value.ID
		valid := m.active == nil && !time.Now().Before(m.nextAt)
		included := false
		for _, identity := range value.Targets {
			target := m.peers[identity.LaneID]
			valid = valid && target.Generation == identity.Generation && target.SendMeasurement != nil
			included = included || identity == source
		}
		if !valid || !included {
			m.rejectBeginLocked(peer, value.ID)
			return nil
		}
		m.startLocked(peer, value.ID, value.Targets)
		return nil
	}
	a := m.active
	if a == nil || a.id != value.ID {
		return nil
	}
	if value.Kind == protocol.MeasurementReady {
		if source.LaneID != a.peer.LaneID || source.Generation != a.peer.Generation {
			return nil
		}
		if a.stage != protocol.MeasurementBegin {
			return ErrUnexpectedFrame
		}
		if !value.Valid {
			m.finishLocked(a)
			return nil
		}
		a.peerReady = true
		a.stage = protocol.MeasurementReady
		if a.prepared {
			m.queueProbesLocked(a)
		}
		return nil
	}
	target := a.targets[source.LaneID]
	if target == nil || target.peer.Generation != source.Generation {
		return nil
	}
	switch value.Kind {
	case protocol.MeasurementProbe:
		if a.stage != protocol.MeasurementReady || target.stage != 0 {
			return ErrUnexpectedFrame
		}
		target.sample.LocalSendMicros = value.ProbeMicros
		target.sample.RemoteReceiveMicros = now
		target.probeBudget = value.HoldMicros
		target.stage = protocol.MeasurementProbe
		peer.SendMeasurement(MeasurementWrite{Build: func(send uint64) (protocol.Measurement, bool) {
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.active != a || a.ctx.Err() != nil {
				return protocol.Measurement{}, false
			}
			target.sample.RemoteSendMicros = send
			if m.usableLocked(a) {
				target.replyBudget = m.budgetLocked(a)
			}
			target.stage = protocol.MeasurementReply
			return protocol.Measurement{Kind: protocol.MeasurementReply, ID: a.id,
				ProbeMicros: target.sample.LocalSendMicros, ReceiveMicros: target.sample.RemoteReceiveMicros,
				ReplyMicros: send, HoldMicros: target.replyBudget}, true
		}})
	case protocol.MeasurementReply:
		if target.stage != protocol.MeasurementProbe || value.ProbeMicros != target.sample.LocalSendMicros {
			return ErrUnexpectedFrame
		}
		target.sample.RemoteReceiveMicros = value.ReceiveMicros
		target.sample.RemoteSendMicros = value.ReplyMicros
		target.sample.LocalReceiveMicros = now
		target.replyBudget = value.HoldMicros
		target.stage = protocol.MeasurementReply
		rtt, valid := m.resultLocked(a, target)
		if valid {
			go s.publishMeasurement(a, target, rtt, func(applied bool) { m.sendResult(a, target, applied) })
		} else {
			go m.sendResult(a, target, false)
		}
	case protocol.MeasurementResult:
		if target.stage != protocol.MeasurementReply || target.done {
			return ErrUnexpectedFrame
		}
		target.sample.LocalReceiveMicros = value.ReceiveMicros
		target.stage = protocol.MeasurementResult
		rtt, valid := m.resultLocked(a, target)
		if value.Valid && valid {
			go s.publishMeasurement(a, target, rtt, func(bool) { m.finishTarget(a, target) })
		} else {
			m.finishTargetLocked(a, target)
		}
	}
	return nil
}

// rejectBeginLocked declines preparation without acquiring a pause or failing a healthy lane.
func (m *measurementCoordinator) rejectBeginLocked(peer LaneRegistration, id uint64) {
	if peer.SendMeasurement != nil {
		peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
			return protocol.Measurement{Kind: protocol.MeasurementReady, ID: id}, true
		}})
	}
}

// resultLocked validates timestamps and both fixed quiet budgets for the actual measured carrier.
func (m *measurementCoordinator) resultLocked(a *measurementAttempt, target *measurementTarget) (uint64, bool) {
	if !m.usableLocked(a) {
		return 0, false
	}
	sample := target.sample
	mapping, err := clockmap.Estimate(sample)
	if err != nil || !ClockMappingUsable(mapping) {
		return 0, false
	}
	span := sample.LocalReceiveMicros - sample.LocalSendMicros
	processing := sample.RemoteSendMicros - sample.RemoteReceiveMicros
	if processing > span {
		return 0, false
	}
	rtt := max(1, span-processing)
	return rtt, rtt < target.replyBudget && span < target.probeBudget
}

// sendResult retains client quiet until the result completes the measured carrier write.
func (m *measurementCoordinator) sendResult(a *measurementAttempt, target *measurementTarget, valid bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != a || a.ctx.Err() != nil {
		return
	}
	target.peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.active != a || a.ctx.Err() != nil {
			return protocol.Measurement{}, false
		}
		value := protocol.Measurement{Kind: protocol.MeasurementResult, ID: a.id, Valid: valid && m.usableLocked(a)}
		if value.Valid {
			value.ReceiveMicros = target.sample.LocalReceiveMicros
		}
		target.stage = protocol.MeasurementResult
		return value, true
	}, Sent: func() { m.finishTarget(a, target) }})
}

// finishTarget retires a completed carrier without releasing other measurements in the same cohort.
func (m *measurementCoordinator) finishTarget(a *measurementAttempt, target *measurementTarget) {
	m.mu.Lock()
	m.finishTargetLocked(a, target)
	m.mu.Unlock()
}

// finishTargetLocked resumes Data only after every target has completed or the common deadline expires.
func (m *measurementCoordinator) finishTargetLocked(a *measurementAttempt, target *measurementTarget) {
	if m.active != a || target.done {
		return
	}
	target.done = true
	a.completed++
	if a.completed == len(a.targets) {
		m.finishLocked(a)
	}
}

// finish retires only the matching operation.
func (m *measurementCoordinator) finish(a *measurementAttempt) {
	m.mu.Lock()
	m.finishLocked(a)
	m.mu.Unlock()
}

// finishLocked requests release without making another operation eligible before the Data gates reopen.
func (m *measurementCoordinator) finishLocked(a *measurementAttempt) {
	if m.active == a {
		a.cancel()
	}
}

// released charges actual Data pause time after the scheduler has reopened every current store.
func (m *measurementCoordinator) released(a *measurementAttempt, cost time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != a {
		return
	}
	m.nextAt = time.Now().Add(max(measurementRetryInterval, cost*measurementQuietRatio))
	m.active = nil
	a.cancel()
}
