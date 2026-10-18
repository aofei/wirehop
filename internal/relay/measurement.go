package relay

import (
	"context"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
)

const (
	// measurementPauseLimit bounds the whole transaction, including preparation before the timed exchange.
	// Two wire-hold intervals leave room for delayed prefix feedback as well as the timed exchange.
	measurementPauseLimit = 2 * time.Duration(protocol.MaxMeasurementHoldMicros) * time.Microsecond
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

// measurementAttempt samples one actual carrier within the session operation's non-extendable pause.
type measurementAttempt struct {
	id          uint64
	peer        LaneRegistration
	sample      clockmap.Sample
	probeBudget uint64
	replyBudget uint64
	ctx         context.Context
	cancel      context.CancelFunc // Only the initial target owns operation cancellation.
	ready       <-chan struct{}
	proof       func() (time.Time, bool)
	stage       protocol.MeasurementKind
	prepared    bool
	peerReady   bool
	readyQueued bool
	finished    bool
}

// measurementCoordinator owns the sole operation across all session carrier readers and writers.
// The scheduler owns Data gates. This mutex never covers carrier I/O or a blocking wait.
type measurementCoordinator struct {
	mu            sync.Mutex
	scheduler     *Scheduler
	peers         map[protocol.LaneID]LaneRegistration
	active        *measurementAttempt
	attempts      map[protocol.LaneID]*measurementAttempt
	probesStarted bool
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
	clear(m.attempts)
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
		time.Now().Before(m.nextAt) {
		return
	}
	if m.active != nil && m.attempts[target.LaneID] != nil {
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
	if m.active != nil {
		m.joinLocked(peer, false)
		return
	}
	m.startClientLocked(peer, true)
}

// startClientLocked allocates an operation for the requesting carrier generation.
func (m *measurementCoordinator) startClientLocked(peer LaneRegistration, localRequest bool) {
	if m.nextID == ^uint64(0) {
		return
	}
	m.nextID++
	m.startLocked(peer, m.nextID, localRequest)
}

// startLocked reserves session ownership before asynchronously preparing both endpoints.
func (m *measurementCoordinator) startLocked(peer LaneRegistration, id uint64, localRequest bool) {
	ctx, cancel := context.WithTimeout(context.Background(), measurementPauseLimit)
	a := &measurementAttempt{id: id, peer: peer, ctx: ctx, cancel: cancel}
	if !peer.MeasurementInitiator {
		a.stage = protocol.MeasurementBegin
	}
	m.active = a
	m.attempts = map[protocol.LaneID]*measurementAttempt{peer.LaneID: a}
	m.probesStarted = false
	target := protocol.LaneGeneration{LaneID: peer.LaneID, Generation: peer.Generation}
	a.ready, a.proof = m.scheduler.beginMeasurementDrain(ctx, target, localRequest, func(cost time.Duration) { m.released(a, cost) })
	if a.ready == nil {
		m.active = nil
		clear(m.attempts)
		m.nextAt = time.Now().Add(measurementRetryInterval)
		a.cancel()
		return
	}
	m.prepareLocked(a)
}

// joinLocked returns a newly enrolled generation within the same fixed session pause, or nil once enrollment ends.
func (m *measurementCoordinator) joinLocked(peer LaneRegistration, receivedBegin bool) *measurementAttempt {
	root := m.active
	if root.ctx.Err() != nil || root.finished || m.probesStarted {
		return nil
	}
	a := &measurementAttempt{
		id: root.id, peer: peer, ctx: root.ctx, ready: root.ready, proof: root.proof,
	}
	if !peer.MeasurementInitiator && receivedBegin {
		a.stage = protocol.MeasurementBegin
	}
	m.attempts[peer.LaneID] = a
	m.prepareLocked(a)
	return a
}

// includePrimary enrolls an active directional primary without requiring a separate timing refresh decision.
func (m *measurementCoordinator) includePrimary(target protocol.LaneGeneration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[target.LaneID]
	if !ok || peer.Generation != target.Generation || m.active == nil || m.active.ctx.Err() != nil ||
		m.attempts[target.LaneID] != nil {
		return
	}
	a := m.joinLocked(peer, false)
	if a == nil || peer.MeasurementInitiator {
		return
	}
	if !peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
		return protocol.Measurement{Kind: protocol.MeasurementRequest}, true
	}}) {
		// An independent client Begin may still enroll this unannounced carrier before timing starts.
		delete(m.attempts, peer.LaneID)
	}
}

// ownsLocked identifies a current member without accepting completions from an earlier session pause.
func (m *measurementCoordinator) ownsLocked(a *measurementAttempt) bool {
	return m.active != nil && m.attempts[a.peer.LaneID] == a && !a.finished
}

// prepareLocked queues one target's preparation while all members share the session drain and its deadline.
func (m *measurementCoordinator) prepareLocked(a *measurementAttempt) {
	peer := a.peer
	if peer.MeasurementInitiator && !peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !m.ownsLocked(a) || a.ctx.Err() != nil {
			return protocol.Measurement{}, false
		}
		a.stage = protocol.MeasurementBegin
		return protocol.Measurement{Kind: protocol.MeasurementBegin, ID: a.id}, true
	}}) {
		m.finishLocked(a)
	}
	go func() {
		select {
		case <-a.ctx.Done():
		case <-a.ready:
			m.prepared(a)
		}
		<-a.ctx.Done()
		m.finish(a)
	}()
}

// prepared announces server readiness or starts the probe after both preparations have completed.
func (m *measurementCoordinator) prepared(a *measurementAttempt) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.usableLocked(a) {
		m.finishLocked(a)
		return
	}
	a.prepared = true
	if a.peer.MeasurementInitiator {
		m.probePreparedLocked()
	} else {
		m.readyPreparedLocked()
	}
}

// readyPreparedLocked confirms each member only after the client has enrolled every server-requested primary.
func (m *measurementCoordinator) readyPreparedLocked() {
	for _, member := range m.attempts {
		if !member.finished && (member.stage == 0 || !member.prepared) {
			return
		}
	}
	for _, member := range m.attempts {
		if member.finished || member.readyQueued {
			continue
		}
		member.readyQueued = true
		m.queueReadyLocked(member)
	}
}

// queueReadyLocked acknowledges one actual carrier's membership and the shared session prefix proof.
func (m *measurementCoordinator) queueReadyLocked(a *measurementAttempt) {
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

// probePreparedLocked seals the cohort once both endpoints have prepared every enrolled carrier.
func (m *measurementCoordinator) probePreparedLocked() {
	if m.probesStarted {
		return
	}
	for _, member := range m.attempts {
		if !member.finished && (!member.prepared || !member.peerReady) {
			return
		}
	}
	m.probesStarted = true
	for _, member := range m.attempts {
		if !member.finished {
			m.queueProbeLocked(member)
		}
	}
}

// queueProbeLocked samples the requesting carrier while the session remains quiet.
func (m *measurementCoordinator) queueProbeLocked(a *measurementAttempt) {
	if !a.peer.SendMeasurement(MeasurementWrite{Build: func(now uint64) (protocol.Measurement, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !m.usableLocked(a) {
			return protocol.Measurement{}, false
		}
		budget := m.budgetLocked(a)
		if budget == 0 {
			return protocol.Measurement{}, false
		}
		a.sample.LocalSendMicros = now
		a.probeBudget = budget
		a.stage = protocol.MeasurementProbe
		return protocol.Measurement{Kind: protocol.MeasurementProbe, ID: a.id, ProbeMicros: now, HoldMicros: budget}, true
	}}) {
		m.finishLocked(a)
	}
}

// usableLocked checks operation ownership, the actual generation, and the scheduler's unchanged session prefix proof.
func (m *measurementCoordinator) usableLocked(a *measurementAttempt) bool {
	if !m.ownsLocked(a) || a.ctx.Err() != nil || a.proof == nil {
		return false
	}
	if m.peers[a.peer.LaneID].Generation != a.peer.Generation {
		return false
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
	return min(protocol.MaxMeasurementHoldMicros, uint64(max(0, time.Until(deadline)/time.Microsecond)))
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
		if !time.Now().Before(m.nextAt) {
			if m.active == nil {
				m.startClientLocked(peer, false)
			} else if m.attempts[peer.LaneID] == nil {
				m.joinLocked(peer, false)
			}
		}
		return nil
	}
	if value.Kind == protocol.MeasurementBegin {
		if m.active != nil && m.active.id == value.ID {
			if member := m.attempts[peer.LaneID]; member == nil {
				m.joinLocked(peer, true)
			} else if member != m.active && member.stage == 0 && !member.finished {
				// An enrolled server primary still requires Begin before accepting its actual Probe.
				member.stage = protocol.MeasurementBegin
				m.readyPreparedLocked()
			}
			return nil
		}
		if value.ID <= m.lastPeerID {
			return nil
		}
		m.lastPeerID = value.ID
		if m.active != nil || time.Now().Before(m.nextAt) {
			m.rejectBeginLocked(peer, value.ID)
			return nil
		}
		m.startLocked(peer, value.ID, false)
		return nil
	}
	a := m.attempts[source.LaneID]
	if a == nil || a.id != value.ID || a.finished {
		return nil
	}
	if source.Generation != a.peer.Generation {
		return nil
	}
	if value.Kind == protocol.MeasurementReady {
		if a.stage != protocol.MeasurementBegin || a.peerReady {
			return ErrUnexpectedFrame
		}
		if !value.Valid {
			m.finishLocked(a)
			m.probePreparedLocked()
			return nil
		}
		a.stage = protocol.MeasurementReady
		a.peerReady = true
		m.probePreparedLocked()
		return nil
	}
	switch value.Kind {
	case protocol.MeasurementProbe:
		if a.stage != protocol.MeasurementReady {
			return ErrUnexpectedFrame
		}
		m.probesStarted = true
		a.sample.LocalSendMicros = value.ProbeMicros
		a.sample.RemoteReceiveMicros = now
		a.probeBudget = value.HoldMicros
		a.stage = protocol.MeasurementProbe
		peer.SendMeasurement(MeasurementWrite{Build: func(send uint64) (protocol.Measurement, bool) {
			m.mu.Lock()
			defer m.mu.Unlock()
			if !m.ownsLocked(a) || a.ctx.Err() != nil {
				return protocol.Measurement{}, false
			}
			a.sample.RemoteSendMicros = send
			if m.usableLocked(a) {
				a.replyBudget = m.budgetLocked(a)
			}
			a.stage = protocol.MeasurementReply
			return protocol.Measurement{Kind: protocol.MeasurementReply, ID: a.id,
				ProbeMicros: a.sample.LocalSendMicros, ReceiveMicros: a.sample.RemoteReceiveMicros,
				ReplyMicros: send, HoldMicros: a.replyBudget}, true
		}})
	case protocol.MeasurementReply:
		if a.stage != protocol.MeasurementProbe || value.ProbeMicros != a.sample.LocalSendMicros {
			return ErrUnexpectedFrame
		}
		a.sample.RemoteReceiveMicros = value.ReceiveMicros
		a.sample.RemoteSendMicros = value.ReplyMicros
		a.sample.LocalReceiveMicros = now
		a.replyBudget = value.HoldMicros
		a.stage = protocol.MeasurementReply
		rtt, valid := m.resultLocked(a)
		if valid {
			go s.publishMeasurement(a, rtt, func(applied bool) { m.sendResult(a, applied) })
		} else {
			go m.sendResult(a, false)
		}
	case protocol.MeasurementResult:
		if a.stage != protocol.MeasurementReply {
			return ErrUnexpectedFrame
		}
		a.sample.LocalReceiveMicros = value.ReceiveMicros
		a.stage = protocol.MeasurementResult
		rtt, valid := m.resultLocked(a)
		if value.Valid && valid {
			go s.publishMeasurement(a, rtt, func(bool) { m.finish(a) })
		} else {
			m.finishLocked(a)
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
func (m *measurementCoordinator) resultLocked(a *measurementAttempt) (uint64, bool) {
	if !m.usableLocked(a) {
		return 0, false
	}
	sample := a.sample
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
	return rtt, rtt < a.replyBudget && span < a.probeBudget
}

// sendResult retains client quiet until the result completes the measured carrier write.
func (m *measurementCoordinator) sendResult(a *measurementAttempt, valid bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ownsLocked(a) || a.ctx.Err() != nil {
		return
	}
	a.peer.SendMeasurement(MeasurementWrite{Build: func(uint64) (protocol.Measurement, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !m.ownsLocked(a) || a.ctx.Err() != nil {
			return protocol.Measurement{}, false
		}
		value := protocol.Measurement{Kind: protocol.MeasurementResult, ID: a.id, Valid: valid && m.usableLocked(a)}
		if value.Valid {
			value.ReceiveMicros = a.sample.LocalReceiveMicros
		}
		a.stage = protocol.MeasurementResult
		return value, true
	}, Sent: func() { m.finish(a) }})
}

// finish retires only the matching operation.
func (m *measurementCoordinator) finish(a *measurementAttempt) {
	m.mu.Lock()
	m.finishLocked(a)
	m.mu.Unlock()
}

// finishLocked requests release without making another operation eligible before the Data gates reopen.
func (m *measurementCoordinator) finishLocked(a *measurementAttempt) {
	if !m.ownsLocked(a) {
		return
	}
	a.finished = true
	for _, member := range m.attempts {
		if !member.finished {
			return
		}
	}
	m.active.cancel()
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
	clear(m.attempts)
	a.cancel()
}
