package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/auth"
	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/lifecycle"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
)

var (
	// ErrLaneLimit indicates that a session has reached its stable lane identifier limit.
	ErrLaneLimit = errors.New("session lane limit reached")
	// ErrSessionClosed indicates admission against a closing or expired session.
	ErrSessionClosed = errors.New("session closed")
	// ErrPathGroupMismatch indicates a stable lane identity changed its correlation group.
	ErrPathGroupMismatch = errors.New("lane path group changed")
)

// sessionLane is one active connection generation and its cancellation function.
type sessionLane struct {
	generation uint64
	cancel     context.CancelFunc
}

// laneHistory retains one lane's immutable path group and highest accepted generation.
type laneHistory struct {
	highestGeneration uint64
	pathGroupID       protocol.PathGroupID
}

// detachState identifies and owns one detached-session expiry interval.
type detachState struct {
	timer *time.Timer
}

// serverSession owns one logical target endpoint and shared data plane across lane generations.
type serverSession struct {
	owner        *Server
	id           protocol.SessionID
	secret       protocol.SessionSecret
	endpoint     *datagram.Remote
	ingressQueue *packetqueue.Queue[relay.Packet]
	ingress      *relay.Ingress
	receiver     *relay.Receiver
	scheduler    *relay.Scheduler
	ctx          context.Context
	cancel       context.CancelFunc
	group        *lifecycle.Group
	closeOnce    sync.Once
	mu           sync.Mutex
	history      map[protocol.LaneID]laneHistory
	lanes        map[protocol.LaneID]sessionLane
	reservations int
	joinNonces   *auth.ReplayCache
	detach       *detachState
}

// newServerSession allocates one target-owning session.
func newServerSession(parent context.Context, owner *Server, id protocol.SessionID, secret protocol.SessionSecret,
	endpoint *datagram.Remote) (*serverSession, error) {
	ingressQueue, err := packetqueue.NewWithBudget[relay.Packet](owner.config.IngressLimits, owner.retention)
	if err != nil {
		return nil, err
	}
	ingress, err := relay.NewIngress(endpoint, ingressQueue, owner.config.Clock, owner.config.Deadlines)
	if err != nil {
		return nil, err
	}
	receiver, err := relay.NewReceiver(relay.ReceiverConfig{
		Endpoint: endpoint, Clock: owner.config.Clock, DeduplicationSize: owner.config.DeduplicationWindow,
	})
	if err != nil {
		return nil, err
	}
	scheduler, err := relay.NewScheduler(ingressQueue)
	if err != nil {
		return nil, err
	}
	joinNonces, err := auth.NewReplayCache(owner.config.JoinNonceEntries)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	session := &serverSession{
		owner: owner, id: id, secret: secret, endpoint: endpoint, ingressQueue: ingressQueue, ingress: ingress,
		receiver: receiver, scheduler: scheduler, ctx: ctx, cancel: cancel,
		history:    make(map[protocol.LaneID]laneHistory),
		lanes:      make(map[protocol.LaneID]sessionLane),
		joinNonces: joinNonces,
	}
	session.group = lifecycle.NewGroup(ctx)
	return session, nil
}

// start begins session workers after the owner registry retains the session.
func (s *serverSession) start() {
	s.group.Go(s.ingress.Run)
	s.group.Go(s.scheduler.Run)
	go func() {
		err := s.group.Wait()
		if err != nil && s.ctx.Err() == nil && s.owner.config.Logger != nil {
			s.owner.config.Logger.Warn("relay session ended", "error", redactTargetError(err))
		}
		s.close()
	}()
}

// reserveLane validates and reserves a strictly increasing generation before acceptance.
func (s *serverSession) reserveLane(laneID protocol.LaneID, generation uint64,
	pathGroupID protocol.PathGroupID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return ErrSessionClosed
	}
	history, known := s.history[laneID]
	if generation <= history.highestGeneration {
		return relay.ErrStaleLane
	}
	if known && history.pathGroupID != pathGroupID {
		return ErrPathGroupMismatch
	}
	if !known && len(s.history) >= s.owner.config.MaxLanesPerSession {
		return ErrLaneLimit
	}
	s.history[laneID] = laneHistory{highestGeneration: generation, pathGroupID: pathGroupID}
	s.reservations++
	if s.detach != nil {
		s.detach.timer.Stop()
		s.detach = nil
	}
	return nil
}

// rejectReservedLane releases one accepted reservation and restarts detached expiry when needed.
func (s *serverSession) rejectReservedLane() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservations--
	s.startDetachTimerLocked()
}

// acceptJoinNonce retains one nonce for the bounded authentication replay window.
func (s *serverSession) acceptJoinNonce(nonce protocol.Nonce, timestamp, now int64) error {
	expires, err := auth.ReplayExpiry(timestamp, s.owner.config.AuthenticationSkew)
	if err != nil {
		return err
	}
	return s.joinNonces.CheckAndStore(nonce, now, expires)
}

// joinSecret returns a copy of the ephemeral secret while the session remains available.
func (s *serverSession) joinSecret() (protocol.SessionSecret, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return protocol.SessionSecret{}, false
	}
	return s.secret, true
}

// creationCredentials returns immutable response credentials while the session remains available.
func (s *serverSession) creationCredentials() (protocol.SessionID, protocol.SessionSecret, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return protocol.SessionID{}, protocol.SessionSecret{}, false
	}
	return s.id, s.secret, true
}

// runLane registers and serves one reserved lane generation.
func (s *serverSession) runLane(connection carrier.Conn, laneID protocol.LaneID, generation uint64,
	pathGroupID protocol.PathGroupID) error {
	defer s.rejectReservedLane()
	store, err := relay.NewTransmissionStoreWithBudget(s.owner.config.LaneLimits, s.owner.retention)
	if err != nil {
		return reportLaneError(err)
	}
	laneContext, cancelLane := context.WithCancelCause(s.ctx)
	abandon := func() { cancelLane(relay.ErrLaneAbandoned) }
	stop := func() { cancelLane(context.Canceled) }
	lane, err := relay.NewLane(relay.LaneConfig{
		Carrier: connection, Receiver: s.receiver, Store: store, Clock: s.owner.config.Clock,
		Observer: s.scheduler, SessionClose: func(protocol.CloseReason) { s.close() }, SessionFailure: s.close,
		LaneID: laneID, Generation: generation,
		RequireClockSync: true,
	})
	if err != nil {
		cancelLane(context.Canceled)
		return reportLaneError(err)
	}
	if err := s.scheduler.Register(s.ctx, relay.LaneRegistration{
		LaneID: laneID, Generation: generation, PathGroupID: pathGroupID, Store: store,
		Abandon: abandon, SendControl: lane.SendControl, ValidateProbeProgress: lane.ValidateProbeProgress,
	}); err != nil {
		cancelLane(context.Canceled)
		return err
	}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		cancelLane(context.Canceled)
		s.scheduler.Remove(s.ctx, laneID, generation)
		return ErrSessionClosed
	}
	if s.history[laneID].highestGeneration != generation {
		s.mu.Unlock()
		cancelLane(context.Canceled)
		s.scheduler.Remove(s.ctx, laneID, generation)
		return relay.ErrStaleLane
	}
	previous := s.lanes[laneID]
	s.lanes[laneID] = sessionLane{generation: generation, cancel: stop}
	s.mu.Unlock()
	if previous.cancel != nil {
		previous.cancel()
	}
	err = lane.Run(laneContext)
	cancelLane(context.Canceled)
	s.scheduler.Remove(s.ctx, laneID, generation)
	s.mu.Lock()
	current := s.lanes[laneID]
	if current.generation == generation {
		delete(s.lanes, laneID)
	}
	s.mu.Unlock()
	if err == nil {
		return nil
	}
	if errors.Is(err, relay.ErrEndpointFailure) {
		s.close()
	}
	return &activeLaneError{cause: err}
}

// startDetachTimerLocked starts grace expiry when no active lane remains.
func (s *serverSession) startDetachTimerLocked() {
	if len(s.lanes) != 0 || s.reservations != 0 || s.detach != nil || s.ctx.Err() != nil {
		return
	}
	detach := new(detachState)
	s.detach = detach
	detach.timer = time.AfterFunc(s.owner.config.ReconnectGrace, func() { s.expireDetached(detach) })
}

// expireDetached closes the session only when detach still identifies its current detached interval.
func (s *serverSession) expireDetached(detach *detachState) {
	s.mu.Lock()
	if s.detach != detach || len(s.lanes) != 0 || s.reservations != 0 || s.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	s.detach = nil
	s.cancel()
	s.mu.Unlock()
	s.close()
}

// close releases session credentials, lanes, queues, and target ownership exactly once.
func (s *serverSession) close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.mu.Lock()
		if s.detach != nil {
			s.detach.timer.Stop()
		}
		s.detach = nil
		for _, lane := range s.lanes {
			lane.cancel()
		}
		s.lanes = nil
		clear(s.secret[:])
		s.mu.Unlock()
		s.ingressQueue.Close()
		s.endpoint.Close()
		s.owner.removeSession(s.id, s)
	})
}
