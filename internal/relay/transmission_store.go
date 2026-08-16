package relay

import (
	"cmp"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

var (
	// ErrInvalidTransmissionStore indicates invalid lane transmission limits or state.
	ErrInvalidTransmissionStore = errors.New("invalid transmission store")
	// ErrInvalidTransmission indicates inconsistent retained packet metadata.
	ErrInvalidTransmission = errors.New("invalid transmission")
	// ErrInvalidDeliveryReport indicates cumulative progress that cannot describe the sent prefix.
	ErrInvalidDeliveryReport = errors.New("invalid delivery report")
)

// maximumRetainedDequeCapacity preserves ordinary report batches without retaining exceptional lane high-water marks.
const maximumRetainedDequeCapacity = 256

// retainedTransmission is one assigned packet retained until parsing is reported or its generation is drained.
type retainedTransmission struct {
	data     protocol.Data
	kind     wgpacket.Kind
	priority packetqueue.Priority
	deadline time.Time
	migrated bool
	size     int
	budget   *retention.Budget
}

// deadlineAssessment summarizes retained deadline state in carrier order.
type deadlineAssessment struct {
	deadline   time.Time
	frameBytes uint64
	retained   bool
	atRisk     bool
}

// release returns a drained transmission's aggregate retention reservation.
func (t *retainedTransmission) release() {
	if t.budget == nil {
		return
	}
	t.budget.Release(1, t.size)
	t.budget = nil
}

// transmissionDeque is a compacting first-in, first-out transmission sequence.
type transmissionDeque struct {
	items []retainedTransmission
	head  int
}

// peek returns the oldest transmission without removing it.
func (d *transmissionDeque) peek() (retainedTransmission, bool) {
	if d.head == len(d.items) {
		return retainedTransmission{}, false
	}
	return d.items[d.head], true
}

// push appends transmission to the deque.
func (d *transmissionDeque) push(transmission retainedTransmission) {
	d.items = append(d.items, transmission)
}

// pop removes and returns the oldest transmission from the nonempty deque.
func (d *transmissionDeque) pop() retainedTransmission {
	transmission := d.items[d.head]
	d.items[d.head] = retainedTransmission{}
	d.head++
	if d.head == len(d.items) {
		d.resetEmpty()
		return transmission
	}
	d.compact()
	return transmission
}

// resetEmpty preserves ordinary reusable capacity while releasing an exceptional historical peak.
func (d *transmissionDeque) resetEmpty() {
	if cap(d.items) > maximumRetainedDequeCapacity {
		d.items = make([]retainedTransmission, 0, maximumRetainedDequeCapacity)
	} else {
		d.items = d.items[:0]
	}
	d.head = 0
}

// compact releases consumed capacity when enough of the backing slice is unused.
func (d *transmissionDeque) compact() {
	if d.head < 64 || d.head*2 < len(d.items) {
		return
	}
	remaining := len(d.items) - d.head
	copy(d.items, d.items[d.head:])
	clear(d.items[remaining:])
	d.items = d.items[:remaining]
	d.head = 0
	d.shrink()
}

// shrink releases exceptional capacity after the retained occupancy falls well below it.
func (d *transmissionDeque) shrink() {
	capacity := cap(d.items)
	if capacity <= maximumRetainedDequeCapacity || len(d.items) > capacity/4 {
		return
	}
	retainedCapacity := max(maximumRetainedDequeCapacity, len(d.items)*2)
	items := make([]retainedTransmission, len(d.items), retainedCapacity)
	copy(items, d.items)
	clear(d.items)
	d.items = items
}

// len returns the number of unconsumed transmissions.
func (d *transmissionDeque) len() int {
	return len(d.items) - d.head
}

// clear releases all retained transmission references.
func (d *transmissionDeque) clear() {
	clear(d.items)
	d.items = nil
	d.head = 0
}

// removeExpired removes queued transmissions at or beyond their local deadlines.
func (d *transmissionDeque) removeExpired(now time.Time) (int, int) {
	write := 0
	removedPackets := 0
	removedBytes := 0
	for read := d.head; read < len(d.items); read++ {
		transmission := d.items[read]
		if !now.Before(transmission.deadline) {
			removedPackets++
			removedBytes += transmission.size
			continue
		}
		d.items[write] = transmission
		write++
	}
	clear(d.items[write:])
	d.items = d.items[:write]
	d.head = 0
	if write == 0 {
		d.resetEmpty()
	} else {
		d.shrink()
	}
	return removedPackets, removedBytes
}

// each visits unconsumed transmissions in FIFO order until visit returns false.
func (d *transmissionDeque) each(visit func(retainedTransmission) bool) {
	for index := d.head; index < len(d.items); index++ {
		if !visit(d.items[index]) {
			return
		}
	}
}

// appendTo appends every unconsumed transmission to destination.
func (d *transmissionDeque) appendTo(destination []retainedTransmission) []retainedTransmission {
	return append(destination, d.items[d.head:]...)
}

// prefixSize returns the encoded size of the requested FIFO prefix.
func (d *transmissionDeque) prefixSize(count uint64) (uint64, bool) {
	if count > uint64(d.len()) {
		return 0, false
	}
	var size uint64
	for index := d.head; index < d.head+int(count); index++ {
		transmissionSize := uint64(d.items[index].size)
		if transmissionSize > math.MaxUint64-size {
			return 0, false
		}
		size += transmissionSize
	}
	return size, true
}

// TransmissionStore owns queued and sent-unreported work for one lane generation.
type TransmissionStore struct {
	mu              sync.Mutex
	limits          packetqueue.Limits
	now             func() time.Time
	control         transmissionDeque
	normal          transmissionDeque
	sent            transmissionDeque
	packets         int
	bytes           int
	budget          *retention.Budget
	sentPackets     uint64
	sentBytes       uint64
	reportedPackets uint64
	reportedBytes   uint64
	backlogPackets  atomic.Int64
	backlogBytes    atomic.Uint64
	notify          chan struct{}
	done            chan struct{}
	closed          bool
}

// NewTransmissionStore returns an empty bounded store using the process wall clock for deadline decisions.
func NewTransmissionStore(limits packetqueue.Limits) (*TransmissionStore, error) {
	return newTransmissionStoreWithBudget(limits, time.Now, nil)
}

// NewTransmissionStoreWithBudget returns an empty store sharing an aggregate retention budget.
func NewTransmissionStoreWithBudget(limits packetqueue.Limits,
	budget *retention.Budget) (*TransmissionStore, error) {
	if budget == nil {
		return nil, ErrInvalidTransmissionStore
	}
	return newTransmissionStoreWithBudget(limits, time.Now, budget)
}

// newTransmissionStore returns an empty bounded store with an injectable deadline clock.
func newTransmissionStore(limits packetqueue.Limits, now func() time.Time) (*TransmissionStore, error) {
	return newTransmissionStoreWithBudget(limits, now, nil)
}

// newTransmissionStoreWithBudget returns an empty store with optional aggregate retention accounting.
func newTransmissionStoreWithBudget(limits packetqueue.Limits, now func() time.Time,
	budget *retention.Budget) (*TransmissionStore, error) {
	if limits.Packets <= 0 || limits.Bytes <= 0 || limits.ControlPreemption || now == nil {
		return nil, ErrInvalidTransmissionStore
	}
	return &TransmissionStore{
		limits: limits,
		now:    now,
		budget: budget,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}, nil
}

// push retains one transmission when all limits and invariants permit it.
func (s *TransmissionStore) push(transmission retainedTransmission) error {
	size, err := protocol.DataFrameSize(transmission.data)
	if err != nil || !transmission.kind.Accepted() ||
		wgpacket.Classify(transmission.data.Payload) != transmission.kind ||
		!transmission.priority.Valid() ||
		(transmission.priority == packetqueue.PriorityControl) != transmission.kind.Control() ||
		transmission.deadline.IsZero() || transmission.migrated && transmission.kind != wgpacket.TransportData {
		return ErrInvalidTransmission
	}
	transmission.size = size
	if transmission.budget != nil && transmission.budget != s.budget {
		return ErrInvalidTransmission
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return packetqueue.ErrClosed
	}
	now := s.now()
	if !now.Before(transmission.deadline) {
		return packetqueue.ErrExpired
	}
	reclaimedExpired := false
	if s.packets == s.limits.Packets || size > s.limits.Bytes-s.bytes {
		s.removeExpiredQueuedLocked(now)
		reclaimedExpired = true
	}
	if s.packets == s.limits.Packets || size > s.limits.Bytes-s.bytes {
		return packetqueue.ErrFull
	}
	if transmission.budget == nil && s.budget != nil {
		if !s.budget.Reserve(1, size) {
			if !reclaimedExpired {
				s.removeExpiredQueuedLocked(now)
			}
			if !s.budget.Reserve(1, size) {
				return packetqueue.ErrFull
			}
		}
		transmission.budget = s.budget
	}
	if transmission.priority == packetqueue.PriorityControl {
		s.control.push(transmission)
	} else {
		s.normal.push(transmission)
	}
	s.addBacklogLocked(1, size)
	s.notifyLocked()
	return nil
}

// takeBatch moves an available write-order batch into the sent prefix before exposing its bytes.
func (s *TransmissionStore) takeBatch(destination []protocol.Data, targetBytes int) (int, error) {
	if len(destination) == 0 || targetBytes <= 0 {
		return 0, ErrInvalidTransmissionStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, packetqueue.ErrClosed
	}
	now := s.now()
	count := 0
	bytes := 0
	for count < len(destination) && bytes < targetBytes {
		source, transmission, ok := s.nextQueuedLocked(now)
		if !ok {
			break
		}
		if s.sentPackets == math.MaxUint64 || uint64(transmission.size) > math.MaxUint64-s.sentBytes {
			if count == 0 {
				return 0, ErrCounterExhausted
			}
			break
		}
		transmission = source.pop()
		s.sent.push(transmission)
		s.sentPackets++
		s.sentBytes += uint64(transmission.size)
		destination[count] = transmission.data
		count++
		bytes += transmission.size
	}
	if s.control.len()+s.normal.len() > 0 {
		s.notifyLocked()
	}
	if count == 0 {
		return 0, packetqueue.ErrEmpty
	}
	return count, nil
}

// nextQueuedLocked returns the next live transmission in carrier priority order.
func (s *TransmissionStore) nextQueuedLocked(now time.Time) (*transmissionDeque, retainedTransmission, bool) {
	for {
		source := &s.control
		transmission, ok := source.peek()
		if !ok {
			source = &s.normal
			transmission, ok = source.peek()
		}
		if !ok {
			return nil, retainedTransmission{}, false
		}
		if now.Before(transmission.deadline) {
			return source, transmission, true
		}
		transmission = source.pop()
		s.releaseBacklogLocked(1, transmission.size)
	}
}

// acknowledge releases the exact sent prefix represented by cumulative packet and byte counters.
func (s *TransmissionStore) acknowledge(packets, bytes uint64) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, true, nil
	}
	direction, err := cumulativeDirection(packets, bytes, s.reportedPackets, s.reportedBytes)
	if err != nil {
		return 0, false, err
	}
	if direction < 0 {
		return 0, true, nil
	}
	if direction == 0 {
		return 0, false, nil
	}
	deltaPackets := packets - s.reportedPackets
	if deltaPackets > uint64(s.sent.len()) || packets > s.sentPackets || bytes > s.sentBytes {
		return 0, false, ErrInvalidDeliveryReport
	}
	releasedBytes, ok := s.sent.prefixSize(deltaPackets)
	if !ok {
		return 0, false, ErrInvalidDeliveryReport
	}
	if releasedBytes != bytes-s.reportedBytes {
		return 0, false, ErrInvalidDeliveryReport
	}
	releasedPackets := int(deltaPackets)
	for range releasedPackets {
		s.sent.pop()
	}
	s.reportedPackets = packets
	s.reportedBytes = bytes
	s.releaseBacklogLocked(releasedPackets, int(releasedBytes))
	return releasedBytes, false, nil
}

// cumulativeDirection compares paired packet and byte counters while rejecting an impossible partial change.
func cumulativeDirection(packets, bytes, previousPackets, previousBytes uint64) (int, error) {
	packetDirection := cmp.Compare(packets, previousPackets)
	byteDirection := cmp.Compare(bytes, previousBytes)
	if packetDirection != byteDirection {
		return 0, ErrInvalidDeliveryReport
	}
	return packetDirection, nil
}

// backlog returns the current queued and sent-unreported packet and byte totals.
func (s *TransmissionStore) backlog() (int, uint64) {
	return int(s.backlogPackets.Load()), s.backlogBytes.Load()
}

// backlogByteCount returns the retained encoded byte count used by scheduler scoring.
func (s *TransmissionStore) backlogByteCount() uint64 {
	return s.backlogBytes.Load()
}

// deliveryConstrained reports whether queued work or retained occupancy makes a lower rate sample meaningful.
func (s *TransmissionStore) deliveryConstrained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	queued := s.control.len()+s.normal.len() > 0
	halfPackets := s.limits.Packets/2 + s.limits.Packets%2
	halfBytes := s.limits.Bytes/2 + s.limits.Bytes%2
	return queued || s.packets >= halfPackets || s.bytes >= halfBytes
}

// canAccept reports whether current retained work leaves capacity for one encoded frame.
func (s *TransmissionStore) canAccept(frameBytes uint64) bool {
	packets, bytes := s.backlog()
	if packets >= s.limits.Packets || bytes > uint64(s.limits.Bytes) {
		return false
	}
	return frameBytes <= uint64(s.limits.Bytes)-bytes
}

// atRisk reports whether retained work is predicted to miss a deadline in carrier order.
func (s *TransmissionStore) atRisk(now time.Time, delay func(uint64) uint64) bool {
	return s.assessDeadlines(now, delay).atRisk
}

// assessDeadlines reclaims queued expiry and evaluates retained work in one locked traversal.
func (s *TransmissionStore) assessDeadlines(now time.Time, delay func(uint64) uint64) deadlineAssessment {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return deadlineAssessment{}
	}
	s.removeExpiredQueuedLocked(now)
	assessment := deadlineAssessment{}
	prefixBytes := uint64(0)
	visit := func(transmission retainedTransmission) bool {
		if !assessment.retained || transmission.deadline.Before(assessment.deadline) {
			assessment.deadline = transmission.deadline
			assessment.frameBytes = uint64(transmission.size)
			assessment.retained = true
		}
		if assessment.atRisk {
			return true
		}
		if uint64(transmission.size) > math.MaxUint64-prefixBytes {
			assessment.atRisk = true
			return true
		}
		prefixBytes += uint64(transmission.size)
		remaining := transmission.deadline.Sub(now)
		if remaining <= 0 || delay(prefixBytes) >= uint64(remaining/time.Microsecond) {
			assessment.atRisk = true
		}
		return true
	}
	s.sent.each(visit)
	s.control.each(visit)
	s.normal.each(visit)
	return assessment
}

// drain closes the generation and returns every queued or sent-unreported transmission.
func (s *TransmissionStore) drain() []retainedTransmission {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.done)
	retained := make([]retainedTransmission, 0, s.packets)
	retained = s.sent.appendTo(retained)
	retained = s.control.appendTo(retained)
	retained = s.normal.appendTo(retained)
	s.sent.clear()
	s.control.clear()
	s.normal.clear()
	s.packets = 0
	s.bytes = 0
	s.backlogPackets.Store(0)
	s.backlogBytes.Store(0)
	return retained
}

// Ready returns a coalesced notification for available queued work.
func (s *TransmissionStore) Ready() <-chan struct{} {
	return s.notify
}

// Done closes when the generation store is drained.
func (s *TransmissionStore) Done() <-chan struct{} {
	return s.done
}

// removeExpiredQueuedLocked releases expired work that has not entered the carrier order.
func (s *TransmissionStore) removeExpiredQueuedLocked(now time.Time) {
	removedPackets, removedBytes := s.control.removeExpired(now)
	normalPackets, normalBytes := s.normal.removeExpired(now)
	removedPackets += normalPackets
	removedBytes += normalBytes
	if removedPackets > 0 {
		s.releaseBacklogLocked(removedPackets, removedBytes)
	}
}

// releaseBacklogLocked updates store accounting and releases aggregate retained capacity.
func (s *TransmissionStore) releaseBacklogLocked(packets, bytes int) {
	s.addBacklogLocked(-packets, -bytes)
	if s.budget != nil {
		s.budget.Release(packets, bytes)
	}
}

// addBacklogLocked updates exact and atomic backlog accounting.
func (s *TransmissionStore) addBacklogLocked(packets, bytes int) {
	s.packets += packets
	s.bytes += bytes
	s.backlogPackets.Store(int64(s.packets))
	s.backlogBytes.Store(uint64(s.bytes))
}

// notifyLocked publishes available queued work without blocking the caller.
func (s *TransmissionStore) notifyLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
