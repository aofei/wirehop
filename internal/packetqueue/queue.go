// Package packetqueue implements bounded priority queues for packet ownership.
package packetqueue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/retention"
)

var (
	// ErrInvalidLimits indicates non-positive packet or byte limits.
	ErrInvalidLimits = errors.New("invalid queue limits")
	// ErrInvalidItem indicates a non-positive item size or missing deadline.
	ErrInvalidItem = errors.New("invalid queue item")
	// ErrFull indicates that accepting an item would exceed a queue limit.
	ErrFull = errors.New("queue full")
	// ErrExpired indicates an item already beyond its deadline.
	ErrExpired = errors.New("item expired")
	// ErrClosed indicates an operation on a closed queue.
	ErrClosed = errors.New("queue closed")
	// ErrEmpty indicates that a nonblocking dequeue found no unexpired item.
	ErrEmpty = errors.New("queue empty")
)

// Priority controls dequeue order without changing packet deadlines.
type Priority uint8

// maximumRetainedDequeCapacity preserves ordinary bursts without retaining exceptional queue high-water marks.
const maximumRetainedDequeCapacity = 256

const (
	// PriorityNormal carries ordinary WireGuard transport data.
	PriorityNormal Priority = iota
	// PriorityControl carries WireGuard handshake and cookie packets.
	PriorityControl
)

// Valid reports whether the priority is defined.
func (p Priority) Valid() bool {
	return p == PriorityNormal || p == PriorityControl
}

// Limits bound a queue independently by packets and bytes.
type Limits struct {
	Packets           int
	Bytes             int
	ControlPreemption bool
}

// Item owns one queued value and its scheduling metadata.
type Item[T any] struct {
	Value          T
	Size           int
	Priority       Priority
	Deadline       time.Time
	retention      *retention.Budget
	retentionBytes int
}

// TakeRetention transfers the item's aggregate reservation after resizing its byte charge.
func (i *Item[T]) TakeRetention(bytes int) (*retention.Budget, bool) {
	if i.retention == nil {
		return nil, true
	}
	if !i.retention.ResizeBytes(i.retentionBytes, bytes) {
		return nil, false
	}
	budget := i.retention
	i.retention = nil
	i.retentionBytes = 0
	return budget, true
}

// RestoreRetention returns a failed transfer to the item and restores its queue byte charge.
func (i *Item[T]) RestoreRetention(budget *retention.Budget, bytes int) {
	if budget == nil {
		return
	}
	if i.retention != nil || !budget.ResizeBytes(bytes, i.Size) {
		panic("restore invalid queue retention")
	}
	i.retention = budget
	i.retentionBytes = i.Size
}

// ReleaseRetention releases aggregate capacity retained by an item outside a queue.
func (i *Item[T]) ReleaseRetention() {
	if i.retention == nil {
		return
	}
	i.retention.Release(1, i.retentionBytes)
	i.retention = nil
	i.retentionBytes = 0
}

// Release relinquishes both aggregate retention and releasable value ownership.
func (i *Item[T]) Release() {
	i.ReleaseRetention()
	i.releaseValue()
}

// releaseValue relinquishes value ownership when its pointer implements a Release method.
func (i *Item[T]) releaseValue() {
	if releasable, ok := any(&i.Value).(interface{ Release() }); ok {
		releasable.Release()
	}
	var zero T
	i.Value = zero
}

// deque is a compacting first-in, first-out item sequence.
type deque[T any] struct {
	items []Item[T]
	head  int
}

// push appends item to the deque.
func (d *deque[T]) push(item Item[T]) {
	d.items = append(d.items, item)
}

// pop transfers the oldest item from the deque into destination.
func (d *deque[T]) pop(destination *Item[T]) bool {
	if d.head == len(d.items) {
		return false
	}
	*destination = d.items[d.head]
	var zero Item[T]
	d.items[d.head] = zero
	d.head++
	if d.head == len(d.items) {
		d.resetEmpty()
		return true
	}
	d.compact()
	return true
}

// resetEmpty preserves ordinary reusable capacity while releasing an exceptional historical peak.
func (d *deque[T]) resetEmpty() {
	if cap(d.items) > maximumRetainedDequeCapacity {
		d.items = make([]Item[T], 0, maximumRetainedDequeCapacity)
	} else {
		d.items = d.items[:0]
	}
	d.head = 0
}

// compact releases consumed slots when enough of the backing slice is unused.
func (d *deque[T]) compact() {
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
func (d *deque[T]) shrink() {
	capacity := cap(d.items)
	if capacity <= maximumRetainedDequeCapacity || len(d.items) > capacity/4 {
		return
	}
	retainedCapacity := max(maximumRetainedDequeCapacity, len(d.items)*2)
	items := make([]Item[T], len(d.items), retainedCapacity)
	copy(items, d.items)
	clear(d.items)
	d.items = items
}

// clear releases every retained value and resets the deque.
func (d *deque[T]) clear() {
	for index := d.head; index < len(d.items); index++ {
		d.items[index].releaseValue()
	}
	clear(d.items)
	d.items = nil
	d.head = 0
}

// removeExpired compacts the deque and returns released packet and byte capacity.
func (d *deque[T]) removeExpired(now time.Time) (int, int) {
	write := 0
	removedPackets := 0
	removedBytes := 0
	for read := d.head; read < len(d.items); read++ {
		item := d.items[read]
		if !now.Before(item.Deadline) {
			removedPackets++
			removedBytes += item.Size
			item.releaseValue()
			continue
		}
		d.items[write] = item
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

// Queue is a concurrent bounded priority queue with explicit item ownership.
type Queue[T any] struct {
	mu      sync.Mutex
	limits  Limits
	now     func() time.Time
	normal  deque[T]
	control deque[T]
	packets int
	bytes   int
	budget  *retention.Budget
	notify  chan struct{}
	done    chan struct{}
	closed  bool
}

// New returns an empty queue using the process wall clock for deadline decisions.
func New[T any](limits Limits) (*Queue[T], error) {
	return newQueue[T](limits, time.Now, nil)
}

// NewWithBudget returns an empty queue sharing an aggregate retention budget.
func NewWithBudget[T any](limits Limits, budget *retention.Budget) (*Queue[T], error) {
	if budget == nil {
		return nil, ErrInvalidLimits
	}
	return newQueue[T](limits, time.Now, budget)
}

// NewWithClock returns an empty queue using now for deterministic deadline decisions.
func NewWithClock[T any](limits Limits, now func() time.Time) (*Queue[T], error) {
	return newQueue[T](limits, now, nil)
}

// newQueue returns an empty queue with optional aggregate retention accounting.
func newQueue[T any](limits Limits, now func() time.Time, budget *retention.Budget) (*Queue[T], error) {
	if limits.Packets <= 0 || limits.Bytes <= 0 || now == nil {
		return nil, ErrInvalidLimits
	}
	return &Queue[T]{
		limits: limits,
		now:    now,
		budget: budget,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}, nil
}

// Push transfers ownership of item to the queue when all limits permit it.
func (q *Queue[T]) Push(item Item[T]) error {
	if !q.validItem(item) {
		return ErrInvalidItem
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.pushLocked(item, q.now())
	q.notifyLocked(err == nil)
	return err
}

// PushBatch transfers the longest accepted item prefix with one queue lock and one readiness notification.
func (q *Queue[T]) PushBatch(items []Item[T]) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	written := 0
	for _, item := range items {
		if !q.validItem(item) {
			q.notifyLocked(written > 0)
			return written, ErrInvalidItem
		}
		if err := q.pushLocked(item, now); err != nil {
			q.notifyLocked(written > 0)
			return written, err
		}
		written++
	}
	q.notifyLocked(true)
	return written, nil
}

// validItem reports whether item can participate in this queue's ownership contract.
func (q *Queue[T]) validItem(item Item[T]) bool {
	if item.Size <= 0 || !item.Priority.Valid() || item.Deadline.IsZero() ||
		item.retention != nil && (item.retention != q.budget || item.retentionBytes != item.Size) {
		return false
	}
	return true
}

// pushLocked transfers one prevalidated item at now while q.mu is held.
func (q *Queue[T]) pushLocked(item Item[T], now time.Time) error {
	if q.closed {
		return ErrClosed
	}
	if !now.Before(item.Deadline) {
		return ErrExpired
	}
	if item.Size > q.limits.Bytes {
		return ErrFull
	}
	reclaimedExpired := false
	if q.packets == q.limits.Packets || item.Size > q.limits.Bytes-q.bytes {
		q.removeExpiredLocked(now)
		reclaimedExpired = true
	}
	preempt := item.Priority == PriorityControl && q.limits.ControlPreemption
	for {
		if q.packets == q.limits.Packets || item.Size > q.limits.Bytes-q.bytes {
			if !preempt || !q.evictNormalLocked() {
				return ErrFull
			}
			continue
		}
		if q.budget == nil || item.retention != nil || q.budget.Reserve(1, item.Size) {
			break
		}
		if !reclaimedExpired {
			q.removeExpiredLocked(now)
			reclaimedExpired = true
			continue
		}
		if !preempt || !q.evictNormalLocked() {
			return ErrFull
		}
	}
	if q.budget != nil && item.retention == nil {
		item.retention = q.budget
		item.retentionBytes = item.Size
	}
	if item.Priority == PriorityControl {
		q.control.push(item)
	} else {
		q.normal.push(item)
	}
	q.packets++
	q.bytes += item.Size
	return nil
}

// notifyLocked publishes nonempty readiness after a completed ownership transfer.
func (q *Queue[T]) notifyLocked(changed bool) {
	if !changed {
		return
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// removeExpiredLocked releases expired capacity from both priority queues.
func (q *Queue[T]) removeExpiredLocked(now time.Time) {
	removedPackets, removedBytes := q.normal.removeExpired(now)
	controlPackets, controlBytes := q.control.removeExpired(now)
	removedPackets += controlPackets
	removedBytes += controlBytes
	q.releaseLocked(removedPackets, removedBytes)
}

// evictNormalLocked evicts the oldest normal item and reports whether one existed.
func (q *Queue[T]) evictNormalLocked() bool {
	var item Item[T]
	ok := q.normal.pop(&item)
	if ok {
		q.releaseLocked(1, item.Size)
		item.releaseValue()
	}
	return ok
}

// Pop waits for and transfers ownership of the next unexpired item to the caller.
func (q *Queue[T]) Pop(ctx context.Context) (Item[T], error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return Item[T]{}, ErrClosed
		}
		now := q.now()
		for {
			var item Item[T]
			ok := q.popLocked(&item)
			if !ok {
				break
			}
			if now.Before(item.Deadline) {
				q.mu.Unlock()
				return item, nil
			}
			item.Release()
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return Item[T]{}, ctx.Err()
		case <-q.done:
			return Item[T]{}, ErrClosed
		case <-q.notify:
		}
	}
}

// TryPop transfers ownership of the next unexpired item into an unowned destination without blocking.
func (q *Queue[T]) TryPop(destination *Item[T]) error {
	*destination = Item[T]{}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	now := q.now()
	for {
		ok := q.popLocked(destination)
		if !ok {
			return ErrEmpty
		}
		if now.Before(destination.Deadline) {
			return nil
		}
		destination.Release()
	}
}

// TryPopPriority transfers the next unexpired item at priority into an unowned destination without considering other
// priorities.
func (q *Queue[T]) TryPopPriority(priority Priority, destination *Item[T]) error {
	*destination = Item[T]{}
	if !priority.Valid() {
		return ErrInvalidItem
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	now := q.now()
	for {
		ok := q.popPriorityLocked(priority, destination)
		if !ok {
			return ErrEmpty
		}
		if now.Before(destination.Deadline) {
			return nil
		}
		destination.Release()
	}
}

// Ready returns a coalesced notification for newly queued work.
func (q *Queue[T]) Ready() <-chan struct{} {
	return q.notify
}

// Len returns the current queued packet count.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.packets
}

// Bytes returns the current queued byte count.
func (q *Queue[T]) Bytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

// Close rejects future pushes and wakes blocked consumers.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.releaseLocked(q.packets, q.bytes)
		q.normal.clear()
		q.control.clear()
		close(q.done)
	}
}

// popLocked transfers one item according to priority and updates queue accounting.
func (q *Queue[T]) popLocked(destination *Item[T]) bool {
	ok := q.popPriorityLocked(PriorityControl, destination)
	if !ok {
		ok = q.popPriorityLocked(PriorityNormal, destination)
	}
	return ok
}

// popPriorityLocked transfers one item at priority into destination with aggregate retention.
func (q *Queue[T]) popPriorityLocked(priority Priority, destination *Item[T]) bool {
	var ok bool
	if priority == PriorityControl {
		ok = q.control.pop(destination)
	} else {
		ok = q.normal.pop(destination)
	}
	if ok {
		q.packets--
		q.bytes -= destination.Size
	}
	return ok
}

// releaseLocked updates local accounting and releases optional aggregate capacity.
func (q *Queue[T]) releaseLocked(packets, bytes int) {
	if packets == 0 {
		return
	}
	q.packets -= packets
	q.bytes -= bytes
	if q.budget != nil {
		q.budget.Release(packets, bytes)
	}
}
