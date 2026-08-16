// Package retention implements process-wide packet retention accounting.
package retention

import (
	"errors"
	"sync/atomic"
)

var (
	// ErrInvalidLimits indicates non-positive packet or byte limits.
	ErrInvalidLimits = errors.New("invalid retention limits")
)

// Limits bound aggregate retained work by packet count and accounted bytes.
type Limits struct {
	Packets int
	Bytes   int
}

// Usage is one point-in-time aggregate retention measurement.
type Usage struct {
	Packets int
	Bytes   int
}

// Budget accounts retained packet capacity shared by independent queues.
type Budget struct {
	limits  Limits
	packets atomic.Int64
	bytes   atomic.Int64
}

// NewBudget returns an empty aggregate retention budget.
func NewBudget(limits Limits) (*Budget, error) {
	if limits.Packets <= 0 || limits.Bytes <= 0 {
		return nil, ErrInvalidLimits
	}
	return &Budget{limits: limits}, nil
}

// Reserve retains capacity only when both aggregate limits permit it.
func (b *Budget) Reserve(packets, bytes int) bool {
	if packets <= 0 || bytes <= 0 || !reserve(&b.packets, int64(packets), int64(b.limits.Packets)) {
		return false
	}
	if reserve(&b.bytes, int64(bytes), int64(b.limits.Bytes)) {
		return true
	}
	b.packets.Add(-int64(packets))
	return false
}

// ResizeBytes changes the byte charge of retained capacity without changing its packet charge.
func (b *Budget) ResizeBytes(previous, next int) bool {
	if previous <= 0 || next <= 0 {
		return false
	}
	if next > previous {
		return reserve(&b.bytes, int64(next-previous), int64(b.limits.Bytes))
	}
	if next < previous && b.bytes.Add(-int64(previous-next)) < 0 {
		panic("resize unreserved retention capacity")
	}
	return true
}

// Release returns previously reserved capacity to the budget.
func (b *Budget) Release(packets, bytes int) {
	if packets <= 0 || bytes <= 0 {
		panic("release invalid retention capacity")
	}
	remainingPackets := b.packets.Add(-int64(packets))
	remainingBytes := b.bytes.Add(-int64(bytes))
	if remainingPackets < 0 || remainingBytes < 0 {
		panic("release unreserved retention capacity")
	}
}

// Usage returns the current packet and byte counts, sampled independently.
func (b *Budget) Usage() Usage {
	return Usage{Packets: int(b.packets.Load()), Bytes: int(b.bytes.Load())}
}

// reserve increments current without exceeding limit.
func reserve(current *atomic.Int64, amount, limit int64) bool {
	for used := current.Load(); ; used = current.Load() {
		if used > limit-amount {
			return false
		}
		if current.CompareAndSwap(used, used+amount) {
			return true
		}
	}
}
