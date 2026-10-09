// Package dedup implements bounded direction-local packet deduplication.
package dedup

import "errors"

var (
	// ErrInvalidCapacity indicates a non-positive deduplication window size.
	ErrInvalidCapacity = errors.New("invalid deduplication capacity")
)

// Result describes how a packet sequence number relates to the retained window.
type Result uint8

const (
	// New identifies a sequence number not previously observed in the retained window.
	New Result = iota
	// Duplicate identifies a sequence number already observed in the retained window.
	Duplicate
	// TooOld identifies a sequence number older than the retained window.
	TooOld
)

// Window retains a fixed number of direction-local packet sequence numbers.
type Window struct {
	bits     []uint64
	capacity uint64
	highest  uint64
}

// NewWindow returns a fixed-capacity deduplication window whose bitmap grows as ring positions are observed.
func NewWindow(capacity int) (*Window, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Window{capacity: uint64(capacity)}, nil
}

// Classify reports how sequence relates to the retained window without recording it.
func (w *Window) Classify(sequence uint64) Result {
	if sequence == 0 {
		return TooOld
	}
	if sequence > w.highest {
		return New
	}
	if w.highest-sequence >= w.capacity {
		return TooOld
	}
	if w.contains(sequence) {
		return Duplicate
	}
	return New
}

// Observe records sequence and returns whether it is new, duplicated, or too old.
func (w *Window) Observe(sequence uint64) Result {
	result := w.Classify(sequence)
	if result != New {
		return result
	}
	if sequence > w.highest {
		w.advance(sequence)
		w.highest = sequence
	}
	w.set(sequence)
	return New
}

// advance clears ring positions that no longer identify retained sequence numbers.
func (w *Window) advance(sequence uint64) {
	distance := sequence - w.highest
	if distance >= w.capacity {
		clear(w.bits)
		return
	}
	start := (w.highest + 1) % w.capacity
	first := min(distance, w.capacity-start)
	w.clearLinear(start, first)
	if first < distance {
		w.clearLinear(0, distance-first)
	}
}

// clearLinear clears count consecutive bitmap positions without wrapping around capacity.
func (w *Window) clearLinear(start, count uint64) {
	end := min(start+count, uint64(len(w.bits))*64)
	if start >= end {
		return
	}
	firstWord := start / 64
	lastWord := (end - 1) / 64
	if firstWord == lastWord {
		w.bits[firstWord] &^= lowBits(end-start) << (start % 64)
		return
	}
	if offset := start % 64; offset != 0 {
		w.bits[firstWord] &^= ^uint64(0) << offset
		firstWord++
	}
	fullWordEnd := end / 64
	clear(w.bits[firstWord:fullWordEnd])
	if offset := end % 64; offset != 0 {
		w.bits[fullWordEnd] &^= lowBits(offset)
	}
}

// lowBits returns a mask containing count low-order one bits.
func lowBits(count uint64) uint64 {
	if count == 64 {
		return ^uint64(0)
	}
	return (uint64(1) << count) - 1
}

// contains reports whether the ring position for sequence is set.
func (w *Window) contains(sequence uint64) bool {
	word, mask := w.position(sequence)
	return word < uint64(len(w.bits)) && w.bits[word]&mask != 0
}

// set records sequence in its ring position.
func (w *Window) set(sequence uint64) {
	word, mask := w.position(sequence)
	if word >= uint64(len(w.bits)) {
		words := min((w.capacity-1)/64+1, max(word+1, uint64(len(w.bits))*2))
		bits := make([]uint64, words)
		copy(bits, w.bits)
		w.bits = bits
	}
	w.bits[word] |= mask
}

// position maps sequence to one word and bit in the circular window.
func (w *Window) position(sequence uint64) (uint64, uint64) {
	position := sequence % w.capacity
	return position / 64, uint64(1) << (position % 64)
}

// Highest returns the largest sequence number observed by the window.
func (w *Window) Highest() uint64 {
	return w.highest
}
