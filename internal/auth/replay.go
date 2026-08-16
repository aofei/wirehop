// Package auth implements bounded handshake authentication state.
package auth

import (
	"container/heap"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

var (
	// ErrInvalidReplayLimit indicates a non-positive replay-cache capacity.
	ErrInvalidReplayLimit = errors.New("invalid replay cache limit")
	// ErrInvalidNonce indicates an unset handshake nonce.
	ErrInvalidNonce = errors.New("invalid nonce")
	// ErrReplay indicates a nonce already retained by the cache.
	ErrReplay = errors.New("replayed nonce")
	// ErrReplayCacheFull indicates admission would exceed the bounded cache.
	ErrReplayCacheFull = errors.New("replay cache full")
	// ErrTimestampOutsideWindow indicates stale or excessively future authentication time.
	ErrTimestampOutsideWindow = errors.New("timestamp outside authentication window")
)

// ReplayCache retains authenticated nonces until their replay window expires.
type ReplayCache struct {
	mu       sync.Mutex
	limit    int
	retained map[protocol.Nonce]struct{}
	expiry   replayHeap
}

// replayEntry associates one retained nonce with its first invalid Unix second.
type replayEntry struct {
	nonce  protocol.Nonce
	expiry int64
}

// replayHeap orders retained nonces by expiration time.
type replayHeap []replayEntry

// Len returns the number of queued expiry records.
func (h replayHeap) Len() int {
	return len(h)
}

// Less reports whether one expiry record precedes another.
func (h replayHeap) Less(left, right int) bool {
	return h[left].expiry < h[right].expiry
}

// Swap exchanges two expiry records.
func (h replayHeap) Swap(left, right int) {
	h[left], h[right] = h[right], h[left]
}

// Push appends one expiry record.
func (h *replayHeap) Push(value any) {
	*h = append(*h, value.(replayEntry))
}

// Pop removes and returns the latest array element selected by [heap.Pop].
func (h *replayHeap) Pop() any {
	last := len(*h) - 1
	value := (*h)[last]
	(*h)[last] = replayEntry{}
	*h = (*h)[:last]
	return value
}

// NewReplayCache returns an empty replay cache with a fixed entry limit.
func NewReplayCache(limit int) (*ReplayCache, error) {
	if limit <= 0 {
		return nil, ErrInvalidReplayLimit
	}
	return &ReplayCache{limit: limit}, nil
}

// CheckAndStore atomically rejects a retained nonce or stores it through expiresAtUnix.
func (c *ReplayCache) CheckAndStore(nonce protocol.Nonce, nowUnix, expiresAtUnix int64) error {
	if nonce == (protocol.Nonce{}) {
		return ErrInvalidNonce
	}
	if expiresAtUnix <= nowUnix {
		return ErrTimestampOutsideWindow
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.expiry) > 0 && c.expiry[0].expiry <= nowUnix {
		entry := heap.Pop(&c.expiry).(replayEntry)
		delete(c.retained, entry.nonce)
	}
	if _, ok := c.retained[nonce]; ok {
		return ErrReplay
	}
	if len(c.retained) >= c.limit {
		return ErrReplayCacheFull
	}
	if c.retained == nil {
		c.retained = make(map[protocol.Nonce]struct{})
	}
	c.retained[nonce] = struct{}{}
	heap.Push(&c.expiry, replayEntry{nonce: nonce, expiry: expiresAtUnix})
	return nil
}

// ValidateTimestamp verifies timestamp against a symmetric wall-clock skew window.
func ValidateTimestamp(timestampUnix, nowUnix int64, skew time.Duration) error {
	if skew <= 0 {
		return ErrTimestampOutsideWindow
	}
	skewSeconds := int64(skew / time.Second)
	if skewSeconds <= 0 {
		return ErrTimestampOutsideWindow
	}
	lower := int64(math.MinInt64)
	if nowUnix >= math.MinInt64+skewSeconds {
		lower = nowUnix - skewSeconds
	}
	upper := int64(math.MaxInt64)
	if nowUnix <= math.MaxInt64-skewSeconds {
		upper = nowUnix + skewSeconds
	}
	if timestampUnix < lower || timestampUnix > upper {
		return ErrTimestampOutsideWindow
	}
	return nil
}

// ReplayExpiry returns the first Unix second after timestamp can pass the inclusive skew window.
func ReplayExpiry(timestampUnix int64, skew time.Duration) (int64, error) {
	skewSeconds := int64(skew / time.Second)
	if skewSeconds <= 0 || timestampUnix > math.MaxInt64-skewSeconds-1 {
		return 0, ErrTimestampOutsideWindow
	}
	return timestampUnix + skewSeconds + 1, nil
}
