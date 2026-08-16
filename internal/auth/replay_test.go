package auth

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestReplayCache(t *testing.T) {
	cache, err := NewReplayCache(2)
	if err != nil {
		t.Fatal(err)
	}
	first := testNonce(1)
	second := testNonce(2)
	third := testNonce(3)
	if err := cache.CheckAndStore(first, 100, 110); err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(first, 100, 110); !errors.Is(err, ErrReplay) {
		t.Fatalf("replay error = %v", err)
	}
	if err := cache.CheckAndStore(second, 100, 120); err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(third, 100, 130); !errors.Is(err, ErrReplayCacheFull) {
		t.Fatalf("capacity error = %v", err)
	}
	if err := cache.CheckAndStore(third, 110, 130); err != nil {
		t.Fatalf("store after expiry: %v", err)
	}
}

func TestReplayCacheExpiresOutOfOrder(t *testing.T) {
	cache, err := NewReplayCache(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(testNonce(1), 100, 130); err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(testNonce(2), 100, 110); err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(testNonce(3), 100, 120); err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(testNonce(4), 110, 140); err != nil {
		t.Fatalf("store after earliest out-of-order expiry: %v", err)
	}
	if err := cache.CheckAndStore(testNonce(2), 120, 140); err != nil {
		t.Fatalf("reuse after out-of-order expiry: %v", err)
	}
}

func TestReplayCacheValidation(t *testing.T) {
	if _, err := NewReplayCache(0); !errors.Is(err, ErrInvalidReplayLimit) {
		t.Fatalf("NewReplayCache() error = %v", err)
	}
	cache, err := NewReplayCache(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.CheckAndStore(protocol.Nonce{}, 1, 2); !errors.Is(err, ErrInvalidNonce) {
		t.Fatalf("zero nonce error = %v", err)
	}
	if err := cache.CheckAndStore(testNonce(1), 2, 2); !errors.Is(err, ErrTimestampOutsideWindow) {
		t.Fatalf("expired replay window error = %v", err)
	}
}

func TestReplayCacheAllocatesStorageLazily(t *testing.T) {
	cache, err := NewReplayCache(4096)
	if err != nil {
		t.Fatal(err)
	}
	if cache.retained != nil {
		t.Fatal("NewReplayCache() allocated nonce storage")
	}
	if err := cache.CheckAndStore(testNonce(1), 100, 110); err != nil {
		t.Fatal(err)
	}
	if cache.retained == nil {
		t.Fatal("CheckAndStore() did not allocate nonce storage")
	}
}

func TestValidateTimestamp(t *testing.T) {
	for _, timestamp := range []int64{90, 100, 110} {
		if err := ValidateTimestamp(timestamp, 100, 10*time.Second); err != nil {
			t.Fatalf("ValidateTimestamp(%d) error = %v", timestamp, err)
		}
	}
	for _, timestamp := range []int64{89, 111} {
		if err := ValidateTimestamp(timestamp, 100, 10*time.Second); !errors.Is(err, ErrTimestampOutsideWindow) {
			t.Fatalf("ValidateTimestamp(%d) error = %v", timestamp, err)
		}
	}
	for _, tt := range []struct {
		name      string
		timestamp int64
		now       int64
		wantError bool
	}{
		{name: "MinimumAccepted", timestamp: math.MinInt64, now: math.MinInt64},
		{name: "MinimumRejected", timestamp: math.MinInt64 + 11, now: math.MinInt64, wantError: true},
		{name: "MaximumAccepted", timestamp: math.MaxInt64, now: math.MaxInt64},
		{name: "MaximumRejected", timestamp: math.MaxInt64 - 11, now: math.MaxInt64, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTimestamp(tt.timestamp, tt.now, 10*time.Second)
			if errors.Is(err, ErrTimestampOutsideWindow) != tt.wantError {
				t.Fatalf("ValidateTimestamp(%d, %d) error = %v", tt.timestamp, tt.now, err)
			}
		})
	}
}

func TestReplayExpiry(t *testing.T) {
	expires, err := ReplayExpiry(110, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if expires != 121 {
		t.Fatalf("ReplayExpiry() = %d, want 121", expires)
	}
	if _, err := ReplayExpiry(110, 0); !errors.Is(err, ErrTimestampOutsideWindow) {
		t.Fatalf("ReplayExpiry() error = %v, want %v", err, ErrTimestampOutsideWindow)
	}
}

func testNonce(value byte) protocol.Nonce {
	var nonce protocol.Nonce
	nonce[0] = value
	return nonce
}
