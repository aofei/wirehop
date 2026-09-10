package monotime

import (
	"runtime"
	"testing"
)

func TestBootMicrosStackGrowth(t *testing.T) {
	for depth := range 256 {
		before := bootMicros()
		result := make(chan uint64, 1)
		go func() { result <- bootMicrosOnGrowingStack(depth) }()
		if reading := <-result; reading < before {
			t.Fatalf("clock moved backward at stack depth %d: %d before %d", depth, reading, before)
		}
	}
}

func bootMicrosOnGrowingStack(depth int) uint64 {
	var padding [64]byte
	reading := bootMicros()
	if depth > 0 {
		reading = min(reading, bootMicrosOnGrowingStack(depth-1))
	}
	runtime.KeepAlive(&padding)
	return reading
}
