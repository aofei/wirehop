package monotime

import (
	"testing"
	"time"
)

func TestClock(t *testing.T) {
	clock := New()
	first := clock.NowMicros()
	time.Sleep(time.Millisecond)
	second := clock.NowMicros()
	if second <= first {
		t.Fatalf("NowMicros() did not advance: %d then %d", first, second)
	}
}
