package monotime

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestResumeDetectorResumed(t *testing.T) {
	for _, tt := range []struct {
		name         string
		protocolSpan time.Duration
		want         bool
	}{
		{name: "OrdinaryElapsed", protocolSpan: time.Second},
		{name: "SamplingTolerance", protocolSpan: 1500 * time.Millisecond},
		{name: "SystemSuspend", protocolSpan: time.Minute, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := uint64(100)
				detector := NewResumeDetector(func() uint64 { return now })
				synctest.Sleep(time.Second)
				now = uint64(100 + tt.protocolSpan/time.Microsecond)
				if got := detector.Resumed(); got != tt.want {
					t.Fatalf("Resumed() = %t, want %t", got, tt.want)
				}
				synctest.Sleep(time.Second)
				now += 1_000_000
				if detector.Resumed() {
					t.Fatal("ordinary progress after resume was detected as another suspend")
				}
			})
		})
	}
	t.Run("DelayedInitialSample", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			detector := NewResumeDetector(func() uint64 {
				calls++
				if calls == 1 {
					synctest.Sleep(5 * time.Second)
					return 0
				}
				return 5_000_000
			})
			if detector.Resumed() {
				t.Fatal("scheduling delay around the initial sample was mistaken for suspend")
			}
		})
	})
}
