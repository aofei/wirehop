package relay

import (
	"testing"
	"time"
)

func TestScheduledLaneMaybeRefreshTimingQuantizedQueue(t *testing.T) {
	for _, tt := range []struct {
		name          string
		rate          uint64
		rtt           uint64
		wantImmediate bool
	}{
		{name: "ExpectedSerialization", rate: 3400, rtt: 1_700_000},
		{name: "CapacityLossWithinQueueHorizon", rate: 3000, rtt: 1_700_000},
		{name: "FloorBelowTimeWindow", rate: 5000, rtt: 1_700_000},
		{name: "UnexplainedDelay", rate: 3400, rtt: 2_400_000, wantImmediate: true},
		{name: "CapacityLossAndUnexplainedDelay", rate: 3000, rtt: 2_600_000, wantImmediate: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			lane := scheduledLane{
				registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
				minimumRTTMicros: 200_000, rttMicros: tt.rtt,
				deliveryRate: tt.rate, rateObserved: true,
			}
			now := time.Unix(100, 0)
			lane.maybeRefreshTiming(now, tt.rtt)
			if tt.wantImmediate {
				if calls != 1 {
					t.Fatal("path deterioration did not bypass the ambiguous queue delay")
				}
				return
			}
			if calls != 0 || lane.ambiguousTimingSince != now || !lane.nextTimingRefresh.IsZero() {
				t.Fatal("expected serialization acquired a pause or consumed its request retry")
			}
			cost := time.Duration(lane.deliveryWindowHorizonMicros()+minimumDeliveryWindow*1_000_000/tt.rate+
				lane.deliveryFeedbackCycleMicros(lane.minimumRTTMicros)) * time.Microsecond
			due := now.Add(cost * measurementQuietRatio)
			lane.maybeRefreshTiming(due.Add(-time.Nanosecond), tt.rtt)
			if calls != 0 {
				t.Fatal("ambiguous calibration bypassed its estimated pause budget")
			}
			lane.maybeRefreshTiming(due, tt.rtt)
			if calls != 1 {
				t.Fatal("byte-floor ambiguity permanently suppressed independent calibration")
			}
			lane.applyRoundTrip(200_000, true)
			if !lane.ambiguousTimingSince.IsZero() {
				t.Fatal("successful calibration retained an old ambiguous interval")
			}
		})
	}
}

func TestScheduledLaneCleanTimingResetsAmbiguity(t *testing.T) {
	calls := 0
	lane := scheduledLane{
		registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
		minimumRTTMicros: 200_000, rttMicros: 1_700_000,
		deliveryRate: 3400, rateObserved: true,
	}
	now := time.Unix(100, 0)
	lane.maybeRefreshTiming(now, 1_700_000)
	if lane.ambiguousTimingSince != now {
		t.Fatal("expected queue did not start its calibration interval")
	}
	lane.maybeRefreshTiming(now.Add(time.Hour), 200_000)
	if !lane.ambiguousTimingSince.IsZero() || calls != 0 {
		t.Fatal("clean ordinary timing retained stale queue evidence or requested a pause")
	}
	next := now.Add(time.Hour + time.Second)
	lane.maybeRefreshTiming(next, 1_700_000)
	if calls != 0 || lane.ambiguousTimingSince != next {
		t.Fatal("a new burst reused an old ambiguous interval after natural queue relief")
	}
}

func TestScheduledLaneTimingIgnoresExpiredStartupPeak(t *testing.T) {
	calls := 0
	lane := scheduledLane{
		registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
		minimumRTTMicros: 200_000, rttMicros: 1_700_000,
	}
	lane.updateDeliveryRate(deliverySample{bytes: 16_384, intervalMicros: 1_000_000}, 1_000_000)
	lane.updateDeliveryRate(deliverySample{bytes: 3400, intervalMicros: 1_000_000}, 3_000_000)
	if lane.deliveryRate != 3400 {
		t.Fatal("expired startup sample still controls committed flight credit")
	}
	lane.maybeRefreshTiming(time.Unix(100, 0), 1_700_000)
	if calls != 0 {
		t.Fatal("expired startup burst made an explainable narrow-link queue trigger immediate calibration")
	}
}
