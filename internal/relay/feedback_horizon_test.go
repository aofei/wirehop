package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
)

func TestScheduledLaneMaybeRefreshTimingObservedCycle(t *testing.T) {
	t.Run("NormalStartupQueueWithinCredit", func(t *testing.T) {
		calls := 0
		lane := scheduledLane{
			registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
			minimumRTTMicros: 300_000, rttMicros: 400_000,
			deliveryRate: 1_000_000, rateObserved: true,
		}
		lane.maybeRefreshTiming(time.Unix(100, 0), lane.rttMicros)
		if horizon := lane.deliveryWindowHorizonMicros(); horizon != 1_300_000 || calls != 0 {
			t.Fatalf("startup horizon = %d, requests = %d", horizon, calls)
		}
	})
	t.Run("DelayStepFirstEWMARequests", func(t *testing.T) {
		calls := 0
		lane := scheduledLane{
			registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
			minimumRTTMicros: 10_000, rttMicros: 10_000, rttObserved: true,
			deliveryRate: 1_000_000, rateObserved: true,
		}
		roundTrip := lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
		lane.maybeRefreshTiming(time.Unix(100, 0), roundTrip)
		if lane.rttMicros != 158_750 || lane.minimumRTTMicros != 10_000 || calls != 1 {
			t.Fatalf("first step: RTT = %d, minimum = %d, requests = %d", lane.rttMicros, lane.minimumRTTMicros, calls)
		}
	})
	t.Run("SamplingAllowanceDelaysButCannotMaskCalibration", func(t *testing.T) {
		lane := schedulerLaneWithLimits(t, 1, 1, 1_200_000, 3863,
			packetqueue.Limits{Packets: 65536, Bytes: 32 << 20})
		lane.minimumRTTMicros = 10_000
		lane.feedbackDelayMicros = 5000
		calls := 0
		lane.registration.RequestMeasurement = func() { calls++ }
		window := lane.deliveryWindowBytes()
		if window != 4636 {
			t.Fatal("low-rate delivery window lost its sampling allowance")
		}
		now := time.Unix(100, 0)
		lane.maybeRefreshTiming(now, lane.rttMicros)
		if calls != 0 || lane.deliveryWindowHorizonMicros() != 140_000 {
			t.Fatal("byte sampling allowance did not delay an explainable loaded RTT")
		}
		lane.maybeRefreshTiming(now.Add(measurementPauseLimit*measurementQuietRatio), lane.rttMicros)
		if calls != 1 {
			t.Fatal("byte sampling allowance permanently hid independent calibration")
		}
	})
	t.Run("FeedbackDelayChangesGateForNewOrdinarySamples", func(t *testing.T) {
		calls := 0
		lane := scheduledLane{
			registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
			minimumRTTMicros: 10_000, rttMicros: 1_200_000, feedbackDelayMicros: 500_000,
			deliveryRate: 1_000_000, rateObserved: true,
		}
		now := time.Unix(100, 0)
		// Each check follows a new ordinary sample. A report-delay change alone cannot request a proof.
		roundTrip := lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
		lane.maybeRefreshTiming(now, roundTrip)
		if lane.deliveryWindowHorizonMicros() != 2_120_000 || calls != 0 {
			t.Fatal("refresh ignored the existing return-feedback credit")
		}
		lane.feedbackDelayMicros = 0
		roundTrip = lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
		lane.maybeRefreshTiming(now, roundTrip)
		if calls != 1 {
			t.Fatal("smaller feedback horizon did not admit the changed timing request")
		}
	})
	t.Run("ObservedCycleEqualitySuppressed", func(t *testing.T) {
		calls := 0
		lane := scheduledLane{
			registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
			minimumRTTMicros: 10_000, rttMicros: 115_000,
			deliveryRate: 1_000_000, rateObserved: true,
		}
		now := time.Unix(100, 0)
		lane.maybeRefreshTiming(now, lane.rttMicros)
		if calls != 0 {
			t.Fatal("observed feedback cycle equal to the horizon requested a proof")
		}
		lane.rttMicros++
		lane.maybeRefreshTiming(now, lane.rttMicros)
		if calls != 0 || lane.ambiguousTimingSince.IsZero() {
			t.Fatal("one microsecond beyond the time window bypassed full-flight serialization")
		}
		// The 144096-byte window takes 144096 microseconds, plus a 35000-microsecond unloaded cycle.
		lane.rttMicros = 154_096
		lane.maybeRefreshTiming(now, lane.rttMicros)
		if calls != 0 {
			t.Fatal("equality with the complete loaded-cycle estimate requested a proof")
		}
		lane.rttMicros++
		lane.maybeRefreshTiming(now, lane.rttMicros)
		if calls != 1 {
			t.Fatal("strictly unexplained loaded-cycle growth did not request a proof")
		}
	})
	t.Run("SaturatedHorizonCannotWrap", func(t *testing.T) {
		for _, feedback := range []uint64{0, math.MaxUint64} {
			calls := 0
			lane := scheduledLane{
				registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
				minimumRTTMicros: math.MaxUint64 / 4, rttMicros: math.MaxUint64,
				feedbackDelayMicros: feedback, deliveryRate: 1, rateObserved: true,
			}
			lane.maybeRefreshTiming(time.Unix(100, 0), lane.rttMicros)
			if lane.deliveryWindowHorizonMicros() != math.MaxUint64 || calls != 0 {
				t.Fatal("overflow fabricated a horizon crossing")
			}
		}
	})
	t.Run("FailedRetryAndSuccessfulReference", func(t *testing.T) {
		calls := 0
		lane := scheduledLane{
			registration:     LaneRegistration{RequestMeasurement: func() { calls++ }},
			minimumRTTMicros: 10_000, rttMicros: 600_000, rttObserved: true,
			deliveryRate: 1_000_000, rateObserved: true,
		}
		now := time.Unix(100, 0)
		lane.maybeRefreshTiming(now, lane.rttMicros)
		lane.maybeRefreshTiming(now.Add(time.Second-time.Nanosecond), lane.rttMicros)
		if calls != 1 || lane.timingReferenceRate != 0 || lane.timingReferenceRTT != 0 {
			t.Fatal("unsuccessful request changed reference state or bypassed retry")
		}
		lane.maybeRefreshTiming(now.Add(time.Second), lane.rttMicros)
		if calls != 2 {
			t.Fatal("unsuccessful request did not retry at one second")
		}
		lane.applyRoundTrip(600_000, true)
		lane.maybeRefreshTiming(now.Add(2*time.Second), lane.rttMicros)
		if calls != 2 || lane.minimumRTTMicros != 600_000 || lane.timingReferenceRTT != 600_000 ||
			lane.timingReferenceRate != 1_000_000 {
			t.Fatal("successful proof did not restore the existing reference contract")
		}
	})
}

func TestSchedulerApplyEventObservedCycleInvalidProofGuards(t *testing.T) {
	for _, tt := range []struct {
		name  string
		event schedulerEvent
	}{
		{name: "ZeroValidatedRTT", event: schedulerEvent{kind: schedulerValidatedRTT, laneID: 1, generation: 1}},
		{name: "ReversedLocalClock", event: schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 1,
			timing: clockmap.Sample{LocalSendMicros: 2, LocalReceiveMicros: 1}}},
		{name: "ReversedRemoteClock", event: schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 1,
			timing: clockmap.Sample{LocalReceiveMicros: 10, RemoteReceiveMicros: 2, RemoteSendMicros: 1}}},
		{name: "NegativeAdjustedRTT", event: schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 1,
			timing: clockmap.Sample{LocalReceiveMicros: 10, RemoteSendMicros: 11}}},
		{name: "StaleValidatedGeneration", event: schedulerEvent{kind: schedulerValidatedRTT, laneID: 1, generation: 2, rttMicros: 600_000}},
		{name: "StaleTimingGeneration", event: schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 2,
			timing: clockmap.Sample{LocalReceiveMicros: 600_000}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := schedulerLane(t, 1, 1, 100_000, 1_000_000)
			lane.minimumRTTMicros = 10_000
			lane.timingReferenceRTT = 100_000
			lane.timingReferenceRate = 1_000_000
			now := time.Unix(100, 0)
			lane.nextTimingRefresh = now.Add(time.Second)
			calls := 0
			lane.registration.RequestMeasurement = func() { calls++ }
			var scheduler Scheduler
			var preferred protocol.LaneID
			scheduler.applyEvent(map[protocol.LaneID]*scheduledLane{1: lane}, &preferred, tt.event, now)
			if lane.minimumRTTMicros != 10_000 || lane.rttMicros != 100_000 || lane.timingReferenceRTT != 100_000 ||
				lane.timingReferenceRate != 1_000_000 || !lane.nextTimingRefresh.Equal(now.Add(time.Second)) || calls != 0 {
				t.Fatal("invalid or stale proof changed timing, references, retry, or requests")
			}
		})
	}
}

func TestScheduledLaneMaybeRefreshTimingIncludesReportWait(t *testing.T) {
	lane := scheduledLane{
		minimumRTTMicros: 10_000, rttMicros: 179_096,
		deliveryRate: 1_000_000, rateObserved: true,
	}
	requests := 0
	lane.registration.RequestMeasurement = func() { requests++ }
	lane.maybeRefreshTiming(time.Unix(100, 0), lane.rttMicros)
	if requests != 1 {
		t.Fatal("RTT equal to the modeled loaded-cycle horizon hid the additional report wait")
	}
}

func TestScheduledLaneDeliveryWindowHorizonBoundaries(t *testing.T) {
	const maximum = 32 << 20
	for _, tt := range []struct {
		name     string
		minimum  uint64
		rtt      uint64
		feedback uint64
		rate     uint64
		horizon  uint64
		window   uint64
	}{
		{name: "MinimumFeedback", minimum: 10_000, rtt: 300_000, rate: 1_000_000, horizon: 140_000, window: 144_096},
		{name: "UnknownMinimumUsesOperatingRTT", rtt: 300_000, rate: 1_000_000, horizon: 1_300_000, window: 1_304_096},
		{name: "RoutedFeedbackDominates", minimum: 10_000, feedback: 500_000, rate: 1_000_000, horizon: 2_120_000, window: 2_124_096},
		{name: "ZeroRateKeepsSamplingAllowance", minimum: 10_000, horizon: 140_000, window: 4096},
		{name: "LastRepresentableHorizon", minimum: math.MaxUint64/4 - 25_000, horizon: math.MaxUint64 - 3, window: 4096},
		{name: "FirstOverflowingHorizon", minimum: math.MaxUint64/4 - 24_999, horizon: math.MaxUint64, window: maximum},
		{name: "FeedbackAdditionSaturates", minimum: 2, feedback: math.MaxUint64, horizon: math.MaxUint64, window: maximum},
		{name: "ReportWaitAdditionSaturates", minimum: math.MaxUint64 - 1, horizon: math.MaxUint64, window: maximum},
		{name: "LastSafeByteProduct", rate: math.MaxUint64 / 100_000, horizon: 100_000, window: maximum},
		{name: "FirstOverflowingByteProduct", rate: math.MaxUint64/100_000 + 1, horizon: 100_000, window: maximum},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := scheduledLane{
				registration:     LaneRegistration{Store: &TransmissionStore{limits: packetqueue.Limits{Packets: 1, Bytes: maximum}}},
				minimumRTTMicros: tt.minimum, rttMicros: tt.rtt, feedbackDelayMicros: tt.feedback,
				deliveryRate: tt.rate, rateObserved: true,
			}
			if got := lane.deliveryWindowHorizonMicros(); got != tt.horizon {
				t.Fatalf("feedback horizon = %d, want %d", got, tt.horizon)
			}
			if got := lane.deliveryWindowBytes(); got != tt.window {
				t.Fatalf("bounded delivery window = %d, want %d", got, tt.window)
			}
		})
	}
}
