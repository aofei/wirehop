package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
)

func TestSchedulerFreshTimingConfirmation(t *testing.T) {
	t.Run("LowFreshRTTRejectsStaleOperatingExcess", func(t *testing.T) {
		f := newFreshTimingFixture(t, 600_000)
		f.ordinary(11_000)
		if f.lane.deliveryFeedbackCycleMicros(f.lane.rttMicros) <= f.lane.deliveryWindowHorizonMicros() {
			t.Fatal("fixture did not retain a smoothed cycle above the admission horizon")
		}
		if f.calls != 0 || !f.lane.nextTimingRefresh.IsZero() {
			t.Fatal("clean ordinary RTT reused old queue delay or lost completion")
		}
	})
	t.Run("ReportDelayDropDoesNotReuseOldEWMA", func(t *testing.T) {
		f := newFreshTimingFixture(t, 121_031)
		f.lane.registration.Store.limits.Bytes = 4 << 20
		// These values construct a reachable event sequence. They do not reconstruct unrecorded timing events.
		f.lane.minimumRTTMicros = 10_498
		f.lane.feedbackDelayMicros = 500_000
		before := f.lane.deliveryWindowBytes()
		f.report(t, 0, 112)
		if f.lane.feedbackDelayMicros != 5361 || f.lane.deliveryWindowHorizonMicros() != 142_440 ||
			f.lane.deliveryFeedbackCycleMicros(f.lane.rttMicros) != 146_031 {
			t.Fatal("report did not recreate the qualifying old-EWMA and changed-feedback state")
		}
		if f.lane.registration.Store.deliveryWindow != f.lane.deliveryWindowBytes() ||
			f.lane.deliveryWindowBytes() == before || f.calls != 0 || !f.lane.nextTimingRefresh.IsZero() {
			t.Fatal("report failed to update credit without requesting from old timing")
		}
		f.ordinary(11_655)
		if f.lane.rttMicros != 107_359 || f.calls != 0 {
			t.Fatal("later clean ordinary RTT did not lower the operating estimate without a pause")
		}
	})
	t.Run("FirstGenuineStepAndOneSecondRetry", func(t *testing.T) {
		f := newFreshTimingFixture(t, 10_000)
		f.ordinary(1_200_000)
		if f.calls != 1 || f.lane.rttMicros != 158_750 || f.lane.minimumRTTMicros != 10_000 {
			t.Fatal("fresh lasting delay step lost the existing first-EWMA refresh")
		}
		first := f.now
		f.now = first.Add(time.Second - time.Nanosecond)
		f.ordinary(1_200_000)
		if f.calls != 1 {
			t.Fatal("fresh evidence bypassed the one-second retry floor")
		}
		f.now = first.Add(time.Second)
		f.ordinary(1_200_000)
		if f.calls != 2 || f.lane.timingReferenceRate != 0 || f.lane.timingReferenceRTT != 0 {
			t.Fatal("unsuccessful proof changed references or lost a retry with new ordinary evidence")
		}
	})
	t.Run("OneRawSpikeDoesNotBypassOperatingGuard", func(t *testing.T) {
		f := newFreshTimingFixture(t, 10_000)
		f.ordinary(140_000)
		if f.lane.deliveryFeedbackCycleMicros(140_000) <= f.lane.deliveryWindowHorizonMicros() ||
			f.lane.deliveryFeedbackCycleMicros(f.lane.rttMicros) > f.lane.deliveryWindowHorizonMicros() {
			t.Fatal("fixture did not separate fresh and operating horizon crossings")
		}
		if f.calls != 0 || !f.lane.nextTimingRefresh.IsZero() {
			t.Fatal("one raw queue spike requested before the existing operating guard")
		}
	})
	t.Run("FirstCapacityReportWaitsForNextOrdinarySample", func(t *testing.T) {
		f := newFreshTimingFixture(t, 600_000)
		f.lane.rateObserved = false
		f.ordinary(600_000)
		if f.calls != 0 {
			t.Fatal("unmeasured direction requested a proof")
		}
		store := f.lane.registration.Store
		for id := uint64(1); id <= 3; id++ {
			if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, store)
		}
		f.now = f.now.Add(20 * time.Millisecond)
		f.report(t, 3, 0)
		if !f.lane.rateObserved || f.calls != 0 || store.reportedPackets != 3 ||
			store.deliveryWindow != f.lane.deliveryWindowBytes() {
			t.Fatal("capacity report did not update real store progress and credit independently of refresh")
		}
		f.now = f.now.Add(time.Second)
		f.ordinary(600_000)
		if f.calls != 1 {
			t.Fatal("new capacity evidence was not used by the next ordinary RTT sample")
		}
	})
	t.Run("MeasuredTimingUpdatesWithoutChainedRequest", func(t *testing.T) {
		f := newFreshTimingFixture(t, 600_000)
		f.measured(200_000)
		if f.lane.minimumRTTMicros != 200_000 || f.lane.timingReferenceRTT != 600_000 ||
			f.lane.timingReferenceRate != f.lane.deliveryRate || f.calls != 0 ||
			f.lane.registration.Store.deliveryWindow != f.lane.deliveryWindowBytes() {
			t.Fatal("measured evidence lost its update or started a chained request")
		}
	})
	t.Run("InvalidAndStaleOrdinaryEvidenceDoesNotRequest", func(t *testing.T) {
		for _, event := range []schedulerEvent{
			{kind: schedulerTiming, laneID: 1, generation: 1, timing: clockmap.Sample{LocalSendMicros: 2, LocalReceiveMicros: 1}},
			{kind: schedulerTiming, laneID: 1, generation: 1, timing: clockmap.Sample{LocalReceiveMicros: 10, RemoteReceiveMicros: 2, RemoteSendMicros: 1}},
			{kind: schedulerTiming, laneID: 1, generation: 1, timing: clockmap.Sample{LocalReceiveMicros: 10, RemoteSendMicros: 11}},
			{kind: schedulerTiming, laneID: 1, generation: 2, timing: clockmap.Sample{LocalReceiveMicros: 1_200_000}},
			{kind: schedulerValidatedRTT, laneID: 1, generation: 1},
		} {
			f := newFreshTimingFixture(t, 600_000)
			f.apply(event)
			if f.calls != 0 || f.lane.rttMicros != 600_000 || f.lane.minimumRTTMicros != 10_000 ||
				!f.lane.nextTimingRefresh.IsZero() || f.lane.timingReferenceRate != 0 || f.lane.timingReferenceRTT != 0 {
				t.Fatal("invalid or stale input reused smoothed evidence, changed state, or lost completion")
			}
		}
	})
}

func TestScheduledLaneFreshTimingBoundaries(t *testing.T) {
	for _, test := range []struct {
		name      string
		raw       uint64
		operating uint64
		minimum   uint64
		feedback  uint64
		want      int
	}{
		{name: "RawCycleEquality", raw: 115_000, operating: 600_000, minimum: 10_000},
		{name: "RawCycleAboveBeforeSerialization", raw: 115_001, operating: 600_000, minimum: 10_000},
		{name: "OperatingCycleEquality", raw: 600_000, operating: 115_000, minimum: 10_000},
		{name: "OperatingCycleAboveBeforeSerialization", raw: 600_000, operating: 115_001, minimum: 10_000},
		{name: "RawLoadedCycleEquality", raw: 154_096, operating: 600_000, minimum: 10_000},
		{name: "RawLoadedCycleAbove", raw: 154_097, operating: 600_000, minimum: 10_000, want: 1},
		{name: "OperatingLoadedCycleEquality", raw: 600_000, operating: 154_096, minimum: 10_000},
		{name: "OperatingLoadedCycleAbove", raw: 600_000, operating: 154_097, minimum: 10_000, want: 1},
		{name: "RawRTTEqualsLoadedHorizonIncludesReportWait", raw: 179_096, operating: 179_096, minimum: 10_000, want: 1},
		{name: "FeedbackCreditSuppressesBoth", raw: 1_200_000, operating: 1_200_000, minimum: 10_000, feedback: 500_000},
		{name: "SaturatedHorizon", raw: math.MaxUint64, operating: math.MaxUint64, minimum: math.MaxUint64 / 4},
		{name: "SaturatedFeedback", raw: math.MaxUint64, operating: math.MaxUint64, minimum: 10_000, feedback: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFreshTimingFixture(t, test.operating)
			f.lane.minimumRTTMicros = test.minimum
			f.lane.feedbackDelayMicros = test.feedback
			f.lane.maybeRefreshTiming(f.now, test.raw)
			if f.calls != test.want {
				t.Fatalf("requests = %d, want %d", f.calls, test.want)
			}
		})
	}
}

func TestScheduledLaneApplyTimingReturnsAdjustedRTT(t *testing.T) {
	for _, test := range []struct {
		name   string
		sample clockmap.Sample
		want   uint64
	}{
		{name: "ProcessingRemoved", sample: clockmap.Sample{LocalSendMicros: 100, LocalReceiveMicros: 1400, RemoteReceiveMicros: 200, RemoteSendMicros: 500}, want: 1000},
		{name: "ZeroTimestamp", want: 1},
		{name: "EqualProcessing", sample: clockmap.Sample{LocalReceiveMicros: 100, RemoteSendMicros: 100}, want: 1},
		{name: "NegativeAdjusted", sample: clockmap.Sample{LocalReceiveMicros: 100, RemoteSendMicros: 101}},
		{name: "ReversedLocal", sample: clockmap.Sample{LocalSendMicros: 2, LocalReceiveMicros: 1}},
		{name: "ReversedRemote", sample: clockmap.Sample{RemoteReceiveMicros: 2, RemoteSendMicros: 1}},
		{name: "Maximum", sample: clockmap.Sample{LocalReceiveMicros: math.MaxUint64}, want: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			var lane scheduledLane
			if got := lane.applyTiming(test.sample, false); got != test.want {
				t.Fatalf("adjusted RTT = %d, want %d", got, test.want)
			}
			if test.want == 0 {
				if lane.rttObserved || lane.rttMicros != 0 || lane.minimumRTTMicros != 0 {
					t.Fatal("rejected sample changed timing state")
				}
			} else if !lane.rttObserved || lane.rttMicros != test.want || lane.minimumRTTMicros != test.want {
				t.Fatal("returned adjusted RTT differed from the estimator input")
			}
		})
	}
}

// freshTimingFixture exercises scheduler event ownership without inventing physical link service.
type freshTimingFixture struct {
	scheduler Scheduler
	lane      *scheduledLane
	lanes     map[protocol.LaneID]*scheduledLane
	preferred protocol.LaneID
	now       time.Time
	calls     int
}

func newFreshTimingFixture(t *testing.T, operating uint64) *freshTimingFixture {
	t.Helper()
	f := &freshTimingFixture{lane: schedulerLane(t, 1, 1, operating, 1_000_000), now: time.Unix(100, 0)}
	f.lanes = map[protocol.LaneID]*scheduledLane{1: f.lane}
	f.lane.minimumRTTMicros = 10_000
	f.lane.registration.RequestMeasurement = func() { f.calls++ }
	f.lane.registration.Store.now = func() time.Time { return f.now }
	t.Cleanup(func() { releaseTransmissions(f.lane.registration.Store.drain()) })
	return f
}

func (f *freshTimingFixture) ordinary(roundTrip uint64) {
	f.apply(schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 1,
		timing: clockmap.Sample{LocalReceiveMicros: roundTrip}})
}

func (f *freshTimingFixture) apply(event schedulerEvent) {
	f.scheduler.applyEvent(f.lanes, &f.preferred, event, f.now)
}

func (f *freshTimingFixture) report(t *testing.T, packets, delay uint64) {
	t.Helper()
	result := make(chan error, 1)
	f.scheduler.applyEvent(f.lanes, &f.preferred, schedulerEvent{
		kind: schedulerReport, laneID: 1, generation: 1, receiveMicros: uint64(f.now.UnixMicro()), result: result,
		report: protocol.DeliveryReport{LaneID: 1, Generation: 1, PingID: f.lane.lastPingID + 1, DataPackets: packets, DelayMicros: delay},
	}, f.now)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func (f *freshTimingFixture) measured(roundTrip uint64) {
	f.lane.applyRoundTrip(roundTrip, true)
	f.lane.registration.Store.setDeliveryWindows(f.lane.deliveryWindowBytes(), f.lane.probeWindowBytes(0))
}
