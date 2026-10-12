package relay

import (
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
)

func TestScheduledLaneTimingReferencesFollowImprovements(t *testing.T) {
	t.Run("ProofUsesOperatingFloor", func(t *testing.T) {
		f := newFreshTimingFixture(t, 600_000)
		f.measured(1_200_000)
		if f.lane.timingReferenceRTT != 1_200_000 || f.lane.timingReferenceRate != 1_000_000 || f.calls != 0 {
			t.Fatal("proof reference did not include its operating RTT floor")
		}
	})
	t.Run("RTTConvergenceDuringCooldownThenRise", func(t *testing.T) {
		f := newFreshTimingFixture(t, 600_000)
		f.lane.applyRoundTrip(10_000, true)
		f.lane.nextTimingRefresh = f.now.Add(time.Second)
		f.ordinary(10_000)
		if f.lane.timingReferenceRTT != 526_250 || f.calls != 0 {
			t.Fatal("clean convergence before raw/cooldown gates did not lower the reference")
		}
		f.now = f.now.Add(time.Second - time.Nanosecond)
		f.ordinary(1_200_000)
		if f.calls != 0 || f.lane.timingReferenceRTT != 526_250 {
			t.Fatal("new deterioration bypassed cooldown or raised its own reference")
		}
		f.now = f.now.Add(time.Nanosecond)
		f.ordinary(1_200_000)
		if f.calls != 1 {
			t.Fatal("genuine rise after convergence did not request a proof")
		}
		f.now = f.now.Add(time.Second)
		f.ordinary(1_200_000)
		if f.calls != 2 || f.lane.timingReferenceRTT != 526_250 {
			t.Fatal("unsuccessful request reset evidence or lost its retry")
		}
	})
	t.Run("FallingOrdinaryRTTDoesNotRequest", func(t *testing.T) {
		f := newFreshTimingFixture(t, 832_969)
		f.lane.applyRoundTrip(10_000, true)
		f.ordinary(195_451)
		if f.lane.rttMicros != 753_279 || f.lane.timingReferenceRTT != 753_279 || f.calls != 0 {
			t.Fatal("RTT improvement requested another timing pause")
		}
	})
	t.Run("CapacityGrowthThenStrictQuarterLoss", func(t *testing.T) {
		for _, rate := range []uint64{1_500_000, 1_499_999} {
			f := newFreshTimingFixture(t, 600_000)
			f.lane.applyRoundTrip(10_000, true)
			f.lane.updateDeliveryRate(deliverySample{bytes: 2_000_000, intervalMicros: 1_000_000}, 1_000_000)
			f.ordinary(600_000)
			if f.calls != 0 || f.lane.timingReferenceRate != 2_000_000 {
				t.Fatal("accepted growth requested a proof or failed to raise the capacity reference")
			}
			f.lane.updateDeliveryRate(deliverySample{bytes: rate, intervalMicros: 1_000_000}, 3_000_000)
			f.ordinary(600_000)
			want := 0
			if rate < 1_500_000 {
				want = 1
			}
			if f.calls != want || f.lane.timingReferenceRate != 2_000_000 {
				t.Fatalf("rate %d requested %d proofs, want %d with high-water reference", rate, f.calls, want)
			}
		}
	})
	t.Run("RejectedObservationsPreserveReferences", func(t *testing.T) {
		f := newFreshTimingFixture(t, 600_000)
		f.lane.applyRoundTrip(10_000, true)
		f.lane.updateDeliveryRate(deliverySample{bytes: 4_000_000, intervalMicros: 999}, 1_000_000)
		f.apply(schedulerEvent{kind: schedulerTiming, laneID: 1, generation: 1,
			timing: clockmap.Sample{LocalSendMicros: 2, LocalReceiveMicros: 1}})
		if f.lane.timingReferenceRate != 1_000_000 || f.lane.timingReferenceRTT != 600_000 || f.calls != 0 {
			t.Fatal("invalid sample changed refresh references")
		}
	})
}
