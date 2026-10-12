package relay

import (
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
)

func TestSchedulerSelectTransportCandidatesHealthyPrimary(t *testing.T) {
	for _, tt := range []struct {
		name, state                    string
		paused, full, queued, measured bool
		group                          byte
		want                           protocol.LaneID
	}{
		{name: "UnknownSameGroup", group: 1, want: 1},
		{name: "UnknownDistinctGroup", group: 2, want: 1},
		{name: "MeasuredTimelyAlternative", measured: true, want: 1},
		{name: "QueuedBeyondFlightCredit", queued: true, want: 1},
		{name: "FullPrimaryWaits", full: true},
		{name: "FullPrimaryMeasuredAlternativeWaits", full: true, measured: true},
		{name: "HealthyUnknownPreferred", state: "Unobserved", want: 1},
		{name: "Startup", state: "Startup", want: 2},
		{name: "MissingPreferred", state: "Missing", want: 2},
		{name: "DegradedPreferred", state: "Degraded", want: 2},
		{name: "AbandoningPreferred", state: "Abandoning", want: 2},
		{name: "PausedMeasuredPromotionWaits", paused: true, measured: true, want: 1},
		{name: "PausedFullPrimary", paused: true, full: true},
		{name: "PausedStartup", paused: true, state: "Startup"},
		{name: "PausedMissing", paused: true, state: "Missing"},
		{name: "PausedDegraded", paused: true, state: "Degraded"},
		{name: "PausedAbandoning", paused: true, state: "Abandoning"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			limits := packetqueue.Limits{Packets: 32, Bytes: 64 * 1024}
			if tt.full {
				limits.Packets = 1
			}
			primary := schedulerLaneWithLimits(t, 1, 1, 1_200_000, 250_000, limits)
			primary.minimumRTTMicros = 10_000
			primary.registration.Store.now = func() time.Time { return now }
			group := tt.group
			if group == 0 {
				group = 2
			}
			alternate := schedulerLane(t, 2, group, 10_000, defaultInitialRateBytesPerSecond)
			alternate.rateObserved = tt.measured
			lanes := map[protocol.LaneID]*scheduledLane{1: primary, 2: alternate}
			for _, lane := range lanes {
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			count := 0
			if tt.full {
				count = 1
			}
			if tt.queued {
				count = 10
			}
			for range count {
				if err := primary.registration.Store.push(retainedTransmission{deadlineMicros: uint64(now.Add(5 * time.Second).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]}}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.queued {
				_, retained := primary.registration.Store.backlog()
				if retained <= primary.deliveryWindowBytes() || !primary.canAccept(1500) {
					t.Fatal("fixture did not isolate buffered work from flight credit")
				}
			}
			preferred := protocol.LaneID(1)
			switch tt.state {
			case "Startup":
				preferred = 0
			case "Missing":
				delete(lanes, 1)
			case "Degraded":
				primary.degraded = true
			case "Abandoning":
				primary.abandoning = true
			case "Unobserved":
				primary.rateObserved = false
			}
			scheduler := Scheduler{transportHoldUntil: now.Add(time.Second)}
			if tt.paused {
				scheduler.measurementStarted = now
				scheduler.transportHoldUntil = time.Time{}
			}
			// The primary cannot meet this predicted deadline. A healthy established
			// assignment remains usable until actual expiry, including exact score equality.
			for _, cutoff := range []uint64{500_000, primary.score(1500)} {
				got := scheduler.selectTransportCandidates(lanes, preferred, 1500, cutoff, now)
				var selected protocol.LaneID
				if got.count != 0 {
					selected = got.lanes[0].registration.LaneID
				}
				if selected != tt.want || got.available != (tt.want != 0) {
					t.Fatalf("selected %d, available %t, want %d", selected, got.available, tt.want)
				}
			}
		})
	}
}
