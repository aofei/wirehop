package relay

import (
	"context"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestLaneProgressStalledRecentRTT(t *testing.T) {
	for _, tt := range []struct {
		name    string
		latest  uint64
		elapsed time.Duration
		want    bool
	}{
		{name: "RecentRTTStep", latest: 1_200_000, elapsed: 1200 * time.Millisecond},
		{name: "ActualStall", latest: 1_200_000, elapsed: 3 * time.Second, want: true},
		{name: "FreshRTTRestored", latest: 10_000, elapsed: 1200 * time.Millisecond, want: true},
		{name: "Overflow", latest: math.MaxUint64, elapsed: time.Hour},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			lane := &scheduledLane{rttMicros: 150_000, latestRTTMicros: tt.latest,
				deliveryRate: 1_000_000, lastProgressAt: now}
			if got := laneProgressStalled(lane, now.Add(tt.elapsed), now, 1200); got != tt.want {
				t.Fatalf("laneProgressStalled() = %t, want %t with latest RTT %d", got, tt.want, tt.latest)
			}
		})
	}
}

func TestSchedulerCheckAbandonmentRecentRTT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192})
		if err != nil {
			t.Fatal(err)
		}
		defer queue.Close()
		scheduler, err := NewScheduler(&Ingress{queue: queue})
		if err != nil {
			t.Fatal(err)
		}
		now := queue.Now()
		primary := schedulerLane(t, 2, 1, 150_000, 1_000_000)
		primary.latestRTTMicros = 1_200_000
		primary.lastProgressAt = now
		alternate := schedulerLane(t, 1, 1, 48_000, 2000)
		alternate.registration.Store.transportReported.Store(0)
		lanes := map[protocol.LaneID]*scheduledLane{1: alternate, 2: primary}
		for _, lane := range lanes {
			lane.registration.Store.now = queue.Now
			defer func() { releaseTransmissions(lane.registration.Store.drain()) }()
		}
		for id := uint64(1); id <= 2; id++ {
			if err := primary.registration.Store.push(schedulerTransmission(id, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
				t.Fatal(err)
			}
		}
		takeOneTransmission(t, primary.registration.Store)
		ctx, cancel := context.WithCancel(t.Context())
		defer func() {
			cancel()
			scheduler.updateMeasurementDrain(lanes, false)
		}()
		scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 2, Generation: 1}, false, nil)
		preferred := protocol.LaneID(2)
		scheduler.applyEvent(lanes, &preferred, <-scheduler.events, now, false)
		if scheduler.measurementQuiet {
			t.Fatal("fixture did not retain queued work while preparing")
		}
		time.Sleep(1200 * time.Millisecond)
		scheduler.checkAbandonment(lanes, &preferred, queue.Now())
		if primary.degraded || preferred != 2 {
			t.Fatal("a fresh feedback cycle was classified as stalled during measurement preparation")
		}
		time.Sleep(2 * time.Second)
		scheduler.checkAbandonment(lanes, &preferred, queue.Now())
		if !primary.degraded {
			t.Fatal("recent RTT prevented detection of a real multi-cycle stall")
		}
	})
}
