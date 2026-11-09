package relay

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerMeasurementRefreshesDrainCredit(t *testing.T) {
	for _, tt := range []struct {
		name   string
		queued bool
		want   uint64
	}{
		{name: "FreshTimingDuringDrain", queued: true, want: 4_904_096},
		{name: "QuietCreditCannotGrow", want: 144_096},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, scheduler := feedbackScheduler(t)
				lane := schedulerLaneWithLimits(t, 1, 1, 10_000, 1_000_000,
					packetqueue.Limits{Packets: 65536, Bytes: 8 << 20})
				lane.minimumRTTMicros, lane.latestRTTMicros, lane.rttObserved = 10_000, 10_000, true
				store := lane.registration.Store
				store.now = queue.Now
				defer func() { releaseTransmissions(store.drain()) }()
				if tt.queued {
					if err := store.push(schedulerTransmission(1, wgpacket.TransportData, queue.Now().Add(time.Hour))); err != nil {
						t.Fatal(err)
					}
				}
				lanes := map[protocol.LaneID]*scheduledLane{1: lane}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, false, nil)
				preferred := protocol.LaneID(1)
				scheduler.applyEvent(lanes, &preferred, <-scheduler.events, queue.Now(), false)
				if store.measurementWindow != 144_096 || scheduler.measurementQuiet == tt.queued {
					t.Fatal("fixture did not capture stale timing at the admission cut")
				}
				scheduler.applyEvent(lanes, &preferred, schedulerEvent{
					kind: schedulerTiming, laneID: 1, generation: 1,
					timing: clockmap.Sample{LocalSendMicros: 1, LocalReceiveMicros: 1_200_001},
				}, queue.Now(), false)
				if store.measurementWindow != tt.want {
					t.Fatalf("late ordinary timing left drain credit at %d, want %d", store.measurementWindow, tt.want)
				}
				if lane.minimumRTTMicros != 10_000 || store.deliveryWindow != 144_096 {
					t.Fatal("a drain-credit refresh changed the ordinary propagation floor or window")
				}
				cancel()
				scheduler.updateMeasurementDrain(lanes, false)
				if store.measurementWindow != 0 || scheduler.input.pauseOwner != nil {
					t.Fatal("completion retained refreshed credit or ingress admission ownership")
				}
			})
		})
	}
}
