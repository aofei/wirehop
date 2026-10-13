package relay

import (
	"slices"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestSchedulerRouteReportAlternateFallbacks(t *testing.T) {
	for _, tt := range []struct {
		name          string
		reject        []protocol.LaneID
		ownAbsent     bool
		ownAbandoning bool
		want          []protocol.LaneID
		wantSent      bool
	}{
		{name: "BestAlternate", want: []protocol.LaneID{4, 1}, wantSent: true},
		{name: "BestAlternateFull", reject: []protocol.LaneID{1}, want: []protocol.LaneID{4, 1, 2}, wantSent: true},
		{name: "TwoAlternatesFull", reject: []protocol.LaneID{1, 2}, want: []protocol.LaneID{4, 1, 2, 3}, wantSent: true},
		{name: "OwnQueueFull", reject: []protocol.LaneID{4}, want: []protocol.LaneID{4, 1}, wantSent: true},
		{name: "AllQueuesFull", reject: []protocol.LaneID{1, 2, 3, 4}, want: []protocol.LaneID{4, 1, 2, 3}},
		{name: "OwnAbsent", ownAbsent: true, reject: []protocol.LaneID{1}, want: []protocol.LaneID{1, 2}, wantSent: true},
		{name: "OwnAbandoning", ownAbandoning: true, want: []protocol.LaneID{1}, wantSent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var attempted []protocol.LaneID
			var callbacks []func()
			lanes := make(map[protocol.LaneID]*scheduledLane)
			for id := byte(1); id <= 5; id++ {
				lane := schedulerLane(t, id, id, uint64(id)*1000, 1_000_000)
				laneID := lane.registration.LaneID
				lane.registration.SendDeliveryReport = func(_ protocol.DeliveryReport, _ time.Time, onSent func()) bool {
					attempted = append(attempted, laneID)
					if slices.Contains(tt.reject, laneID) {
						return false
					}
					callbacks = append(callbacks, onSent)
					return true
				}
				lanes[laneID] = lane
			}
			lanes[2].rttMicros = lanes[1].rttMicros
			lanes[1].degraded = true
			lanes[5].rttMicros = 0
			lanes[5].abandoning = true
			lanes[4].abandoning = tt.ownAbandoning
			if tt.ownAbsent {
				delete(lanes, 4)
			}
			completed := 0
			sent := false
			var scheduler Scheduler
			scheduler.routeReport(lanes, protocol.DeliveryReport{LaneID: 4, Generation: 1}, time.Now(), func(ok bool) {
				completed++
				sent = ok
			})
			if !slices.Equal(attempted, tt.want) {
				t.Fatalf("attempted lanes = %v, want %v", attempted, tt.want)
			}
			if tt.wantSent && completed != 0 {
				t.Fatal("report completed before a carrier write")
			}
			for _, callback := range callbacks {
				callback()
				callback()
			}
			if completed != 1 || sent != tt.wantSent {
				t.Fatalf("completion = %d calls, sent %t, want 1, %t", completed, sent, tt.wantSent)
			}
		})
	}
}
