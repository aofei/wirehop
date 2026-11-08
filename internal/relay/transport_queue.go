package relay

import (
	"math"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

// transportQueueControl signals persistent ingress congestion while retaining room for transient bursts.
type transportQueueControl struct {
	lane        protocol.LaneGeneration
	aboveSince  time.Time
	dropNext    time.Time
	lastDequeue time.Time
	packetBytes uint64
	count       uint64
	dropping    bool
}

// drop distinguishes sustained queue delay from a burst using a CoDel-style interval and inverse-square-root control.
// The target permits feedback-paced bursts. The interval covers a complete feedback flight, two operating RTTs,
// and two serialization allowances.
func (q *transportQueueControl) drop(now time.Time, queuedMicros uint64, backlog int, frameBytes uint64,
	lane *scheduledLane) bool {
	identity := protocol.LaneGeneration{LaneID: lane.registration.LaneID, Generation: lane.registration.Generation}
	if q.lane != identity {
		*q = transportQueueControl{lane: identity}
	}
	q.packetBytes = max(q.packetBytes, frameBytes)
	minimumRTT := lane.minimumRTTMicros
	if minimumRTT == 0 {
		minimumRTT = lane.rttMicros
	}
	target := max(lane.deliveryFeedbackCycleMicros(minimumRTT), 2*q.packetBytes*1_000_000/lane.deliveryRate)
	rtt := max(lane.rttMicros, lane.latestRTTMicros)
	intervalMicros := max(uint64(100_000), lane.deliveryWindowHorizonMicros(),
		saturatingAdd(rtt, rtt), saturatingAdd(target, target))
	interval := time.Duration(min(intervalMicros, uint64(math.MaxInt64/int64(time.Microsecond)))) * time.Microsecond
	if !q.lastDequeue.IsZero() && now.Sub(q.lastDequeue) > interval {
		q.aboveSince = time.Time{}
		q.dropping = false
	}
	q.lastDequeue = now
	at := uint64(now.UnixMicro())
	if queuedMicros == 0 || at < queuedMicros || at-queuedMicros < target ||
		uint64(backlog) <= max(q.packetBytes, minimumDeliveryWindow) {
		q.aboveSince = time.Time{}
		q.dropping = false
		return false
	}
	if q.aboveSince.IsZero() {
		q.aboveSince = now
		return false
	}
	if now.Sub(q.aboveSince) < interval {
		return false
	}
	if !q.dropping {
		q.dropping = true
		q.count = 1
		q.dropNext = now.Add(interval)
		return true
	}
	if now.Before(q.dropNext) {
		return false
	}
	q.count++
	q.dropNext = q.dropNext.Add(time.Duration(float64(interval) / math.Sqrt(float64(q.count))))
	return true
}
