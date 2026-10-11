package relay

import (
	"math"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/protocol"
)

const (
	// initialDeliveryWindow bounds startup before any delivery-rate sample is available.
	initialDeliveryWindow = 16 * 1024
	// minimumDeliveryWindow permits useful rate samples without a large low-rate backlog.
	minimumDeliveryWindow = 4 * 1024
	// transportProbeInterval bounds redundant discovery traffic on established paths.
	transportProbeInterval = 30 * time.Second
	// minimumTransportProbeDuration lets a high-delay path grow beyond its initial flight.
	minimumTransportProbeDuration = 4 * time.Second
	// maximumTransportProbeDuration bounds discovery on very high-delay paths.
	maximumTransportProbeDuration = 8 * time.Second
	// maximumTransportProbeBytes bounds the total padding sent in one discovery period.
	maximumTransportProbeBytes = 2 * 1024 * 1024
	// minimumTransportHoldDuration prevents repeated primary changes while the inner flow adjusts to a new RTT.
	minimumTransportHoldDuration = 2 * time.Second
	// maximumTransportHoldDuration bounds primary hysteresis on very high-delay paths.
	maximumTransportHoldDuration = 8 * time.Second
)

// selectTransportCandidates keeps a capacity-selected transport primary. A full primary
// waits for progress rather than spilling unique packets onto a path with a different arrival time.
func (s *Scheduler) selectTransportCandidates(lanes map[protocol.LaneID]*scheduledLane, preferred protocol.LaneID,
	frameBytes, maximumScore uint64, now time.Time) laneCandidates {
	primary := lanes[preferred]
	holding := primary != nil && !primary.degraded && !primary.abandoning && now.Before(s.transportHoldUntil)
	if primary == nil || primary.degraded || primary.abandoning {
		primary = nil
		for _, candidate := range lanes {
			if candidate.degraded || candidate.abandoning || frameBytes > uint64(candidate.registration.Store.limits.Bytes) {
				continue
			}
			if primary == nil || candidate.rttMicros < primary.rttMicros ||
				candidate.rttMicros == primary.rttMicros && candidate.registration.LaneID < primary.registration.LaneID {
				primary = candidate
			}
		}
		if primary == nil {
			return laneCandidates{}
		}
	}
	if !holding {
		var best *scheduledLane
		for _, candidate := range lanes {
			if candidate == primary || candidate.degraded || candidate.abandoning || !candidate.rateObserved ||
				frameBytes > uint64(candidate.registration.Store.limits.Bytes) || candidate.score(frameBytes) >= maximumScore {
				continue
			}
			faster := candidate.deliveryRate > saturatingAdd(primary.deliveryRate, primary.deliveryRate/4)
			if !faster {
				continue
			}
			if best == nil || candidate.deliveryRate > best.deliveryRate ||
				candidate.deliveryRate == best.deliveryRate && candidate.registration.LaneID < best.registration.LaneID {
				best = candidate
			}
		}
		if best != nil {
			primary = best
		}
	}
	if primary.score(frameBytes) >= maximumScore {
		return selectCandidates(lanes, preferred, frameBytes, maximumScore)
	}
	if !primary.canAccept(frameBytes) {
		return laneCandidates{}
	}
	return laneCandidates{lanes: [2]*scheduledLane{primary}, count: 1, available: true}
}

// probePadding supplies immutable payload storage shared by capacity probes.
var probePadding [protocol.ProbePayloadSize]byte

// fillTransportProbe samples one eligible path without consuming packet IDs or producing UDP traffic.
func (s *Scheduler) fillTransportProbe(lanes map[protocol.LaneID]*scheduledLane, preferred protocol.LaneID,
	now time.Time, reserveBytes uint64) bool {
	var group protocol.PathGroupID
	distinct := false
	for _, lane := range lanes {
		if lane.degraded || lane.abandoning {
			continue
		}
		if group.IsZero() {
			group = lane.registration.PathGroupID
		} else if lane.registration.PathGroupID != group {
			distinct = true
			break
		}
	}
	if !distinct {
		return false
	}
	size, _ := protocol.DataFrameSize(protocol.Data{Payload: probePadding[:]})
	var first, next *scheduledLane
	for _, candidate := range lanes {
		active := now.Before(candidate.probeUntil)
		if candidate.degraded || candidate.abandoning || candidate.registration.LaneID == preferred && !active {
			continue
		}
		if active && candidate.probeBytes+uint64(size) > maximumTransportProbeBytes {
			continue
		}
		if !active && (now.Before(candidate.nextProbeAt) || !candidate.nextProbeAt.IsZero() &&
			(s.lastTransportAt.IsZero() || now.Sub(s.lastTransportAt) > time.Second)) {
			continue
		}
		packets, retained := candidate.registration.Store.backlog()
		if reserveBytes > 0 && packets >= candidate.registration.Store.limits.Packets-1 {
			continue
		}
		limit := candidate.probeWindowBytes(reserveBytes)
		if retained > limit || uint64(size) > limit-retained || !candidate.registration.Store.canAccept(uint64(size)) {
			continue
		}
		if (!candidate.rateObserved || candidate.deliveryWindowBytes() < minimumDeliveryWindow+2*uint64(size)) &&
			candidate.registration.Store.hasProbe() {
			continue
		}
		id := candidate.registration.LaneID
		if first == nil || id < first.registration.LaneID {
			first = candidate
		}
		if id > s.lastProbeLane && (next == nil || id < next.registration.LaneID) {
			next = candidate
		}
	}
	probe := next
	if probe == nil {
		probe = first
	}
	if probe == nil {
		return false
	}
	if !now.Before(probe.probeUntil) {
		probe.probeBytes = 0
	}
	limit := probe.probeWindowBytes(reserveBytes)
	singleProbe := !probe.rateObserved || probe.deliveryWindowBytes() < minimumDeliveryWindow+2*uint64(size)
	progressed := false
	s.lastProbeLane = probe.registration.LaneID
	duration := minimumTransportProbeDuration
	if probe.rttMicros < uint64(maximumTransportProbeDuration/time.Microsecond)/8 {
		duration = max(duration, time.Duration(probe.rttMicros*8)*time.Microsecond)
	} else {
		duration = maximumTransportProbeDuration
	}
	deadline := probe.probeUntil
	if !now.Before(deadline) {
		deadline = now.Add(duration)
	}
	for range maximumDataBatchFrames {
		packets, bytes := probe.registration.Store.backlog()
		if reserveBytes > 0 && packets >= probe.registration.Store.limits.Packets-1 {
			break
		}
		if bytes > limit || uint64(size) > limit-bytes || !probe.registration.Store.canAccept(uint64(size)) ||
			probe.probeBytes+uint64(size) > maximumTransportProbeBytes {
			break
		}
		transmission := retainedTransmission{deadlineMicros: uint64(deadline.UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]}}
		if err := probe.registration.Store.pushAt(transmission, now); err != nil {
			break
		}
		if !now.Before(probe.probeUntil) {
			probe.probeBytes = 0
			probe.probeUntil = deadline
			probe.nextProbeAt = deadline.Add(transportProbeInterval)
		}
		probe.probeBytes += uint64(size)
		progressed = true
		if singleProbe {
			break
		}
	}
	return progressed
}

// recordTransportAssignment commits discovery state only after a successful store transfer.
func (s *Scheduler) recordTransportAssignment(lanes map[protocol.LaneID]*scheduledLane, previous protocol.LaneID,
	lane *scheduledLane, now time.Time) {
	s.lastTransportAt = now
	if previous == lane.registration.LaneID {
		return
	}
	if old := lanes[previous]; old != nil && !now.Before(old.probeUntil) {
		old.nextProbeAt = now.Add(transportProbeInterval)
	}
	if !now.Before(lane.probeUntil) {
		lane.nextProbeAt = now.Add(transportProbeInterval)
	}
	hold := minimumTransportHoldDuration
	if lane.rttMicros < uint64(maximumTransportHoldDuration/time.Microsecond)/4 {
		hold = max(hold, time.Duration(lane.rttMicros*4)*time.Microsecond)
	} else {
		hold = maximumTransportHoldDuration
	}
	if !previous.IsZero() {
		s.transportHoldUntil = now.Add(hold)
	} else {
		s.transportHoldUntil = time.Time{}
	}
}

// probeWindowBytes grows measured discovery while preserving useful-traffic byte headroom.
func (l *scheduledLane) probeWindowBytes(reserveBytes uint64) uint64 {
	window := l.deliveryWindowBytes()
	if l.rateObserved && reserveBytes == 0 {
		size, _ := protocol.DataFrameSize(protocol.Data{Payload: probePadding[:]})
		capacity := window - min(window, minimumDeliveryWindow)
		if capacity >= 2*uint64(size) {
			window = min(uint64(l.registration.Store.limits.Bytes), saturatingAdd(window, capacity))
		}
	}
	return window - min(window, reserveBytes)
}

// deliveryWindowBytes bounds retained work by four unloaded feedback cycles and a small sampling allowance.
func (l *scheduledLane) deliveryWindowBytes() uint64 {
	maximum := uint64(l.registration.Store.limits.Bytes)
	if !l.rateObserved {
		return min(maximum, initialDeliveryWindow)
	}
	rtt := l.minimumRTTMicros
	if rtt == 0 {
		rtt = l.rttMicros
	}
	cycle := max(rtt, saturatingAdd(rtt/2, l.feedbackDelayMicros))
	cycle = saturatingAdd(cycle, uint64(defaultReportInterval/time.Microsecond))
	if cycle > math.MaxUint64/4 {
		return maximum
	}
	interval := cycle * 4
	if l.deliveryRate > math.MaxUint64/interval {
		return maximum
	}
	bytes := l.deliveryRate * interval / 1_000_000
	return min(maximum, max(uint64(minimumDeliveryWindow), saturatingAdd(bytes, minimumDeliveryWindow)))
}
