package relay

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestScheduledLaneMaybeRefreshTiming(t *testing.T) {
	for _, tt := range []struct {
		name          string
		rate          uint64
		rtt           uint64
		minimum       uint64
		referenceRate uint64
		referenceRTT  uint64
		unobserved    bool
		want          int
	}{
		{name: "FirstCapacityLoss", rate: 100_000, rtt: 600_000, minimum: 10_000, want: 1},
		{name: "FirstCapacityGrowth", rate: 1_000_000, rtt: 600_000, minimum: 10_000, want: 1},
		{name: "NoMeasuredCapacity", rate: 1_000_000, rtt: 600_000, minimum: 10_000, unobserved: true},
		{name: "StableDelay", rate: 100_000, rtt: 10_000, minimum: 10_000},
		{name: "SmallDelayChange", rate: 100_000, rtt: 40_000, minimum: 10_000},
		{name: "SmallRelativeChange", rate: 100_000, rtt: 650_000, minimum: 600_000},
		{name: "UnknownDelay", rate: 100_000, rtt: 600_000},
		{name: "KnownQueue", rate: 1_000_000, rtt: 600_000, minimum: 10_000, referenceRate: 1_000_000, referenceRTT: 600_000},
		{name: "RateLossBoundary", rate: 750_000, rtt: 600_000, minimum: 10_000, referenceRate: 1_000_000, referenceRTT: 600_000},
		{name: "RateLoss", rate: 749_999, rtt: 600_000, minimum: 10_000, referenceRate: 1_000_000, referenceRTT: 600_000, want: 1},
		{name: "RateGrowth", rate: 1_250_001, rtt: 600_000, minimum: 10_000, referenceRate: 1_000_000, referenceRTT: 600_000},
		{name: "DelayGrowth", rate: 1_000_000, rtt: 650_000, minimum: 10_000, referenceRate: 1_000_000, referenceRTT: 600_000, want: 1},
		{name: "DelayReduction", rate: 1_000_000, rtt: 550_000, minimum: 10_000, referenceRate: 1_000_000, referenceRTT: 600_000},
		{name: "HighDelayJitter", rate: 1_000_000, rtt: 2_400_000, minimum: 600_000, referenceRate: 1_000_000, referenceRTT: 2_500_000},
		{name: "HighDelayReduction", rate: 1_000_000, rtt: 2_350_000, minimum: 600_000, referenceRate: 1_000_000, referenceRTT: 2_500_000},
		{name: "MaximumRateStable", rate: math.MaxUint64, rtt: 600_000, minimum: 10_000, referenceRate: math.MaxUint64, referenceRTT: 600_000},
		{name: "MaximumRateLoss", rate: math.MaxUint64 / 2, rtt: 600_000, minimum: 10_000, referenceRate: math.MaxUint64, referenceRTT: 600_000, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			lane := &scheduledLane{
				registration: LaneRegistration{RequestMeasurement: func() { calls++ }},
				deliveryRate: tt.rate, rateObserved: !tt.unobserved,
				rttMicros: tt.rtt, minimumRTTMicros: tt.minimum,
				timingReferenceRate: tt.referenceRate, timingReferenceRTT: tt.referenceRTT,
			}
			now := time.Unix(100, 0)
			lane.maybeRefreshTiming(now, lane.rttMicros)
			lane.maybeRefreshTiming(now.Add(timingRefreshInterval-time.Nanosecond), lane.rttMicros)
			if calls != tt.want {
				t.Fatalf("requests before cooldown = %d, want %d", calls, tt.want)
			}
			lane.maybeRefreshTiming(now.Add(timingRefreshInterval), lane.rttMicros)
			if calls != tt.want*2 {
				t.Fatalf("requests after unsuccessful attempt = %d, want %d", calls, tt.want*2)
			}
		})
	}
}

func TestScheduledLaneMaybeRefreshTimingAfterWarmupQueue(t *testing.T) {
	lane := schedulerLane(t, 1, 1, 70_000, 2_000_000)
	lane.minimumRTTMicros = 10_000
	calls := 0
	lane.registration.RequestMeasurement = func() { calls++ }
	now := time.Unix(100, 0)
	lane.maybeRefreshTiming(now, lane.rttMicros)
	if calls != 0 {
		t.Fatal("startup queue within the admission horizon requested an unnecessary proof")
	}
	// A real delay step still requests a proof before queued packets expire.
	lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 1_200_000}, false)
	lane.updateDeliveryRate(deliverySample{bytes: 800_000, intervalMicros: 2_000_000}, 3_000_000)
	lane.maybeRefreshTiming(now.Add(3*time.Second), lane.rttMicros)
	if calls != 1 {
		t.Fatal("suppressed startup queue prevented refresh after a real delay step")
	}
}

func TestScheduledLaneApplyTimingRequiresDrainedRefresh(t *testing.T) {
	lane := schedulerLane(t, 1, 1, 10_000, 100_000)
	lane.rttObserved = false
	// Admission timing may use a peer clock origin larger than the later local clock.
	lane.applyTiming(clockmap.Sample{LocalSendMicros: 999_990_000, LocalReceiveMicros: 1_000_000_000}, false)
	ordinary := clockmap.Sample{LocalSendMicros: 400_000, LocalReceiveMicros: 1_000_000}
	for range 32 {
		lane.applyTiming(ordinary, false)
	}
	if lane.minimumRTTMicros != 10_000 || lane.rttMicros < 590_000 {
		t.Fatal("ordinary queue delay changed the minimum or a peer clock origin suppressed local samples")
	}
	lane.applyTiming(clockmap.Sample{LocalSendMicros: 2, LocalReceiveMicros: 1}, true)
	lane.applyTiming(clockmap.Sample{LocalSendMicros: 1, LocalReceiveMicros: 2, RemoteReceiveMicros: 3, RemoteSendMicros: 2}, true)
	if lane.minimumRTTMicros != 10_000 || lane.timingReferenceRate != 0 || lane.timingReferenceRTT != 0 {
		t.Fatal("invalid refresh changed timing or retry evidence")
	}
	lane.applyTiming(ordinary, true)
	if lane.minimumRTTMicros != 600_000 || lane.timingReferenceRate != lane.deliveryRate || lane.timingReferenceRTT != 600_000 {
		t.Fatal("drained refresh did not replace the minimum and save the operating point after applying the validated floor")
	}
	if lane.rttMicros != 600_000 {
		t.Fatal("operating RTT fell below the validated propagation floor")
	}
	lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 10_000}, true)
	if lane.minimumRTTMicros != 10_000 || lane.rttMicros != 600_000 {
		t.Fatal("draining the queue artificially reduced the ordinary RTT average")
	}
}

func TestScheduledLaneApplyTimingClampsZero(t *testing.T) {
	for _, tt := range []struct {
		name   string
		sample clockmap.Sample
	}{
		{name: "ZeroTimestamp"},
		{name: "EqualProcessing", sample: clockmap.Sample{LocalReceiveMicros: 20, RemoteSendMicros: 20}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var lane scheduledLane
			lane.applyTiming(tt.sample, true)
			if lane.minimumRTTMicros != 1 || lane.rttMicros != 1 || lane.latestRTTMicros != 1 {
				t.Fatalf("minimum = %d, RTT = %d, want positive clamped timing", lane.minimumRTTMicros, lane.rttMicros)
			}
		})
	}
}

func TestScheduledLaneApplyTimingRejectsNegativeRTT(t *testing.T) {
	for _, tt := range []struct {
		name        string
		revalidated bool
	}{
		{name: "Ordinary"},
		{name: "Revalidated", revalidated: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := schedulerLane(t, 1, 1, 100_000, 1_000_000)
			lane.minimumRTTMicros = 100_000
			lane.timingReferenceRate = 900_000
			lane.timingReferenceRTT = 150_000
			lane.latestRTTMicros = 150_000
			window := lane.deliveryWindowBytes()
			// A one-microsecond clock-rate difference over a long peer wait is not a path RTT.
			lane.applyTiming(clockmap.Sample{
				LocalSendMicros: 100, LocalReceiveMicros: 5_000_100,
				RemoteReceiveMicros: 200, RemoteSendMicros: 5_000_201,
			}, tt.revalidated)
			if lane.minimumRTTMicros != 100_000 || lane.rttMicros != 100_000 || lane.deliveryWindowBytes() != window ||
				lane.timingReferenceRate != 900_000 || lane.timingReferenceRTT != 150_000 || lane.latestRTTMicros != 150_000 {
				t.Fatal("negative RTT changed the minimum, operating point, delivery window, or retry evidence")
			}
		})
	}
}

func TestScheduledLaneRefreshTimingAfterCapacityAndDelayIncrease(t *testing.T) {
	for _, tt := range []struct {
		name      string
		priorRate uint64
	}{
		{name: "CapacityIncrease", priorRate: 4000},
		{name: "StableCapacity", priorRate: 1_000_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(1, 0)
			store, err := newTransmissionStore(packetqueue.Limits{Packets: 4096, Bytes: 8 << 20}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store)}
			lane.applyTiming(clockmap.Sample{LocalSendMicros: 990_000, LocalReceiveMicros: 1_000_000}, false)
			lane.updateDeliveryRate(deliverySample{bytes: tt.priorRate * 2, intervalMicros: 2_000_000}, 1_000_000)
			requests := 0
			started := now
			lane.registration.RequestMeasurement = func() {
				requests++
				if now.Sub(started) > measurementPauseLimit*measurementQuietRatio+2*time.Second {
					t.Fatal("ambiguous byte-floor evidence exceeded the bounded calibration delay")
				}
				receive := uint64(now.UnixMicro())
				lane.applyTiming(clockmap.Sample{LocalSendMicros: receive - 600_000, LocalReceiveMicros: receive}, true)
			}
			// Report complete flights over a path whose capacity and RTT have both increased.
			// This exercises the real store's application-limited classification and rate samples.
			for range 400 {
				store.setDeliveryWindows(lane.deliveryWindowBytes(), lane.probeWindowBytes(0))
				queued := 0
				for uint64(queued+1)*4096 <= lane.deliveryWindowBytes() && lane.canAccept(4096) {
					if err := store.push(deliverySampleTransmission(t, store, store.sentPackets+uint64(queued)+1)); err != nil {
						t.Fatal(err)
					}
					queued++
				}
				var data [16]protocol.Data
				var ownership [16]datagram.Packet
				for committed := 0; committed < queued; {
					n, err := store.takeBatch(data[:], ownership[:], 64*1024, 0)
					releaseBatchOwnership(ownership[:n])
					if err != nil || n == 0 {
						t.Fatalf("take batch: count %d, error %v", n, err)
					}
					committed += n
				}
				now = now.Add(time.Duration(max(uint64(600_000), uint64(queued)*4096)) * time.Microsecond)
				receive := uint64(now.UnixMicro())
				progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: store.sentPackets}, receive, now)
				if err != nil || !progressed {
					t.Fatalf("report: progress %t, error %v", progressed, err)
				}
				roundTrip := lane.applyTiming(clockmap.Sample{LocalSendMicros: receive - 600_000, LocalReceiveMicros: receive}, false)
				lane.maybeRefreshTiming(now, roundTrip)
			}
			if lane.deliveryRate != 1_000_000 || lane.minimumRTTMicros != 600_000 || requests != 1 {
				t.Fatalf("rate = %d, minimum = %d, refreshes = %d", lane.deliveryRate, lane.minimumRTTMicros, requests)
			}
		})
	}
}

func TestSchedulerApplyMeasurementUpdatesRoutedFeedback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newMeasurementFixture(t, true, 2)
		f.target.LaneID = 2
		f.scheduler.RequestMeasurement(f.target)
		f.apply(t)
		attempt := f.scheduler.measurements.active
		f.scheduler.publishMeasurement(attempt, 600_000, func(bool) {})
		event := <-f.scheduler.events
		event.generation = 2
		var preferred protocol.LaneID
		f.scheduler.applyEvent(f.lanes, &preferred, event, time.Now())
		if f.lanes[2].minimumRTTMicros != 10_000 {
			t.Fatal("another generation changed the minimum")
		}
		event.generation = 1
		f.scheduler.applyEvent(f.lanes, &preferred, event, time.Now())
		result := make(chan error, 1)
		f.scheduler.applyEvent(f.lanes, &preferred, schedulerEvent{
			kind: schedulerReport, laneID: 2, generation: 1, result: result,
			report: protocol.DeliveryReport{LaneID: 1, Generation: 1, PingID: 1, DelayMicros: 25_000},
		}, time.Now())
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		lane := f.lanes[1]
		if f.lanes[2].minimumRTTMicros != 600_000 || lane.feedbackDelayMicros != 325_000 ||
			lane.registration.Store.deliveryWindow != lane.deliveryWindowBytes() {
			t.Fatal("routed feedback did not use the refreshed return delay")
		}
	})
}

func TestScheduledLaneDeliveryWindowFollowsStoreCapacity(t *testing.T) {
	lane, now := newRTTWindowStoreLane(t, 32<<10, 10_000)
	for _, capacity := range []uint64{1_000_000, 10_000, 1_000_000} {
		// Each phase starts empty. Even the first old-credit flight on the slow path
		// serializes within the original five-second packet lifetime.
		constrained := runRTTWindowStorePhase(t, lane, now, capacity, 10_000)
		if constrained == 0 || lane.deliveryRate < capacity*9/10 || lane.deliveryRate > capacity {
			t.Fatalf("capacity %d: rate %d, constrained reports %d", capacity, lane.deliveryRate, constrained)
		}
		window := lane.deliveryWindowBytes()
		if capacity == 10_000 && window >= 32<<10 || capacity == 1_000_000 && window != 32<<10 {
			t.Fatalf("capacity %d: retained credit %d", capacity, window)
		}
	}
}

func TestScheduledLaneDeliveryWindowRespectsCapAfterObservedRestoration(t *testing.T) {
	for _, limit := range []int{64 << 10, 512 << 10, 1 << 20} {
		t.Run(fmt.Sprintf("Cap%d", limit), func(t *testing.T) {
			lane, now := newRTTWindowStoreLane(t, limit, 600_000)
			for _, propagation := range []uint64{600_000, 10_000} {
				// A lower RTT is explicitly observed while the store is empty. This does not
				// claim that a standing queue exposes restored physical propagation.
				sent := uint64(now.UnixMicro())
				*now = now.Add(time.Duration(propagation) * time.Microsecond)
				lane.applyTiming(clockmap.Sample{LocalSendMicros: sent, LocalReceiveMicros: uint64(now.UnixMicro())}, false)
				constrained := runRTTWindowStorePhase(t, lane, now, 1_000_000, propagation)
				// The cap may be smaller than one feedback flight. Include the final frame's
				// serialization and the explicit report delay in that achievable-rate bound.
				bound := min(uint64(1_000_000), uint64(limit)*1_000_000/(propagation+4096+1000))
				if constrained == 0 || lane.deliveryRate < bound*9/10 || lane.deliveryRate > 1_000_000 {
					t.Fatalf("propagation %d: rate %d, cap bound %d, constrained reports %d",
						propagation, lane.deliveryRate, bound, constrained)
				}
				if lane.minimumRTTMicros != propagation || lane.deliveryWindowBytes() > uint64(limit) {
					t.Fatal("observed restoration or retained cap was lost")
				}
			}
		})
	}
}

func newRTTWindowStoreLane(t *testing.T, limit int, propagation uint64) (*scheduledLane, *time.Time) {
	t.Helper()
	now := time.Unix(1, 0)
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 4096, Bytes: limit}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	lane := &scheduledLane{registration: schedulerRegistration(1, 1, store), deliveryRate: defaultInitialRateBytesPerSecond}
	lane.applyTiming(clockmap.Sample{LocalSendMicros: uint64(now.UnixMicro()) - propagation, LocalReceiveMicros: uint64(now.UnixMicro())}, false)
	return lane, &now
}

// runRTTWindowStorePhase feeds an ideal forward FIFO from real store credit and refills after each cumulative ACK.
// Reports arrive one millisecond after parsing plus symmetric return propagation. A one-millisecond report interval is
// supported by LaneConfig, and these 4096-byte frames serialize no faster than that interval in the tested paths.
// This isolates delivery sampling and committed flight credit. It does not model ordinary Ping, proof epochs, TCP, or drops.
func runRTTWindowStorePhase(t *testing.T, lane *scheduledLane, now *time.Time, capacity, propagation uint64) int {
	t.Helper()
	store := lane.registration.Store
	if packets, _ := store.backlog(); packets != 0 {
		t.Fatal("capacity phase started with retained packets")
	}
	end := now.Add(12 * time.Second)
	serialized := *now
	var acknowledgements []time.Time
	constrained := 0
	packetID := store.sentPackets
	for {
		store.setDeliveryWindows(lane.deliveryWindowBytes(), lane.probeWindowBytes(0))
		for now.Before(end) && lane.canAccept(4096) {
			packetID++
			transmission := schedulerTransmission(packetID, wgpacket.TransportData, now.Add(5*time.Second))
			size, err := protocol.DataFrameSize(transmission.data())
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, len(transmission.packet.Payload)+4096-size)
			copy(payload, transmission.packet.Payload)
			transmission.packet.Payload = payload
			// Increasing the frame-length varint can add one byte after the payload grows.
			size, err = protocol.DataFrameSize(transmission.data())
			if err != nil {
				t.Fatal(err)
			}
			transmission.packet.Payload = transmission.packet.Payload[:len(transmission.packet.Payload)+4096-size]
			if err := store.push(transmission); err != nil {
				t.Fatal(err)
			}
		}
		var data [16]protocol.Data
		var ownership [16]datagram.Packet
		for {
			count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes, 0)
			releaseBatchOwnership(ownership[:count])
			if errors.Is(err, packetqueue.ErrEmpty) {
				break
			}
			if err != nil || count == 0 {
				t.Fatalf("takeBatch() = %d, %v", count, err)
			}
			for range count {
				if serialized.Before(*now) {
					serialized = *now
				}
				serialized = serialized.Add(time.Duration((4096*1_000_000+capacity-1)/capacity) * time.Microsecond)
				acknowledgements = append(acknowledgements, serialized.Add(time.Duration(propagation+1000)*time.Microsecond))
			}
		}
		if len(acknowledgements) == 0 {
			break
		}
		*now = acknowledgements[0]
		acknowledgements = acknowledgements[1:]
		first, ok := store.sent.peek()
		if !ok || first.size != 4096 || uint64(now.UnixMicro()) >= first.deadlineMicros {
			t.Fatal("FIFO acknowledgement lost its exact frame or original deadline")
		}
		if !first.applicationLimited {
			constrained++
		}
		progressed, err := lane.applyReport(protocol.DeliveryReport{
			DataPackets: store.reportedPackets + 1, DelayMicros: 1000 + lane.minimumRTTMicros/2,
		}, uint64(now.UnixMicro()), *now)
		if err != nil || !progressed {
			t.Fatalf("applyReport() = %t, %v", progressed, err)
		}
	}
	return constrained
}
