package relay

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
)

func TestScheduledLaneTransportProbeByteBudget(t *testing.T) {
	for _, tt := range []struct {
		name          string
		primaryRate   uint64
		rtt           uint64
		feedbackDelay uint64
		storeBytes    int
		noPrimary     bool
		want          uint64
	}{
		{name: "NoPrimary", rtt: 300_000, storeBytes: 32 * 1024 * 1024, noPrimary: true, want: 2 * 1024 * 1024},
		{name: "SmallTarget", primaryRate: 1_000_000, rtt: 300_000, storeBytes: 32 * 1024 * 1024, want: 2 * 1024 * 1024},
		{name: "HighBDP", primaryRate: 12_500_000, rtt: 300_000, storeBytes: 32 * 1024 * 1024, want: 20_312_500},
		{name: "RoutedFeedback", primaryRate: 12_500_000, rtt: 300_000, feedbackDelay: 1_000_000, storeBytes: 128 * 1024 * 1024, want: 73_437_500},
		{name: "StoreBound", primaryRate: 12_500_000, rtt: 300_000, storeBytes: 4 * 1024 * 1024, want: 4 * 1024 * 1024},
		{name: "SmallStoreAllowance", primaryRate: 12_500_000, rtt: 300_000, storeBytes: 64 * 1024, want: 2 * 1024 * 1024},
		{name: "RateOverflow", primaryRate: math.MaxUint64, rtt: 300_000, storeBytes: 32 * 1024 * 1024, want: 32 * 1024 * 1024},
		{name: "HorizonOverflow", primaryRate: 1, rtt: math.MaxUint64, storeBytes: 32 * 1024 * 1024, want: 32 * 1024 * 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := schedulerLaneWithLimits(t, 2, 2, tt.rtt, 1_000_000,
				packetqueue.Limits{Packets: 8192, Bytes: tt.storeBytes})
			lane.feedbackDelayMicros = tt.feedbackDelay
			var primary *scheduledLane
			if !tt.noPrimary {
				primary = &scheduledLane{deliveryRate: tt.primaryRate}
			}
			if got := lane.transportProbeByteBudget(primary); got != tt.want {
				t.Fatalf("discovery budget = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSchedulerHighBDPTransportProbe(t *testing.T) {
	const incumbentRate = 12_500_000
	const promotionRate = incumbentRate + incumbentRate/4
	for _, tt := range []struct {
		name         string
		capacity     uint64
		storeBytes   int
		wantCapacity bool
	}{
		{name: "FastAlternative", capacity: 125_000_000, storeBytes: 32 * 1024 * 1024, wantCapacity: true},
		{name: "SlowAlternative", capacity: 1_250_000, storeBytes: 32 * 1024 * 1024},
		{name: "FixedTwoMiBBound", capacity: 125_000_000, storeBytes: 2 * 1024 * 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			started := now
			primary := schedulerLane(t, 1, 1, 10_000, incumbentRate)
			candidate := schedulerLaneWithLimits(t, 2, 2, 300_000, defaultInitialRateBytesPerSecond,
				packetqueue.Limits{Packets: 8192, Bytes: tt.storeBytes})
			candidate.rateObserved = false
			store := candidate.registration.Store
			budget, err := retention.NewBudget(retention.Limits{Packets: 8192, Bytes: tt.storeBytes})
			if err != nil {
				t.Fatal(err)
			}
			store.budget = budget
			store.now = func() time.Time { return now }
			store.setDeliveryWindows(candidate.deliveryWindowBytes(), candidate.probeWindowBytes(0))
			t.Cleanup(func() { releaseTransmissions(primary.registration.Store.drain()) })
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			lanes := map[protocol.LaneID]*scheduledLane{1: primary, 2: candidate}
			scheduler := Scheduler{lastTransportAt: now}
			var sentPackets uint64
			var peakRetained uint64
			var data [maximumDataBatchFrames]protocol.Data
			var ownership [maximumDataBatchFrames]datagram.Packet
			for now.Before(started.Add(maximumTransportProbeDuration)) {
				for scheduler.fillTransportProbe(lanes, 1, now, 0) {
				}
				_, retained := store.backlog()
				peakRetained = max(peakRetained, retained)
				var flightBytes uint64
				for {
					count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes, 0)
					if errors.Is(err, packetqueue.ErrEmpty) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					for _, frame := range data[:count] {
						if frame.PacketID != 0 || frame.DeadlineMicros != 0 {
							t.Fatal("discovery consumed useful packet identity or deadline")
						}
						flightBytes += uint64(dataFrameSize(frame))
					}
					releaseBatchOwnership(ownership[:count])
					sentPackets += uint64(count)
				}
				if flightBytes == 0 {
					break
				}
				// Serialize each flight before returning its cumulative report over a 300 ms feedback path.
				now = now.Add(300*time.Millisecond + time.Duration(flightBytes*1_000_000/tt.capacity)*time.Microsecond)
				if progressed, err := candidate.applyReport(protocol.DeliveryReport{DataPackets: sentPackets},
					uint64(now.UnixMicro()), now); err != nil || !progressed {
					t.Fatalf("capacity report = %t, %v", progressed, err)
				}
				if usage := budget.Usage(); usage != (retention.Usage{}) {
					t.Fatalf("capacity report retained aggregate ownership: %+v", usage)
				}
			}
			if got := candidate.deliveryRate > promotionRate; got != tt.wantCapacity {
				t.Fatalf("rate %d crosses promotion threshold %d: %t, want %t", candidate.deliveryRate,
					promotionRate, got, tt.wantCapacity)
			}
			if peakRetained > uint64(tt.storeBytes) || candidate.probeBytes > candidate.transportProbeByteBudget(primary) ||
				scheduler.packetID != 0 {
				t.Fatal("capacity discovery exceeded retention, bandwidth, or identity bounds")
			}
			if !tt.wantCapacity && candidate.deliveryRate > baseTransportProbeBytes*1_000_000/300_000 &&
				tt.storeBytes == baseTransportProbeBytes {
				t.Fatal("fixed budget exceeded its feedback-limited analytical sample ceiling")
			}
			t.Logf("serial-flight model: rate=%d B/s padding=%d B peak_retained=%d B elapsed=%s",
				candidate.deliveryRate, candidate.probeBytes, peakRetained, now.Sub(started))
		})
	}
}

func TestSchedulerHighBDPMultiLaneProbe(t *testing.T) {
	for _, alternatives := range []int{1, 3, 15} {
		t.Run(fmt.Sprintf("Alternatives%d", alternatives), func(t *testing.T) {
			now := time.Unix(100, 0)
			primary := schedulerLane(t, 1, 1, 10_000, 12_500_000)
			lanes := map[protocol.LaneID]*scheduledLane{1: primary}
			var candidates []*scheduledLane
			for index := range alternatives {
				candidate := schedulerLaneWithLimits(t, byte(index+2), byte(index+2), 300_000, defaultInitialRateBytesPerSecond,
					packetqueue.Limits{Packets: 8192, Bytes: 32 * 1024 * 1024})
				candidate.rateObserved = false
				candidate.registration.Store.now = func() time.Time { return now }
				candidate.registration.Store.setDeliveryWindows(candidate.deliveryWindowBytes(), candidate.probeWindowBytes(0))
				lanes[candidate.registration.LaneID] = candidate
				candidates = append(candidates, candidate)
				t.Cleanup(func() { releaseTransmissions(candidate.registration.Store.drain()) })
			}
			scheduler := Scheduler{lastTransportAt: now}
			var data [maximumDataBatchFrames]protocol.Data
			var ownership [maximumDataBatchFrames]datagram.Packet
			for {
				for scheduler.fillTransportProbe(lanes, 1, now, 0) {
				}
				var longestFlight uint64
				flights := make([]uint64, len(candidates))
				for index, candidate := range candidates {
					store := candidate.registration.Store
					for {
						count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes, 0)
						if errors.Is(err, packetqueue.ErrEmpty) {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						for _, frame := range data[:count] {
							flights[index] += uint64(dataFrameSize(frame))
						}
						releaseBatchOwnership(ownership[:count])
					}
					longestFlight = max(longestFlight, flights[index])
				}
				if longestFlight == 0 {
					break
				}
				// All alternatives have independent 1 Gbit/s capacity and the same feedback delay.
				now = now.Add(300*time.Millisecond + time.Duration(longestFlight*1_000_000/125_000_000)*time.Microsecond)
				for index, candidate := range candidates {
					if flights[index] == 0 {
						continue
					}
					if progressed, err := candidate.applyReport(protocol.DeliveryReport{DataPackets: candidate.registration.Store.sentPackets},
						uint64(now.UnixMicro()), now); err != nil || !progressed {
						t.Fatalf("capacity report = %t, %v", progressed, err)
					}
				}
			}
			qualified := 0
			var padding uint64
			var rates []uint64
			var boosted *scheduledLane
			for _, candidate := range candidates {
				if candidate.deliveryRate > 15_625_000 {
					qualified++
				}
				padding += candidate.probeBytes
				rates = append(rates, candidate.deliveryRate)
				if candidate.probeByteLimit > baseTransportProbeBytes {
					if boosted != nil {
						t.Fatal("multiple active periods received larger discovery budgets")
					}
					boosted = candidate
				}
			}
			t.Logf("alternatives=%d qualified=%d padding=%d rates=%v", alternatives, qualified, padding, rates)
			if qualified == 0 {
				t.Fatal("sharing discovery bandwidth prevented every fast alternative from demonstrating promotion capacity")
			}
			if boosted == nil || padding > boosted.probeByteLimit+uint64(alternatives-1)*baseTransportProbeBytes {
				t.Fatal("simultaneous discovery periods exceeded their aggregate bandwidth funding")
			}
			now = candidates[0].nextProbeAt
			scheduler.lastTransportAt = now
			if !scheduler.fillTransportProbe(lanes, 1, now, 0) {
				t.Fatal("active traffic did not start the next discovery period")
			}
			next := lanes[scheduler.lastProbeLane]
			if next.probeByteLimit <= baseTransportProbeBytes || alternatives > 1 && next == boosted {
				t.Fatal("round-robin discovery did not rotate larger capacity samples across cooldowns")
			}
		})
	}
}

func TestSchedulerTransportProbeBudgetImmutable(t *testing.T) {
	for _, tt := range []struct {
		name string
		rate uint64
	}{
		{name: "RisingIncumbent", rate: 125_000_000},
		{name: "FallingIncumbent", rate: 1_000_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			primary := schedulerLane(t, 1, 1, 10_000, 12_500_000)
			candidate := schedulerLaneWithLimits(t, 2, 2, 300_000, 1_000_000,
				packetqueue.Limits{Packets: 8192, Bytes: 32 * 1024 * 1024})
			store := candidate.registration.Store
			store.now = func() time.Time { return now }
			t.Cleanup(func() { releaseTransmissions(primary.registration.Store.drain()) })
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			lanes := map[protocol.LaneID]*scheduledLane{1: primary, 2: candidate}
			scheduler := Scheduler{lastTransportAt: now}
			if !scheduler.fillTransportProbe(lanes, 1, now, 0) {
				t.Fatal("larger discovery period did not start")
			}
			limit, until, next := candidate.probeByteLimit, candidate.probeUntil, candidate.nextProbeAt
			packets, _ := store.backlog()
			for range packets {
				takeOneTransmission(t, store)
			}
			if _, stale, err := store.acknowledge(uint64(packets), uint64(now.UnixMicro())+300_000); err != nil || stale {
				t.Fatalf("initial discovery feedback = stale %t, %v", stale, err)
			}
			candidate.probeBytes = limit - uint64(dataFrameSize(protocol.Data{Payload: probePadding[:]}))
			primary.deliveryRate = tt.rate
			if !scheduler.fillTransportProbe(lanes, 1, now, 0) || candidate.probeBytes != limit {
				t.Fatal("incumbent rate change altered the captured discovery allowance")
			}
			if scheduler.fillTransportProbe(lanes, 1, now, 0) || candidate.probeByteLimit != limit ||
				candidate.probeUntil != until || candidate.nextProbeAt != next {
				t.Fatal("incumbent rate change extended discovery funding, duration, or cooldown")
			}
		})
	}
}
