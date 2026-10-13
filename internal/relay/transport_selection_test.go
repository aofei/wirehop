package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerSelectTransportCandidatesDeliveryImprovement(t *testing.T) {
	for _, tt := range []struct {
		name                           string
		primaryRTT, candidateRTT       uint64
		primaryRate, candidateRate     uint64
		primaryQueued, candidateQueued int
		primaryFull, candidateFull     bool
		degraded, holding              bool
		maximumScore                   uint64
		frameBytes                     uint64
		recent                         bool
		initial                        bool
		gap                            time.Duration
		want                           protocol.LaneID
	}{
		{name: "SparseHighDelayAlternative", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, want: 1},
		{name: "InitialLowDelayPrimary", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, initial: true, want: 1},
		{name: "CadencedHighDelayAlternative", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, recent: true, gap: 10 * time.Millisecond, want: 1},
		{name: "ContinuousOffering", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, recent: true, gap: 100 * time.Microsecond, want: 2},
		{name: "ExactSerializationBoundary", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, recent: true, gap: 800 * time.Microsecond, want: 2},
		{name: "AboveSerializationBoundary", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, recent: true, gap: 800*time.Microsecond + time.Nanosecond, want: 1},
		{name: "SmallPacketCadence", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, recent: true, gap: 100 * time.Microsecond, frameBytes: 44, want: 1},
		{name: "LargePacketSameCadence", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, recent: true, gap: 100 * time.Microsecond, frameBytes: 1500, want: 2},
		{name: "BulkHighDelayAlternative", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, primaryQueued: 16, want: 2},
		{name: "CandidateBacklogPreventsImprovement", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, primaryQueued: 16, candidateQueued: 64, want: 1},
		{name: "CapacityThresholdIsStrict", primaryRTT: 10_000, candidateRTT: 10_000,
			primaryRate: 1_000_000, candidateRate: 1_250_000, primaryQueued: 16, want: 1},
		{name: "SameDelayCapacityImprovement", primaryRTT: 10_000, candidateRTT: 10_000,
			primaryRate: 1_000_000, candidateRate: 2_000_000, want: 2},
		{name: "EqualDeliveryScore", primaryRTT: 20_000, candidateRTT: 21_000,
			primaryRate: 1_000_000, candidateRate: 2_000_000, want: 1},
		{name: "FullPrimaryCanPromote", primaryRTT: 300_000, candidateRTT: 10_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, primaryQueued: 1, primaryFull: true, want: 2},
		{name: "FullCandidateRetainsUsablePrimary", primaryRTT: 300_000, candidateRTT: 10_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, candidateQueued: 1, candidateFull: true, want: 1},
		{name: "HoldPreservesBulkPrimary", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, primaryQueued: 16, holding: true, want: 1},
		{name: "FailureBypassesScoreAndHold", primaryRTT: 10_000, candidateRTT: 300_000,
			primaryRate: 1_250_000, candidateRate: 12_500_000, degraded: true, holding: true, want: 2},
		{name: "ExactDeadlineRejectsPromotion", primaryRTT: 300_000, candidateRTT: 10_000,
			primaryRate: 1_000_000, candidateRate: 2_000_000, maximumScore: 5500, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			limits := packetqueue.Limits{Packets: 128, Bytes: 2 * 1024 * 1024}
			primary := schedulerLaneWithLimits(t, 1, 1, tt.primaryRTT, tt.primaryRate, limits)
			candidate := schedulerLaneWithLimits(t, 2, 2, tt.candidateRTT, tt.candidateRate, limits)
			if tt.primaryFull {
				primary.registration.Store.limits.Packets = tt.primaryQueued
			}
			if tt.candidateFull {
				candidate.registration.Store.limits.Packets = tt.candidateQueued
			}
			for _, lane := range []*scheduledLane{primary, candidate} {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
				queued := tt.primaryQueued
				if lane == candidate {
					queued = tt.candidateQueued
				}
				for index := range queued {
					transmission := schedulerTransmission(uint64(index+1), wgpacket.TransportData, now.Add(time.Second))
					transmission.packet.Payload = make([]byte, 16*1024)
					copy(transmission.packet.Payload, relayWireGuardPacket(wgpacket.TransportData))
					if err := lane.registration.Store.pushAt(transmission, now); err != nil {
						t.Fatal(err)
					}
				}
			}
			primary.degraded = tt.degraded
			var scheduler Scheduler
			if tt.recent {
				scheduler.lastTransportAt = now.Add(-tt.gap)
			}
			if tt.holding {
				scheduler.transportHoldUntil = now.Add(time.Second)
			}
			maximumScore := tt.maximumScore
			if maximumScore == 0 {
				maximumScore = math.MaxUint64
			}
			frameBytes := tt.frameBytes
			if frameBytes == 0 {
				frameBytes = 1000
			}
			preferred := protocol.LaneID(1)
			if tt.initial {
				preferred = 0
			}
			got := scheduler.selectTransportCandidates(map[protocol.LaneID]*scheduledLane{1: primary, 2: candidate},
				preferred, frameBytes, maximumScore, now)
			if got.count != 1 || got.lanes[0].registration.LaneID != tt.want {
				t.Fatalf("selection = %+v, want lane %d", got, tt.want)
			}
		})
	}
}

func TestSchedulerScheduleSparseAndBulkTransport(t *testing.T) {
	for _, tt := range []struct {
		name    string
		packets int
		cadence time.Duration
		sizes   []int
		want    protocol.LaneID
	}{
		{name: "Isolated", packets: 1, cadence: 20 * time.Millisecond, want: 1},
		{name: "Cadenced", packets: 200, cadence: 20 * time.Millisecond, want: 1},
		{name: "ContinuousBulk", packets: 200, want: 2},
		{name: "TwoPacketBurst", packets: 2, want: 2},
		{name: "SmallThenLarge", packets: 2, cadence: 100 * time.Microsecond, sizes: []int{64, 1400}, want: 2},
		{name: "LargeThenSmall", packets: 2, cadence: 100 * time.Microsecond, sizes: []int{1400, 64}, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			limits := packetqueue.Limits{Packets: 256, Bytes: 2 * 1024 * 1024}
			first := schedulerLaneWithLimits(t, 1, 1, 10_000, 1_250_000, limits)
			second := schedulerLaneWithLimits(t, 2, 2, 300_000, 12_500_000, limits)
			lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
			for _, lane := range lanes {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			var scheduler Scheduler
			preferred := protocol.LaneID(1)
			for index := range tt.packets {
				size := 1400
				if len(tt.sizes) != 0 {
					size = tt.sizes[index%len(tt.sizes)]
				}
				payload := make([]byte, size)
				copy(payload, relayWireGuardPacket(wgpacket.TransportData))
				item := packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{
					Kind: wgpacket.TransportData, Payload: payload,
				}), Deadline: now.Add(5 * time.Second), Size: len(payload)}
				if ok, err := scheduler.schedule(lanes, &preferred, &item, now); err != nil || !ok {
					t.Fatalf("schedule = %t, %v", ok, err)
				}
				store := lanes[preferred].registration.Store
				data := takeOneTransmission(t, store)
				if data.PacketID != scheduler.packetID {
					t.Fatal("transport delivery changed packet identity")
				}
				now = now.Add(tt.cadence)
				if _, _, err := store.acknowledge(store.sentPackets, uint64(now.UnixMicro())); err != nil {
					t.Fatal(err)
				}
			}
			if preferred != tt.want {
				t.Fatalf("preferred = %d, want %d", preferred, tt.want)
			}
			firstPackets, _ := first.registration.Store.backlog()
			secondPackets, _ := second.registration.Store.backlog()
			if firstPackets+secondPackets != 0 {
				t.Fatalf("unexpected retained assignment: %d primary, %d alternative", firstPackets, secondPackets)
			}
		})
	}
}
