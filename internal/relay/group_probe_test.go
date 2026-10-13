package relay

import (
	"slices"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
)

func TestSchedulerFillTransportProbeGroups(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tt := range []struct {
		name          string
		preferred     protocol.LaneID
		configure     func(first, second, third *scheduledLane)
		blockedSecond bool
		want          []protocol.LaneID
	}{
		{name: "LowestRTT", want: []protocol.LaneID{2, 3}},
		{name: "PrimaryPreferred", preferred: 1, want: []protocol.LaneID{3}},
		{
			name: "StableLaneID", want: []protocol.LaneID{1, 3},
			configure: func(first, _, _ *scheduledLane) { first.rttMicros = 10_000 },
		},
		{
			name: "DegradedSibling", want: []protocol.LaneID{1, 3},
			configure: func(_, second, _ *scheduledLane) { second.degraded = true },
		},
		{
			name: "AbandoningSibling", want: []protocol.LaneID{1, 3},
			configure: func(_, second, _ *scheduledLane) { second.abandoning = true },
		},
		{
			name: "DegradedPrimary", preferred: 1, want: []protocol.LaneID{2, 3},
			configure: func(first, _, _ *scheduledLane) { first.degraded = true },
		},
		{
			name: "AbandoningPrimary", preferred: 1, want: []protocol.LaneID{2, 3},
			configure: func(first, _, _ *scheduledLane) { first.abandoning = true },
		},
		{
			name:      "SingleHealthyGroup",
			configure: func(_, _, third *scheduledLane) { third.degraded = true },
		},
		{
			name: "ActivePeriodBeatsChangedRTT", want: []protocol.LaneID{1, 3},
			configure: func(first, _, _ *scheduledLane) { first.probeUntil = now.Add(time.Second) },
		},
		{
			name: "ActivePrimary", preferred: 1, want: []protocol.LaneID{1, 3},
			configure: func(first, _, _ *scheduledLane) { first.probeUntil = now.Add(time.Second) },
		},
		{
			name: "ActivePrimarySibling", preferred: 1, want: []protocol.LaneID{2, 3},
			configure: func(_, second, _ *scheduledLane) { second.probeUntil = now.Add(time.Second) },
		},
		{name: "BlockedRepresentative", blockedSecond: true, want: []protocol.LaneID{3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			first := schedulerLaneWithLimits(t, 1, 1, 20_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
			second := schedulerLaneWithLimits(t, 2, 1, 10_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
			third := schedulerLaneWithLimits(t, 3, 2, 30_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
			lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second, 3: third}
			for _, lane := range lanes {
				lane.rateObserved = false
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			if tt.configure != nil {
				tt.configure(first, second, third)
			}
			if tt.blockedSecond {
				if err := second.registration.Store.pushAt(retainedTransmission{
					deadlineMicros: uint64(now.Add(time.Hour).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]},
				}, now); err != nil {
					t.Fatal(err)
				}
			}
			before := make(map[protocol.LaneID]int, len(lanes))
			deadlines := make(map[protocol.LaneID]time.Time, len(lanes))
			for id, lane := range lanes {
				before[id], _ = lane.registration.Store.backlog()
				deadlines[id] = lane.probeUntil
			}
			var scheduler Scheduler
			for range len(lanes) + 1 {
				if !scheduler.fillTransportProbe(lanes, tt.preferred, now, 0) {
					break
				}
			}
			var probed []protocol.LaneID
			for id, lane := range lanes {
				packets, _ := lane.registration.Store.backlog()
				if packets > before[id] {
					probed = append(probed, id)
				}
				if now.Before(deadlines[id]) && (!lane.probeUntil.Equal(deadlines[id]) || lane.probeBytes == 0) {
					t.Fatal("active discovery period changed its deadline or did not continue")
				}
			}
			slices.Sort(probed)
			if !slices.Equal(probed, tt.want) {
				t.Fatalf("probed lanes = %v, want %v", probed, tt.want)
			}
			if len(scheduler.probeGroups) != 0 {
				t.Fatal("group scratch storage retained lane references")
			}
		})
	}
}

func TestSchedulerFillTransportProbeReelectsAfterActivePeriod(t *testing.T) {
	now := time.Unix(100, 0)
	first := schedulerLaneWithLimits(t, 1, 1, 20_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
	second := schedulerLaneWithLimits(t, 2, 1, 10_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
	third := schedulerLaneWithLimits(t, 3, 2, 30_000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 128 * 1024})
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second, 3: third}
	for _, lane := range lanes {
		lane.rateObserved = false
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	first.probeUntil = now.Add(time.Second)
	var scheduler Scheduler
	if !scheduler.fillTransportProbe(lanes, 0, now, 0) {
		t.Fatal("active representative did not continue")
	}
	now = first.probeUntil
	if !scheduler.fillTransportProbe(lanes, 0, now, 0) {
		t.Fatal("expired representative prevented a fresh election")
	}
	if packets, _ := second.registration.Store.backlog(); packets != 1 {
		t.Fatalf("new RTT winner retained %d probes, want 1", packets)
	}
}
