package relay

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerRecoversStallBeforeSalvageWindowCloses(t *testing.T) {
	for _, expiredPrefix := range []bool{false, true} {
		name := "FreshPrefix"
		if expiredPrefix {
			name = "ExpiredPrefix"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			source := schedulerLane(t, 1, 1, 10_000, 12_500_000)
			source.rateObserved = true
			source.registration.Store.now = func() time.Time { return now }
			source.lastProgressAt = now
			abandonments := 0
			source.registration.Abandon = func() { abandonments++ }
			deadline := now.Add(5 * time.Second)
			if expiredPrefix {
				deadline = now.Add(100 * time.Millisecond)
			}
			if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, deadline)); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			if expiredPrefix {
				if err := source.registration.Store.push(schedulerTransmission(2, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
					t.Fatal(err)
				}
			}
			alternate := schedulerLane(t, 2, 2, 300_000, 2_500_000)
			t.Cleanup(func() {
				releaseTransmissions(source.registration.Store.drain())
				releaseTransmissions(alternate.registration.Store.drain())
			})
			lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: alternate}
			var scheduler Scheduler
			scheduler.checkAbandonment(lanes, now.Add(249*time.Millisecond))
			if abandonments != 0 {
				t.Fatal("generation abandoned before the progress guard elapsed")
			}
			scheduler.checkAbandonment(lanes, now.Add(250*time.Millisecond))
			scheduler.checkAbandonment(lanes, now.Add(500*time.Millisecond))
			if abandonments != 1 || !source.degraded || !source.abandoning {
				t.Fatalf("stall recovery: %d abandonments, degraded %t, abandoning %t", abandonments, source.degraded, source.abandoning)
			}
		})
	}
}

func TestSchedulerRecoveryRequiresMigratableTransport(t *testing.T) {
	for _, tt := range []struct {
		name     string
		kind     wgpacket.Kind
		migrated bool
	}{
		{name: "HandshakeInitiation", kind: wgpacket.HandshakeInitiation},
		{name: "HandshakeResponse", kind: wgpacket.HandshakeResponse},
		{name: "CookieReply", kind: wgpacket.CookieReply},
		{name: "AlreadyMigrated", kind: wgpacket.TransportData, migrated: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			source := schedulerLane(t, 1, 1, 10_000, 12_500_000)
			alternate := schedulerLane(t, 2, 2, 10_000, 12_500_000)
			source.lastProgressAt = now
			source.registration.Store.now = func() time.Time { return now }
			source.registration.Abandon = func() { t.Fatal("unmigratable work abandoned a generation") }
			for _, lane := range []*scheduledLane{source, alternate} {
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			transmission := schedulerTransmission(1, tt.kind, now.Add(time.Second))
			transmission.migrated = tt.migrated
			if err := source.registration.Store.push(transmission); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			var scheduler Scheduler
			lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: alternate}
			scheduler.checkAbandonment(lanes, now.Add(500*time.Millisecond))
			if source.abandoning || source.degraded {
				t.Fatal("fresh unmigratable work justified predictive recovery")
			}
			scheduler.checkAbandonment(lanes, now.Add(2*time.Second))
			if source.abandoning || !source.degraded {
				t.Fatal("expired unmigratable work did not preserve degradation without abandonment")
			}
		})
	}
}

func TestSchedulerProbeCapacityDivertsBeforeProvenRecovery(t *testing.T) {
	now := time.Unix(100, 0)
	source := schedulerLane(t, 1, 1, 10_000, 12_500_000)
	alternate := schedulerLane(t, 2, 2, 10_000, 12_500_000)
	alternate.registration.Store.transportReported.Store(0)
	abandonments := 0
	source.registration.Abandon = func() { abandonments++ }
	source.lastProgressAt = now
	for _, lane := range []*scheduledLane{source, alternate} {
		lane.registration.Store.now = func() time.Time { return now }
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source.registration.Store)
	lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: alternate}
	var scheduler Scheduler
	now = now.Add(250 * time.Millisecond)
	scheduler.checkAbandonment(lanes, now)
	if !source.degraded || source.abandoning || abandonments != 0 {
		t.Fatal("padding-only capacity did not divert new traffic without abandoning the source")
	}
	transmission := schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second))
	transmission.packet.Payload = make([]byte, minimumRateSampleBytes)
	transmission.packet.Payload[0] = 4
	if err := alternate.registration.Store.push(transmission); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, alternate.registration.Store)
	now = now.Add(10 * time.Millisecond)
	if _, err := alternate.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(now.UnixMicro()), now); err != nil {
		t.Fatal(err)
	}
	scheduler.checkAbandonment(lanes, now)
	if !source.abandoning || abandonments != 1 {
		t.Fatal("validated real transport progress did not permit recovery")
	}
}

func TestTransmissionStoreRecoveryProofExcludesPaddingAndControls(t *testing.T) {
	now := time.Unix(100, 0)
	store := schedulerStore(t, packetqueue.Limits{Packets: 8, Bytes: 64 * 1024})
	store.now = func() time.Time { return now }
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	for index, tt := range []struct {
		kind wgpacket.Kind
		size int
		want uint64
	}{
		{kind: wgpacket.NonWireGuard, size: protocol.ProbePayloadSize},
		{kind: wgpacket.HandshakeResponse, size: 92},
		{kind: wgpacket.TransportData, size: 2048, want: 2048},
		{kind: wgpacket.TransportData, size: 2048, want: minimumRateSampleBytes},
		{kind: wgpacket.TransportData, size: 32, want: minimumRateSampleBytes},
	} {
		transmission := retainedTransmission{deadlineMicros: uint64(now.Add(time.Second).UnixMicro()), packet: datagram.Packet{Payload: probePadding[:]}}
		if tt.kind != wgpacket.NonWireGuard {
			transmission = schedulerTransmission(uint64(index), tt.kind, now.Add(time.Second))
			transmission.packet.Payload = make([]byte, tt.size)
			copy(transmission.packet.Payload, relayWireGuardPacket(tt.kind))
		}
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, store)
		now = now.Add(time.Millisecond)
		if _, _, err := store.acknowledge(uint64(index+1), uint64(now.UnixMicro())); err != nil {
			t.Fatal(err)
		}
		if got := store.transportReported.Load(); got != tt.want {
			t.Fatalf("recovery proof after kind %d = %d bytes, want %d", tt.kind, got, tt.want)
		}
	}
}

func TestProgressGuardIncludesSerializationAndFeedback(t *testing.T) {
	for _, tt := range []struct {
		name     string
		rtt      uint64
		rate     uint64
		feedback uint64
		bytes    uint64
		want     uint64
	}{
		{name: "Fast", rtt: 10_000, rate: 12_500_000, bytes: 1452, want: 250_000},
		{name: "Slow", rtt: 200_000, rate: 4_000, bytes: 1500, want: 4_521_000},
		{name: "AlternateFeedback", rtt: 10_000, rate: 1_000_000, feedback: 300_000, bytes: 1500, want: 661_500},
		{name: "UnknownRate", want: math.MaxUint64},
		{name: "OverflowRTT", rtt: math.MaxUint64, rate: 1, want: math.MaxUint64},
		{name: "OverflowBytes", rate: 1, bytes: math.MaxUint64, want: math.MaxUint64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := &scheduledLane{rttMicros: tt.rtt, deliveryRate: tt.rate, feedbackDelayMicros: tt.feedback}
			if got := progressGuardMicros(lane, tt.bytes); got != tt.want {
				t.Fatalf("progress guard = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSchedulerRecoveryRequiresObservedCapacity(t *testing.T) {
	for _, tt := range []struct {
		name              string
		sourceObserved    bool
		alternateObserved bool
		expiredPrefix     bool
		wantAbandon       bool
	}{
		{name: "BothInitialEstimates"},
		{name: "InitialEstimatesWithExpiredPrefix", expiredPrefix: true},
		{name: "ObservedSource", sourceObserved: true},
		{name: "ObservedAlternative", alternateObserved: true},
		{name: "BothObserved", sourceObserved: true, alternateObserved: true, wantAbandon: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			source := schedulerLane(t, 1, 1, 10_000, defaultInitialRateBytesPerSecond)
			source.registration.Store.now = func() time.Time { return now }
			source.lastProgressAt = now
			source.rateObserved = tt.sourceObserved
			deadline := now.Add(5 * time.Second)
			if tt.expiredPrefix {
				deadline = now.Add(100 * time.Millisecond)
			}
			if err := source.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, deadline)); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			if tt.expiredPrefix {
				if err := source.registration.Store.push(schedulerTransmission(2, wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
					t.Fatal(err)
				}
			}
			alternate := schedulerLane(t, 2, 2, 10_000, defaultInitialRateBytesPerSecond)
			alternate.rateObserved = tt.alternateObserved
			t.Cleanup(func() {
				releaseTransmissions(source.registration.Store.drain())
				releaseTransmissions(alternate.registration.Store.drain())
			})
			var scheduler Scheduler
			lanes := map[protocol.LaneID]*scheduledLane{1: source, 2: alternate}
			scheduler.checkAbandonment(lanes, now.Add(249*time.Millisecond))
			if source.abandoning {
				t.Fatal("generation abandoned before the progress guard elapsed")
			}
			scheduler.checkAbandonment(lanes, now.Add(250*time.Millisecond))
			scheduler.checkAbandonment(lanes, now.Add(500*time.Millisecond))
			if source.abandoning != tt.wantAbandon {
				t.Fatalf("abandoning = %t, want %t", source.abandoning, tt.wantAbandon)
			}
			if tt.expiredPrefix && !source.degraded {
				t.Fatal("expired sent prefix did not mark the lane as degraded")
			}
		})
	}
}

func TestSchedulerDoesNotRecoverThroughAnotherStalledLane(t *testing.T) {
	now := time.Unix(100, 0)
	lanes := make(map[protocol.LaneID]*scheduledLane)
	for id := byte(1); id <= 2; id++ {
		lane := schedulerLane(t, id, id, 10_000, 12_500_000)
		lane.rateObserved = true
		lane.lastProgressAt = now
		lane.registration.Store.now = func() time.Time { return now }
		lane.registration.Abandon = func() { t.Fatal("a stalled alternate caused generation churn") }
		if err := lane.registration.Store.push(schedulerTransmission(uint64(id), wgpacket.TransportData, now.Add(5*time.Second))); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, lane.registration.Store)
		lanes[lane.registration.LaneID] = lane
		t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
	}
	var scheduler Scheduler
	for range 10 {
		scheduler.checkAbandonment(lanes, now.Add(time.Second))
	}
}

func TestSchedulerRecoversThroughHealthyLastLaneInGroup(t *testing.T) {
	for _, tt := range []struct {
		name              string
		count             byte
		reverse           bool
		beforeMaintenance bool
		replace           bool
	}{
		{name: "ThreeLanes", count: 3},
		{name: "ThreeLanesReverseRemoval", count: 3, reverse: true},
		{name: "FourLanes", count: 4},
		{name: "FourLanesReverseRemoval", count: 4, reverse: true},
		{name: "ThreeLanesBeforeMaintenance", count: 3, beforeMaintenance: true},
		{name: "ThreeLanesReverseRemovalBeforeMaintenance", count: 3, reverse: true, beforeMaintenance: true},
		{name: "FourLanesBeforeMaintenance", count: 4, beforeMaintenance: true},
		{name: "FourLanesReverseRemovalBeforeMaintenance", count: 4, reverse: true, beforeMaintenance: true},
		{name: "ThreeLanesReplacementBeforeMaintenance", count: 3, beforeMaintenance: true, replace: true},
		{name: "ThreeLanesReverseReplacementBeforeMaintenance", count: 3, reverse: true, beforeMaintenance: true, replace: true},
		{name: "FourLanesReplacementBeforeMaintenance", count: 4, beforeMaintenance: true, replace: true},
		{name: "FourLanesReverseReplacementBeforeMaintenance", count: 4, reverse: true, beforeMaintenance: true, replace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 1, Bytes: 4096}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer ingress.Close()
			scheduler, err := NewScheduler(ingress)
			if err != nil {
				t.Fatal(err)
			}
			lanes := make(map[protocol.LaneID]*scheduledLane)
			abandoned := make(map[byte]bool)
			want := make(map[uint64]protocol.Data)
			for id := byte(1); id <= tt.count; id++ {
				lane := schedulerLane(t, id, 1, uint64(id)*10_000, 12_500_000)
				lane.rateObserved = true
				lane.lastProgressAt = now
				lane.registration.Store.now = func() time.Time { return now }
				lane.registration.Abandon = func() { abandoned[id] = true }
				if id != tt.count {
					for index := uint64(0); index < 2; index++ {
						packetID := uint64(id)*2 - index
						transmission := schedulerTransmission(packetID, wgpacket.TransportData, now.Add(5*time.Second))
						transmission.packet.Payload[len(transmission.packet.Payload)-1] = byte(packetID)
						data := transmission.data()
						data.Payload = bytes.Clone(data.Payload)
						want[packetID] = data
						if err := lane.registration.Store.push(transmission); err != nil {
							t.Fatal(err)
						}
						if index == 0 {
							takeOneTransmission(t, lane.registration.Store)
						}
					}
				}
				lanes[lane.registration.LaneID] = lane
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			now = now.Add(250 * time.Millisecond)
			if !tt.beforeMaintenance {
				scheduler.checkAbandonment(lanes, now)
			}
			for id := byte(1); id <= tt.count; id++ {
				wantAbandon := !tt.beforeMaintenance && id != tt.count
				if abandoned[id] != wantAbandon {
					t.Fatalf("lane %d abandonment = %t, want %t", id, abandoned[id], wantAbandon)
				}
			}
			var preferred protocol.LaneID
			for index := byte(1); index < tt.count; index++ {
				id := index
				if tt.reverse {
					id = tt.count - index
				}
				event := schedulerEvent{kind: schedulerRemove, laneID: protocol.LaneID(id), generation: 1, result: make(chan error, 1)}
				if tt.replace {
					replacement := schedulerLane(t, id, 1, defaultInitialRTTMicros, defaultInitialRateBytesPerSecond)
					replacement.registration.Generation = 2
					replacement.registration.Store.now = func() time.Time { return now }
					t.Cleanup(func() { releaseTransmissions(replacement.registration.Store.drain()) })
					event.kind = schedulerRegister
					event.registration = replacement.registration
				}
				scheduler.applyEvent(lanes, &preferred, event, now)
				if err := <-event.result; err != nil {
					t.Fatal(err)
				}
			}
			store := lanes[protocol.LaneID(tt.count)].registration.Store
			var data [6]protocol.Data
			var ownership [6]datagram.Packet
			count, err := store.takeBatch(data[:], ownership[:], protocol.MaxEncodedFrameSize)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseBatchOwnership(ownership[:count])
			if count != len(want) {
				t.Fatalf("healthy lane received %d packets, want %d", count, len(want))
			}
			for _, packet := range data[:count] {
				expected, ok := want[packet.PacketID]
				if !ok || packet.DeadlineMicros != expected.DeadlineMicros || !bytes.Equal(packet.Payload, expected.Payload) {
					t.Fatalf("unexpected migrated packet: %+v", packet)
				}
				delete(want, packet.PacketID)
			}
			if _, _, err := store.acknowledge(uint64(count), uint64(now.UnixMicro())); err != nil {
				t.Fatal(err)
			}
			if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
				t.Fatalf("healthy lane backlog after acknowledgement = %d packets, %d bytes", packets, bytes)
			}
		})
	}
}

func TestSchedulerRecoveryUsesObservedStallWithoutPrematurelyClosingSlowLinks(t *testing.T) {
	for _, tt := range []struct {
		name         string
		sourceRate   uint64
		alternateRTT uint64
		checkAt      time.Duration
		wantAbandon  bool
	}{
		{name: "SlowSerializationPending", sourceRate: 4_000, alternateRTT: 10_000, checkAt: 400 * time.Millisecond},
		{name: "SlowFlightPending", sourceRate: 4_000, alternateRTT: 10_000, checkAt: 4500 * time.Millisecond},
		{name: "SlowSerializationElapsed", sourceRate: 4_000, alternateRTT: 10_000, checkAt: 4600 * time.Millisecond, wantAbandon: true},
		{name: "SlowAlternativePending", sourceRate: 12_500_000, alternateRTT: 1_000_000, checkAt: 250 * time.Millisecond},
		{name: "SlowAlternativeElapsed", sourceRate: 12_500_000, alternateRTT: 1_000_000, checkAt: 550 * time.Millisecond, wantAbandon: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			source := schedulerLane(t, 1, 1, 10_000, tt.sourceRate)
			source.rateObserved = true
			source.lastProgressAt = now
			source.registration.Store.now = func() time.Time { return now }
			transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(5*time.Second))
			transmission.packet.Payload = make([]byte, 1452)
			transmission.packet.Payload[0] = 4
			if err := source.registration.Store.push(transmission); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			alternate := schedulerLane(t, 2, 2, tt.alternateRTT, 2_500_000)
			t.Cleanup(func() {
				releaseTransmissions(source.registration.Store.drain())
				releaseTransmissions(alternate.registration.Store.drain())
			})
			var scheduler Scheduler
			scheduler.checkAbandonment(map[protocol.LaneID]*scheduledLane{1: source, 2: alternate}, now.Add(tt.checkAt))
			if source.abandoning != tt.wantAbandon {
				t.Fatalf("abandoning = %t, want %t", source.abandoning, tt.wantAbandon)
			}
		})
	}
}

func TestSchedulerMigrateTransmissionsChecksCurrentProgress(t *testing.T) {
	for _, tt := range []struct {
		name          string
		rate          uint64
		checkAt       time.Duration
		recentReport  bool
		expiredQueued bool
		wantMigration bool
	}{
		{name: "BeforeProgressGuard", rate: 12_500_000, checkAt: 249 * time.Millisecond, wantMigration: true},
		{name: "AtProgressGuard", rate: 12_500_000, checkAt: 250 * time.Millisecond},
		{name: "RecentPartialReport", rate: 12_500_000, checkAt: 500 * time.Millisecond, recentReport: true, wantMigration: true},
		{name: "SlowSerializationPending", rate: 4_000, checkAt: 400 * time.Millisecond, wantMigration: true},
		{name: "SlowFlightPending", rate: 4_000, checkAt: 4500 * time.Millisecond, wantMigration: true},
		{name: "SlowSerializationElapsed", rate: 4_000, checkAt: 4600 * time.Millisecond},
		{name: "ExpiredQueuedCapacity", rate: 12_500_000, checkAt: 500 * time.Millisecond, expiredQueued: true, wantMigration: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			budget, err := retention.NewBudget(retention.Limits{Packets: 3, Bytes: 32 * 1024})
			if err != nil {
				t.Fatal(err)
			}
			limits := packetqueue.Limits{Packets: 4, Bytes: 32 * 1024}
			ingress, err := packetqueue.NewWithClock[Packet](limits, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer ingress.Close()
			scheduler, err := NewScheduler(ingress)
			if err != nil {
				t.Fatal(err)
			}
			source, err := NewTransmissionStoreWithBudget(limits, budget, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { releaseTransmissions(source.drain()) }()
			if tt.expiredQueued {
				limits.Packets = 1
			}
			destination, err := NewTransmissionStoreWithBudget(limits, budget, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { releaseTransmissions(destination.drain()) }()
			transmission := schedulerTransmission(77, wgpacket.TransportData, now.Add(5*time.Second))
			want := transmission.data()
			want.Payload = bytes.Clone(want.Payload)
			if err := source.push(transmission); err != nil {
				t.Fatal(err)
			}
			pending := schedulerTransmission(88, wgpacket.TransportData, now.Add(5*time.Second))
			pending.packet.Payload = make([]byte, 1452)
			pending.packet.Payload[0] = 4
			if tt.expiredQueued {
				pending.deadlineMicros = uint64(now.Add(100 * time.Millisecond).UnixMicro())
			}
			if err := destination.push(pending); err != nil {
				t.Fatal(err)
			}
			if !tt.expiredQueued {
				takeOneTransmission(t, destination)
			}
			if tt.recentReport {
				pending.packetID++
				if err := destination.push(pending); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, destination)
			}
			lane := &scheduledLane{
				registration: schedulerRegistration(2, 1, destination), rttMicros: 10_000,
				deliveryRate: tt.rate, lastProgressAt: now,
			}
			now = now.Add(tt.checkAt)
			if tt.recentReport {
				received := now.Add(-10 * time.Millisecond)
				if _, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(received.UnixMicro()), received); err != nil {
					t.Fatal(err)
				}
			}
			scheduler.migrateTransmissions(map[protocol.LaneID]*scheduledLane{2: lane},
				&scheduledLane{registration: schedulerRegistration(1, 1, source)})
			migrated, gotMigration := destination.normal.peek()
			if gotMigration != tt.wantMigration {
				t.Fatalf("migration = %t, want %t", gotMigration, tt.wantMigration)
			}
			if gotMigration {
				data := migrated.data()
				if !migrated.migrated || data.PacketID != want.PacketID || data.DeadlineMicros != want.DeadlineMicros ||
					!bytes.Equal(data.Payload, want.Payload) || migrated.budget != budget {
					t.Fatal("migration changed packet metadata or aggregate ownership")
				}
			}
			wantPackets := 1
			if tt.expiredQueued {
				wantPackets = 0
			}
			if tt.wantMigration {
				wantPackets++
			}
			if got := budget.Usage(); got.Packets != wantPackets {
				t.Fatalf("budget usage after migration = %+v, want %d packets", got, wantPackets)
			}
			releaseTransmissions(destination.drain())
			if got := budget.Usage(); got != (retention.Usage{}) {
				t.Fatalf("budget usage after cleanup = %+v", got)
			}
		})
	}
}
