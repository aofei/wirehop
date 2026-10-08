package relay

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"net/netip"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerCloseSession(t *testing.T) {
	frame, err := protocol.MarshalSessionClose(protocol.CloseClientShutdown)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name       string
		registered bool
		abandoning bool
	}{
		{name: "NoActiveLane"},
		{name: "ControlRejected", registered: true},
		{name: "AbandoningLane", registered: true, abandoning: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lanes := make(map[protocol.LaneID]*scheduledLane)
			if tt.registered {
				lane := schedulerLane(t, 1, 1, 1, 1_000_000)
				lane.abandoning = tt.abandoning
				lane.registration.SendControl = func(protocol.Frame, func()) bool {
					if tt.abandoning {
						t.Fatal("close routed through an abandoning lane")
					}
					return false
				}
				lanes[lane.registration.LaneID] = lane
			}
			result := make(chan error, 1)
			var preferred protocol.LaneID
			new(Scheduler).applyEvent(lanes, &preferred,
				schedulerEvent{kind: schedulerCloseSession, frame: frame, result: result}, time.Now())
			if err := <-result; !errors.Is(err, ErrNoActiveLane) {
				t.Fatalf("close error = %v, want no active lane", err)
			}
		})
	}

	t.Run("DeferredCallbacks", func(t *testing.T) {
		first := schedulerLane(t, 1, 1, 1, 1_000_000)
		attempts := 0
		first.registration.SendControl = func(protocol.Frame, func()) bool {
			attempts++
			return false
		}
		lane := schedulerLane(t, 2, 2, 2, 1_000_000)
		var callbacks []func()
		lane.registration.SendControl = func(_ protocol.Frame, complete func()) bool {
			callbacks = append(callbacks, complete)
			return true
		}
		lanes := map[protocol.LaneID]*scheduledLane{
			first.registration.LaneID: first,
			lane.registration.LaneID:  lane,
		}
		var preferred protocol.LaneID
		var results [2]chan error
		for index := range results {
			results[index] = make(chan error, 1)
			new(Scheduler).applyEvent(lanes, &preferred,
				schedulerEvent{kind: schedulerCloseSession, frame: frame, result: results[index]}, time.Now())
			select {
			case <-results[index]:
				t.Fatal("close completed before the carrier write callback")
			default:
			}
		}
		if len(callbacks) != len(results) {
			t.Fatalf("close callbacks = %d, want %d", len(callbacks), len(results))
		}
		if attempts != len(results) {
			t.Fatal("close did not fall back after the best lane rejected control")
		}
		for index := len(callbacks) - 1; index >= 0; index-- {
			callbacks[index]()
			select {
			case err := <-results[index]:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("callback did not complete its own close request")
			}
		}
	})

	t.Run("SynchronousCallback", func(t *testing.T) {
		lane := schedulerLane(t, 1, 1, 1, 1_000_000)
		lane.registration.SendControl = func(_ protocol.Frame, complete func()) bool {
			complete()
			return true
		}
		result := make(chan error, 1)
		var preferred protocol.LaneID
		new(Scheduler).applyEvent(map[protocol.LaneID]*scheduledLane{lane.registration.LaneID: lane}, &preferred,
			schedulerEvent{kind: schedulerCloseSession, frame: frame, result: result}, time.Now())
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("CanceledCaller", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer ingress.Close()
			scheduler, err := NewScheduler(ingress)
			if err != nil {
				t.Fatal(err)
			}
			lane := schedulerLane(t, 1, 1, 1, 1_000_000)
			var complete func()
			lane.registration.SendControl = func(frame protocol.Frame, callback func()) bool {
				reason, err := protocol.ParseSessionClose(frame)
				if err != nil || reason != protocol.CloseClientShutdown {
					t.Fatalf("session close = %v, %v", reason, err)
				}
				complete = callback
				return true
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- scheduler.CloseSession(ctx, protocol.CloseClientShutdown) }()
			synctest.Wait()
			var preferred protocol.LaneID
			scheduler.applyEvent(map[protocol.LaneID]*scheduledLane{lane.registration.LaneID: lane},
				&preferred, <-scheduler.events, time.Now())
			cancel()
			synctest.Wait()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("CloseSession() = %v, want canceled", err)
			}
			complete()
			for range cap(scheduler.events) {
				scheduler.events <- schedulerEvent{}
			}
			if err := scheduler.CloseSession(ctx, protocol.CloseClientShutdown); !errors.Is(err, context.Canceled) {
				t.Fatalf("CloseSession() with full event queue = %v, want canceled", err)
			}
		})
	})
}

func TestSelectCandidates(t *testing.T) {
	first := schedulerLane(t, 1, 1, 10, 1_000_000)
	sameGroup := schedulerLane(t, 2, 1, 12, 1_000_000)
	otherGroup := schedulerLane(t, 3, 2, 20, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		first.registration.LaneID:      first,
		sameGroup.registration.LaneID:  sameGroup,
		otherGroup.registration.LaneID: otherGroup,
	}

	transport := selectCandidates(lanes, sameGroup.registration.LaneID, 1500, math.MaxUint64)
	if transport.count != 1 || transport.lanes[0] != sameGroup {
		t.Fatalf("transport candidates = %v, want preferred lane", transport)
	}
	control := selectControlCandidates(lanes, 1500, math.MaxUint64)
	if control.count != 2 || control.lanes[0] != first || control.lanes[1] != otherGroup {
		t.Fatalf("control candidates = %v, want fastest lane and a distinct path group", control)
	}
}

func TestSelectCandidatesUsesSerializationAndCapacity(t *testing.T) {
	slow := schedulerLane(t, 1, 1, 2_000, 100_000)
	fast := schedulerLane(t, 2, 2, 20_000, 10_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		slow.registration.LaneID: slow,
		fast.registration.LaneID: fast,
	}
	if got := selectCandidates(lanes, protocol.LaneID(0), 0, math.MaxUint64); got.lanes[0] != slow {
		t.Fatal("zero-byte scheduling ignored the lower RTT lane")
	}
	if got := selectCandidates(lanes, protocol.LaneID(0), 1500, math.MaxUint64); got.lanes[0] != fast {
		t.Fatal("large-frame scheduling ignored serialization delay")
	}

	full := schedulerLaneWithLimits(t, 3, 3, 1, 1_000_000, packetqueue.Limits{Packets: 1, Bytes: 4096})
	transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := full.registration.Store.push(transmission); err != nil {
		t.Fatal(err)
	}
	lanes[full.registration.LaneID] = full
	if full.canAccept(uint64(transmission.size)) {
		t.Fatal("full retained store accepted another frame")
	}
}

func TestSelectCandidatesSeparatesQueuedAndUnreportedData(t *testing.T) {
	for _, test := range []struct {
		name string
		sent bool
	}{
		{name: "Queued"},
		{name: "AwaitingFeedback", sent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lowDelay := schedulerLaneWithLimits(t, 1, 1, 20_000, 2_000_000, packetqueue.Limits{
				Packets: 64, Bytes: 128 * 1024,
			})
			highDelay := schedulerLane(t, 2, 2, 60_000, 8_000_000)
			store := lowDelay.registration.Store
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			deadline := time.Now().Add(5 * time.Second)
			for packetID := uint64(1); packetID <= 40; packetID++ {
				transmission := schedulerTransmission(packetID, wgpacket.TransportData, deadline)
				payload := make([]byte, 1400)
				copy(payload, transmission.packet.Payload)
				transmission.packet.Payload = payload
				transmission.size = dataFrameSize(transmission.data())
				if err := store.push(transmission); err != nil {
					t.Fatal(err)
				}
			}
			if test.sent {
				for range 40 {
					takeOneTransmission(t, store)
				}
			}
			lanes := map[protocol.LaneID]*scheduledLane{
				lowDelay.registration.LaneID:  lowDelay,
				highDelay.registration.LaneID: highDelay,
			}
			want := highDelay
			if test.sent {
				want = lowDelay
			}
			got := selectCandidates(lanes, lowDelay.registration.LaneID, 1500, math.MaxUint64)
			if got.count != 1 || got.lanes[0] != want {
				t.Fatalf("candidates = %v, want lane %v", got, want.registration.LaneID)
			}
		})
	}
}

func TestSelectCandidatesAppliesDeadlineBeforePreference(t *testing.T) {
	fast := schedulerLane(t, 1, 1, 1_000, 1_000_000)
	preferred := schedulerLane(t, 2, 2, 4_000, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		fast.registration.LaneID:      fast,
		preferred.registration.LaneID: preferred,
	}
	got := selectCandidates(lanes, preferred.registration.LaneID, 0, 1_000)
	if got.count != 1 || got.lanes[0] != fast {
		t.Fatalf("candidates = %v, want timely fastest lane", got)
	}
	late := selectCandidates(lanes, protocol.LaneID(0), 0, 400)
	if late.count != 0 || !late.available {
		t.Fatalf("late candidates = %v, want available but predicted-late lanes", late)
	}
}

func TestSelectCandidatesGroupRankAndCutoff(t *testing.T) {
	first := schedulerLane(t, 1, 1, 1000, 1_000_000)
	second := schedulerLane(t, 2, 1, 1000, 1_000_000)
	third := schedulerLane(t, 3, 1, 1000, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		first.registration.LaneID: first, second.registration.LaneID: second, third.registration.LaneID: third,
	}
	for _, test := range []struct {
		name      string
		preferred protocol.LaneID
		control   bool
		cutoff    uint64
		want      [2]*scheduledLane
		count     int
	}{
		{name: "SecondRankPreference", preferred: second.registration.LaneID, cutoff: 501,
			want: [2]*scheduledLane{second}, count: 1},
		{name: "ThirdRankPreference", preferred: third.registration.LaneID, cutoff: 501,
			want: [2]*scheduledLane{first}, count: 1},
		{name: "SameGroupControl", control: true, cutoff: 501,
			want: [2]*scheduledLane{first, second}, count: 2},
		{name: "TransportEqualCutoff", cutoff: 500},
		{name: "ControlEqualCutoff", control: true, cutoff: 500},
	} {
		t.Run(test.name, func(t *testing.T) {
			for range 100 {
				got := selectCandidates(lanes, test.preferred, 0, test.cutoff)
				if test.control {
					got = selectControlCandidates(lanes, 0, test.cutoff)
				}
				if !got.available || got.count != test.count || got.lanes != test.want {
					t.Fatalf("candidates = %+v, want count %d and lanes %v", got, test.count, test.want)
				}
			}
		})
	}
}

func TestSelectCandidatesMatchesReference(t *testing.T) {
	const laneCount = 32
	lanes := make(map[protocol.LaneID]*scheduledLane, laneCount)
	for index := range laneCount {
		lane := schedulerLaneWithLimits(t, byte(index+1), 1, 1, 1, packetqueue.Limits{
			Packets: 4, Bytes: 8192,
		})
		lanes[lane.registration.LaneID] = lane
	}
	state := uint64(1)
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state
	}
	for iteration := range 5000 {
		for _, lane := range lanes {
			lane.registration.PathGroupID = protocol.PathGroupID(byte(next()%20 + 1))
			lane.rttMicros = next() % 20_000
			lane.deliveryRate = next() % 20_000_001
			lane.degraded = next()%11 == 0
			lane.abandoning = next()%13 == 0
			lane.registration.Store.backlogPackets.Store(int64(next() % 5))
			retainedBytes := next() % 10_001
			lane.registration.Store.backlogBytes.Store(retainedBytes)
			lane.registration.Store.bytes = int(retainedBytes)
			lane.registration.Store.sentBytes = next() % (retainedBytes + 1)
			if next()%3 == 0 {
				lane.registration.Store.sentBytes = retainedBytes
			}
			lane.feedbackDelayMicros = next() % 5000
		}
		var preferred protocol.LaneID
		if value := next() % (laneCount + 1); value != 0 {
			preferred = protocol.LaneID(byte(value))
		}
		frameBytes := next() % 3001
		maximumScore := next() % 30_001
		if next()%5 == 0 {
			maximumScore = math.MaxUint64
		}
		for _, control := range []bool{false, true} {
			got := selectCandidates(lanes, preferred, frameBytes, maximumScore)
			if control {
				got = selectControlCandidates(lanes, frameBytes, maximumScore)
			}
			want := referenceSelectCandidates(lanes, preferred, control, frameBytes, maximumScore)
			if got != want {
				t.Fatalf("iteration %d selectCandidates() = %v, want %v", iteration, got, want)
			}
		}
	}
}

func TestSelectCandidatesExhaustiveGroupScores(t *testing.T) {
	const laneCount = 4
	var members [laneCount]*scheduledLane
	lanes := make(map[protocol.LaneID]*scheduledLane, laneCount)
	states := 1
	for index := range members {
		lane := schedulerLane(t, byte(index+1), 1, 0, 1_000_000)
		members[index] = lane
		lanes[lane.registration.LaneID] = lane
		states *= 8
	}
	// Enumerate two groups and scores of zero, the hysteresis boundary, one beyond it, and infinity.
	for state := range states {
		remaining := state
		for _, lane := range members {
			choice := remaining % 8
			remaining /= 8
			lane.registration.PathGroupID = protocol.PathGroupID(byte(choice/4 + 1))
			lane.rttMicros = [4]uint64{0, 4000, 4002, 0}[choice%4]
			lane.deliveryRate = 1_000_000
			if choice%4 == 3 {
				lane.deliveryRate = 0
			}
		}
		for _, cutoff := range [...]uint64{0, 2000, 2001, math.MaxUint64} {
			for _, preferred := range [...]protocol.LaneID{0, 1, 2, 3, 4} {
				got := selectCandidates(lanes, preferred, 0, cutoff)
				want := referenceSelectCandidates(lanes, preferred, false, 0, cutoff)
				if got != want {
					t.Fatalf("state %d, cutoff %d, preferred %v: candidates = %v, want %v",
						state, cutoff, preferred, got, want)
				}
			}
			got := selectControlCandidates(lanes, 0, cutoff)
			want := referenceSelectCandidates(lanes, protocol.LaneID(0), true, 0, cutoff)
			if got != want {
				t.Fatalf("state %d, cutoff %d: control candidates = %v, want %v", state, cutoff, got, want)
			}
		}
	}
}

func TestSchedulerDuplicatesControlAcrossPathGroups(t *testing.T) {
	payload := relayWireGuardPacket(wgpacket.HandshakeInitiation)
	encodedSize := dataFrameSize(protocol.Data{PacketID: 1, DeadlineMicros: 1_000_000, Payload: payload})
	budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: 2 * encodedSize})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{
		Packets: 1, Bytes: len(payload),
	}, budget, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	if err := ingress.Push(packetqueue.Item[Packet]{
		Value: Packet{
			DeadlineMicros: 1_000_000, Packet: datagram.Packet{Kind: wgpacket.HandshakeInitiation, Payload: payload},
		},
		Size: len(payload), Priority: packetqueue.PriorityControl, Deadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	var item packetqueue.Item[Packet]
	err = ingress.TryPop(&item, ingress.Now())
	if err != nil {
		t.Fatal(err)
	}
	firstStore, err := NewTransmissionStoreWithBudget(packetqueue.Limits{
		Packets: 1, Bytes: encodedSize,
	}, budget, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := NewTransmissionStoreWithBudget(packetqueue.Limits{
		Packets: 1, Bytes: encodedSize,
	}, budget, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	first := &scheduledLane{
		registration: schedulerRegistration(1, 1, firstStore),
		rttMicros:    10, deliveryRate: 1_000_000, rttObserved: true,
	}
	second := &scheduledLane{
		registration: schedulerRegistration(2, 2, secondStore),
		rttMicros:    20, deliveryRate: 1_000_000, rttObserved: true,
	}
	lanes := map[protocol.LaneID]*scheduledLane{
		first.registration.LaneID: first, second.registration.LaneID: second,
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	var preferred protocol.LaneID
	scheduled, err := scheduler.schedule(lanes, &preferred, &item, scheduler.ingress.Now())
	if err != nil || !scheduled {
		t.Fatalf("schedule() = %t, %v", scheduled, err)
	}
	firstData := takeOneTransmission(t, firstStore)
	secondData := takeOneTransmission(t, secondStore)
	if firstData.PacketID != 1 || secondData.PacketID != firstData.PacketID ||
		!bytes.Equal(firstData.Payload, payload) || !bytes.Equal(secondData.Payload, payload) {
		t.Fatalf("duplicated control data = %+v and %+v", firstData, secondData)
	}
	if got := budget.Usage(); got != (retention.Usage{Packets: 2, Bytes: 2 * encodedSize}) {
		t.Fatalf("budget usage after duplication = %+v", got)
	}
	releaseTransmissions(firstStore.drain())
	releaseTransmissions(secondStore.drain())
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after release = %+v", got)
	}
}

func referenceSelectCandidates(lanes map[protocol.LaneID]*scheduledLane, preferred protocol.LaneID,
	control bool, frameBytes, maximumScore uint64) laneCandidates {
	result := laneCandidates{}
	var first *scheduledLane
	for _, lane := range lanes {
		if lane.degraded || lane.abandoning || !referenceLaneEligible(lanes, lane, frameBytes) {
			continue
		}
		result.available = true
		if lane.score(frameBytes) >= maximumScore {
			continue
		}
		if first == nil || referenceLaneBetterForFrame(lane, first, frameBytes) {
			first = lane
		}
	}
	if first == nil {
		return result
	}
	if !control {
		preferredLimit := first.score(frameBytes)
		if preferredLimit <= math.MaxUint64-preferredLaneHysteresisMicros {
			preferredLimit += preferredLaneHysteresisMicros
		} else {
			preferredLimit = math.MaxUint64
		}
		if preferredLane := lanes[preferred]; preferredLane != nil && !preferredLane.degraded && !preferredLane.abandoning &&
			(frameBytes == 0 || preferredLane.canAccept(frameBytes)) && preferredLane.score(frameBytes) < maximumScore {
			queued, _ := preferredLane.registration.Store.deliveryBacklog()
			if frameBytes > 0 && queued == 0 && preferredLane.registration.PathGroupID == first.registration.PathGroupID ||
				referenceLaneEligible(lanes, preferredLane, frameBytes) && preferredLane.score(frameBytes) <= preferredLimit {
				first = preferredLane
			}
		}
		result.lanes[0] = first
		result.count = 1
		return result
	}
	var second *scheduledLane
	for _, lane := range lanes {
		if lane == first || lane.degraded || lane.abandoning || !referenceLaneEligible(lanes, lane, frameBytes) ||
			lane.score(frameBytes) >= maximumScore {
			continue
		}
		candidateDistinct := lane.registration.PathGroupID != first.registration.PathGroupID
		if second == nil {
			second = lane
			continue
		}
		secondDistinct := second.registration.PathGroupID != first.registration.PathGroupID
		if candidateDistinct && !secondDistinct ||
			candidateDistinct == secondDistinct && referenceLaneBetterForFrame(lane, second, frameBytes) {
			second = lane
		}
	}
	if second == nil {
		result.lanes[0] = first
		result.count = 1
		return result
	}
	result.lanes = [2]*scheduledLane{first, second}
	result.count = 2
	return result
}

func referenceLaneBetterForFrame(left, right *scheduledLane, frameBytes uint64) bool {
	leftScore, rightScore := left.score(frameBytes), right.score(frameBytes)
	return leftScore < rightScore || leftScore == rightScore &&
		left.registration.LaneID < right.registration.LaneID
}

func referenceLaneEligible(lanes map[protocol.LaneID]*scheduledLane, lane *scheduledLane, frameBytes uint64) bool {
	if frameBytes > 0 && !lane.canAccept(frameBytes) {
		return false
	}
	better := 0
	for _, candidate := range lanes {
		if candidate == lane || candidate.degraded || candidate.abandoning ||
			candidate.registration.PathGroupID != lane.registration.PathGroupID ||
			frameBytes > 0 && !candidate.canAccept(frameBytes) {
			continue
		}
		if referenceLaneBetterForFrame(candidate, lane, frameBytes) {
			better++
			if better == 2 {
				return false
			}
		}
	}
	return true
}

func TestSchedulerWaitsForPrimaryProgress(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	first := schedulerLaneWithLimits(t, 1, 1, 10_000, 1_000_000, packetqueue.Limits{Packets: 1, Bytes: 2048})
	second := schedulerLane(t, 2, 2, 100_000, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	defer func() { releaseTransmissions(first.registration.Store.drain()) }()
	defer func() { releaseTransmissions(second.registration.Store.drain()) }()
	payload := append(relayWireGuardPacket(wgpacket.TransportData), make([]byte, 16)...)
	item := packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}, 1_000_000), Size: len(payload), Deadline: time.Now().Add(time.Second)}
	var preferred protocol.LaneID
	if ok, err := scheduler.schedule(lanes, &preferred, &item, ingress.Now()); err != nil || !ok {
		t.Fatalf("initial schedule = %t, %v", ok, err)
	}
	item.Value = newPacket(datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}, 1_000_000)
	if ok, err := scheduler.schedule(lanes, &preferred, &item, ingress.Now()); err != nil || ok {
		t.Fatalf("full primary schedule = %t, %v", ok, err)
	}
	if packets, _ := second.registration.Store.backlog(); packets != 0 {
		t.Fatal("unique data spilled onto the alternate")
	}
	first.degraded = true
	if ok, err := scheduler.schedule(lanes, &preferred, &item, ingress.Now()); err != nil || !ok || preferred != 2 {
		t.Fatalf("failover = %t, %v, lane %d", ok, err, preferred)
	}
}

func TestSchedulerQueuesUntilLane(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 8, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- scheduler.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for index := range 2 {
		payload := relayWireGuardPacket(wgpacket.TransportData)
		payload[4] = byte(index + 1)
		if err := ingress.Push(packetqueue.Item[Packet]{
			Value: Packet{
				DeadlineMicros: uint64(10_000 + index), Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload},
			},
			Size: len(payload), Priority: packetqueue.PriorityNormal, Deadline: deadline,
		}); err != nil {
			t.Fatal(err)
		}
	}

	store := schedulerStore(t, packetqueue.Limits{Packets: 8, Bytes: 8192})
	registration := schedulerRegistration(1, 1, store)
	if err := scheduler.Register(ctx, registration); err != nil {
		t.Fatal(err)
	}

	var received [2]protocol.Data
	var ownership [2]datagram.Packet
	count := 0
	for count < len(received) {
		select {
		case <-store.Ready():
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for scheduled packets")
		}
		added, err := store.takeBatch(received[count:], ownership[count:], 8192)
		if err != nil {
			t.Fatal(err)
		}
		count += added
	}
	defer releaseBatchOwnership(ownership[:count])
	for index := range received {
		want := uint64(index + 1)
		if received[index].PacketID != want || received[index].Payload[4] != byte(want) {
			t.Fatalf("received[%d] = %+v, want PacketID and marker %d", index, received[index], want)
		}
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
}

func TestSchedulerPreemptsHeldTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 4, Bytes: 8192})
		if err != nil {
			t.Fatal(err)
		}
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- scheduler.Run(ctx) }()

		store := schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096})
		existing := schedulerTransmission(99, wgpacket.TransportData, time.Now().Add(time.Second))
		if err := store.push(existing); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, store)
		registration := schedulerRegistration(1, 1, store)
		source := protocol.LaneGeneration{LaneID: registration.LaneID, Generation: registration.Generation}
		if err := scheduler.Register(ctx, registration); err != nil {
			t.Fatal(err)
		}

		deadline := time.Now().Add(time.Second)
		transportPayload := relayWireGuardPacket(wgpacket.TransportData)
		transportPayload[4] = 1
		if err := ingress.Push(packetqueue.Item[Packet]{
			Value: Packet{
				DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: transportPayload},
			},
			Size: len(transportPayload), Priority: packetqueue.PriorityNormal, Deadline: deadline,
		}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if ingress.Len() != 0 {
			t.Fatal("scheduler did not hold the blocked transport packet")
		}
		controlPayload := relayWireGuardPacket(wgpacket.HandshakeInitiation)
		if err := ingress.Push(packetqueue.Item[Packet]{
			Value: Packet{
				DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: wgpacket.HandshakeInitiation, Payload: controlPayload},
			},
			Size: len(controlPayload), Priority: packetqueue.PriorityControl, Deadline: deadline,
		}); err != nil {
			t.Fatal(err)
		}
		if err := scheduler.ObserveDeliveryReport(ctx, source, protocol.DeliveryReport{
			LaneID: registration.LaneID, Generation: registration.Generation,
			DataPackets: 1,
		}, 1000); err != nil {
			t.Fatal(err)
		}

		control := awaitOneTransmission(t, store)
		if control.PacketID != 1 || wgpacket.Classify(control.Payload) != wgpacket.HandshakeInitiation {
			t.Fatalf("first scheduled packet = %+v, want control PacketID 1", control)
		}
		if err := scheduler.ObserveDeliveryReport(ctx, source, protocol.DeliveryReport{
			LaneID: registration.LaneID, Generation: registration.Generation,
			DataPackets: 2,
		}, 2000); err != nil {
			t.Fatal(err)
		}

		transport := awaitOneTransmission(t, store)
		if transport.PacketID != 2 || transport.Payload[4] != 1 {
			t.Fatalf("second scheduled packet = %+v, want held transport PacketID 2", transport)
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
	})
}

func TestSchedulerRestoresHeldPackets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 4, Bytes: 8192})
		if err != nil {
			t.Fatal(err)
		}
		defer ingress.Close()
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- scheduler.Run(ctx) }()

		store := schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096})
		if err := store.push(schedulerTransmission(99, wgpacket.TransportData, time.Now().Add(time.Second))); err != nil {
			t.Fatal(err)
		}
		if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for index, kind := range []wgpacket.Kind{wgpacket.TransportData, wgpacket.HandshakeInitiation, wgpacket.HandshakeInitiation} {
			payload := relayWireGuardPacket(kind)
			if err := ingress.Push(packetqueue.Item[Packet]{
				Value: Packet{DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: kind, Payload: payload}},
				Size:  len(payload), Priority: packetPriority(kind.Control()), Deadline: deadline,
			}); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if ingress.Len() != max(0, index-1) {
				t.Fatal("scheduler overwrote held work or did not take the first control")
			}
		}

		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if ingress.Len() != 3 {
			t.Fatalf("restored ingress length = %d, want 3", ingress.Len())
		}
		var first packetqueue.Item[Packet]
		err = ingress.TryPop(&first, ingress.Now())
		if err != nil {
			t.Fatal(err)
		}
		var second packetqueue.Item[Packet]
		err = ingress.TryPop(&second, ingress.Now())
		if err != nil {
			t.Fatal(err)
		}
		var third packetqueue.Item[Packet]
		if err := ingress.TryPop(&third, ingress.Now()); err != nil {
			t.Fatal(err)
		}
		defer first.Release()
		defer second.Release()
		defer third.Release()
		if first.Value.Kind != wgpacket.HandshakeInitiation || second.Value.Kind != wgpacket.HandshakeInitiation ||
			third.Value.Kind != wgpacket.TransportData {
			t.Fatalf("restored order = %s, %s, %s", first.Value.Kind, second.Value.Kind, third.Value.Kind)
		}
	})
}

func TestSchedulerCommitsPacketIDAfterAdmission(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 2, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	full := schedulerLaneWithLimits(t, 1, 1, 1, 1_000_000, packetqueue.Limits{Packets: 1, Bytes: 4096})
	if err := full.registration.Store.push(schedulerTransmission(
		99, wgpacket.TransportData, time.Now().Add(time.Second),
	)); err != nil {
		t.Fatal(err)
	}
	lanes := map[protocol.LaneID]*scheduledLane{full.registration.LaneID: full}
	payload := relayWireGuardPacket(wgpacket.TransportData)
	item := packetqueue.Item[Packet]{
		Value: Packet{DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}},
		Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
	}
	var preferred protocol.LaneID
	scheduled, err := scheduler.schedule(lanes, &preferred, &item, scheduler.ingress.Now())
	if err != nil || scheduled || scheduler.packetID != 0 {
		t.Fatalf("full schedule = %t, %v, PacketID %d", scheduled, err, scheduler.packetID)
	}

	available := schedulerLane(t, 2, 2, 1, 1_000_000)
	lanes[available.registration.LaneID] = available
	full.degraded = true
	scheduled, err = scheduler.schedule(lanes, &preferred, &item, scheduler.ingress.Now())
	if err != nil || !scheduled || scheduler.packetID != 1 {
		t.Fatalf("available schedule = %t, %v, PacketID %d", scheduled, err, scheduler.packetID)
	}
	data := takeOneTransmission(t, available.registration.Store)
	if data.PacketID != 1 {
		t.Fatalf("PacketID = %d, want 1", data.PacketID)
	}
}

func TestSchedulerDeliveryReportValidation(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- scheduler.Run(ctx) }()
	store := schedulerStore(t, packetqueue.Limits{Packets: 4, Bytes: 4096})
	registration := schedulerRegistration(1, 1, store)
	source := protocol.LaneGeneration{LaneID: registration.LaneID, Generation: registration.Generation}
	registration.ValidatePingProgress = func(identifier uint64) bool { return identifier <= 1 }
	invalidRegistration := registration
	invalidRegistration.ValidatePingProgress = nil
	if err := scheduler.Register(ctx, invalidRegistration); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("registration without ping validation error = %v, want %v", err, ErrInvalidRegistration)
	}
	if err := scheduler.Register(ctx, registration); err != nil {
		t.Fatal(err)
	}

	first := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	second := schedulerTransmission(2, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(first); err != nil {
		t.Fatal(err)
	}
	if err := store.push(second); err != nil {
		t.Fatal(err)
	}
	var batch [2]protocol.Data
	var ownership [2]datagram.Packet
	count, err := store.takeBatch(batch[:], ownership[:], 4096)
	if err != nil || count != 2 {
		t.Fatalf("takeBatch() = %d, %v", count, err)
	}
	releaseBatchOwnership(ownership[:count])
	report := protocol.DeliveryReport{
		LaneID: registration.LaneID, Generation: registration.Generation, DataPackets: 1,
	}
	if err := scheduler.ObserveDeliveryReport(ctx, source, report, 1000); err != nil {
		t.Fatal(err)
	}
	if packets, _ := store.backlog(); packets != 1 {
		t.Fatalf("backlog packets = %d, want 1", packets)
	}

	report.DataPackets = 3
	if err := scheduler.ObserveDeliveryReport(ctx, source, report, 2000); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("invalid report error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	if packets, _ := store.backlog(); packets != 1 {
		t.Fatalf("invalid report changed backlog to %d packets", packets)
	}

	report.DataPackets = 2
	if err := scheduler.ObserveDeliveryReport(ctx, source, report, 3000); err != nil {
		t.Fatal(err)
	}
	if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
		t.Fatalf("released backlog = %d packets, %d bytes", packets, bytes)
	}

	report.PingID = 1
	if err := scheduler.ObserveDeliveryReport(ctx, source, report, 5000); err != nil {
		t.Fatal(err)
	}
	report.PingID = 2
	if err := scheduler.ObserveDeliveryReport(ctx, source, report, 6000); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("unexposed ping report error = %v", err)
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
}

func TestSchedulerDeliveryReportUsesFeedbackCarrierDelay(t *testing.T) {
	for _, test := range []struct {
		name              string
		unknownSource     bool
		unmeasuredSource  bool
		changedSource     bool
		invalidReport     bool
		changedTarget     bool
		wantFeedbackDelay uint64
	}{
		{name: "MeasuredSource", wantFeedbackDelay: 100_000},
		{name: "UnknownSource", unknownSource: true},
		{name: "UnmeasuredSource", unmeasuredSource: true},
		{name: "ChangedSourceGeneration", changedSource: true},
		{name: "InvalidReport", invalidReport: true, wantFeedbackDelay: 1234},
		{name: "ChangedTargetGeneration", changedTarget: true, wantFeedbackDelay: 1234},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := schedulerLane(t, 1, 1, 20_000, 1_000_000)
			source := schedulerLane(t, 2, 2, 200_000, 1_000_000)
			target.feedbackDelayMicros = 1234
			if !test.unmeasuredSource {
				source.applyTiming(clockmap.Sample{
					LocalSendMicros: 1000, RemoteReceiveMicros: 1000, RemoteSendMicros: 1000,
					LocalReceiveMicros: 201_000,
				})
				source.applyTiming(clockmap.Sample{
					LocalSendMicros: 301_000, RemoteReceiveMicros: 301_000, RemoteSendMicros: 301_000,
					LocalReceiveMicros: 1_101_000,
				})
			}
			if test.changedSource {
				source.registration.Generation++
			}
			store := target.registration.Store
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
			if err := store.push(transmission); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, store)
			report := protocol.DeliveryReport{
				LaneID: target.registration.LaneID, Generation: 1, DataPackets: 1,
			}
			if test.invalidReport {
				report.DataPackets++
			}
			if test.changedTarget {
				report.Generation++
			}
			lanes := map[protocol.LaneID]*scheduledLane{target.registration.LaneID: target}
			if !test.unknownSource {
				lanes[source.registration.LaneID] = source
			}
			result := make(chan error, 1)
			event := schedulerEvent{
				kind: schedulerReport, laneID: source.registration.LaneID, generation: 1,
				report: report, receiveMicros: 2000, result: result,
			}
			var scheduler Scheduler
			var preferred protocol.LaneID
			scheduler.applyEvent(lanes, &preferred, event, time.Now())
			err := <-result
			if test.invalidReport {
				if !errors.Is(err, ErrInvalidDeliveryReport) {
					t.Fatalf("report error = %v, want %v", err, ErrInvalidDeliveryReport)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if target.feedbackDelayMicros != test.wantFeedbackDelay {
				t.Fatalf("feedback delay = %d, want %d", target.feedbackDelayMicros, test.wantFeedbackDelay)
			}
			if !test.invalidReport && !test.changedTarget {
				if packets, _ := store.backlog(); packets != 0 {
					t.Fatalf("valid report retained %d packets", packets)
				}
			}
		})
	}
}

func TestSchedulerObserveTiming(t *testing.T) {
	for _, test := range []struct {
		name string
		full bool
	}{
		{name: "AvailableCapacity"},
		{name: "FullQueue", full: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				scheduler := &Scheduler{events: make(chan schedulerEvent, 1)}
				want := schedulerEvent{
					kind: schedulerTiming, laneID: protocol.LaneID(1), generation: 7,
					timing: clockmap.Sample{LocalReceiveMicros: 4000},
				}
				if test.full {
					want.laneID = protocol.LaneID(2)
					want.timing.LocalReceiveMicros = 9000
					scheduler.events <- want
				}
				scheduler.ObserveTiming(protocol.LaneID(1), 7, clockmap.Sample{LocalReceiveMicros: 4000})
				event := <-scheduler.events
				if event.kind != want.kind || event.laneID != want.laneID || event.generation != want.generation ||
					event.timing != want.timing || len(scheduler.events) != 0 {
					t.Fatal("timing observation replaced queued work or lost an accepted sample")
				}
			})
		})
	}
}

func TestSchedulerApplyEventInitialTiming(t *testing.T) {
	for _, test := range []struct {
		name   string
		sample *clockmap.Sample
		want   uint64
	}{
		{name: "MissingSample", want: defaultInitialRTTMicros},
		{name: "AdmissionSample", sample: &clockmap.Sample{
			LocalSendMicros: 1000, RemoteReceiveMicros: 9000, RemoteSendMicros: 11_000, LocalReceiveMicros: 7000,
		}, want: 4000},
		{name: "HighRTTSample", sample: &clockmap.Sample{LocalReceiveMicros: 1_200_000}, want: 1_200_000},
		{name: "ZeroSpan", sample: &clockmap.Sample{}, want: 1},
		{name: "ProcessingExceedsSpan", sample: &clockmap.Sample{
			LocalReceiveMicros: 1, RemoteSendMicros: 2,
		}, want: 1},
		{name: "InvalidLocalOrder", sample: &clockmap.Sample{LocalSendMicros: 1}, want: defaultInitialRTTMicros},
		{name: "InvalidRemoteOrder", sample: &clockmap.Sample{RemoteReceiveMicros: 1}, want: defaultInitialRTTMicros},
	} {
		t.Run(test.name, func(t *testing.T) {
			var scheduler Scheduler
			var preferred protocol.LaneID
			lanes := make(map[protocol.LaneID]*scheduledLane)
			registration := schedulerRegistration(1, 1, schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096}))
			registration.InitialTiming = test.sample
			result := make(chan error, 1)
			scheduler.applyEvent(lanes, &preferred, schedulerEvent{
				kind: schedulerRegister, registration: registration, result: result,
			}, time.Unix(1, 0))
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			lane := lanes[registration.LaneID]
			if lane.rttMicros != test.want {
				t.Fatalf("initial RTT = %d, want %d", lane.rttMicros, test.want)
			}
			if lane.rttObserved && lane.minimumRTTMicros != test.want {
				t.Fatal("admission sample did not initialize the generation minimum RTT")
			}
			before := lane.rttMicros
			scheduler.applyEvent(lanes, &preferred, schedulerEvent{
				kind: schedulerTiming, laneID: registration.LaneID, generation: registration.Generation + 1,
				timing: clockmap.Sample{LocalReceiveMicros: 999},
			}, time.Unix(2, 0))
			if lane.rttMicros != before {
				t.Fatal("timing from another generation changed the lane")
			}
		})
	}
	t.Run("GenerationReplacement", func(t *testing.T) {
		ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ingress.Close)
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		old := schedulerLane(t, 1, 1, 1234, 9_000_000)
		old.minimumRTTMicros = 1000
		old.feedbackDelayMicros = 500
		old.lastDataPackets = 99
		lanes := map[protocol.LaneID]*scheduledLane{old.registration.LaneID: old}
		registration := schedulerRegistration(1, 1, schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096}))
		registration.Generation = 2
		registration.InitialTiming = &clockmap.Sample{LocalReceiveMicros: 12_000}
		result := make(chan error, 1)
		var preferred protocol.LaneID
		scheduler.applyEvent(lanes, &preferred, schedulerEvent{
			kind: schedulerRegister, registration: registration, result: result,
		}, time.Unix(2, 0))
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		lane := lanes[registration.LaneID]
		if lane.rttMicros != 12_000 || lane.minimumRTTMicros != 12_000 || !lane.rttObserved ||
			lane.feedbackDelayMicros != 0 || lane.lastDataPackets != 0 ||
			lane.deliveryRate != defaultInitialRateBytesPerSecond {
			t.Fatal("replacement inherited prediction or feedback from the previous generation")
		}
	})
}

func TestSchedulerApplyEventIgnoresRepeatedFeedback(t *testing.T) {
	for _, test := range []struct {
		name      string
		stale     bool
		ping      bool
		slowFirst bool
	}{
		{name: "DuplicateData"},
		{name: "StaleData", stale: true},
		{name: "DuplicatePing", ping: true},
		{name: "StalePing", ping: true, stale: true},
		{name: "SlowFirstDuplicateData", slowFirst: true},
		{name: "SlowFirstStaleData", slowFirst: true, stale: true},
		{name: "SlowFirstDuplicatePing", slowFirst: true, ping: true},
		{name: "SlowFirstStalePing", slowFirst: true, ping: true, stale: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := schedulerLane(t, 1, 1, 20_000, 1_000_000)
			fast := schedulerLane(t, 2, 2, 2_000, 1_000_000)
			slow := schedulerLane(t, 3, 3, 200_000, 1_000_000)
			fast.minimumRTTMicros = fast.rttMicros
			slow.minimumRTTMicros = slow.rttMicros
			store := target.registration.Store
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			var bytes uint64
			for packetID := uint64(1); packetID <= 3; packetID++ {
				transmission := schedulerTransmission(packetID, wgpacket.TransportData, time.Now().Add(time.Second))
				if err := store.push(transmission); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, store)
				if packetID <= 2 {
					bytes += uint64(transmission.size)
				}
			}
			lanes := map[protocol.LaneID]*scheduledLane{
				target.registration.LaneID: target,
				fast.registration.LaneID:   fast,
				slow.registration.LaneID:   slow,
			}
			result := make(chan error, 1)
			event := schedulerEvent{
				kind: schedulerReport, laneID: fast.registration.LaneID, generation: 1,
				report: protocol.DeliveryReport{
					LaneID: target.registration.LaneID, Generation: 1, DataPackets: 2,
				},
				receiveMicros: 2000, result: result,
			}
			if test.ping {
				event.report.DataPackets = 0
				event.report.PingID = 1
			}
			if test.slowFirst {
				event.laneID = slow.registration.LaneID
			}
			var scheduler Scheduler
			var preferred protocol.LaneID
			scheduler.applyEvent(lanes, &preferred, event, time.Unix(2, 0))
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			before := *target
			packets, retainedBytes := store.backlog()
			event.laneID = slow.registration.LaneID
			if test.slowFirst {
				event.laneID = fast.registration.LaneID
			}
			event.receiveMicros = 9000
			if test.stale {
				event.report.DataPackets = 0
				event.report.PingID = 0
			}
			scheduler.applyEvent(lanes, &preferred, event, time.Unix(9, 0))
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if target.feedbackDelayMicros != before.feedbackDelayMicros ||
				target.deliveryRate != before.deliveryRate || target.lastProgressAt != before.lastProgressAt ||
				target.score(1500) != before.score(1500) {
				t.Fatal("repeated feedback changed prediction or progress")
			}
			if gotPackets, gotBytes := store.backlog(); gotPackets != packets || gotBytes != retainedBytes {
				t.Fatal("repeated feedback released additional retained data")
			}
		})
	}
}

func TestScheduledLaneApplyReport(t *testing.T) {
	for _, test := range []struct {
		name           string
		dataPackets    uint64
		pingID         uint64
		invalidPackets bool
		closedStore    bool
		progressed     bool
		err            error
	}{
		{name: "StaleBoth"},
		{name: "StaleData", pingID: 1},
		{name: "StalePing", dataPackets: 1},
		{name: "Duplicate", dataPackets: 1, pingID: 1},
		{name: "AdvanceData", dataPackets: 2, pingID: 1, progressed: true},
		{name: "AdvancePing", dataPackets: 1, pingID: 2, progressed: true},
		{name: "AdvanceBoth", dataPackets: 2, pingID: 2, progressed: true},
		{name: "AdvanceDataStalePing", dataPackets: 2, err: ErrInvalidDeliveryReport},
		{name: "StaleDataAdvancePing", pingID: 2, err: ErrInvalidDeliveryReport},
		{name: "InvalidPrefixCount", dataPackets: 2, pingID: 1, invalidPackets: true,
			err: ErrInvalidDeliveryReport},
		{name: "ClosedStore", dataPackets: 2, pingID: 2, closedStore: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lane := schedulerLane(t, 1, 1, 20_000, 1_000_000)
			store := lane.registration.Store
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			var prefixBytes [3]uint64
			for index := 1; index <= 2; index++ {
				transmission := schedulerTransmission(uint64(index), wgpacket.TransportData, time.Now().Add(time.Second))
				if err := store.push(transmission); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, store)
				prefixBytes[index] = prefixBytes[index-1] + uint64(transmission.size)
			}
			report := protocol.DeliveryReport{DataPackets: 1, PingID: 1}
			if progressed, err := lane.applyReport(report, 2000, time.Unix(2, 0)); err != nil || !progressed {
				t.Fatalf("initial report = %t, %v", progressed, err)
			}
			if test.closedStore {
				releaseTransmissions(store.drain())
			}
			lane.degraded = true
			before := *lane
			packets, bytes := store.backlog()
			report.DataPackets = test.dataPackets
			report.PingID = test.pingID
			if test.invalidPackets {
				report.DataPackets = 3
			}
			progressed, err := lane.applyReport(report, 9000, time.Unix(9, 0))
			if progressed != test.progressed || !errors.Is(err, test.err) {
				t.Fatalf("applyReport() = %t, %v, want %t, %v", progressed, err, test.progressed, test.err)
			}
			if !progressed {
				if lane.lastDataPackets != before.lastDataPackets || lane.lastPingID != before.lastPingID ||
					lane.deliveryRate != before.deliveryRate || lane.lastProgressAt != before.lastProgressAt ||
					lane.degraded != before.degraded {
					t.Fatal("nonadvancing report changed lane state")
				}
				if gotPackets, gotBytes := store.backlog(); gotPackets != packets || gotBytes != bytes {
					t.Fatal("nonadvancing report released retained data")
				}
			} else if lane.lastProgressAt != time.Unix(9, 0) || lane.degraded ||
				lane.lastDataPackets != report.DataPackets || lane.lastPingID != report.PingID {
				t.Fatal("advancing report did not update cumulative progress")
			}
		})
	}
}

func TestScheduledLaneReportIgnoresStaleCounters(t *testing.T) {
	lane := schedulerLane(t, 1, 1, 1000, 1_000_000)
	lane.lastDataPackets = 2
	lane.lastPingID = 1
	if _, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 1}, 2000, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if lane.lastDataPackets != 2 || lane.lastPingID != 1 {
		t.Fatal("stale report changed cumulative state")
	}
	lane.registration.Store.deliveredMicros = 1000
	if _, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 2, PingID: 1}, 9000, time.Unix(9, 0)); err != nil {
		t.Fatal(err)
	}
	if lane.registration.Store.deliveredMicros != 1000 {
		t.Fatal("duplicate report changed the sample baseline")
	}
	for _, report := range []protocol.DeliveryReport{{DataPackets: 1, PingID: 2}, {DataPackets: 3}} {
		if _, err := lane.applyReport(report, 3000, time.Unix(3, 0)); !errors.Is(err, ErrInvalidDeliveryReport) {
			t.Fatal(err)
		}
	}
}

func TestScheduledLaneDeliveryRateRejectsCompressedFeedback(t *testing.T) {
	lane := schedulerLane(t, 1, 1, 100_000, 1_000_000)
	store := lane.registration.Store
	now := time.UnixMicro(1000)
	store.now = func() time.Time { return now }
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	for packetID := uint64(1); packetID <= 2; packetID++ {
		transmission := schedulerTransmission(packetID, wgpacket.TransportData, now.Add(time.Second))
		payload := make([]byte, 4096)
		copy(payload, transmission.packet.Payload)
		transmission.packet.Payload = payload
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, store)
		now = now.Add(100 * time.Millisecond)
	}
	for packetID := uint64(1); packetID <= 2; packetID++ {
		report := protocol.DeliveryReport{
			LaneID: lane.registration.LaneID, Generation: lane.registration.Generation,
			DataPackets: packetID,
		}
		if _, err := lane.applyReport(report, uint64(now.UnixMicro()), now); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Microsecond)
	}
	if lane.deliveryRate > 1_000_000 {
		t.Fatalf("delivery rate = %d bytes/s after two spaced transmissions and compressed feedback", lane.deliveryRate)
	}
	if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
		t.Fatalf("valid reports retained %d packets and %d bytes", packets, bytes)
	}
}

func TestScheduledLaneDeliveryRateRejectsApplicationLimitedDecrease(t *testing.T) {
	for _, tt := range []struct {
		name                string
		dataBytes           uint64
		observed            bool
		deliveryConstrained bool
		want                uint64
	}{
		{name: "ApplicationLimitedDecrease", observed: true, dataBytes: 15_000, want: 1_000_000},
		{name: "ConstrainedDecrease", observed: true, dataBytes: 15_000, deliveryConstrained: true, want: 937_500},
		{name: "InitialEstimate", dataBytes: 15_000, want: 500_000},
		{name: "ApplicationLimitedIncrease", observed: true, dataBytes: 60_000, want: 2_000_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := &scheduledLane{
				deliveryRate: 1_000_000, rateObserved: tt.observed,
			}
			lane.updateDeliveryRate(deliverySample{bytes: tt.dataBytes, intervalMicros: 30_000}, tt.deliveryConstrained)
			if lane.deliveryRate != tt.want {
				t.Fatalf("delivery rate = %d, want %d", lane.deliveryRate, tt.want)
			}
		})
	}
}

func TestScheduledLaneDeliveryRateUsesPressureBeforeAcknowledgement(t *testing.T) {
	const transmissionCount = 128
	lane := schedulerLaneWithLimits(t, 1, 1, 1000, 1_000_000, packetqueue.Limits{
		Packets: transmissionCount,
		Bytes:   64 * 1024,
	})
	now := time.UnixMicro(1000)
	lane.registration.Store.now = func() time.Time { return now }
	var batch [transmissionCount]protocol.Data
	var ownership [transmissionCount]datagram.Packet
	var dataBytes uint64
	for packetID := uint64(1); packetID <= transmissionCount; packetID++ {
		transmission := schedulerTransmission(packetID, wgpacket.TransportData, now.Add(time.Second))
		if err := lane.registration.Store.push(transmission); err != nil {
			t.Fatal(err)
		}
		dataBytes += uint64(transmission.size)
	}
	count, err := lane.registration.Store.takeBatch(batch[:], ownership[:], math.MaxInt)
	if err != nil || count != transmissionCount {
		t.Fatalf("takeBatch() = %d, %v", count, err)
	}
	releaseBatchOwnership(ownership[:count])
	report := protocol.DeliveryReport{
		LaneID: lane.registration.LaneID, Generation: lane.registration.Generation,
		DataPackets: transmissionCount,
	}
	if _, err := lane.applyReport(report, 31_000, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	if lane.deliveryRate >= 1_000_000 {
		t.Fatalf("delivery rate = %d, want a constrained decrease", lane.deliveryRate)
	}
	if packets, bytes := lane.registration.Store.backlog(); packets != 0 || bytes != 0 {
		t.Fatalf("backlog = %d packets, %d bytes, want empty", packets, bytes)
	}
}

func TestSchedulerPacketIDExhaustion(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.packetID = math.MaxUint64
	lane := schedulerLane(t, 1, 1, 1, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{lane.registration.LaneID: lane}
	payload := relayWireGuardPacket(wgpacket.TransportData)
	item := packetqueue.Item[Packet]{
		Value: Packet{DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}},
		Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
	}
	var preferred protocol.LaneID
	if _, err := scheduler.schedule(lanes, &preferred, &item, scheduler.ingress.Now()); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("schedule() error = %v, want %v", err, ErrCounterExhausted)
	}
}

func TestSchedulerPendingPacketRetainsAggregateCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024}, budget, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: 1024}, budget, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.push(schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))); err != nil {
			t.Fatal(err)
		}
		payload := relayWireGuardPacket(wgpacket.TransportData)
		if err := ingress.Push(packetqueue.Item[Packet]{
			Value: Packet{DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}},
			Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- scheduler.Run(ctx) }()
		if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if ingress.Len() != 0 {
			t.Fatal("scheduler did not retain the blocked packet")
		}
		if got := budget.Usage(); got.Packets != 2 {
			t.Fatalf("budget usage with pending packet = %+v, want 2 packets", got)
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: len(payload)}) {
			t.Fatalf("budget usage after scheduler exit = %+v", got)
		}
		ingress.Close()
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Fatalf("budget usage after queue close = %+v", got)
		}
	})
}

func TestScheduledLaneTransfersAggregateCapacity(t *testing.T) {
	const payloadSize = 32
	encodedSize := dataFrameSize(protocol.Data{PacketID: 1, DeadlineMicros: 10_000, Payload: make([]byte, payloadSize)})
	for _, test := range []struct {
		name       string
		byteLimit  int
		wantQueued bool
	}{
		{name: "FitsEncodedFrame", byteLimit: encodedSize, wantQueued: true},
		{name: "CannotGrowReservation", byteLimit: encodedSize - 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: test.byteLimit})
			if err != nil {
				t.Fatal(err)
			}
			ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{
				Packets: 1,
				Bytes:   payloadSize,
			}, budget, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{
				Packets: 1,
				Bytes:   encodedSize,
			}, budget, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			payload := relayWireGuardPacket(wgpacket.TransportData)
			if err := ingress.Push(packetqueue.Item[Packet]{
				Value: Packet{DeadlineMicros: 10_000, Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}},
				Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			var item packetqueue.Item[Packet]
			err = ingress.TryPop(&item, ingress.Now())
			if err != nil {
				t.Fatal(err)
			}
			lane := &scheduledLane{registration: LaneRegistration{Store: store}}
			if queued := lane.enqueue(&item, 1, time.Now()); queued != test.wantQueued {
				t.Fatalf("enqueue() = %t, want %t", queued, test.wantQueued)
			}
			if test.wantQueued {
				if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: encodedSize}) {
					t.Fatalf("budget usage after enqueue = %+v", got)
				}
				releaseTransmissions(store.drain())
			} else {
				if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: payloadSize}) {
					t.Fatalf("budget usage after failed enqueue = %+v", got)
				}
				item.ReleaseRetention()
			}
			if got := budget.Usage(); got != (retention.Usage{}) {
				t.Fatalf("budget usage after release = %+v", got)
			}
		})
	}
}

func TestSchedulerMigratesTransportOnce(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	source := schedulerLane(t, 1, 1, 1, 1_000_000)
	destination := schedulerLane(t, 2, 2, 1, 1_000_000)
	now := time.Now()
	if err := source.registration.Store.push(schedulerTransmission(
		7, wgpacket.TransportData, now.Add(2*time.Second),
	)); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source.registration.Store)
	if err := source.registration.Store.push(schedulerTransmission(
		8, wgpacket.HandshakeInitiation, now.Add(time.Second),
	)); err != nil {
		t.Fatal(err)
	}
	if err := source.registration.Store.push(schedulerTransmission(
		9, wgpacket.TransportData, now.Add(time.Second),
	)); err != nil {
		t.Fatal(err)
	}
	lanes := map[protocol.LaneID]*scheduledLane{destination.registration.LaneID: destination}
	scheduler.migrateTransmissions(lanes, source)

	var migrated [2]protocol.Data
	var ownership [2]datagram.Packet
	count, err := destination.registration.Store.takeBatch(migrated[:], ownership[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBatchOwnership(ownership[:count])
	if count != 2 || migrated[0].PacketID != 9 || migrated[1].PacketID != 7 {
		t.Fatalf("migrated PacketIDs = %d, %d", migrated[0].PacketID, migrated[1].PacketID)
	}
	if packets, _ := destination.registration.Store.backlog(); packets != 2 {
		t.Fatalf("destination retained packets = %d, want 2", packets)
	}

	third := schedulerLane(t, 3, 3, 1, 1_000_000)
	scheduler.migrateTransmissions(map[protocol.LaneID]*scheduledLane{
		third.registration.LaneID: third,
	}, destination)
	if packets, _ := third.registration.Store.backlog(); packets != 0 {
		t.Fatalf("already migrated packet moved again, backlog = %d", packets)
	}
}

func TestSchedulerMigrateTransmissions(t *testing.T) {
	for _, test := range []struct {
		name          string
		sourceWrite   bool
		sourcePending bool
		overlap       bool
		expired       bool
	}{
		{name: "Queued"},
		{name: "Sent", sourceWrite: true},
		{name: "ActiveWrite", sourceWrite: true, sourcePending: true},
		{name: "OverlappingWrites", sourceWrite: true, sourcePending: true, overlap: true},
		{name: "ExpiredActiveWrite", sourceWrite: true, sourcePending: true, expired: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			limits := packetqueue.Limits{Packets: 1, Bytes: 4096}
			current := time.Unix(123, 0)
			now := func() time.Time { return current }
			ingress, err := packetqueue.NewWithClock[Packet](limits, now)
			if err != nil {
				t.Fatal(err)
			}
			defer ingress.Close()
			scheduler, err := NewScheduler(ingress)
			if err != nil {
				t.Fatal(err)
			}
			sourceStore, err := NewTransmissionStoreWithBudget(limits, budget, now)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { releaseTransmissions(sourceStore.drain()) }()
			destinationStore, err := NewTransmissionStoreWithBudget(limits, budget, now)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { releaseTransmissions(destinationStore.drain()) }()
			transmission := schedulerTransmission(1, wgpacket.TransportData, now().Add(time.Second))
			local, err := datagram.ListenLocal(netip.MustParseAddrPort("127.0.0.1:0"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			peer, err := net.ListenUDP("udp4", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			if _, err := peer.WriteToUDPAddrPort(transmission.packet.Payload, local.LocalAddr()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			packet, err := local.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			transmission.packet = packet
			wantPayload := string(packet.Payload)
			if err := sourceStore.push(transmission); err != nil {
				packet.Release()
				t.Fatal(err)
			}
			var sourceData, destinationData [1]protocol.Data
			var sourceOwnership, destinationOwnership [1]datagram.Packet
			defer sourceOwnership[0].Release()
			defer destinationOwnership[0].Release()
			if test.sourceWrite {
				if count, err := sourceStore.takeBatch(sourceData[:], sourceOwnership[:], 4096); err != nil || count != 1 {
					t.Fatalf("source takeBatch() = %d, %v", count, err)
				}
				if !test.sourcePending {
					sourceOwnership[0].Release()
				}
			}
			if test.expired {
				current = transmission.deadline
				sourceStore.expire(current)
			}
			source := &scheduledLane{registration: schedulerRegistration(1, 1, sourceStore)}
			destination := &scheduledLane{
				registration: schedulerRegistration(2, 2, destinationStore), rttMicros: 1000, deliveryRate: 1_000_000, rateObserved: true,
			}
			scheduler.migrateTransmissions(map[protocol.LaneID]*scheduledLane{
				destination.registration.LaneID: destination,
			}, source)
			if packets, bytes := sourceStore.backlog(); packets != 0 || bytes != 0 {
				t.Fatalf("source backlog = %d packets, %d bytes", packets, bytes)
			}
			select {
			case <-sourceStore.Done():
			default:
				t.Fatal("migration did not close the source store")
			}
			if test.expired {
				if packets, bytes := destinationStore.backlog(); packets != 0 || bytes != 0 {
					t.Fatalf("expired transport migrated %d packets and %d bytes", packets, bytes)
				}
				if got := budget.Usage(); got != (retention.Usage{}) {
					t.Fatalf("expired migration retained capacity: %+v", got)
				}
				if string(sourceData[0].Payload) != wantPayload {
					t.Fatal("expired migration changed the active writer payload")
				}
				retained := sourceOwnership[0].Retain()
				retained.Release()
				return
			}
			if packets, bytes := destinationStore.backlog(); packets != 1 || bytes != uint64(transmission.size) {
				t.Fatalf("destination backlog = %d packets, %d bytes", packets, bytes)
			}
			if got := budget.Usage(); got != (retention.Usage{Packets: 1, Bytes: transmission.size}) {
				t.Fatalf("budget usage after migration = %+v", got)
			}
			if test.sourcePending && !test.overlap {
				if got := string(sourceData[0].Payload); got != wantPayload {
					t.Fatalf("source write payload = %q, want %q", got, wantPayload)
				}
				// Complete the old write before the destination acquires its write reference.
				sourceOwnership[0].Release()
			}
			if count, err := destinationStore.takeBatch(destinationData[:], destinationOwnership[:], 4096); err != nil || count != 1 {
				t.Fatalf("destination takeBatch() = %d, %v", count, err)
			}
			if destinationData[0].PacketID != transmission.packetID ||
				destinationData[0].DeadlineMicros != transmission.wireDeadline {
				t.Fatalf("migrated packet metadata = %+v", destinationData[0])
			}
			if _, _, err := destinationStore.acknowledge(1, uint64(now().UnixMicro())); err != nil {
				t.Fatal(err)
			}
			if got := budget.Usage(); got != (retention.Usage{}) {
				t.Fatalf("budget usage after acknowledgement = %+v", got)
			}
			if test.overlap {
				if got := string(sourceData[0].Payload); got != wantPayload {
					t.Fatalf("source write payload after acknowledgement = %q, want %q", got, wantPayload)
				}
				retained := sourceOwnership[0].Retain()
				retained.Release()
				sourceOwnership[0].Release()
			}
			if got := string(destinationData[0].Payload); got != wantPayload {
				t.Fatalf("destination write payload = %q, want %q", got, wantPayload)
			}
			retained := destinationOwnership[0].Retain()
			retained.Release()
		})
	}
}

func TestSchedulerRunReleasesAggregateBudget(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: 4096}, budget, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- scheduler.Run(ctx) }()
	registration := schedulerRegistration(1, 1, store)
	if err := scheduler.Register(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if err := store.push(schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after scheduler exit = %+v", got)
	}
}

func TestSchedulerRetainsExpiredSentPrefixUntilReport(t *testing.T) {
	for _, alternate := range []bool{false, true} {
		name := "SoleLane"
		if alternate {
			name = "WithAlternative"
		}
		t.Run(name, func(t *testing.T) {
			scheduler := new(Scheduler)
			now := time.Now()
			source := schedulerLane(t, 1, 1, 1_200_000, 1_000_000)
			source.lastProgressAt = now
			source.registration.Abandon = func() { t.Fatal("packet expiry abandoned a connected lane") }
			store := source.registration.Store
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			if err := store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
				t.Fatal(err)
			}
			packet := takeOneTransmission(t, store)
			lanes := map[protocol.LaneID]*scheduledLane{source.registration.LaneID: source}
			if alternate {
				other := schedulerLane(t, 2, 2, 1, 1_000_000)
				lanes[other.registration.LaneID] = other
			}
			scheduler.checkAbandonment(lanes, now.Add(1100*time.Millisecond))
			if source.abandoning || source.degraded {
				t.Fatalf("expired lane: abandoning=%t degraded=%t", source.abandoning, source.degraded)
			}
			_, err := protocol.DataFrameSize(packet)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.applyReport(protocol.DeliveryReport{
				LaneID: source.registration.LaneID, Generation: 1, DataPackets: 1,
			}, 1_200_000, now.Add(1200*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if source.degraded || source.abandoning {
				t.Fatal("delayed parsing report did not restore the same lane")
			}
			if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
				t.Fatalf("reported backlog = %d packets, %d bytes", packets, bytes)
			}
		})
	}
}

func TestSchedulerReportsOverDegradedLanes(t *testing.T) {
	for _, allDegraded := range []bool{false, true} {
		name := "SoleDegradedLane"
		if allDegraded {
			name = "AllDegradedLanes"
		}
		t.Run(name, func(t *testing.T) {
			scheduler := new(Scheduler)
			source := schedulerLane(t, 1, 1, 100_000, 1_000_000)
			now := time.Now()
			source.lastProgressAt = now.Add(-3 * time.Second)
			transmission := schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))
			if err := source.registration.Store.push(transmission); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			t.Cleanup(func() { releaseTransmissions(source.registration.Store.drain()) })
			lanes := map[protocol.LaneID]*scheduledLane{source.registration.LaneID: source}
			scheduler.checkAbandonment(lanes, now.Add(960*time.Millisecond))
			if !source.degraded || source.abandoning {
				t.Fatal("expected an unexpired degraded lane")
			}
			sent := 0
			write := func(_ protocol.Frame, done func()) bool {
				sent++
				if done != nil {
					done()
				}
				return true
			}
			source.registration.SendControl = write
			if allDegraded {
				other := schedulerLane(t, 2, 2, 100_000, 1_000_000)
				other.degraded = true
				other.registration.SendControl = write
				lanes[other.registration.LaneID] = other
			}
			completed := 0
			scheduler.routeReport(lanes, protocol.DeliveryReport{
				LaneID: source.registration.LaneID, Generation: 1, DataPackets: 1,
			}, time.Now(), func(ok bool) {
				if ok {
					completed++
				}
			})
			if sent != len(lanes) || completed != 1 {
				t.Fatalf("report writes=%d completions=%d, want %d and 1", sent, completed, len(lanes))
			}
		})
	}
}

func TestSchedulerDoesNotCountIdleTimeAsProgressStall(t *testing.T) {
	now := time.Now()
	source := schedulerLane(t, 1, 1, 100_000, 112)
	store := source.registration.Store
	store.now = func() time.Time { return now }
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	source.lastProgressAt = now.Add(-time.Minute)
	source.registration.Abandon = func() { t.Fatal("an idle interval caused a fresh burst to be abandoned") }
	alternate := schedulerLane(t, 2, 2, 10_000, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		source.registration.LaneID: source, alternate.registration.LaneID: alternate,
	}
	var scheduler Scheduler
	var bytes uint64
	for burst := range 2 {
		for index := range 2 {
			transmission := schedulerTransmission(uint64(burst*2+index+1), wgpacket.TransportData, now.Add(time.Second))
			if source.score(uint64(transmission.size)) >= 1_000_000 {
				t.Fatal("test burst was not initially eligible for this lane")
			}
			if err := store.push(transmission); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, store)
			bytes += uint64(transmission.size)
		}
		scheduler.checkAbandonment(lanes, now.Add(25*time.Millisecond))
		if source.degraded {
			t.Fatal("fresh burst was degraded before one report could return")
		}
		if _, err := source.applyReport(protocol.DeliveryReport{
			LaneID: source.registration.LaneID, Generation: 1, DataPackets: uint64((burst + 1) * 2),
		}, uint64(burst+1)*1_000_000, now.Add(100*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
}

func TestSchedulerRequiresProgressStallForEarlyAbandonment(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, tt := range []struct {
		name             string
		lastProgressAt   time.Time
		wantAbandonments int
	}{
		{name: "RecentProgress", lastProgressAt: now.Add(-minimumProgressStall / 2)},
		{name: "StalledProgress", lastProgressAt: now.Add(-minimumProgressStall), wantAbandonments: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			abandonments := 0
			source := schedulerLane(t, 1, 1, 80_000, 1_000_000)
			source.rateObserved = true
			source.registration.Store.now = func() time.Time { return now.Add(-minimumProgressStall) }
			source.lastProgressAt = tt.lastProgressAt
			source.registration.Abandon = func() { abandonments++ }
			if err := source.registration.Store.push(schedulerTransmission(
				1, wgpacket.TransportData, now.Add(time.Second),
			)); err != nil {
				t.Fatal(err)
			}
			takeOneTransmission(t, source.registration.Store)
			alternate := schedulerLane(t, 2, 2, 10_000, 10_000_000)
			scheduler.checkAbandonment(map[protocol.LaneID]*scheduledLane{
				source.registration.LaneID:    source,
				alternate.registration.LaneID: alternate,
			}, now)
			if abandonments != tt.wantAbandonments {
				t.Fatalf("abandonments = %d, want %d", abandonments, tt.wantAbandonments)
			}
			if source.abandoning != (tt.wantAbandonments == 1) || source.degraded != (tt.wantAbandonments == 1) {
				t.Fatalf("abandoning = %t, degraded = %t", source.abandoning, source.degraded)
			}
		})
	}
}

func TestLaneProgressStalled(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name           string
		rttMicros      uint64
		lastProgressAt time.Time
		want           bool
	}{
		{name: "MissingProgress", rttMicros: 1},
		{name: "FutureProgress", rttMicros: 1, lastProgressAt: now.Add(time.Second)},
		{name: "MinimumPending", rttMicros: 1, lastProgressAt: now.Add(-minimumProgressStall + time.Microsecond)},
		{name: "MinimumElapsed", rttMicros: 1, lastProgressAt: now.Add(-minimumProgressStall), want: true},
		{name: "RTTPending", rttMicros: 200_000, lastProgressAt: now.Add(-400 * time.Millisecond)},
		{name: "RTTElapsed", rttMicros: 200_000,
			lastProgressAt: now.Add(-400*time.Millisecond - progressStallReportMargin), want: true},
		{name: "Overflow", rttMicros: math.MaxUint64, lastProgressAt: now.Add(-time.Hour)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := &scheduledLane{rttMicros: tt.rttMicros, deliveryRate: 1_000_000, lastProgressAt: tt.lastProgressAt}
			if got := laneProgressStalled(lane, now, now.Add(-time.Hour), 0); got != tt.want {
				t.Fatalf("laneProgressStalled() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestSchedulerRecoversAfterQueuedExpiry(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 1, Bytes: 4096}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	lane := &scheduledLane{
		registration: schedulerRegistration(1, 1, store),
		rttMicros:    1, deliveryRate: 1_000_000, degraded: true,
	}
	if err := store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Millisecond)
	scheduler.checkAbandonment(map[protocol.LaneID]*scheduledLane{
		lane.registration.LaneID: lane,
	}, now)
	if lane.degraded {
		t.Fatal("lane remained degraded after its final queued transmission expired")
	}
	if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
		t.Fatalf("expired queued backlog = %d packets, %d bytes", packets, bytes)
	}
}

func TestSchedulerDuplicatesReportAcrossLanes(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	first := schedulerLane(t, 1, 1, 1, 1_000_000)
	second := schedulerLane(t, 2, 2, 2, 1_000_000)
	var firstWrites int
	var secondWrites int
	first.registration.SendControl = func(_ protocol.Frame, sent func()) bool {
		firstWrites++
		if sent != nil {
			sent()
		}
		return true
	}
	second.registration.SendControl = func(_ protocol.Frame, sent func()) bool {
		secondWrites++
		if sent != nil {
			sent()
		}
		return true
	}
	completed := 0
	scheduler.routeReport(map[protocol.LaneID]*scheduledLane{
		first.registration.LaneID:  first,
		second.registration.LaneID: second,
	}, protocol.DeliveryReport{
		LaneID: first.registration.LaneID, Generation: 1,
	}, time.Now(), func(sent bool) {
		if sent {
			completed++
		}
	})
	if firstWrites != 1 || secondWrites != 1 || completed != 1 {
		t.Fatalf("report writes = first %d, second %d, completed %d", firstWrites, secondWrites, completed)
	}
}

func TestSchedulerRejectsUnroutableReport(t *testing.T) {
	scheduler := new(Scheduler)
	lane := schedulerLane(t, 1, 1, 1, 1_000_000)
	lane.registration.SendControl = func(protocol.Frame, func()) bool { return false }
	completed := make(chan bool, 1)
	scheduler.routeReport(map[protocol.LaneID]*scheduledLane{
		lane.registration.LaneID: lane,
	}, protocol.DeliveryReport{
		LaneID: lane.registration.LaneID, Generation: 1,
	}, time.Now(), func(sent bool) { completed <- sent })
	if sent := <-completed; sent {
		t.Fatal("unroutable report completed as sent")
	}
}

func TestSchedulerReusesControlLaneOrder(t *testing.T) {
	scheduler := new(Scheduler)
	lanes := make(map[protocol.LaneID]*scheduledLane, 16)
	for id := byte(1); id <= 16; id++ {
		lane := schedulerLane(t, id, id, uint64(2*(17-id)), 1_000_000)
		lanes[lane.registration.LaneID] = lane
	}

	ordered := scheduler.orderedControlLanes(lanes)
	if len(ordered) != len(lanes) {
		t.Fatalf("ordered lanes = %d, want %d", len(ordered), len(lanes))
	}
	for index, candidate := range ordered {
		lane := candidate.lane
		want := byte(len(ordered) - index)
		if lane.registration.LaneID != protocol.LaneID(want) {
			t.Fatalf("ordered lane %d = %d, want %d", index, lane.registration.LaneID, want)
		}
	}

	allocations := testing.AllocsPerRun(100, func() {
		ordered = scheduler.orderedControlLanes(lanes)
	})
	if allocations != 0 {
		t.Fatalf("steady-state control lane ordering allocations = %f, want 0", allocations)
	}
}

func TestScheduledLaneTimingUsesFirstSample(t *testing.T) {
	lane := &scheduledLane{rttMicros: defaultInitialRTTMicros}
	lane.applyTiming(clockmap.Sample{
		LocalSendMicros: 1000, RemoteReceiveMicros: 1010, RemoteSendMicros: 1020,
		LocalReceiveMicros: 1110,
	})
	if lane.rttMicros != 100 || lane.minimumRTTMicros != 100 || !lane.rttObserved {
		t.Fatalf("first timing state = %d, observed %t", lane.rttMicros, lane.rttObserved)
	}
	lane.applyTiming(clockmap.Sample{
		LocalSendMicros: 2000, RemoteReceiveMicros: 2010, RemoteSendMicros: 2020,
		LocalReceiveMicros: 2190,
	})
	if lane.rttMicros != weightedAverage7(100, 180) || lane.minimumRTTMicros != 100 {
		t.Fatalf("smoothed RTT = %d", lane.rttMicros)
	}
	lane.applyTiming(clockmap.Sample{
		LocalSendMicros: 3000, RemoteReceiveMicros: 3010, RemoteSendMicros: 3020,
		LocalReceiveMicros: 3070,
	})
	if lane.minimumRTTMicros != 60 {
		t.Fatalf("minimum RTT = %d, want 60", lane.minimumRTTMicros)
	}
}

func schedulerLane(t *testing.T, id, group byte, rtt, rate uint64) *scheduledLane {
	t.Helper()
	return schedulerLaneWithLimits(t, id, group, rtt, rate, packetqueue.Limits{
		Packets: 8,
		Bytes:   64 * 1024,
	})
}

func schedulerLaneWithLimits(t *testing.T, id, group byte, rtt, rate uint64,
	limits packetqueue.Limits) *scheduledLane {
	t.Helper()
	store := schedulerStore(t, limits)
	store.transportReported.Store(minimumRateSampleBytes)
	lane := &scheduledLane{
		registration: schedulerRegistration(id, group, store),
		rttMicros:    rtt, deliveryRate: rate, rttObserved: true, rateObserved: true,
	}
	lane.registration.SendDeliveryReport = func(report protocol.DeliveryReport, _ time.Time, sent func()) bool {
		frame, err := protocol.MarshalDeliveryReport(report)
		return err == nil && lane.registration.SendControl(frame, sent)
	}
	return lane
}

func schedulerStore(t *testing.T, limits packetqueue.Limits) *TransmissionStore {
	t.Helper()
	store, err := NewTransmissionStore(limits)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func schedulerRegistration(id, group byte, store *TransmissionStore) LaneRegistration {
	return LaneRegistration{
		LaneID: protocol.LaneID(id), Generation: 1, PathGroupID: protocol.PathGroupID(group),
		Store: store, Abandon: func() {}, SendControl: func(protocol.Frame, func()) bool { return true },
		ValidatePingProgress: func(uint64) bool { return true },
		SendDeliveryReport:   func(protocol.DeliveryReport, time.Time, func()) bool { return true },
	}
}

func schedulerTransmission(packetID uint64, kind wgpacket.Kind, deadline time.Time) retainedTransmission {
	data := protocol.Data{PacketID: packetID, DeadlineMicros: packetID + 1000, Payload: relayWireGuardPacket(kind)}
	return retainedTransmission{
		packetID: packetID, wireDeadline: data.DeadlineMicros, deadline: deadline,
		packet: datagram.Packet{Kind: kind, Payload: data.Payload},
		size:   dataFrameSize(data),
	}
}

func dataFrameSize(data protocol.Data) int {
	size, err := protocol.DataFrameSize(data)
	if err != nil {
		panic(err)
	}
	return size
}

func takeOneTransmission(t *testing.T, store *TransmissionStore) protocol.Data {
	t.Helper()
	var batch [1]protocol.Data
	var ownership [1]datagram.Packet
	count, err := store.takeBatch(batch[:], ownership[:], protocol.MaxEncodedFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("takeBatch() count = %d, want 1", count)
	}
	data := batch[0]
	data.Payload = bytes.Clone(data.Payload)
	releaseBatchOwnership(ownership[:count])
	return data
}

func awaitOneTransmission(t *testing.T, store *TransmissionStore) protocol.Data {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		var batch [1]protocol.Data
		var ownership [1]datagram.Packet
		count, err := store.takeBatch(batch[:], ownership[:], protocol.MaxEncodedFrameSize)
		switch {
		case err == nil && count == 1:
			data := batch[0]
			data.Payload = bytes.Clone(data.Payload)
			releaseBatchOwnership(ownership[:count])
			return data
		case errors.Is(err, packetqueue.ErrEmpty):
		default:
			t.Fatalf("takeBatch() = %d, %v", count, err)
		}
		select {
		case <-store.Ready():
		case <-timeout.C:
			t.Fatal("timed out waiting for transmission")
		}
	}
}

func TestSchedulerScheduleVariableFrameSize(t *testing.T) {
	for _, tt := range []struct {
		name        string
		id          uint64
		deadline    uint64
		payloadSize int
		size        int
	}{
		{name: "OneByteID", id: 127, deadline: 128000, payloadSize: 32, size: 37},
		{name: "TwoByteID", id: 128, deadline: 128000, payloadSize: 32, size: 38},
		{name: "ThreeByteID", id: 16384, deadline: 128000, payloadSize: 32, size: 39},
		{name: "FullWidthID", id: math.MaxUint64, deadline: 128000, payloadSize: 32, size: 46},
		{name: "OneByteDeadline", id: 1, deadline: 127000, payloadSize: 32, size: 36},
		{name: "TwoByteDeadline", id: 1, deadline: 128000, payloadSize: 32, size: 37},
		{name: "FullWidthDeadline", id: 1, deadline: math.MaxUint64 - math.MaxUint64%1000, payloadSize: 32, size: 43},
		{name: "TwoByteHeaderMaximum", id: 1, deadline: 128000, payloadSize: 1020, size: 1025},
		{name: "ThreeByteHeader", id: 1, deadline: 128000, payloadSize: 1021, size: 1027},
		{name: "MediumPayload", id: 1, deadline: 128000, payloadSize: 125, size: 130},
		{name: "LargePayload", id: 1, deadline: 128000, payloadSize: 16381, size: 16387},
		{name: "MaximumFrame", id: math.MaxUint64, deadline: math.MaxUint64 - math.MaxUint64%1000, payloadSize: 65535, size: 65556},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, fits := range []bool{true, false} {
				ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: protocol.MaxPacketSize})
				if err != nil {
					t.Fatal(err)
				}
				defer ingress.Close()
				scheduler, err := NewScheduler(ingress)
				if err != nil {
					t.Fatal(err)
				}
				scheduler.packetID = tt.id - 1
				limit := tt.size
				if !fits {
					limit--
				}
				lane := schedulerLaneWithLimits(t, 1, 1, 10, 1_000_000, packetqueue.Limits{Packets: 1, Bytes: limit})
				defer func() { releaseTransmissions(lane.registration.Store.drain()) }()
				payload := make([]byte, tt.payloadSize)
				copy(payload, relayWireGuardPacket(wgpacket.TransportData))
				if err := ingress.Push(packetqueue.Item[Packet]{Value: Packet{DeadlineMicros: tt.deadline, Packet: datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}},
					Size: len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second)}); err != nil {
					t.Fatal(err)
				}
				var item packetqueue.Item[Packet]
				if err := ingress.TryPop(&item, ingress.Now()); err != nil {
					t.Fatal(err)
				}
				var preferred protocol.LaneID
				scheduled, err := scheduler.schedule(map[protocol.LaneID]*scheduledLane{lane.registration.LaneID: lane}, &preferred, &item, scheduler.ingress.Now())
				if err != nil || scheduled != fits {
					t.Fatalf("fits %t: scheduled %t, error %v", fits, scheduled, err)
				}
				if fits {
					data := takeOneTransmission(t, lane.registration.Store)
					wire, err := protocol.MarshalDataFrame(data)
					if err != nil || data.PacketID != tt.id || data.DeadlineMicros != tt.deadline ||
						len(wire) != tt.size || scheduler.packetID != tt.id {
						t.Fatalf("candidate size or committed identity changed: %d, %v", len(wire), err)
					}
				} else {
					if scheduler.packetID != tt.id-1 {
						t.Fatal("failed admission advanced PacketID")
					}
					item.Release()
				}
			}
		})
	}
}
