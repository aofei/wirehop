package relay

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSelectCandidates(t *testing.T) {
	first := schedulerLane(t, 1, 1, 10, 1_000_000)
	sameGroup := schedulerLane(t, 2, 1, 12, 1_000_000)
	otherGroup := schedulerLane(t, 3, 2, 20, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		first.registration.LaneID:      first,
		sameGroup.registration.LaneID:  sameGroup,
		otherGroup.registration.LaneID: otherGroup,
	}

	transport := selectCandidates(lanes, sameGroup.registration.LaneID, false, 1500, math.MaxUint64)
	if transport.count != 1 || transport.lanes[0] != sameGroup {
		t.Fatalf("transport candidates = %v, want preferred lane", transport)
	}
	control := selectCandidates(lanes, protocol.LaneID{}, true, 1500, math.MaxUint64)
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
	if got := selectCandidates(lanes, protocol.LaneID{}, false, 0, math.MaxUint64); got.lanes[0] != slow {
		t.Fatal("zero-byte scheduling ignored the lower RTT lane")
	}
	if got := selectCandidates(lanes, protocol.LaneID{}, false, 1500, math.MaxUint64); got.lanes[0] != fast {
		t.Fatal("large-frame scheduling ignored serialization delay")
	}

	full := schedulerLaneWithLimits(t, 3, 3, 1, 1_000_000, packetqueue.Limits{Packets: 1, Bytes: 4096})
	transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := full.registration.Store.push(transmission); err != nil {
		t.Fatal(err)
	}
	lanes[full.registration.LaneID] = full
	if full.canAccept(uint64(protocol.DataFrameOverhead + 32)) {
		t.Fatal("full retained store accepted another frame")
	}
}

func TestSelectCandidatesAppliesDeadlineBeforePreference(t *testing.T) {
	fast := schedulerLane(t, 1, 1, 1_000, 1_000_000)
	preferred := schedulerLane(t, 2, 2, 4_000, 1_000_000)
	lanes := map[protocol.LaneID]*scheduledLane{
		fast.registration.LaneID:      fast,
		preferred.registration.LaneID: preferred,
	}
	got := selectCandidates(lanes, preferred.registration.LaneID, false, 0, 1_000)
	if got.count != 1 || got.lanes[0] != fast {
		t.Fatalf("candidates = %v, want timely fastest lane", got)
	}
	late := selectCandidates(lanes, protocol.LaneID{}, false, 0, 400)
	if late.count != 0 || !late.available {
		t.Fatalf("late candidates = %v, want available but predicted-late lanes", late)
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
			lane.registration.PathGroupID = protocol.PathGroupID{byte(next()%20 + 1)}
			lane.rttMicros = next() % 20_000
			lane.deliveryRate = next() % 20_000_001
			lane.degraded = next()%11 == 0
			lane.abandoning = next()%13 == 0
			lane.registration.Store.backlogPackets.Store(int64(next() % 5))
			lane.registration.Store.backlogBytes.Store(next() % 10_001)
		}
		var preferred protocol.LaneID
		if value := next() % (laneCount + 1); value != 0 {
			preferred = protocol.LaneID{byte(value)}
		}
		control := next()&1 != 0
		frameBytes := next() % 3001
		maximumScore := next() % 30_001
		if next()%5 == 0 {
			maximumScore = math.MaxUint64
		}
		got := selectCandidates(lanes, preferred, control, frameBytes, maximumScore)
		want := referenceSelectCandidates(lanes, preferred, control, frameBytes, maximumScore)
		if got != want {
			t.Fatalf("iteration %d selectCandidates() = %v, want %v", iteration, got, want)
		}
	}
}

func TestSchedulerDuplicatesControlAcrossPathGroups(t *testing.T) {
	payload := relayWireGuardPacket(wgpacket.HandshakeInitiation)
	encodedSize := protocol.DataFrameOverhead + len(payload)
	budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: 2 * encodedSize})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{
		Packets: 1, Bytes: len(payload),
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	if err := ingress.Push(packetqueue.Item[Packet]{
		Value: Packet{
			Kind: wgpacket.HandshakeInitiation, Payload: payload, DeadlineMicros: 1_000_000,
		},
		Size: len(payload), Priority: packetqueue.PriorityControl, Deadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	item, err := ingress.TryPop()
	if err != nil {
		t.Fatal(err)
	}
	firstStore, err := NewTransmissionStoreWithBudget(packetqueue.Limits{
		Packets: 1, Bytes: encodedSize,
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := NewTransmissionStoreWithBudget(packetqueue.Limits{
		Packets: 1, Bytes: encodedSize,
	}, budget)
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
	scheduled, err := scheduler.schedule(lanes, &preferred, &item)
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
		if lane.degraded || lane.abandoning || !laneEligible(lanes, lane, frameBytes) {
			continue
		}
		result.available = true
		if lane.score(frameBytes) >= maximumScore {
			continue
		}
		if first == nil || laneBetterForFrame(lane, first, frameBytes) {
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
		if preferredLane := lanes[preferred]; preferredLane != nil &&
			!preferredLane.degraded && !preferredLane.abandoning && laneEligible(lanes, preferredLane, frameBytes) &&
			preferredLane.score(frameBytes) < maximumScore && preferredLane.score(frameBytes) <= preferredLimit {
			first = preferredLane
		}
		result.lanes[0] = first
		result.count = 1
		return result
	}
	var second *scheduledLane
	for _, lane := range lanes {
		if lane == first || lane.degraded || lane.abandoning || !laneEligible(lanes, lane, frameBytes) ||
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
			candidateDistinct == secondDistinct && laneBetterForFrame(lane, second, frameBytes) {
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

func TestSchedulerSpillsSustainedTrafficAcrossLanes(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	limits := packetqueue.Limits{Packets: 64, Bytes: 128 * 1024}
	first := schedulerLaneWithLimits(t, 1, 1, 10_000, 1_000_000, limits)
	second := schedulerLaneWithLimits(t, 2, 1, 10_000, 1_000_000, limits)
	lanes := map[protocol.LaneID]*scheduledLane{
		first.registration.LaneID: first, second.registration.LaneID: second,
	}
	payload := make([]byte, 1400)
	copy(payload, relayWireGuardPacket(wgpacket.TransportData))
	deadline := time.Now().Add(time.Second)
	var preferred protocol.LaneID
	for range 32 {
		item := packetqueue.Item[Packet]{
			Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 1_000_000},
			Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: deadline,
		}
		scheduled, err := scheduler.schedule(lanes, &preferred, &item)
		if err != nil || !scheduled {
			t.Fatalf("schedule() = %t, %v", scheduled, err)
		}
	}
	firstPackets, _ := first.registration.Store.backlog()
	secondPackets, _ := second.registration.Store.backlog()
	if firstPackets < 8 || secondPackets < 8 || firstPackets+secondPackets != 32 {
		t.Fatalf("sustained distribution = %d and %d packets, want both lanes used", firstPackets, secondPackets)
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
				Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: uint64(10_000 + index),
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
	count := 0
	for count < len(received) {
		select {
		case <-store.Ready():
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for scheduled packets")
		}
		added, err := store.takeBatch(received[count:], 8192)
		if err != nil {
			t.Fatal(err)
		}
		count += added
	}
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
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 4, Bytes: 8192})
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

	store := schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096})
	existing := schedulerTransmission(99, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := store.push(existing); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
	registration := schedulerRegistration(1, 1, store)
	if err := scheduler.Register(ctx, registration); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	transportPayload := relayWireGuardPacket(wgpacket.TransportData)
	transportPayload[4] = 1
	if err := ingress.Push(packetqueue.Item[Packet]{
		Value: Packet{
			Kind: wgpacket.TransportData, Payload: transportPayload, DeadlineMicros: 10_000,
		},
		Size: len(transportPayload), Priority: packetqueue.PriorityNormal, Deadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	for waitDeadline := time.Now().Add(time.Second); ingress.Len() != 0; {
		if time.Now().After(waitDeadline) {
			t.Fatal("scheduler did not hold the blocked transport packet")
		}
		time.Sleep(time.Millisecond)
	}
	controlPayload := relayWireGuardPacket(wgpacket.HandshakeInitiation)
	if err := ingress.Push(packetqueue.Item[Packet]{
		Value: Packet{
			Kind: wgpacket.HandshakeInitiation, Payload: controlPayload, DeadlineMicros: 10_000,
		},
		Size: len(controlPayload), Priority: packetqueue.PriorityControl, Deadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.ObserveDeliveryReport(ctx, protocol.DeliveryReport{
		LaneID: registration.LaneID, Generation: registration.Generation,
		DataPackets: 1, DataBytes: uint64(existing.size),
	}, 1000); err != nil {
		t.Fatal(err)
	}

	control := awaitOneTransmission(t, store)
	if control.PacketID != 1 || wgpacket.Classify(control.Payload) != wgpacket.HandshakeInitiation {
		t.Fatalf("first scheduled packet = %+v, want control PacketID 1", control)
	}
	if err := scheduler.ObserveDeliveryReport(ctx, protocol.DeliveryReport{
		LaneID: registration.LaneID, Generation: registration.Generation,
		DataPackets: 2, DataBytes: uint64(existing.size + protocol.DataFrameOverhead + len(controlPayload)),
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
}

func TestSchedulerRestoresHeldPackets(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 4, Bytes: 8192})
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

	store := schedulerStore(t, packetqueue.Limits{Packets: 1, Bytes: 4096})
	if err := store.push(schedulerTransmission(99, wgpacket.TransportData, time.Now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for _, kind := range []wgpacket.Kind{wgpacket.TransportData, wgpacket.HandshakeInitiation} {
		payload := relayWireGuardPacket(kind)
		if err := ingress.Push(packetqueue.Item[Packet]{
			Value: Packet{Kind: kind, Payload: payload, DeadlineMicros: 10_000},
			Size:  len(payload), Priority: packetPriority(kind.Control()), Deadline: deadline,
		}); err != nil {
			t.Fatal(err)
		}
		for waitDeadline := time.Now().Add(time.Second); ingress.Len() != 0; {
			if time.Now().After(waitDeadline) {
				t.Fatal("scheduler did not take the held packet")
			}
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
	if ingress.Len() != 2 {
		t.Fatalf("restored ingress length = %d, want 2", ingress.Len())
	}
	first, err := ingress.TryPop()
	if err != nil {
		t.Fatal(err)
	}
	second, err := ingress.TryPop()
	if err != nil {
		t.Fatal(err)
	}
	if first.Value.Kind != wgpacket.HandshakeInitiation || second.Value.Kind != wgpacket.TransportData {
		t.Fatalf("restored order = %s then %s", first.Value.Kind, second.Value.Kind)
	}
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
		Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 10_000},
		Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
	}
	var preferred protocol.LaneID
	scheduled, err := scheduler.schedule(lanes, &preferred, &item)
	if err != nil || scheduled || scheduler.packetID != 0 {
		t.Fatalf("full schedule = %t, %v, PacketID %d", scheduled, err, scheduler.packetID)
	}

	available := schedulerLane(t, 2, 2, 1, 1_000_000)
	lanes[available.registration.LaneID] = available
	scheduled, err = scheduler.schedule(lanes, &preferred, &item)
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
	registration.ValidateProbeProgress = func(packets, bytes uint64) bool {
		return packets <= 1 && bytes == packets*uint64(protocol.ProbeFrameOverhead)
	}
	invalidRegistration := registration
	invalidRegistration.ValidateProbeProgress = nil
	if err := scheduler.Register(ctx, invalidRegistration); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("registration without probe validation error = %v, want %v", err, ErrInvalidRegistration)
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
	if count, err := store.takeBatch(batch[:], 4096); err != nil || count != 2 {
		t.Fatalf("takeBatch() = %d, %v", count, err)
	}
	firstSize := uint64(first.size)
	report := protocol.DeliveryReport{
		LaneID: registration.LaneID, Generation: registration.Generation, DataPackets: 1,
		DataBytes: firstSize,
	}
	if err := scheduler.ObserveDeliveryReport(ctx, report, 1000); err != nil {
		t.Fatal(err)
	}
	if packets, _ := store.backlog(); packets != 1 {
		t.Fatalf("backlog packets = %d, want 1", packets)
	}

	report.DataPackets = 2
	report.DataBytes++
	if err := scheduler.ObserveDeliveryReport(ctx, report, 2000); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("invalid report error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	if packets, _ := store.backlog(); packets != 1 {
		t.Fatalf("invalid report changed backlog to %d packets", packets)
	}

	report.DataBytes = uint64(first.size + second.size)
	if err := scheduler.ObserveDeliveryReport(ctx, report, 3000); err != nil {
		t.Fatal(err)
	}
	if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
		t.Fatalf("released backlog = %d packets, %d bytes", packets, bytes)
	}

	report.ProbePackets = 1
	report.ProbeBytes = uint64(protocol.ProbeFrameOverhead - 1)
	if err := scheduler.ObserveDeliveryReport(ctx, report, 4000); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("invalid probe byte report error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	report.ProbeBytes++
	if err := scheduler.ObserveDeliveryReport(ctx, report, 5000); err != nil {
		t.Fatal(err)
	}
	report.ProbePackets = 2
	report.ProbeBytes *= 2
	if err := scheduler.ObserveDeliveryReport(ctx, report, 6000); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("unwritten probe report error = %v, want %v", err, ErrInvalidDeliveryReport)
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
}

func TestScheduledLaneReportIgnoresStaleCounters(t *testing.T) {
	lane := schedulerLane(t, 1, 1, 1000, 1_000_000)
	lane.lastDataBytes = 100
	lane.lastDataPackets = 2
	lane.lastProbeBytes = 50
	lane.lastProbePackets = 1
	report := protocol.DeliveryReport{
		DataBytes: 99, DataPackets: 1, ProbeBytes: 40,
	}
	if err := lane.applyReport(report, 2000, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if lane.lastDataBytes != 100 || lane.lastProbeBytes != 50 {
		t.Fatal("stale report changed cumulative state")
	}
	lane.lastReportMicros = 1000
	report = protocol.DeliveryReport{
		DataBytes: 100, DataPackets: 2, ProbeBytes: 50, ProbePackets: 1,
	}
	if err := lane.applyReport(report, 9000, time.Unix(9, 0)); err != nil {
		t.Fatal(err)
	}
	if lane.lastReportMicros != 1000 {
		t.Fatal("duplicate report changed the delivery-rate time baseline")
	}
	report.DataBytes = 99
	report.DataPackets = 1
	report.ProbeBytes = 100
	report.ProbePackets = 2
	if err := lane.applyReport(report, 3000, time.Unix(3, 0)); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("mixed report error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
	report = protocol.DeliveryReport{
		DataBytes: 100, DataPackets: 1, ProbeBytes: 50, ProbePackets: 1,
	}
	if err := lane.applyReport(report, 4000, time.Unix(4, 0)); !errors.Is(err, ErrInvalidDeliveryReport) {
		t.Fatalf("partially changed report error = %v, want %v", err, ErrInvalidDeliveryReport)
	}
}

func TestScheduledLaneDeliveryRateRejectsApplicationLimitedDecrease(t *testing.T) {
	for _, tt := range []struct {
		name                string
		dataBytes           uint64
		deliveryConstrained bool
		want                uint64
	}{
		{name: "ApplicationLimitedDecrease", dataBytes: 5000, want: 1_000_000},
		{name: "ConstrainedDecrease", dataBytes: 5000, deliveryConstrained: true, want: 937_500},
		{name: "ApplicationLimitedIncrease", dataBytes: 20_000, want: 1_125_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := &scheduledLane{
				deliveryRate: 1_000_000, lastReportMicros: 1000, rateSampleStartMicros: 1000,
			}
			lane.updateDeliveryRate(tt.dataBytes, 11_000, tt.deliveryConstrained)
			if lane.deliveryRate != tt.want {
				t.Fatalf("delivery rate = %d, want %d", lane.deliveryRate, tt.want)
			}
		})
	}
}

func TestScheduledLaneDeliveryRateUsesPressureBeforeAcknowledgement(t *testing.T) {
	const transmissionCount = 80
	lane := schedulerLaneWithLimits(t, 1, 1, 1000, 1_000_000, packetqueue.Limits{
		Packets: transmissionCount,
		Bytes:   64 * 1024,
	})
	var batch [transmissionCount]protocol.Data
	var dataBytes uint64
	for packetID := uint64(1); packetID <= transmissionCount; packetID++ {
		transmission := schedulerTransmission(packetID, wgpacket.TransportData, time.Now().Add(time.Second))
		if err := lane.registration.Store.push(transmission); err != nil {
			t.Fatal(err)
		}
		dataBytes += uint64(transmission.size)
	}
	if count, err := lane.registration.Store.takeBatch(batch[:], math.MaxInt); err != nil || count != transmissionCount {
		t.Fatalf("takeBatch() = %d, %v", count, err)
	}
	lane.lastReportMicros = 1000
	lane.rateSampleStartMicros = 1000
	report := protocol.DeliveryReport{
		LaneID: lane.registration.LaneID, Generation: lane.registration.Generation,
		DataPackets: transmissionCount, DataBytes: dataBytes,
	}
	if err := lane.applyReport(report, 11_000, time.Unix(1, 0)); err != nil {
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
		Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 10_000},
		Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
	}
	var preferred protocol.LaneID
	if _, err := scheduler.schedule(lanes, &preferred, &item); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("schedule() error = %v, want %v", err, ErrCounterExhausted)
	}
}

func TestSchedulerPendingPacketRetainsAggregateCapacity(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 2, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024}, budget)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: 1024}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.push(schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	payload := relayWireGuardPacket(wgpacket.TransportData)
	if err := ingress.Push(packetqueue.Item[Packet]{
		Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 10_000},
		Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- scheduler.Run(ctx) }()
	if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for ingress.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
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
}

func TestScheduledLaneTransfersAggregateCapacity(t *testing.T) {
	const payloadSize = 32
	encodedSize := protocol.DataFrameOverhead + payloadSize
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
			}, budget)
			if err != nil {
				t.Fatal(err)
			}
			store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{
				Packets: 1,
				Bytes:   encodedSize,
			}, budget)
			if err != nil {
				t.Fatal(err)
			}
			payload := relayWireGuardPacket(wgpacket.TransportData)
			if err := ingress.Push(packetqueue.Item[Packet]{
				Value: Packet{Kind: wgpacket.TransportData, Payload: payload, DeadlineMicros: 10_000},
				Size:  len(payload), Priority: packetqueue.PriorityNormal, Deadline: time.Now().Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			item, err := ingress.TryPop()
			if err != nil {
				t.Fatal(err)
			}
			lane := &scheduledLane{registration: LaneRegistration{Store: store}}
			if queued := lane.enqueue(&item, 1); queued != test.wantQueued {
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
	count, err := destination.registration.Store.takeBatch(migrated[:], 4096)
	if err != nil {
		t.Fatal(err)
	}
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

func TestSchedulerTransfersAggregateBudgetDuringMigration(t *testing.T) {
	budget, err := retention.NewBudget(retention.Limits{Packets: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	limits := packetqueue.Limits{Packets: 1, Bytes: 4096}
	sourceStore, err := NewTransmissionStoreWithBudget(limits, budget)
	if err != nil {
		t.Fatal(err)
	}
	destinationStore, err := NewTransmissionStoreWithBudget(limits, budget)
	if err != nil {
		t.Fatal(err)
	}
	transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
	if err := sourceStore.push(transmission); err != nil {
		t.Fatal(err)
	}
	source := &scheduledLane{registration: schedulerRegistration(1, 1, sourceStore)}
	destination := &scheduledLane{
		registration: schedulerRegistration(2, 2, destinationStore), rttMicros: 1000, deliveryRate: 1_000_000,
	}
	new(Scheduler).migrateTransmissions(map[protocol.LaneID]*scheduledLane{
		destination.registration.LaneID: destination,
	}, source)
	if packets, _ := destinationStore.backlog(); packets != 1 {
		t.Fatalf("destination retained packets = %d, want 1", packets)
	}
	if got := budget.Usage(); got.Packets != 1 {
		t.Fatalf("budget usage after migration = %+v", got)
	}
	releaseTransmissions(destinationStore.drain())
	if got := budget.Usage(); got != (retention.Usage{}) {
		t.Fatalf("budget usage after release = %+v", got)
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
	store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 1, Bytes: 4096}, budget)
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

func TestSchedulerAbandonsExpiredSentPrefix(t *testing.T) {
	ingress, err := packetqueue.New[Packet](packetqueue.Limits{Packets: 1, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	abandoned := make(chan struct{}, 1)
	announced := make(chan protocol.LaneGeneration, 1)
	source := schedulerLane(t, 1, 1, 1, 1_000_000)
	source.registration.Generation = 2
	source.registration.Abandon = func() { abandoned <- struct{}{} }
	alternate := schedulerLane(t, 2, 2, 1, 1_000_000)
	alternate.registration.SendControl = func(frame protocol.Frame, sent func()) bool {
		generation, err := protocol.ParseLaneAbandon(frame)
		if err == nil {
			announced <- generation
		}
		if sent != nil {
			sent()
		}
		return true
	}
	deadline := time.Now().Add(10 * time.Millisecond)
	if err := source.registration.Store.push(schedulerTransmission(
		1, wgpacket.TransportData, deadline,
	)); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, source.registration.Store)
	lanes := map[protocol.LaneID]*scheduledLane{
		source.registration.LaneID:    source,
		alternate.registration.LaneID: alternate,
	}
	scheduler.checkAbandonment(lanes, deadline.Add(time.Microsecond))

	select {
	case <-abandoned:
	default:
		t.Fatal("expired sent prefix did not abandon its lane")
	}
	select {
	case got := <-announced:
		if got.LaneID != source.registration.LaneID || got.Generation != 2 {
			t.Fatalf("announced generation = %+v", got)
		}
	default:
		t.Fatal("abandonment was not announced over another lane")
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
			source := schedulerLane(t, 1, 1, 80_000, 1)
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
			if source.abandoning != (tt.wantAbandonments == 1) {
				t.Fatalf("abandoning = %t", source.abandoning)
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
			lane := &scheduledLane{rttMicros: tt.rttMicros, lastProgressAt: tt.lastProgressAt}
			if got := laneProgressStalled(lane, now); got != tt.want {
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
	}, func(sent bool) {
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
	}, func(sent bool) { completed <- sent })
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
	for index, lane := range ordered {
		want := byte(len(ordered) - index)
		if lane.registration.LaneID[0] != want {
			t.Fatalf("ordered lane %d = %d, want %d", index, lane.registration.LaneID[0], want)
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
	lane.applyTiming(protocol.TimingPong{
		PingSendMicros: 1000, ReceiveMicros: 1010, SendMicros: 1020,
	}, 1110)
	if lane.rttMicros != 100 || !lane.rttObserved {
		t.Fatalf("first timing state = %d, observed %t", lane.rttMicros, lane.rttObserved)
	}
	lane.applyTiming(protocol.TimingPong{
		PingSendMicros: 2000, ReceiveMicros: 2010, SendMicros: 2020,
	}, 2190)
	if lane.rttMicros != weightedAverage7(100, 180) {
		t.Fatalf("smoothed RTT = %d", lane.rttMicros)
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
	return &scheduledLane{
		registration: schedulerRegistration(id, group, store),
		rttMicros:    rtt, deliveryRate: rate, rttObserved: true,
	}
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
		LaneID: protocol.LaneID{id}, Generation: 1, PathGroupID: protocol.PathGroupID{group},
		Store: store, Abandon: func() {}, SendControl: func(protocol.Frame, func()) bool { return true },
		ValidateProbeProgress: func(uint64, uint64) bool { return true },
	}
}

func schedulerTransmission(packetID uint64, kind wgpacket.Kind, deadline time.Time) retainedTransmission {
	payload := relayWireGuardPacket(kind)
	return retainedTransmission{
		data: protocol.Data{
			PacketID: packetID, DeadlineMicros: packetID + 1000, Payload: payload,
		},
		kind: kind, priority: packetPriority(kind.Control()), deadline: deadline,
		size: protocol.DataFrameOverhead + len(payload),
	}
}

func takeOneTransmission(t *testing.T, store *TransmissionStore) protocol.Data {
	t.Helper()
	var batch [1]protocol.Data
	count, err := store.takeBatch(batch[:], protocol.MaxEncodedFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("takeBatch() count = %d, want 1", count)
	}
	return batch[0]
}

func awaitOneTransmission(t *testing.T, store *TransmissionStore) protocol.Data {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		var batch [1]protocol.Data
		count, err := store.takeBatch(batch[:], protocol.MaxEncodedFrameSize)
		switch {
		case err == nil && count == 1:
			return batch[0]
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
