package relay

import (
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestScheduledLaneCanAcceptTransport(t *testing.T) {
	for _, tt := range []struct {
		name        string
		rate, limit uint64
	}{
		{name: "Startup", limit: 16 * 1024},
		{name: "LowRate", rate: 1000, limit: 16 * 1024},
		{name: "MeasuredRate", rate: 1_000_000, limit: 50_000},
		{name: "FractionalRate", rate: 1_000_019, limit: 50_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newFlightCreditStore(t, &now, 2*1024*1024)
			lane := scheduledLane{registration: LaneRegistration{Store: store}, deliveryRate: tt.rate}
			queued := pushFlightCreditPacket(t, store, 1, 4096, now.Add(time.Second))
			remaining := tt.limit - queued
			if !lane.canAcceptTransport(remaining) || lane.canAcceptTransport(remaining+1) {
				t.Fatal("queued admission did not enforce the exact estimated-rate boundary")
			}
			if !lane.canAccept(remaining + 1) {
				t.Fatal("transport queueing consumed hard control capacity")
			}
		})
	}

	t.Run("CommittedFlightIsSeparate", func(t *testing.T) {
		now := time.Unix(100, 0)
		store := newFlightCreditStore(t, &now, 2*1024*1024)
		store.setDeliveryWindows(256*1024, 256*1024, 0)
		lane := scheduledLane{registration: LaneRegistration{Store: store}, deliveryRate: 1000}
		for id := uint64(1); id <= 8; id++ {
			pushFlightCreditPacket(t, store, id, 4096, now.Add(time.Second))
		}
		if lane.canAcceptTransport(1200) {
			t.Fatal("excess queued work admitted ordinary transport")
		}
		assertFlightCreditTake(t, store, 1, 2, 3, 4, 5, 6, 7, 8)
		if !lane.canAcceptTransport(1200) || !lane.canAcceptTransport(32*1024) {
			t.Fatal("committed flight consumed the queued budget or blocked an empty-queue large frame")
		}
		assertTransmissionStoreInvariants(t, store)
	})

	t.Run("HardLimitsStillApply", func(t *testing.T) {
		now := time.Unix(100, 0)
		store := newFlightCreditStore(t, &now, 65536)
		lane := scheduledLane{registration: LaneRegistration{Store: store}, deliveryRate: math.MaxUint64}
		if !lane.canAcceptTransport(65536) || lane.canAcceptTransport(65537) || lane.canAcceptTransport(math.MaxUint64) {
			t.Fatal("large estimated rate bypassed the hard byte limit")
		}
		pushFlightCreditPacket(t, store, 1, 32, now.Add(time.Second))
		store.limits.Packets = 1
		if lane.canAcceptTransport(1) {
			t.Fatal("queued admission bypassed the hard packet limit")
		}
		releaseTransmissions(store.drain())
		if lane.canAcceptTransport(1) {
			t.Fatal("a drained generation admitted transport")
		}
	})

	t.Run("MaximumDatagramMakesProgress", func(t *testing.T) {
		now := time.Unix(100, 0)
		store := newFlightCreditStore(t, &now, 2*1024*1024)
		lane := scheduledLane{registration: LaneRegistration{Store: store}, deliveryRate: 1000}
		if !lane.canAcceptTransport(protocol.MaxEncodedFrameSize) {
			t.Fatal("empty queued prefix rejected a maximum-size datagram")
		}
		pushFlightCreditPacket(t, store, 1, protocol.MaxPacketSize, now.Add(time.Second))
		if lane.canAcceptTransport(1200) {
			t.Fatal("an oversized queued frame admitted more transport")
		}
		assertFlightCreditTake(t, store, 1)
		if !lane.canAcceptTransport(1200) {
			t.Fatal("committing the maximum datagram did not restore queued admission")
		}
	})
}

func TestSchedulerScheduleQueuedTransport(t *testing.T) {
	for _, paused := range []bool{false, true} {
		name := "Active"
		if paused {
			name = "MeasurementPause"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			primary := schedulerLane(t, 1, 1, 100_000, 250_000)
			alternate := schedulerLane(t, 2, 2, 100_000, 250_000)
			for _, lane := range []*scheduledLane{primary, alternate} {
				lane.registration.Store.now = func() time.Time { return now }
				lane.registration.Store.setDeliveryWindows(64*1024, 64*1024, 0)
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			lanes := map[protocol.LaneID]*scheduledLane{1: primary, 2: alternate}
			scheduler := Scheduler{transportHoldUntil: now.Add(time.Second)}
			if paused {
				scheduler.measurementStarted = now
				primary.registration.Store.beginMeasurementPause()
				alternate.registration.Store.beginMeasurementPause()
			}
			preferred := protocol.LaneID(1)
			first := ingressProbeItem(wgpacket.TransportData, 32*1024, now.Add(time.Second))
			if done, err := scheduler.schedule(lanes, &preferred, &first, now); err != nil || !done {
				t.Fatalf("empty queue rejected a legal large frame: %t, %v", done, err)
			}
			pending := ingressProbeItem(wgpacket.TransportData, 1200, now.Add(time.Second))
			defer pending.Release()
			if done, err := scheduler.schedule(lanes, &preferred, &pending, now); err != nil || done || scheduler.packetID != 1 {
				t.Fatalf("queued pressure consumed pending transport or its ID: %t, %v, %d", done, err, scheduler.packetID)
			}
			if count, _ := alternate.registration.Store.backlog(); count != 0 || preferred != 1 {
				t.Fatal("queue pressure striped transport onto an alternate")
			}
			control := ingressProbeItem(wgpacket.HandshakeInitiation, 0, now.Add(time.Second))
			if done, err := scheduler.schedule(lanes, &preferred, &control, now); err != nil || !done || scheduler.packetID != 2 {
				t.Fatalf("transport queue pressure blocked WireGuard control: %t, %v", done, err)
			}
			if paused {
				assertFlightCreditTake(t, primary.registration.Store)
				scheduler.measurementStarted = time.Time{}
				primary.registration.Store.endMeasurementPause()
				alternate.registration.Store.endMeasurementPause()
			}
			if got := takeOneTransmission(t, primary.registration.Store); got.PacketID != 2 {
				t.Fatal("control did not retain its queue priority")
			}
			if got := takeOneTransmission(t, primary.registration.Store); got.PacketID != 1 {
				t.Fatal("large transport frame lost its original identity")
			}
			if done, err := scheduler.schedule(lanes, &preferred, &pending, now); err != nil || !done || scheduler.packetID != 3 {
				t.Fatalf("committing queued work did not restore transport admission: %t, %v", done, err)
			}
		})
	}
}

func TestSchedulerRunRetriesAfterLocalCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newIngressProbeFixture(t)
		fixture.register(t, 1, packetqueue.Limits{Packets: 128, Bytes: 256 * 1024})
		deadline := time.Now().Add(time.Second)
		for range 32 {
			if err := fixture.ingress.Push(ingressProbeItem(wgpacket.TransportData, 4096, deadline)); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		store := fixture.stores[0]
		before := time.Now()
		packets, _ := store.backlog()
		waiting := fixture.ingress.Len()
		if packets == 0 || waiting == 0 {
			t.Fatal("fixture did not exhaust queued admission with ingress still waiting")
		}
		var data [datagram.MaximumBatchSize]protocol.Data
		var ownership [datagram.MaximumBatchSize]datagram.Packet
		count, err := store.takeBatch(data[:], ownership[:], 64*1024, 0)
		releaseBatchOwnership(ownership[:count])
		if err != nil || count == 0 || count >= packets {
			t.Fatalf("partially commit blocked queue: %d of %d, %v", count, packets, err)
		}
		synctest.Wait()
		after, _ := store.backlog()
		if after <= packets || fixture.ingress.Len() >= waiting || !time.Now().Equal(before) || store.reportedPackets != 0 {
			t.Fatal("local commit did not refill queued work before feedback or a maintenance tick")
		}
		assertTransmissionStoreInvariants(t, store)
	})
}

func TestSelectCandidatesQueuedEligibility(t *testing.T) {
	for _, preferred := range []protocol.LaneID{0, 3} {
		name := "NoPreferred"
		if preferred != 0 {
			name = "MissingPreferred"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			busy := schedulerLane(t, 1, 1, 1000, 1000)
			available := schedulerLane(t, 2, 2, 10_000, 1000)
			for _, lane := range []*scheduledLane{busy, available} {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			pushFlightCreditPacket(t, busy.registration.Store, 1, 32*1024, now.Add(time.Second))
			lanes := map[protocol.LaneID]*scheduledLane{1: busy, 2: available}
			selected := selectCandidates(lanes, preferred, 1200, math.MaxUint64)
			if selected.count != 1 || selected.lanes[0] != available {
				t.Fatal("an inadmissible early candidate hid an available fallback lane")
			}
		})
	}
}

func TestTransmissionStoreCanAcceptQueuedAfterDrain(t *testing.T) {
	now := time.Unix(100, 0)
	store := newFlightCreditStore(t, &now, 65536)
	pushFlightCreditPacket(t, store, 1, 32, now.Add(time.Second))
	var data [1]protocol.Data
	var ownership [1]datagram.Packet
	count, err := store.takeBatch(data[:], ownership[:], 65536, 0)
	if err != nil || count != 1 {
		t.Fatalf("commit prefix: %d, %v", count, err)
	}
	releaseBatchOwnership(ownership[:count])
	releaseTransmissions(store.drain())
	if store.canAcceptQueued(1, 65536) {
		t.Fatal("drained committed counters underflowed into an admissible queue")
	}
	if queued, retained := store.deliveryBacklog(); queued != 0 || retained != 0 {
		t.Fatalf("drained generation retained a wrapped queued snapshot: %d, %d", queued, retained)
	}
}

func TestTransmissionStoreQueuedAdmissionWake(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		blocked, ready, expired bool
	}{
		{name: "NoBlockedAdmission"},
		{name: "LocalCommit", blocked: true},
		{name: "CoalescedReady", blocked: true, ready: true},
		{name: "ExpiryWithoutCommit", blocked: true, expired: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newFlightCreditStore(t, &now, 65536)
			ready := make(chan struct{}, 1)
			store.admissionReady = ready
			pushFlightCreditPacket(t, store, 1, 32*1024, now.Add(time.Second))
			if tt.blocked && store.canAcceptQueued(1200, 16*1024) {
				t.Fatal("fixture did not block queued admission")
			}
			if tt.ready {
				ready <- struct{}{}
			}
			if tt.expired {
				now = now.Add(time.Second)
				assertFlightCreditTake(t, store)
			} else {
				assertFlightCreditTake(t, store, 1)
			}
			select {
			case <-ready:
				if !tt.blocked {
					t.Fatal("unblocked traffic produced an admission wake")
				}
			default:
				if tt.blocked {
					t.Fatal("freed queued space did not wake admission")
				}
			}
			select {
			case <-ready:
				t.Fatal("one queued transition produced multiple wakes")
			default:
			}
			assertTransmissionStoreInvariants(t, store)
		})
	}
}

func TestScheduledLaneScoreClosedGeneration(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "Empty"
		if committed {
			name = "CommittedPrefix"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			closed := schedulerLane(t, 1, 1, 1000, 1_000_000)
			live := schedulerLane(t, 2, 2, 10_000, 1_000_000)
			for _, lane := range []*scheduledLane{closed, live} {
				lane.registration.Store.now = func() time.Time { return now }
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
			}
			if committed {
				pushFlightCreditPacket(t, closed.registration.Store, 1, 32, now.Add(time.Second))
				assertFlightCreditTake(t, closed.registration.Store, 1)
			}
			releaseTransmissions(closed.registration.Store.drain())
			if closed.canAccept(1200) || closed.canAcceptTransport(1200) {
				t.Fatal("closed generation reported usable retained capacity")
			}
			for _, size := range []uint64{0, 1200} {
				if score := closed.score(size); score != math.MaxUint64 {
					t.Fatalf("closed generation has a usable delivery score: %d", score)
				}
			}
			selected := selectControlCandidates(map[protocol.LaneID]*scheduledLane{1: closed, 2: live}, 1200, math.MaxUint64)
			if selected.count != 1 || selected.lanes[0] != live {
				t.Fatal("drained generation displaced a live control destination")
			}
			if _, eligible := recoveryCandidate(closed, now, 1200, 1_000_000); eligible {
				t.Fatal("drained generation remained eligible for lifecycle recovery")
			}
			unavailable := selectControlCandidates(map[protocol.LaneID]*scheduledLane{1: closed}, 1200, math.MaxUint64)
			if unavailable.count != 0 || unavailable.available {
				t.Fatal("a session with only a drained lane reported control capacity")
			}
		})
	}
}
