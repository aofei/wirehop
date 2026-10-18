package relay

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerTimingDrainPreservesDiscovery(t *testing.T) {
	t.Run("ActivePeriodFinishesBeforePause", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			scheduler, lanes, protocolNow := newProbeDrainRegressionState(t)
			preferred := protocol.LaneID(1)
			probe := lanes[2]
			if !scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) {
				t.Fatal("initial discovery did not admit its first probe")
			}
			until, next, initialBytes := probe.probeUntil, probe.nextProbeAt, probe.probeBytes
			if until.Sub(protocolNow()) != minimumTransportProbeDuration {
				t.Fatal("fixture did not start a bounded four-second discovery period")
			}
			realDeadline := protocolNow().Add(5 * time.Second)
			if err := lanes[1].registration.Store.push(schedulerTransmission(99, wgpacket.TransportData, realDeadline)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer func() {
				cancel()
				scheduler.updateMeasurementDrain(lanes)
			}()
			ready, valid := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, true, nil)
			scheduler.applyEvent(lanes, &preferred, <-scheduler.events, protocolNow())
			if measurementDrainValid(valid) {
				t.Error("pending refresh published a pause proof during active discovery")
			}
			for _, lane := range lanes {
				if lane.registration.Store.measurementDrain.Load() {
					t.Error("pending refresh paused Data before the active discovery period ended")
				}
			}
			time.Sleep(2 * time.Second)
			var data [1]protocol.Data
			var ownership [1]datagram.Packet
			for ordinal := uint64(1); ordinal <= 2; ordinal++ {
				if ordinal == 2 && !scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) {
					t.Fatal("pending refresh prevented progress within the existing probe period")
				}
				count, err := probe.registration.Store.takeBatch(data[:], ownership[:], 4096, 0)
				if count != 1 || err != nil {
					t.Fatalf("active discovery lost its send opportunity while refresh waited: count %d, error %v", count, err)
				}
				ownership[0].Release()
				if data[0].PacketID != 0 || data[0].DeadlineMicros != 0 {
					t.Fatal("discovery changed the capacity probe wire representation")
				}
				_, stale, err := probe.registration.Store.acknowledge(ordinal, uint64(protocolNow().UnixMicro())+ordinal)
				if err != nil || stale {
					t.Fatalf("probe progress: stale %t, error %v", stale, err)
				}
			}
			if probe.probeUntil != until || probe.nextProbeAt != next || probe.probeBytes != 2*initialBytes {
				t.Fatal("waiting for refresh changed discovery duration, cooldown, or cumulative padding accounting")
			}
			select {
			case <-ready:
				t.Fatal("refresh became ready before the active discovery period ended")
			default:
			}
			time.Sleep(until.Sub(protocolNow()))
			scheduler.updateMeasurementDrain(lanes)
			select {
			case <-ready:
			default:
				t.Fatal("finished discovery did not permit the already empty session to pause")
			}
			if !measurementDrainValid(valid) {
				t.Fatal("completed discovery did not permit an ordinary validated drain")
			}
			for _, lane := range lanes {
				if !lane.registration.Store.measurementDrain.Load() {
					t.Fatal("validated drain did not pause every session writer")
				}
			}
			if probe.probeUntil != until || probe.nextProbeAt != next || probe.probeBytes != 2*initialBytes {
				t.Fatal("drain admission restarted or extended the completed discovery period")
			}
			cancel()
			scheduler.updateMeasurementDrain(lanes)
			realPacket := takeOneTransmission(t, lanes[1].registration.Store)
			if realPacket.PacketID != 99 || realPacket.DeadlineMicros != uint64(realDeadline.UnixMicro()) {
				t.Fatal("waiting for discovery or pausing changed a real packet's identity or deadline")
			}
		})
	})
	t.Run("PausedSessionDoesNotStartDiscovery", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			scheduler, lanes, protocolNow := newProbeDrainRegressionState(t)
			preferred := protocol.LaneID(1)
			ctx, cancel := context.WithCancel(t.Context())
			defer func() {
				cancel()
				scheduler.updateMeasurementDrain(lanes)
			}()
			_, valid := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, true, nil)
			scheduler.applyEvent(lanes, &preferred, <-scheduler.events, protocolNow())
			if !measurementDrainValid(valid) {
				t.Fatal("empty session did not enter a validated timing pause")
			}
			if scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) {
				t.Error("paused session started and charged a new discovery period")
			}
			probe := lanes[2]
			if packets, bytes := probe.registration.Store.backlog(); packets != 0 || bytes != 0 {
				t.Errorf("paused session queued capacity padding: packets %d, bytes %d", packets, bytes)
			}
			if !probe.probeUntil.IsZero() || !probe.nextProbeAt.IsZero() || probe.probeBytes != 0 {
				t.Error("paused session consumed fresh discovery time, cooldown, or byte budget")
			}
		})
	})
	t.Run("WaitingDrainBlocksFreshPeriodsUntilCancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			scheduler, lanes, protocolNow := newProbeDrainRegressionState(t)
			preferred := protocol.LaneID(1)
			if !scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) {
				t.Fatal("initial discovery did not admit a probe")
			}
			until, next := lanes[2].probeUntil, lanes[2].nextProbeAt
			fresh := schedulerLane(t, 3, 3, 300_000, 1_000_000)
			fresh.rateObserved = false
			fresh.registration.Store.now = protocolNow
			t.Cleanup(func() { releaseTransmissions(fresh.registration.Store.drain()) })
			lanes[3] = fresh
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ready, valid := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, true, nil)
			for _, applied := range []bool{false, true} {
				if applied {
					scheduler.applyEvent(lanes, &preferred, <-scheduler.events, protocolNow())
				}
				if scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) {
					t.Fatalf("waiting refresh admitted a fresh discovery period: applied %t", applied)
				}
				if !fresh.probeUntil.IsZero() || !fresh.nextProbeAt.IsZero() || fresh.probeBytes != 0 {
					t.Fatal("waiting refresh consumed the new lane's discovery budget")
				}
			}
			cancel()
			scheduler.updateMeasurementDrain(lanes)
			select {
			case <-ready:
			default:
				t.Fatal("canceled discovery wait left its timing caller blocked")
			}
			if measurementDrainValid(valid) || !scheduler.measurementStarted.IsZero() {
				t.Fatal("canceled discovery wait retained a proof, pause, or quiet interval")
			}
			if !scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) || fresh.probeBytes == 0 {
				t.Fatal("cancellation did not restore the new lane's discovery opportunity")
			}
			if lanes[2].probeUntil != until || lanes[2].nextProbeAt != next {
				t.Fatal("request cancellation changed an existing discovery period")
			}
			time.Sleep(until.Sub(protocolNow()))
			scheduler.updateMeasurementDrain(lanes)
			for _, lane := range lanes {
				if lane.registration.Store.measurementDrain.Load() {
					t.Fatal("canceled waiting request acquired a later pause")
				}
			}
		})
	})
	t.Run("CompletedDiscoveryTimerRearmsInSeparateClockDomain", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			scheduler, lanes, protocolNow := newProbeDrainRegressionState(t)
			preferred := protocol.LaneID(1)
			if !scheduler.fillTransportProbe(lanes, preferred, protocolNow(), 0) {
				t.Fatal("initial discovery did not admit a probe")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer func() {
				cancel()
				scheduler.updateMeasurementDrain(lanes)
			}()
			ready, valid := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, true, nil)
			scheduler.applyEvent(lanes, &preferred, <-scheduler.events, protocolNow())
			if measurementDrainValid(valid) || scheduler.measurementDrain == nil || scheduler.measurementTimer == nil {
				t.Fatal("active discovery did not retain an applied timing waiter and its timer")
			}
			time.Sleep(time.Second)
			runtimeOrigin, protocolOrigin := time.Now(), protocolNow()
			// Slow the protocol clock continuously after admission. It loses one microsecond over the
			// next three seconds without moving backward or changing the active discovery deadline.
			protocolNow = func() time.Time {
				elapsed := time.Since(runtimeOrigin)
				return protocolOrigin.Add(elapsed - elapsed/3_000_000)
			}
			for _, lane := range lanes {
				lane.registration.Store.now = protocolNow
			}
			time.Sleep(3 * time.Second)
			synctest.Wait()
			select {
			case <-scheduler.measurementChanged:
			default:
				t.Fatal("initial discovery timer did not notify its applied waiter")
			}
			if remaining := lanes[2].probeUntil.Sub(protocolNow()); remaining != time.Microsecond {
				t.Fatalf("protocol discovery time remaining = %s, want one microsecond", remaining)
			}
			scheduler.updateMeasurementDrain(lanes)
			select {
			case <-ready:
				t.Fatal("completed runtime timer bypassed the remaining protocol discovery duration")
			default:
			}
			time.Sleep(time.Microsecond)
			synctest.Wait()
			select {
			case <-scheduler.measurementChanged:
			default:
				t.Fatal("completed timer was not rearmed for the remaining protocol duration")
			}
			scheduler.updateMeasurementDrain(lanes)
			select {
			case <-ready:
			default:
				t.Fatal("discovery timer did not make the pending empty-prefix drain ready")
			}
			if !measurementDrainValid(valid) {
				t.Fatal("discovery deadline did not permit a validated pause")
			}
		})
	})
}

// newProbeDrainRegressionState separates protocol deadline time from the synctest wall clock.
func newProbeDrainRegressionState(t *testing.T) (*Scheduler, map[protocol.LaneID]*scheduledLane, func() time.Time) {
	t.Helper()
	wallStart := time.Now()
	protocolStart := time.Unix(100, 0)
	protocolNow := func() time.Time { return protocolStart.Add(time.Since(wallStart)) }
	ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 1, Bytes: 2048}, protocolNow)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ingress.Close() })
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if scheduler.measurementTimer != nil {
			scheduler.measurementTimer.Stop()
		}
	})
	lanes := map[protocol.LaneID]*scheduledLane{
		1: schedulerLane(t, 1, 1, 10_000, 1_000_000),
		2: schedulerLane(t, 2, 2, 300_000, 1_000_000),
	}
	lanes[2].rateObserved = false
	for _, lane := range lanes {
		store := lane.registration.Store
		store.now = protocolNow
		t.Cleanup(func() { releaseTransmissions(store.drain()) })
	}
	return scheduler, lanes, protocolNow
}
