package relay

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerBeginMeasurementDrain(t *testing.T) {
	t.Run("AllCommittedPrefixes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, scheduler := feedbackScheduler(t)
			lanes := map[protocol.LaneID]*scheduledLane{
				1: schedulerLane(t, 1, 1, 10_000, 1_000_000),
				2: schedulerLane(t, 2, 2, 10_000, 1_000_000),
			}
			for _, lane := range lanes {
				t.Cleanup(func() { releaseTransmissions(lane.registration.Store.drain()) })
				if err := lane.registration.Store.push(schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Hour))); err != nil {
					t.Fatal(err)
				}
				takeOneTransmission(t, lane.registration.Store)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ready, proof := scheduler.beginMeasurementDrain(ctx, nil)
			var preferred protocol.LaneID
			scheduler.applyEvent(lanes, &preferred, <-scheduler.events, time.Now())
			for _, lane := range lanes {
				if !lane.registration.Store.measurementDrain.Load() || measurementDrainValid(proof) {
					t.Fatal("pause did not close every Data gate before checking prefixes")
				}
				if _, _, err := lane.registration.Store.acknowledge(1, uint64(time.Now().UnixMicro())); err != nil {
					t.Fatal(err)
				}
				scheduler.updateMeasurementDrain(lanes)
			}
			select {
			case <-ready:
			default:
				t.Fatal("all parsed prefixes did not complete preparation")
			}
			if !measurementDrainValid(proof) {
				t.Fatal("prepared pause has no proof")
			}
			if other, _ := scheduler.beginMeasurementDrain(ctx, nil); other != nil {
				t.Fatal("a second transaction acquired the session pause")
			}
			cancel()
			scheduler.updateMeasurementDrain(lanes)
			for _, lane := range lanes {
				if lane.registration.Store.measurementDrain.Load() {
					t.Fatal("completion did not reopen Data")
				}
			}
			if measurementDrainValid(proof) || scheduler.pendingMeasurementDrain.Load() {
				t.Fatal("completion retained proof or admission ownership")
			}
		})
	})
	t.Run("GenerationChangesRevokeProof", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, scheduler := feedbackScheduler(t)
			lane := schedulerLane(t, 1, 1, 10_000, 1_000_000)
			lanes := map[protocol.LaneID]*scheduledLane{1: lane}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, proof := scheduler.beginMeasurementDrain(ctx, nil)
			var preferred protocol.LaneID
			scheduler.applyEvent(lanes, &preferred, <-scheduler.events, time.Now())
			if !measurementDrainValid(proof) {
				t.Fatal("empty prefix was not prepared")
			}
			scheduler.invalidateMeasurementDrain()
			scheduler.updateMeasurementDrain(lanes)
			if measurementDrainValid(proof) || !lane.registration.Store.measurementDrain.Load() {
				t.Fatal("invalidation retracted an exposed quiet promise")
			}
			time.Sleep(measurementPauseLimit)
			scheduler.updateMeasurementDrain(lanes)
			if lane.registration.Store.measurementDrain.Load() || scheduler.measurementDrain != nil {
				t.Fatal("invalidated promise outlived its fixed deadline")
			}
			releaseTransmissions(lane.registration.Store.drain())
		})
	})
	t.Run("QueueRejectionAndQueuedCancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, scheduler := feedbackScheduler(t)
			for range cap(scheduler.events) {
				scheduler.events <- schedulerEvent{}
			}
			if ready, proof := scheduler.beginMeasurementDrain(t.Context(), nil); ready != nil || proof != nil || scheduler.pendingMeasurementDrain.Load() {
				t.Fatal("full event queue leaked admission ownership")
			}
			for len(scheduler.events) > 0 {
				<-scheduler.events
			}
			ctx, cancel := context.WithCancel(t.Context())
			ready, proof := scheduler.beginMeasurementDrain(ctx, nil)
			cancel()
			var preferred protocol.LaneID
			scheduler.applyEvent(nil, &preferred, <-scheduler.events, time.Now())
			select {
			case <-ready:
			default:
				t.Fatal("queued cancellation did not wake preparation")
			}
			if measurementDrainValid(proof) || scheduler.pendingMeasurementDrain.Load() {
				t.Fatal("queued cancellation retained admission ownership")
			}
		})
	})
}

func measurementDrainValid(proof func() (time.Time, bool)) bool {
	_, valid := proof()
	return valid
}

func runMeasurementScheduler(t *testing.T, lanes ...*Lane) *Scheduler {
	t.Helper()
	ingress, scheduler := feedbackScheduler(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
		ingress.Close()
	})
	for _, lane := range lanes {
		lane.observer = scheduler
		registration := schedulerRegistration(byte(lane.laneID), 1, lane.store)
		registration.SendControl = lane.SendControl
		registration.SendDeliveryReport = lane.SendDeliveryReport
		registration.ValidatePingProgress = lane.ValidatePingProgress
		registration.RequestMeasurement = lane.RequestMeasurement
		registration.SendMeasurement = lane.SendMeasurement
		if err := scheduler.Register(ctx, registration); err != nil {
			t.Fatal(err)
		}
	}
	return scheduler
}
