package relay

import (
	"context"
	"math"
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
			ready, proof := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, nil)
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
			if other, _ := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, nil); other != nil {
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
			_, proof := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, nil)
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
			time.Sleep(measurementPauseDuration(lane.rttMicros))
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
			if ready, proof := scheduler.beginMeasurementDrain(t.Context(), protocol.LaneGeneration{LaneID: 1, Generation: 1}, nil); ready != nil || proof != nil || scheduler.pendingMeasurementDrain.Load() {
				t.Fatal("full event queue leaked admission ownership")
			}
			for len(scheduler.events) > 0 {
				<-scheduler.events
			}
			ctx, cancel := context.WithCancel(t.Context())
			ready, proof := scheduler.beginMeasurementDrain(ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1}, nil)
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

func TestSchedulerMeasurementPauseDeadline(t *testing.T) {
	for _, tt := range []struct {
		name      string
		rttMicros uint64
		latest    uint64
		operation time.Duration
		want      time.Duration
	}{
		{name: "UnknownRTT", want: 250 * time.Millisecond},
		{name: "LowRTT", rttMicros: 10_000, want: 250 * time.Millisecond},
		{name: "MinimumBoundary", rttMicros: 37_500, want: 250 * time.Millisecond},
		{name: "AboveMinimum", rttMicros: 37_501, want: 250*time.Millisecond + 4*time.Microsecond},
		{name: "RTTIncrease", rttMicros: 600_000, want: 2500 * time.Millisecond},
		{name: "LatestRTTIncrease", rttMicros: 83_750, latest: 600_000, want: 2500 * time.Millisecond},
		{name: "LatestRTTReduction", rttMicros: 600_000, latest: 10_000, want: 2500 * time.Millisecond},
		{name: "BelowMaximum", rttMicros: 974_999, want: 4*time.Second - 4*time.Microsecond},
		{name: "MaximumBoundary", rttMicros: 975_000, want: 4 * time.Second},
		{name: "Overflow", rttMicros: math.MaxUint64, want: 4 * time.Second},
		{name: "EarlierOperationDeadline", rttMicros: 600_000, operation: 100 * time.Millisecond, want: 100 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newMeasurementFixture(t, true, 1)
				f.lanes[1].rttMicros = tt.rttMicros
				f.lanes[1].latestRTTMicros = tt.latest
				operation := tt.operation
				if operation == 0 {
					operation = measurementPauseLimit
				}
				ctx, cancel := context.WithTimeout(t.Context(), operation)
				defer cancel()
				started := time.Now()
				_, proof := f.scheduler.beginMeasurementDrain(ctx, f.target, nil)
				f.apply(t)
				deadline, valid := proof()
				if !valid || !f.paused() || deadline.Sub(started) != tt.want {
					t.Fatalf("admitted pause = %s, valid %t, want %s", deadline.Sub(started), valid, tt.want)
				}
				// Ordinary timing changes cannot extend or shorten an exposed quiet promise.
				f.lanes[1].rttMicros = 1
				f.apply(t)
				if unchanged, valid := proof(); !valid || unchanged != deadline {
					t.Fatal("new timing changed the admitted fixed deadline")
				}
			})
		})
	}
}

func TestMeasurementCoordinatorStalledStandbyPauseBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newMeasurementFixture(t, true, 2)
		f.lanes[2].rttMicros = 12_000_000
		f.commit(t, 2)
		f.scheduler.RequestMeasurement(f.target)
		f.apply(t)
		attempt := f.scheduler.measurements.active
		if attempt == nil || !f.paused() || measurementDrainValid(attempt.proof) {
			t.Fatal("stalled standby did not hold every gate without publishing prefix proof")
		}
		operationDeadline, _ := attempt.ctx.Deadline()
		if time.Until(operationDeadline) != measurementPauseLimit {
			t.Fatal("adaptive pause changed the independent transaction limit")
		}
		time.Sleep(250*time.Millisecond - time.Nanosecond)
		f.apply(t)
		if !f.paused() {
			t.Fatal("stalled preparation retracted its pause before the admitted deadline")
		}
		time.Sleep(time.Nanosecond)
		f.apply(t)
		if f.paused() || f.scheduler.measurements.active != nil || f.scheduler.pendingMeasurementDrain.Load() {
			t.Fatal("stalled standby extended a low-RTT pause beyond 250 milliseconds")
		}
		if measurementDrainValid(attempt.proof) || f.lanes[2].registration.Store.sentEmpty() {
			t.Fatal("pause expiry fabricated proof of the stalled standby's parsed prefix")
		}
		if cooldown := time.Until(f.scheduler.measurements.nextAt); cooldown != 250*time.Millisecond*measurementQuietRatio {
			t.Fatalf("cooldown = %s, want the actual 250-millisecond pause cost", cooldown)
		}
	})
}

func TestMeasurementCoordinatorAsymmetricPauseDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newMeasurementFixture(t, true, 1)
		server := newMeasurementFixture(t, false, 1)
		server.lanes[1].rttMicros = 600_000
		prepareMeasurement(t, client, server)
		serverDeadline := server.scheduler.measurementDeadline
		if time.Until(client.scheduler.measurementDeadline) != 250*time.Millisecond ||
			time.Until(serverDeadline) != 2500*time.Millisecond {
			t.Fatal("endpoints did not admit pauses from their own operating RTTs")
		}
		time.Sleep(250 * time.Millisecond)
		client.apply(t)
		server.apply(t)
		if client.paused() || !server.paused() || server.scheduler.measurementDeadline != serverDeadline {
			t.Fatal("local expiry retracted or extended the peer's independent quiet promise")
		}
		select {
		case queued := <-client.writes:
			if _, valid := queued.request.Build(250_000); valid {
				t.Fatal("expired client probe retained permission to start measurement")
			}
		default:
			t.Fatal("client did not queue its probe before local expiry")
		}
		time.Sleep(time.Until(serverDeadline))
		server.apply(t)
		if server.paused() || server.scheduler.measurements.active != nil ||
			client.lanes[1].minimumRTTMicros != 10_000 || server.lanes[1].minimumRTTMicros != 10_000 {
			t.Fatal("asymmetric expiry retained a pause or published unvalidated timing")
		}
	})
}

func TestSchedulerMeasurementDrainRejectsInvalidTarget(t *testing.T) {
	for _, tt := range []struct {
		name       string
		target     protocol.LaneGeneration
		abandoning bool
	}{
		{name: "MissingLane", target: protocol.LaneGeneration{LaneID: 2, Generation: 1}},
		{name: "WrongGeneration", target: protocol.LaneGeneration{LaneID: 1, Generation: 2}},
		{name: "ZeroGeneration", target: protocol.LaneGeneration{LaneID: 1}},
		{name: "AbandoningTarget", target: protocol.LaneGeneration{LaneID: 1, Generation: 1}, abandoning: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newMeasurementFixture(t, true, 1)
				f.lanes[1].abandoning = tt.abandoning
				calls := 0
				ready, proof := f.scheduler.beginMeasurementDrain(t.Context(), tt.target, func(cost time.Duration) {
					calls++
					if cost != 0 {
						t.Fatal("rejected target charged a Data pause")
					}
				})
				f.apply(t)
				select {
				case <-ready:
				default:
					t.Fatal("rejected target left preparation blocked")
				}
				if calls != 1 || f.paused() || measurementDrainValid(proof) || f.scheduler.pendingMeasurementDrain.Load() {
					t.Fatal("invalid target acquired or retained a session pause")
				}
			})
		})
	}
	t.Run("TargetChangesDuringDiscovery", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newMeasurementFixture(t, true, 2)
			f.lanes[2].probeUntil = time.Now().Add(time.Second)
			ready, proof := f.scheduler.beginMeasurementDrain(t.Context(), f.target, nil)
			f.apply(t)
			if f.paused() || f.scheduler.measurementDrain == nil {
				t.Fatal("active discovery did not defer pause admission")
			}
			f.lanes[1].registration.Generation++
			f.apply(t)
			select {
			case <-ready:
			default:
				t.Fatal("changed target left discovery preparation blocked")
			}
			if f.paused() || measurementDrainValid(proof) || f.scheduler.pendingMeasurementDrain.Load() {
				t.Fatal("changed target admitted a stale pause after discovery")
			}
		})
	})
}

func measurementDrainValid(proof func() (time.Time, bool)) bool {
	_, valid := proof()
	return valid
}
