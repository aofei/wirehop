package relay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

type pendingTestReport struct {
	report   protocol.DeliveryReport
	complete func(bool)
}

type reportTimingObserver struct {
	testLaneObserver
	reports     chan pendingTestReport
	rejectFirst bool
}

func (o *reportTimingObserver) RouteDeliveryReport(report protocol.DeliveryReport, _ time.Time, complete func(bool)) bool {
	accepted := !o.rejectFirst
	o.rejectFirst = false
	o.reports <- pendingTestReport{report: report, complete: complete}
	return accepted
}

func TestLaneReport(t *testing.T) {
	t.Run("IdleAndRetry", testLaneReportIdleAndRetry)
	t.Run("CompletionAndIdle", testLaneReportCompletionAndIdle)
	t.Run("RoutingRejection", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			observer := &reportTimingObserver{reports: make(chan pendingTestReport, 16), rejectFirst: true}
			lane := &Lane{laneID: 1, generation: 1, observer: observer, reportInterval: 25 * time.Millisecond,
				progress: deliveryProgress{notify: make(chan struct{}, 1)}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- lane.report(ctx) }()
			if err := lane.progress.addData(1, 10); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			first := <-observer.reports
			if first.report.DataPackets != 1 {
				t.Fatalf("first report = %+v", first.report)
			}
			if err := lane.progress.addData(1, 20); err != nil {
				t.Fatal(err)
			}
			synctest.Sleep(4*lane.reportInterval - time.Nanosecond)
			synctest.Wait()
			if len(observer.reports) != 0 {
				t.Fatal("routing rejection retried too early")
			}
			synctest.Sleep(time.Nanosecond)
			synctest.Wait()
			retry := <-observer.reports
			if retry.report.DataPackets != 2 {
				t.Fatalf("retry = %+v", retry.report)
			}
			retry.complete(true)
			synctest.Wait()
			synctest.Sleep(time.Second)
			synctest.Wait()
			if len(observer.reports) != 0 {
				t.Fatal("completed report retried")
			}
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	})
}

func TestSchedulerRouteReportWriteCompletion(t *testing.T) {
	for _, tt := range []struct {
		name           string
		ownFails       bool
		alternateFails bool
		ownFull        bool
		alternateFull  bool
		ownGone        bool
		wantSent       bool
		wantCompleted  int64
	}{
		{name: "BothSucceed", wantSent: true, wantCompleted: 1},
		{name: "OwnWriteFails", ownFails: true, wantSent: true, wantCompleted: 1},
		{name: "AlternateWriteFails", alternateFails: true, wantSent: true, wantCompleted: 1},
		{name: "BothWritesFail", ownFails: true, alternateFails: true},
		{name: "OwnQueueFull", ownFull: true, wantSent: true, wantCompleted: 1},
		{name: "AlternateQueueFull", alternateFull: true, wantSent: true, wantCompleted: 1},
		{name: "BothQueuesFull", ownFull: true, alternateFull: true, wantCompleted: 1},
		{name: "OwnGenerationGone", ownGone: true, wantSent: true, wantCompleted: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				own := newTestLane(t, newTestCarrier(), newTestEndpoint())
				alternate := newTestLane(t, newTestCarrier(), newTestEndpoint())
				alternate.laneID = 2
				lanes := make(map[protocol.LaneID]*scheduledLane)
				for _, lane := range []*Lane{own, alternate} {
					registration := schedulerRegistration(byte(lane.laneID), byte(lane.laneID), lane.store)
					registration.SendDeliveryReport = lane.SendDeliveryReport
					lanes[lane.laneID] = &scheduledLane{registration: registration, rttMicros: 1000, deliveryRate: 1_000_000}
				}
				if tt.ownGone {
					delete(lanes, own.laneID)
				}
				if err := own.progress.addData(16, 16_000); err != nil {
					t.Fatal(err)
				}
				now := time.Now()
				const retryAfter = 100 * time.Millisecond
				snapshot, changed := own.progress.claim(own.laneID, own.generation, now, retryAfter)
				if !changed || snapshot.report.DataPackets != 16 {
					t.Fatal("initial progress was not claimed")
				}
				if err := own.progress.addData(1, 64); err != nil {
					t.Fatal(err)
				}
				for _, state := range []struct {
					lane *Lane
					full bool
				}{{own, tt.ownFull}, {alternate, tt.alternateFull}} {
					if state.full {
						for range cap(state.lane.control) {
							state.lane.control <- controlWrite{}
						}
					}
				}
				var completed atomic.Int64
				var sent atomic.Bool
				var scheduler Scheduler
				scheduler.routeReport(lanes, snapshot.report, snapshot.parsedAt, func(ok bool) {
					completed.Add(1)
					sent.Store(ok)
					own.progress.complete(snapshot, ok)
				})
				queuedCompletion := int64(0)
				if tt.ownFull && tt.alternateFull {
					queuedCompletion = 1
				}
				if completed.Load() != queuedCompletion || sent.Load() {
					t.Fatal("queueing a report was treated as carrier-write completion")
				}
				var writers sync.WaitGroup
				start := make(chan struct{})
				var release sync.Once
				begin := func() { release.Do(func() { close(start) }) }
				defer begin()
				for _, state := range []struct {
					lane   *Lane
					full   bool
					fails  bool
					absent bool
				}{{own, tt.ownFull, tt.ownFails, tt.ownGone}, {alternate, tt.alternateFull, tt.alternateFails, false}} {
					if state.full {
						if len(state.lane.control) != cap(state.lane.control) {
							t.Fatal("a full control queue changed during report routing")
						}
						continue
					}
					if state.absent {
						if len(state.lane.control) != 0 {
							t.Fatal("the removed generation received a report write")
						}
						continue
					}
					if len(state.lane.control) != 1 {
						t.Fatalf("routed reports = %d, want 1", len(state.lane.control))
					}
					connection := state.lane.carrier.(*testCarrier)
					if state.fails {
						connection.writes = make(chan []protocol.Frame)
						connection.Close()
					}
					request := <-state.lane.control
					writers.Go(func() {
						<-start
						count, err := state.lane.writeControlBatch(t.Context(), request, maximumConsecutiveControlFrames)
						if count != 1 || (err != nil) != state.fails {
							t.Errorf("control write = %d frames, %v, want failure %t", count, err, state.fails)
						}
						if !state.fails && err == nil {
							batch := <-connection.writes
							if report, err := protocol.ParseDeliveryReport(batch[0]); err != nil || report != snapshot.report {
								t.Errorf("written report = %+v, %v, want %+v", report, err, snapshot.report)
							}
						}
					})
				}
				begin()
				writers.Wait()
				if completed.Load() != tt.wantCompleted || sent.Load() != tt.wantSent {
					t.Fatalf("report completion = %d calls, sent %t, want %d and %t", completed.Load(), sent.Load(), tt.wantCompleted, tt.wantSent)
				}
				if !tt.wantSent {
					if _, changed := own.progress.claim(own.laneID, own.generation, now.Add(retryAfter-time.Nanosecond), retryAfter); changed {
						t.Fatal("an unsent report retried before its pending deadline")
					}
					now = now.Add(retryAfter)
				}
				next, changed := own.progress.claim(own.laneID, own.generation, now, retryAfter)
				if !changed || next.report.DataPackets != 17 || next.dataBytes != 16_064 {
					t.Fatalf("next progress = %+v, changed %t", next, changed)
				}
				own.progress.complete(next, true)
				own.progress.complete(snapshot, true)
				if _, changed := own.progress.claim(own.laneID, own.generation, now.Add(retryAfter), retryAfter); changed ||
					own.progress.reportedDataPackets != 17 || own.progress.reportedDataBytes != 16_064 {
					t.Fatal("late completion repeated or rolled back completed progress")
				}
			})
		})
	}
}

func testLaneReportCompletionAndIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer := &reportTimingObserver{reports: make(chan pendingTestReport, 16)}
		lane := &Lane{laneID: 1, generation: 1, observer: observer, reportInterval: 25 * time.Millisecond,
			progress: deliveryProgress{notify: make(chan struct{}, 1)}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- lane.report(ctx) }()
		readReport := func() pendingTestReport {
			t.Helper()
			synctest.Wait()
			select {
			case report := <-observer.reports:
				return report
			default:
				t.Fatal("expected a report")
			}
			return pendingTestReport{}
		}
		assertNoReport := func() {
			t.Helper()
			synctest.Wait()
			if len(observer.reports) != 0 {
				t.Fatal("unexpected report")
			}
		}
		synctest.Sleep(time.Second)
		assertNoReport()
		if err := lane.progress.addData(16, 160); err != nil {
			t.Fatal(err)
		}
		first := readReport()
		if first.report.DataPackets != 16 {
			t.Fatal("first feedback was not immediate")
		}
		if err := lane.progress.addPing(1); err != nil {
			t.Fatal(err)
		}
		first.complete(false)
		synctest.Sleep(4*lane.reportInterval - time.Nanosecond)
		assertNoReport()
		synctest.Sleep(time.Nanosecond)
		retry := readReport()
		if retry.report.PingID != 1 {
			t.Fatal("retry lost ping progress")
		}
		first.complete(true)
		assertNoReport()
		if err := lane.progress.addData(1, 20); err != nil {
			t.Fatal(err)
		}
		retry.complete(true)
		next := readReport()
		if next.report.DataPackets != 17 {
			t.Fatal("pending changes were lost")
		}
		next.complete(true)
		assertNoReport()
		synctest.Sleep(time.Second)
		assertNoReport()
		if err := lane.progress.addPing(2); err != nil {
			t.Fatal(err)
		}
		ping := readReport()
		if ping.report.PingID != 2 {
			t.Fatal("idle ping feedback was delayed")
		}
		ping.complete(true)
		assertNoReport()
		for range reportPacketThreshold / 16 {
			if err := lane.progress.addData(16, 16); err != nil {
				t.Fatal(err)
			}
		}
		threshold := readReport()
		if threshold.report.DataPackets != reportPacketThreshold+17 {
			t.Fatal("packet threshold lost progress")
		}
		threshold.complete(true)
		assertNoReport()
		if err := lane.progress.addData(16, reportByteThreshold); err != nil {
			t.Fatal(err)
		}
		byteThreshold := readReport()
		if err := lane.progress.addData(16, reportByteThreshold); err != nil {
			t.Fatal(err)
		}
		assertNoReport()
		byteThreshold.complete(true)
		pending := readReport()
		if pending.report.DataPackets != reportPacketThreshold+49 {
			t.Fatal("pending byte threshold was delayed")
		}
		pending.complete(true)
		if lane.progress.reportedDataBytes != 180+reportPacketThreshold+2*reportByteThreshold {
			t.Fatal("completion lost exact byte threshold accounting")
		}
		assertNoReport()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
