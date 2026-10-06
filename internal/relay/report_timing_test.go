package relay

import (
	"context"
	"errors"
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
