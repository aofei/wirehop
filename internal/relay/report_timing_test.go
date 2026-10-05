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

func (o *reportTimingObserver) RouteDeliveryReport(report protocol.DeliveryReport, complete func(bool)) bool {
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
			lane := &Lane{
				laneID: protocol.LaneID{1}, generation: 1, observer: observer,
				reportInterval: 25 * time.Millisecond, progress: deliveryProgress{notify: make(chan struct{}, 1)},
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- lane.report(ctx) }()
			if err := lane.progress.addData(10); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			synctest.Sleep(lane.reportInterval)
			synctest.Wait()
			select {
			case attempt := <-observer.reports:
				if attempt.report.DataPackets != 1 || attempt.report.DataBytes != 10 {
					t.Fatalf("rejected report = %+v", attempt.report)
				}
			default:
				t.Fatal("initial routing attempt was not made")
			}
			if err := lane.progress.addData(20); err != nil {
				t.Fatal(err)
			}
			synctest.Sleep(4*lane.reportInterval - time.Nanosecond)
			synctest.Wait()
			select {
			case attempt := <-observer.reports:
				t.Fatalf("report retried before its retry interval: %+v", attempt.report)
			default:
			}
			synctest.Sleep(time.Nanosecond)
			synctest.Wait()
			select {
			case attempt := <-observer.reports:
				if attempt.report.DataPackets != 2 || attempt.report.DataBytes != 30 {
					t.Fatalf("retried report = %+v", attempt.report)
				}
				attempt.complete(true)
			default:
				t.Fatal("rejected report was not retried")
			}
			synctest.Wait()
			synctest.Sleep(time.Second)
			synctest.Wait()
			select {
			case attempt := <-observer.reports:
				t.Fatalf("completed report was retried: %+v", attempt.report)
			default:
			}
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("report() error = %v, want %v", err, context.Canceled)
			}
		})
	})
}

func testLaneReportCompletionAndIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observer := &reportTimingObserver{reports: make(chan pendingTestReport, 16)}
		lane := &Lane{
			laneID: protocol.LaneID{1}, generation: 1, observer: observer,
			reportInterval: 25 * time.Millisecond, progress: deliveryProgress{notify: make(chan struct{}, 1)},
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- lane.report(ctx) }()
		assertNoReport := func() {
			t.Helper()
			synctest.Wait()
			select {
			case report := <-observer.reports:
				t.Fatalf("unexpected report: %+v", report.report)
			default:
			}
		}
		readReport := func() pendingTestReport {
			t.Helper()
			synctest.Wait()
			select {
			case report := <-observer.reports:
				return report
			default:
				t.Fatal("expected report was not queued")
				return pendingTestReport{}
			}
		}
		synctest.Sleep(time.Second)
		assertNoReport()
		if err := lane.progress.addData(10); err != nil {
			t.Fatal(err)
		}
		assertNoReport()
		synctest.Sleep(lane.reportInterval - time.Nanosecond)
		assertNoReport()
		synctest.Sleep(time.Nanosecond)
		first := readReport()
		if first.report.DataPackets != 1 || first.report.DataBytes != 10 {
			t.Fatalf("sparse report = %+v", first.report)
		}
		// Changes arriving during a pending write remain coalesced until its retry interval.
		if err := lane.progress.addProbe(32); err != nil {
			t.Fatal(err)
		}
		first.complete(false)
		synctest.Sleep(4*lane.reportInterval - time.Nanosecond)
		assertNoReport()
		synctest.Sleep(time.Nanosecond)
		retry := readReport()
		if retry.report.ProbePackets != 1 || retry.report.ProbeBytes != 32 {
			t.Fatalf("retry report = %+v", retry.report)
		}
		// A late completion must not clear a newer pending claim.
		first.complete(true)
		assertNoReport()
		if err := lane.progress.addData(20); err != nil {
			t.Fatal(err)
		}
		retry.complete(true)
		assertNoReport()
		synctest.Sleep(lane.reportInterval)
		next := readReport()
		if next.report.DataPackets != 2 || next.report.DataBytes != 30 {
			t.Fatalf("progress after pending completion = %+v", next.report)
		}
		next.complete(true)
		assertNoReport()
		synctest.Sleep(time.Second)
		assertNoReport()
		// A probe must wake a fully idle report worker without a data threshold.
		if err := lane.progress.addProbe(32); err != nil {
			t.Fatal(err)
		}
		assertNoReport()
		synctest.Sleep(lane.reportInterval)
		probe := readReport()
		if probe.report.ProbePackets != 2 {
			t.Fatalf("probe-only report = %+v", probe.report)
		}
		probe.complete(true)
		assertNoReport()
		for range reportPacketThreshold {
			if err := lane.progress.addData(1); err != nil {
				t.Fatal(err)
			}
		}
		threshold := readReport()
		if threshold.report.DataPackets != reportPacketThreshold+2 {
			t.Fatalf("immediate threshold report = %+v", threshold.report)
		}
		threshold.complete(true)
		assertNoReport()
		if err := lane.progress.addData(reportByteThreshold); err != nil {
			t.Fatal(err)
		}
		byteThreshold := readReport()
		if byteThreshold.report.DataBytes != 30+reportPacketThreshold+reportByteThreshold {
			t.Fatalf("immediate byte threshold report = %+v", byteThreshold.report)
		}
		// A threshold reached during a pending write becomes immediate when that write completes.
		if err := lane.progress.addData(reportByteThreshold); err != nil {
			t.Fatal(err)
		}
		assertNoReport()
		byteThreshold.complete(true)
		pendingThreshold := readReport()
		if pendingThreshold.report.DataBytes != 30+reportPacketThreshold+2*reportByteThreshold ||
			pendingThreshold.report.DataPackets != reportPacketThreshold+4 {
			t.Fatalf("threshold accumulated during a pending write = %+v", pendingThreshold.report)
		}
		pendingThreshold.complete(true)
		assertNoReport()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("report() error = %v, want %v", err, context.Canceled)
		}
	})
}
