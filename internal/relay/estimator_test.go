package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestScheduledLaneReportIgnoresLaterQueuePressure(t *testing.T) {
	for _, queuedAtReport := range []bool{false, true} {
		name := "IdleAtReport"
		if queuedAtReport {
			name = "NewWorkAtReport"
		}
		t.Run(name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			store := newDeliverySampleStore(t, &now)
			lane := &scheduledLane{
				registration: schedulerRegistration(1, 1, store),
				deliveryRate: 1_000_000, rateObserved: true, rttMicros: 10_000, minimumRTTMicros: 10_000,
			}
			sendDeliverySamplePacket(t, store, 1)
			now = now.Add(300 * time.Millisecond)
			if queuedAtReport {
				if err := store.push(schedulerTransmission(2, wgpacket.TransportData, now.Add(time.Second))); err != nil {
					t.Fatal(err)
				}
			}
			progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(now.UnixMicro()), now)
			if err != nil || !progressed {
				t.Fatalf("applyReport() = %t, %v", progressed, err)
			}
			if lane.deliveryRate != 1_000_000 {
				t.Fatalf("application-limited transmission changed rate to %d B/s", lane.deliveryRate)
			}
		})
	}
}

func TestScheduledLaneDeliveryRateAgesSamples(t *testing.T) {
	for _, test := range []struct {
		name    string
		rtt     uint64
		meanRTT uint64
		elapsed uint64
		want    uint64
	}{
		{name: "RecentPeak", elapsed: 999_999, want: 10_000_000},
		{name: "ExpiredPeak", elapsed: 1_000_000, want: 4096},
		{name: "SparseFeedback", elapsed: 30_000_000, want: 4096},
		{name: "MinimumRTT", rtt: 10_000, meanRTT: 2_000_000, elapsed: 1_000_000, want: 4096},
		{name: "MaximumRTT", rtt: math.MaxUint64, elapsed: 30_000_000, want: 10_000_000},
		{name: "RTTWindowOverflow", rtt: math.MaxUint64/4 + 1, elapsed: 30_000_000, want: 10_000_000},
		{name: "LongRTTPeak", rtt: 1_000_000, elapsed: 3_999_999, want: 10_000_000},
		{name: "LongRTTExpiredPeak", rtt: 1_000_000, elapsed: 4_000_000, want: 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			lane := &scheduledLane{minimumRTTMicros: test.rtt, rttMicros: test.meanRTT}
			lane.updateDeliveryRate(deliverySample{bytes: 100_000, intervalMicros: 10_000}, 1000)
			lane.updateDeliveryRate(deliverySample{bytes: 4096, intervalMicros: 1_000_000}, 1000+test.elapsed)
			if lane.deliveryRate != test.want {
				t.Fatalf("delivery rate = %d B/s, want %d", lane.deliveryRate, test.want)
			}
		})
	}
}

func TestScheduledLaneApplicationLimitedSamplesPreserveHistory(t *testing.T) {
	lane := &scheduledLane{}
	lane.updateDeliveryRate(deliverySample{bytes: 100_000, intervalMicros: 10_000}, 1000)
	before := *lane
	for index := range 20 {
		lane.updateDeliveryRate(deliverySample{bytes: 4096, intervalMicros: 1_000_000, applicationLimited: true}, uint64(index+1)*1_000_000)
	}
	if lane.deliveryRate != before.deliveryRate || lane.rateHistory != before.rateHistory ||
		lane.rateHistoryCount != before.rateHistoryCount || lane.rateHistoryNext != before.rateHistoryNext {
		t.Fatal("application idleness changed the capacity estimate or evicted its history")
	}
	lane.updateDeliveryRate(deliverySample{bytes: 200_000, intervalMicros: 10_000, applicationLimited: true}, 21_000_000)
	if lane.deliveryRate != 20_000_000 {
		t.Fatal("application-limited evidence of higher capacity was ignored")
	}
}

func TestTransmissionStoreCapturesSupplyPressureAtSend(t *testing.T) {
	for _, test := range []struct {
		name        string
		window      uint64
		packetCap   int
		queued      bool
		wantLimited bool
	}{
		{name: "Sparse", window: 32_768, packetCap: 64, wantLimited: true},
		{name: "HalfWindow", window: 8192, packetCap: 64},
		{name: "OddHalfWindow", window: 8193, packetCap: 64, wantLimited: true},
		{name: "PacketPressure", window: 32_768, packetCap: 1},
		{name: "QueuedBelowWindow", window: 32_768, packetCap: 64, queued: true, wantLimited: true},
		{name: "QueuedHalfWindow", window: 16_384, packetCap: 64, queued: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			store := newDeliverySampleStore(t, &now)
			store.setDeliveryWindows(test.window, test.window)
			store.limits.Packets = test.packetCap
			for id := uint64(1); id <= 2; id++ {
				if id == 2 && !test.queued {
					break
				}
				if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
					t.Fatal(err)
				}
			}
			takeOneTransmission(t, store)
			sample, stale, err := store.acknowledge(1, 2000)
			if err != nil || stale || sample.applicationLimited != test.wantLimited {
				t.Fatalf("sample = %+v, stale %t, error %v, want limited %t", sample, stale, err, test.wantLimited)
			}
		})
	}
}

func TestScheduledLanePartialBurstPreservesCapacity(t *testing.T) {
	for _, test := range []struct {
		name      string
		window    uint64
		packetCap int
		wantLower bool
	}{
		{name: "BelowWindow", window: 262_144, packetCap: 64},
		{name: "HalfWindow", window: 65_536, packetCap: 64, wantLower: true},
		{name: "PacketPressure", window: 262_144, packetCap: 16, wantLower: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			store := newDeliverySampleStore(t, &now)
			store.setDeliveryWindows(test.window, test.window)
			store.limits.Packets = test.packetCap
			lane := &scheduledLane{
				registration: schedulerRegistration(1, 1, store),
				rttMicros:    400_000, minimumRTTMicros: 300_000,
				timingReferenceRate: 1_000_000, timingReferenceRTT: 400_000,
			}
			lane.updateDeliveryRate(deliverySample{bytes: 100_000, intervalMicros: 100_000}, uint64(now.UnixMicro()))
			refreshes := 0
			lane.registration.RequestMeasurement = func() { refreshes++ }
			// The old capacity sample has aged out before the application supplies a finite burst.
			now = now.Add(2 * time.Second)
			for id := uint64(1); id <= 8; id++ {
				if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
					t.Fatal(err)
				}
			}
			// Each carrier batch leaves queued work until the last batch. Feedback can report any prefix.
			var data [2]protocol.Data
			var ownership [2]datagram.Packet
			for range 4 {
				count, err := store.takeBatch(data[:], ownership[:], math.MaxInt, 0)
				releaseBatchOwnership(ownership[:count])
				if err != nil || count != len(data) {
					t.Fatalf("takeBatch() = %d, %v", count, err)
				}
				now = now.Add(time.Microsecond)
			}
			now = now.Add(300 * time.Millisecond)
			for packets := uint64(2); packets <= 8; packets += 2 {
				progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: packets}, uint64(now.UnixMicro()), now)
				if err != nil || !progressed {
					t.Fatalf("report %d = %t, %v", packets, progressed, err)
				}
				lane.maybeRefreshTiming(now, lane.rttMicros)
				if !test.wantLower && lane.deliveryRate != 1_000_000 {
					t.Errorf("finite burst prefix %d lowered known capacity to %d B/s", packets, lane.deliveryRate)
				}
				now = now.Add(time.Millisecond)
			}
			if test.wantLower {
				if lane.deliveryRate >= 1_000_000 || refreshes != 0 {
					t.Fatalf("saturated burst did not expose capacity loss without a within-horizon refresh: rate %d B/s, refreshes %d", lane.deliveryRate, refreshes)
				}
			} else if refreshes != 0 {
				t.Fatalf("finite burst requested %d unnecessary timing refreshes", refreshes)
			}
		})
	}
}

func TestScheduledLaneTimingResumePreservesCapacity(t *testing.T) {
	for _, test := range []struct {
		name          string
		latePrefix    bool
		emptyResumes  int
		sparseFirst   bool
		repeatedPause bool
	}{
		{name: "CompletedPrefix"},
		{name: "LatePrefixReport", latePrefix: true},
		{name: "RepeatedEmptyResume", emptyResumes: 3},
		{name: "MixedSupply", sparseFirst: true},
		{name: "RepeatedPauseBeforeReport", repeatedPause: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			store, err := newTransmissionStore(packetqueue.Limits{Packets: 64, Bytes: 65_536}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { releaseTransmissions(store.drain()) })
			store.setDeliveryWindows(65_536, 65_536)
			lane := &scheduledLane{
				registration: schedulerRegistration(1, 1, store),
				rttMicros:    400_000, minimumRTTMicros: 300_000,
				timingReferenceRate: 1_000_000, timingReferenceRTT: 400_000,
			}
			lane.updateDeliveryRate(deliverySample{bytes: 100_000, intervalMicros: 100_000}, uint64(now.UnixMicro()))
			refreshes := 0
			lane.registration.RequestMeasurement = func() { refreshes++ }
			queue := func(first, count uint64) {
				t.Helper()
				for id := first; id < first+count; id++ {
					if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
						t.Fatal(err)
					}
				}
			}
			commit := func() {
				t.Helper()
				var data [2]protocol.Data
				var ownership [2]datagram.Packet
				count, err := store.takeBatch(data[:], ownership[:], math.MaxInt, 0)
				releaseBatchOwnership(ownership[:count])
				if err != nil || count != len(data) {
					t.Fatalf("takeBatch() = %d, %v", count, err)
				}
				now = now.Add(time.Microsecond)
			}
			report := func(packets uint64) {
				t.Helper()
				now = now.Add(time.Millisecond)
				progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: packets}, uint64(now.UnixMicro()), now)
				if err != nil || !progressed {
					t.Fatalf("report %d = %t, %v", packets, progressed, err)
				}
				lane.maybeRefreshTiming(now, lane.rttMicros)
			}
			// An aged peak must survive the intentional pause, without relying on recent rate history.
			now = now.Add(2 * time.Second)
			queue(1, 2)
			commit()
			now = now.Add(20 * time.Millisecond)
			store.beginMeasurementPause()
			if !test.latePrefix {
				report(2)
			}
			store.endMeasurementPause()
			for range test.emptyResumes {
				store.beginMeasurementPause()
				store.endMeasurementPause()
			}
			if test.sparseFirst {
				queue(3, 2)
			} else {
				queue(3, 12)
			}
			commit()
			if test.sparseFirst {
				queue(5, 10)
			}
			if test.latePrefix {
				// A canceled or expired drain can receive this old prefix after resuming Data.
				report(2)
			}
			firstReport := uint64(4)
			if test.repeatedPause {
				store.beginMeasurementPause()
				store.endMeasurementPause()
				now = now.Add(20 * time.Millisecond)
				report(4)
				firstReport = 6
			}
			for range 5 {
				commit()
			}
			now = now.Add(300 * time.Millisecond)
			report(firstReport)
			// Cleanup can end an already ended pause and must not arm another recovery interval.
			store.endMeasurementPause()
			// Six new frames fit the remaining flight credit and preserve half-window pressure.
			// These commits must regain normal rate-loss detection after the first resumed ACK.
			queue(15, 6)
			for range 3 {
				commit()
			}
			for packets := firstReport + 2; packets <= 14; packets += 2 {
				report(packets)
			}
			if lane.deliveryRate != 1_000_000 || refreshes != 0 {
				t.Errorf("resumed flight changed capacity to %d B/s and requested %d refreshes", lane.deliveryRate, refreshes)
			}
			now = now.Add(300 * time.Millisecond)
			report(16)
			if lane.deliveryRate >= 1_000_000 || refreshes != 0 {
				t.Fatalf("self-clocked constrained flight hid capacity loss or refreshed within the horizon: rate %d B/s, refreshes %d", lane.deliveryRate, refreshes)
			}
		})
	}
}

func TestTransmissionStorePreservesApplicationLimitedFlight(t *testing.T) {
	now := time.UnixMicro(1000)
	store := newDeliverySampleStore(t, &now)
	sendDeliverySamplePacket(t, store, 1)
	store.setDeliveryWindows(32_768, 32_768)
	var data [8]protocol.Data
	var ownership [8]datagram.Packet
	for flight := uint64(0); flight < 2; flight++ {
		start := uint64(2)
		if flight == 1 {
			start = 9
		}
		for id := start; id <= start+6; id++ {
			if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
				t.Fatal(err)
			}
		}
		count, err := store.takeBatch(data[:], ownership[:], math.MaxInt, 0)
		releaseBatchOwnership(ownership[:count])
		if err != nil || count != 7 {
			t.Fatalf("takeBatch() = %d, %v", count, err)
		}
		now = now.Add(time.Millisecond)
		sample, stale, err := store.acknowledge(start+6, uint64(now.UnixMicro()))
		if err != nil || stale || sample.applicationLimited != (flight == 0) {
			t.Fatalf("flight %d sample = %+v, stale %t, error %v", flight, sample, stale, err)
		}
	}
}

func TestTransmissionStorePreservesSupplyStateAcrossPartialBatchReports(t *testing.T) {
	now := time.UnixMicro(1000)
	store := newDeliverySampleStore(t, &now)
	for id := uint64(1); id <= 2; id++ {
		if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
			t.Fatal(err)
		}
	}
	var data [2]protocol.Data
	var ownership [2]datagram.Packet
	count, err := store.takeBatch(data[:], ownership[:], math.MaxInt, 0)
	releaseBatchOwnership(ownership[:count])
	if err != nil || count != 2 {
		t.Fatalf("takeBatch() = %d, %v", count, err)
	}
	for id := uint64(1); id <= 2; id++ {
		sample, stale, err := store.acknowledge(id, 1000+id*1000)
		if err != nil || stale || !sample.applicationLimited {
			t.Fatalf("report %d lost the batch supply state: %+v, stale %t, error %v", id, sample, stale, err)
		}
	}
}
