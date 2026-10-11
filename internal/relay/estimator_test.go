package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
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
		{name: "QueuedPressure", window: 32_768, packetCap: 64, queued: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			store := newDeliverySampleStore(t, &now)
			store.deliveryWindow.Store(test.window)
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

func TestTransmissionStorePreservesApplicationLimitedFlight(t *testing.T) {
	now := time.UnixMicro(1000)
	store := newDeliverySampleStore(t, &now)
	sendDeliverySamplePacket(t, store, 1)
	store.deliveryWindow.Store(initialDeliveryWindow)
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
		count, err := store.takeBatch(data[:], ownership[:], math.MaxInt)
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
	count, err := store.takeBatch(data[:], ownership[:], math.MaxInt)
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
