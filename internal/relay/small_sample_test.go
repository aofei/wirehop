package relay

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestScheduledLaneDeliveryRateRecoversSmallFlights(t *testing.T) {
	for _, test := range []struct {
		name      string
		pairedACK bool
	}{
		{name: "CompleteFlight"},
		{name: "PairedReports", pairedACK: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newRateRecoveryStore(t, &now, 512, 1<<20)
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store)}
			lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 10_000}, false)
			initialWindow := store.deliveryWindow
			pushRateRecoveryFrame(t, store, 1, 4096, now.Add(5*time.Second))
			assertFlightCreditTake(t, store, 1)
			seedBytes := store.sentBytes
			// Even a late parse report measures carrier progress after payload eligibility expires.
			now = now.Add(8192 * time.Millisecond)
			if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(now.UnixMicro()), now); err != nil || !progressed {
				t.Fatalf("seed report = %t, %v", progressed, err)
			}
			wantRate := seedBytes * 1_000_000 / 8_192_000
			if !lane.rateObserved || lane.deliveryRate != wantRate || store.deliveryWindow >= initialWindow || store.deliveryWindow >= 3*1458 {
				t.Fatalf("seed rate/window = %d/%d, want measured rate %d with less than three MTU frames", lane.deliveryRate, store.deliveryWindow, wantRate)
			}
			if wantRate != 500 || store.deliveryWindow != 4166 {
				t.Fatalf("quantization fixture rate/window = %d/%d, want 500/4166", wantRate, store.deliveryWindow)
			}
			otherStore := newRateRecoveryStore(t, &now, 8, 65536)
			other := &scheduledLane{registration: schedulerRegistration(2, 1, otherStore), rateObserved: true, deliveryRate: 1000, rttMicros: 10_000, minimumRTTMicros: 10_000}
			lanes := map[protocol.LaneID]*scheduledLane{1: lane, 2: other}
			if available, proven := hasTimelyAlternative(lanes, other, now, 1, 5_000_000); !available || proven {
				t.Fatalf("seed sample recovery proof = %t/%t, want available but unproven", available, proven)
			}
			packetID := uint64(1)
			for round := range 12 {
				for store.normal.len() < 128 {
					packetID++
					pushRateRecoveryFrame(t, store, packetID, 1458, now.Add(5*time.Second))
				}
				for {
					var data [16]protocol.Data
					var ownership [16]datagram.Packet
					count, err := store.takeBatch(data[:], ownership[:], targetDataBatchBytes, 0)
					releaseBatchOwnership(ownership[:count])
					if errors.Is(err, packetqueue.ErrEmpty) {
						break
					}
					if err != nil || count == 0 {
						t.Fatalf("round %d commitment = %d, %v", round, count, err)
					}
				}
				if store.normal.len() == 0 || store.sent.len() < 2 {
					t.Fatalf("round %d did not retain continuous demand and at least two committed frames", round)
				}
				if round == 0 && store.sent.len() != 2 {
					t.Fatalf("initial strict flight has %d frames, want two", store.sent.len())
				}
				acknowledged := store.sentPackets
				bytes := store.sentBytes - store.reportedBytes
				if test.pairedACK {
					// Two-frame reports model 2916 bytes every 37916 us, about 76906 bytes/s.
					acknowledged = store.reportedPackets + 2
					bytes = 2916
				}
				// Complete flights model 1 MB/s serialization plus fixed 35 ms feedback latency.
				now = now.Add(time.Duration(bytes+35_000) * time.Microsecond)
				if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: acknowledged}, uint64(now.UnixMicro()), now); err != nil || !progressed {
					t.Fatalf("round %d report = %t, %v", round, progressed, err)
				}
				if !test.pairedACK && store.sent.len() != 0 {
					t.Fatal("complete-flight report left an unacknowledged prefix")
				}
				assertTransmissionStoreInvariants(t, store)
			}
			if test.pairedACK {
				want := uint64(2916 * 1_000_000 / 37_916)
				if lane.deliveryRate < want-want/10 || lane.deliveryRate > want+want/10 {
					t.Fatalf("paired report recovery = %d bytes/s, want within 10 percent of %d", lane.deliveryRate, want)
				}
			} else if lane.deliveryRate <= 100_000 {
				t.Fatalf("continuous MTU traffic did not recover capacity: %d bytes/s", lane.deliveryRate)
			}
			if available, proven := hasTimelyAlternative(lanes, other, now, 1, 5_000_000); !available || !proven || store.transportReported.Load() != minimumTransportProofBytes {
				t.Fatalf("real transport recovery proof = %t/%t, bytes=%d", available, proven, store.transportReported.Load())
			}
		})
	}
}

func TestScheduledLaneSmallSampleGuards(t *testing.T) {
	for _, test := range []struct {
		name   string
		sample deliverySample
	}{
		{name: "ZeroBytes", sample: deliverySample{intervalMicros: 1000}},
		{name: "ZeroInterval", sample: deliverySample{bytes: 1}},
		{name: "SubMillisecond", sample: deliverySample{bytes: 1, intervalMicros: 999}},
		{name: "Overflow", sample: deliverySample{bytes: math.MaxUint64/1_000_000 + 1, intervalMicros: 1000}},
		{name: "TruncatedZeroRate", sample: deliverySample{bytes: 1, intervalMicros: 1_000_001}},
	} {
		t.Run(test.name, func(t *testing.T) {
			lane := &scheduledLane{}
			lane.updateDeliveryRate(test.sample, 1_000_000)
			if lane.rateObserved || lane.deliveryRate != 0 || lane.rateHistoryCount != 0 || lane.rateHistoryNext != 0 {
				t.Fatalf("unusable sample established capacity: %+v", test.sample)
			}
		})
	}
	t.Run("MillisecondBoundary", func(t *testing.T) {
		lane := &scheduledLane{rateObserved: true}
		lane.updateDeliveryRate(deliverySample{bytes: 1, intervalMicros: 1000}, 1_000_000)
		if !lane.rateObserved || lane.deliveryRate != 1000 || lane.rateHistoryCount != 1 {
			t.Fatalf("valid boundary sample = %d B/s, observed %t", lane.deliveryRate, lane.rateObserved)
		}
	})
	t.Run("ApplicationLimitedHistory", func(t *testing.T) {
		lane := &scheduledLane{}
		lane.updateDeliveryRate(deliverySample{bytes: 8000, intervalMicros: 2_000_000}, 1_000_000)
		before := *lane
		for _, bytes := range []uint64{1, 4} {
			lane.updateDeliveryRate(deliverySample{bytes: bytes, intervalMicros: 1000, applicationLimited: true}, 20_000_000)
			if lane.deliveryRate != before.deliveryRate || lane.rateHistory != before.rateHistory || lane.rateHistoryCount != before.rateHistoryCount || lane.rateHistoryNext != before.rateHistoryNext {
				t.Fatalf("lower or equal limited sample of %d bytes replaced capacity history", bytes)
			}
		}
		lane.updateDeliveryRate(deliverySample{bytes: 5, intervalMicros: 1000, applicationLimited: true}, 20_000_001)
		if lane.deliveryRate != 5000 || lane.rateHistoryCount != before.rateHistoryCount+1 {
			t.Fatal("higher limited sample failed to raise measured capacity")
		}
	})
}

func newRateRecoveryStore(t *testing.T, now *time.Time, packets, bytes int) *TransmissionStore {
	t.Helper()
	budget, err := retention.NewBudget(retention.Limits{Packets: packets, Bytes: bytes})
	if err != nil {
		t.Fatal(err)
	}
	store, err := newTransmissionStoreWithBudget(packetqueue.Limits{Packets: packets, Bytes: bytes}, func() time.Time { return *now }, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseTransmissions(store.drain())
		if got := budget.Usage(); got != (retention.Usage{}) {
			t.Errorf("cleanup retained ownership: %+v", got)
		}
	})
	return store
}

func pushRateRecoveryFrame(t *testing.T, store *TransmissionStore, id uint64, size int, deadline time.Time) {
	t.Helper()
	transmission := schedulerTransmission(id, wgpacket.TransportData, deadline)
	metadata := transmission.data()
	metadata.Payload = nil
	frame, err := protocol.MarshalData(metadata)
	if err != nil {
		t.Fatal(err)
	}
	contentSize := size - 2
	if contentSize >= 2048 {
		contentSize--
	}
	transmission.packet.Payload = make([]byte, contentSize-len(frame.Payload))
	transmission.packet.Payload[0] = 4
	if got := dataFrameSize(transmission.data()); got != size {
		t.Fatalf("encoded frame size = %d, want %d", got, size)
	}
	if err := store.push(transmission); err != nil {
		t.Fatal(err)
	}
}
