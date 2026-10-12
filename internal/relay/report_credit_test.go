package relay

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestScheduledLaneApplyReportCredit(t *testing.T) {
	for _, test := range []struct {
		name      string
		rate      uint64
		committed uint64
		reported  uint64
		elapsed   time.Duration
		wantRate  uint64
		want      uint64
	}{
		{name: "ShrinkWithExistingPrefix", rate: 400_000, committed: 8, reported: 4,
			elapsed: 400 * time.Millisecond, wantRate: 40_960, want: 9830},
		{name: "GrowFromReportedCapacity", rate: 10_000, committed: 1, reported: 1,
			elapsed: 10 * time.Millisecond, wantRate: 409_600, want: 61_440},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newFlightCreditStore(t, &now, 65_536)
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store),
				rttMicros: 10_000, minimumRTTMicros: 10_000, deliveryRate: test.rate, rateObserved: true}
			lane.registration.ValidatePingProgress = func(uint64) bool {
				if !store.mu.TryLock() {
					t.Fatal("report validation ran under the store lock")
				}
				store.mu.Unlock()
				return true
			}
			store.setDeliveryWindows(lane.deliveryWindowBytes(), lane.probeWindowBytes(0))
			for id := uint64(1); id <= 16; id++ {
				if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
					t.Fatal(err)
				}
			}
			for range test.committed {
				takeOneTransmission(t, store)
			}
			consumeFlightCreditReady(store)
			now = now.Add(test.elapsed)
			progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: test.reported}, uint64(now.UnixMicro()), now)
			if err != nil || !progressed {
				t.Fatalf("applyReport() = %t, %v", progressed, err)
			}
			if lane.deliveryRate != test.wantRate || store.deliveryWindow != test.want ||
				store.probeWindow != lane.probeWindowBytes(0) {
				t.Fatalf("report did not publish sampled flight credit: rate %d, normal %d, probe %d, want rate %d, normal %d, probe %d",
					lane.deliveryRate, store.deliveryWindow, store.probeWindow, test.wantRate, test.want, lane.probeWindowBytes(0))
			}
			if usage := store.budget.Usage(); usage.Packets != 16-int(test.reported) || usage.Bytes != (16-int(test.reported))*4096 {
				t.Fatalf("report lost retained ownership: %+v", usage)
			}
			assertFlightCreditReady(t, store)
			if test.reported < test.committed {
				// The old committed suffix survives a shrink. Newly queued frames must wait for it.
				if store.sentBytes-store.reportedBytes != (test.committed-test.reported)*4096 {
					t.Fatal("shrinking credit changed the already committed suffix")
				}
				assertFlightCreditTake(t, store)
				assertFlightCreditSilent(t, store)
				now = now.Add(test.elapsed)
				if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: test.committed}, uint64(now.UnixMicro()), now); err != nil || !progressed {
					t.Fatalf("remaining prefix report = %t, %v", progressed, err)
				}
				assertFlightCreditReady(t, store)
				assertFlightCreditTake(t, store, 9, 10)
			} else {
				assertFlightCreditTake(t, store, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
			}
			assertTransmissionStoreInvariants(t, store)
		})
	}
}

func TestScheduledLaneApplyReportCreditUnchanged(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   uint64
		ping   uint64
		closed bool
		want   error
	}{
		{name: "Duplicate", data: 1, ping: 1},
		{name: "Stale"},
		{name: "InvalidPrefix", data: 5, ping: 1, want: ErrInvalidDeliveryReport},
		{name: "InvalidPing", data: 2, ping: 2, want: ErrInvalidDeliveryReport},
		{name: "MixedDirections", data: 2, want: ErrInvalidDeliveryReport},
		{name: "ClosedStore", data: 2, ping: 1, closed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newFlightCreditStore(t, &now, 65_536)
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store),
				rttMicros: 10_000, minimumRTTMicros: 10_000, deliveryRate: 400_000, rateObserved: true}
			lane.registration.ValidatePingProgress = func(id uint64) bool { return id <= 1 }
			for id := uint64(1); id <= 5; id++ {
				if err := store.push(deliverySampleTransmission(t, store, id)); err != nil {
					t.Fatal(err)
				}
			}
			for range 4 {
				takeOneTransmission(t, store)
			}
			now = now.Add(10 * time.Millisecond)
			if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 1, PingID: 1, DelayMicros: 1234}, uint64(now.UnixMicro()), now); err != nil || !progressed {
				t.Fatalf("initial report = %t, %v", progressed, err)
			}
			if test.closed {
				releaseTransmissions(store.drain())
			}
			lane.degraded = true
			before := *lane
			before.registration = LaneRegistration{}
			credit := reportCreditSnapshot(store)
			usage := store.budget.Usage()
			consumeFlightCreditReady(store)
			progressed, err := lane.applyReport(protocol.DeliveryReport{
				DataPackets: test.data, PingID: test.ping, DelayMicros: math.MaxUint64,
			}, uint64(now.Add(time.Second).UnixMicro()), now.Add(time.Second))
			if progressed || !errors.Is(err, test.want) {
				t.Fatalf("nonadvancing report = %t, %v, want false, %v", progressed, err, test.want)
			}
			after := *lane
			after.registration = LaneRegistration{}
			if !reflect.DeepEqual(before, after) || credit != reportCreditSnapshot(store) || usage != store.budget.Usage() {
				t.Fatal("nonadvancing report changed estimates, credit, prefix state, or retained ownership")
			}
			assertFlightCreditSilent(t, store)
		})
	}
}

func TestSchedulerApplyReportRoutedCredit(t *testing.T) {
	for _, delay := range []uint64{50_000, math.MaxUint64} {
		name := "RawAndReturnDelay"
		if delay == math.MaxUint64 {
			name = "SaturatedDelay"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newFlightCreditStore(t, &now, 1<<20)
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store),
				rttMicros: 10_000, minimumRTTMicros: 10_000, deliveryRate: 10_000, rateObserved: true}
			source := schedulerLane(t, 2, 2, 200_000, 10_000)
			source.minimumRTTMicros = 200_000
			lanes := map[protocol.LaneID]*scheduledLane{1: lane, 2: source}
			report := protocol.DeliveryReport{LaneID: 1, Generation: 1, PingID: 1, DelayMicros: delay}
			result := make(chan error, 1)
			var scheduler Scheduler
			var preferred protocol.LaneID
			scheduler.applyEvent(lanes, &preferred, schedulerEvent{
				kind: schedulerReport, laneID: 2, generation: 1, report: report, result: result,
			}, now)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			want := uint64(11_296)
			if delay == math.MaxUint64 {
				want = 1 << 20
			}
			if lane.feedbackDelayMicros != saturatingAdd(delay, 100_000) ||
				store.deliveryWindow != want || store.probeWindow != lane.probeWindowBytes(0) {
				t.Fatal("routed report published credit without its return-path delay")
			}
		})
	}
}

func reportCreditSnapshot(store *TransmissionStore) [14]uint64 {
	return [14]uint64{store.deliveryWindow, store.probeWindow, store.writeBudget.Load(),
		store.sentPackets, store.sentBytes, store.reportedPackets, store.reportedBytes,
		store.firstSentMicros, store.deliveredMicros, store.applicationLimitedUntil,
		store.transportReported.Load(), uint64(store.packets), uint64(store.bytes), uint64(store.sent.len())}
}
