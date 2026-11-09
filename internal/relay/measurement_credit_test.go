package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/protocol"
)

func TestScheduledLaneDrainWindowBytes(t *testing.T) {
	for _, tt := range []struct {
		name     string
		observed bool
		rtt      uint64
		want     uint64
	}{
		{name: "Startup", rtt: 1_200_000, want: initialDeliveryWindow},
		{name: "Stable", observed: true, rtt: 10_000, want: 144_096},
		{name: "Increased", observed: true, rtt: 1_200_000, want: 4_904_096},
		{name: "Saturated", observed: true, rtt: math.MaxUint64, want: 8 << 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newFlightCreditStore(t, &now, 8<<20)
			lane := scheduledLane{
				registration: LaneRegistration{Store: store}, minimumRTTMicros: 10_000,
				rttMicros: 10_000, latestRTTMicros: tt.rtt, deliveryRate: 1_000_000, rateObserved: tt.observed,
			}
			if got := lane.drainWindowBytes(); got != tt.want {
				t.Fatalf("drainWindowBytes() = %d, want %d", got, tt.want)
			}
			if lane.minimumRTTMicros != 10_000 || lane.deliveryWindowHorizonMicros() != 140_000 {
				t.Fatal("drain credit altered the ordinary propagation horizon")
			}
		})
	}
}

func TestTransmissionStoreMeasurementSupplyPause(t *testing.T) {
	now := time.Unix(100, 0)
	store := newFlightCreditStore(t, &now, 1<<20)
	defer func() { releaseTransmissions(store.drain()) }()
	store.setDeliveryWindows(16*1024, 32*1024, 0)
	for id := uint64(1); id <= 12; id++ {
		pushFlightCreditPacket(t, store, id, 4000, now.Add(5*time.Second))
	}
	store.beginMeasurementSupplyPause(64 * 1024)
	frames := make([]protocol.Data, 16)
	owners := make([]datagram.Packet, len(frames))
	count, err := store.takeBatch(frames, owners, 64*1024, 0)
	for _, owner := range owners[:count] {
		owner.Release()
	}
	if err != nil || count != 12 {
		t.Fatalf("finite measurement cut remained trapped behind stale flight credit: count=%d, error=%v", count, err)
	}
	for _, transmission := range store.sent.items[store.sent.head:] {
		if !transmission.applicationLimited || transmission.deadlineMicros != uint64(now.Add(5*time.Second).UnixMicro()) {
			t.Fatal("finite supply lost its provenance or changed an accepted deadline")
		}
	}
	store.endMeasurementPause()
	if store.measurementSupplyPaused || store.measurementWindow != 0 || store.deliveryWindow != 16*1024 || store.probeWindow != 32*1024 {
		t.Fatal("measurement completion changed ordinary credit or retained temporary drain credit")
	}
	if _, stale, err := store.acknowledge(12, uint64(now.Add(time.Millisecond).UnixMicro())); err != nil || stale {
		t.Fatalf("acknowledge() = %t, %v", stale, err)
	}
	for id := uint64(13); id <= 24; id++ {
		pushFlightCreditPacket(t, store, id, 4000, now.Add(5*time.Second))
	}
	count, err = store.takeBatch(frames, owners, 64*1024, 0)
	for _, owner := range owners[:count] {
		owner.Release()
	}
	if err != nil || count != 4 {
		t.Fatalf("ordinary post-measurement credit was not restored: count=%d, error=%v", count, err)
	}
}
