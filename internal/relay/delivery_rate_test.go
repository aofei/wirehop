package relay

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestTransmissionStoreDeliverySample(t *testing.T) {
	now := time.UnixMicro(1000)
	store := newDeliverySampleStore(t, &now)
	sendDeliverySamplePacket(t, store, 1)
	now = time.UnixMicro(51_000)
	sendDeliverySamplePacket(t, store, 2)
	sample, stale, err := store.acknowledge(1, 4096, 101_000)
	if err != nil || stale || sample != (deliverySample{bytes: 4096, intervalMicros: 100_000}) {
		t.Fatalf("first acknowledgement = %+v, stale %t, error %v", sample, stale, err)
	}
	now = time.UnixMicro(102_000)
	sendDeliverySamplePacket(t, store, 3)
	sample, stale, err = store.acknowledge(3, 3*4096, 152_000)
	if err != nil || stale || sample != (deliverySample{bytes: 2 * 4096, intervalMicros: 101_000}) {
		t.Fatalf("send-limited sample = %+v, stale %t, error %v", sample, stale, err)
	}
	now = time.UnixMicro(2_000_000)
	sendDeliverySamplePacket(t, store, 4)
	sample, stale, err = store.acknowledge(4, 4*4096, 2_010_000)
	if err != nil || stale || sample != (deliverySample{bytes: 4096, intervalMicros: 10_000}) {
		t.Fatalf("sample after idle = %+v, stale %t, error %v", sample, stale, err)
	}
}

func TestTransmissionStoreDeliverySamplePreservesProgress(t *testing.T) {
	for _, test := range []struct {
		name       string
		packets    uint64
		bytes      uint64
		receive    uint64
		stale      bool
		err        error
		sampleSize uint64
	}{
		{name: "Duplicate", packets: 1, bytes: 4096, receive: 200_000, sampleSize: 3 * 4096},
		{name: "Stale", receive: 200_000, stale: true, sampleSize: 3 * 4096},
		{name: "Invalid", packets: 2, bytes: 8193, receive: 200_000, err: ErrInvalidDeliveryReport, sampleSize: 3 * 4096},
		{name: "EarlierReceiveTime", packets: 2, bytes: 8192, receive: 100_000, sampleSize: 2 * 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.UnixMicro(1000)
			store := newDeliverySampleStore(t, &now)
			for index, sent := range []int64{1000, 51_000, 52_000} {
				now = time.UnixMicro(sent)
				sendDeliverySamplePacket(t, store, uint64(index+1))
			}
			if _, _, err := store.acknowledge(1, 4096, 101_000); err != nil {
				t.Fatal(err)
			}
			sample, stale, err := store.acknowledge(test.packets, test.bytes, test.receive)
			if !errors.Is(err, test.err) || stale != test.stale || sample != (deliverySample{}) {
				t.Fatalf("non-sampling report = %+v, stale %t, error %v", sample, stale, err)
			}
			now = time.UnixMicro(112_000)
			sendDeliverySamplePacket(t, store, 4)
			sample, stale, err = store.acknowledge(4, 4*4096, 220_000)
			want := deliverySample{bytes: test.sampleSize, intervalMicros: 119_000}
			if err != nil || stale || sample != want {
				t.Fatalf("next valid report = %+v, stale %t, error %v, want %+v", sample, stale, err, want)
			}
			if packets, bytes := store.backlog(); packets != 0 || bytes != 0 {
				t.Fatalf("valid reports retained %d packets and %d bytes", packets, bytes)
			}
		})
	}
}

func TestTransmissionStoreDeliverySampleClockRange(t *testing.T) {
	for _, test := range []struct {
		name  string
		start uint64
	}{
		{name: "Zero", start: 0},
		{name: "Ordinary", start: 1000},
		{name: "BeyondSignedMicros", start: math.MaxInt64 + 1000},
		{name: "NearUnsignedLimit", start: math.MaxUint64 - 10_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := monotime.Time(test.start)
			store := newDeliverySampleStore(t, &now)
			sendDeliverySamplePacket(t, store, 1)
			sample, stale, err := store.acknowledge(1, 4096, test.start+1000)
			want := deliverySample{bytes: 4096, intervalMicros: 1000}
			if err != nil || stale || sample != want {
				t.Fatalf("clock sample = %+v, stale %t, error %v, want %+v", sample, stale, err, want)
			}
		})
	}
}

func TestTransmissionStoreDeliverySampleMigratedPacket(t *testing.T) {
	now := time.UnixMicro(1000)
	source := newDeliverySampleStore(t, &now)
	sendDeliverySamplePacket(t, source, 1)
	retained := source.drain()
	if len(retained) != 1 {
		t.Fatalf("drained %d packets, want 1", len(retained))
	}
	now = time.UnixMicro(11_000)
	destination := newDeliverySampleStore(t, &now)
	transmission := retained[0]
	transmission.migrated = true
	if err := destination.push(transmission); err != nil {
		releaseTransmissions(retained)
		t.Fatal(err)
	}
	takeOneTransmission(t, destination)
	sample, stale, err := destination.acknowledge(1, 4096, 12_000)
	want := deliverySample{bytes: 4096, intervalMicros: 1000}
	if err != nil || stale || sample != want {
		t.Fatalf("migrated sample = %+v, stale %t, error %v, want %+v", sample, stale, err, want)
	}
}

func newDeliverySampleStore(t *testing.T, now *time.Time) *TransmissionStore {
	t.Helper()
	store, err := newTransmissionStore(packetqueue.Limits{Packets: 64, Bytes: 64 * 4096}, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	return store
}

func sendDeliverySamplePacket(t *testing.T, store *TransmissionStore, packetID uint64) {
	t.Helper()
	transmission := schedulerTransmission(packetID, wgpacket.TransportData, store.now().Add(time.Second))
	payload := make([]byte, 4096-protocol.DataFrameOverhead)
	copy(payload, transmission.data.Payload)
	transmission.data.Payload = payload
	if err := store.push(transmission); err != nil {
		t.Fatal(err)
	}
	takeOneTransmission(t, store)
}
