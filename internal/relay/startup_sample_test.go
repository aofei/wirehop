package relay

import (
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestScheduledLaneInitialSampleQualification(t *testing.T) {
	for _, test := range []struct {
		name string
		kind wgpacket.Kind
		mtu  bool
	}{
		{name: "Handshake", kind: wgpacket.HandshakeInitiation},
		{name: "Keepalive", kind: wgpacket.TransportData},
		{name: "IsolatedTransport", kind: wgpacket.TransportData, mtu: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := newRateRecoveryStore(t, &now, 64, 65536)
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store), deliveryRate: defaultInitialRateBytesPerSecond}
			lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 10_000}, false)
			if test.mtu {
				pushRateRecoveryFrame(t, store, 1, 1458, now.Add(time.Second))
			} else if err := store.push(schedulerTransmission(1, test.kind, now.Add(time.Second))); err != nil {
				t.Fatal(err)
			}
			assertFlightCreditTake(t, store, 1)
			now = now.Add(100 * time.Millisecond)
			if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 1}, uint64(now.UnixMicro()), now); err != nil || !progressed {
				t.Fatalf("isolated report = %t, %v", progressed, err)
			}
			if lane.rateObserved || lane.deliveryRate != defaultInitialRateBytesPerSecond || store.deliveryWindow != initialDeliveryWindow || lane.rateHistoryCount != 0 {
				t.Fatalf("isolated startup frame established capacity: observed=%t rate=%d window=%d history=%d", lane.rateObserved, lane.deliveryRate, store.deliveryWindow, lane.rateHistoryCount)
			}
			// Three real MTU frames share a qualifying cumulative delivery sample, with no preceding ACK.
			for id := uint64(2); id <= 4; id++ {
				pushRateRecoveryFrame(t, store, id, 1458, now.Add(5*time.Second))
			}
			assertFlightCreditTake(t, store, 2, 3, 4)
			now = now.Add(time.Second)
			if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 4}, uint64(now.UnixMicro()), now); err != nil || !progressed {
				t.Fatalf("qualifying report = %t, %v", progressed, err)
			}
			if !lane.rateObserved || lane.deliveryRate != 4374 || lane.rateHistoryCount != 1 || store.transportReported.Load() != minimumTransportProofBytes {
				t.Fatalf("real startup cohort did not qualify: observed=%t rate=%d history=%d transport=%d", lane.rateObserved, lane.deliveryRate, lane.rateHistoryCount, store.transportReported.Load())
			}
			// Once initialized, a smaller positive sample can reveal greater capacity.
			pushRateRecoveryFrame(t, store, 5, 1458, now.Add(time.Second))
			assertFlightCreditTake(t, store, 5)
			now = now.Add(100 * time.Millisecond)
			if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: 5}, uint64(now.UnixMicro()), now); err != nil || !progressed {
				t.Fatalf("small measured report = %t, %v", progressed, err)
			}
			if lane.deliveryRate != 14580 || lane.rateHistoryCount != 2 {
				t.Fatalf("small positive sample was blocked after initialization: rate=%d history=%d", lane.deliveryRate, lane.rateHistoryCount)
			}
			assertTransmissionStoreInvariants(t, store)
		})
	}
}

func TestScheduledLaneIsolatedReverseFramesDoNotRequestTiming(t *testing.T) {
	now := time.Unix(100, 0)
	store := newRateRecoveryStore(t, &now, 64, 65536)
	lane := &scheduledLane{registration: schedulerRegistration(1, 1, store), deliveryRate: defaultInitialRateBytesPerSecond}
	lane.applyTiming(clockmap.Sample{LocalReceiveMicros: 100_000}, false)
	requests := 0
	lane.registration.RequestMeasurement = func() { requests++ }
	for id := uint64(1); id <= 64; id++ {
		// A 96-byte TransportData payload stands in for an encrypted TCP ACK. No payload content is parsed here.
		transmission := schedulerTransmission(id, wgpacket.TransportData, now.Add(5*time.Second))
		transmission.packet.Payload = make([]byte, 96)
		transmission.packet.Payload[0] = 4
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
		assertFlightCreditTake(t, store, id)
		now = now.Add(800 * time.Millisecond)
		if progressed, err := lane.applyReport(protocol.DeliveryReport{DataPackets: id}, uint64(now.UnixMicro()), now); err != nil || !progressed {
			t.Fatalf("report %d = %t, %v", id, progressed, err)
		}
		if store.sent.len() != 0 {
			t.Fatal("isolated report did not empty the prefix")
		}
		// Opposite-direction loading may inflate ordinary RTT without supplying a larger sample in this direction.
		roundTrip := lane.applyTiming(clockmap.Sample{LocalSendMicros: uint64(now.Add(-800 * time.Millisecond).UnixMicro()), LocalReceiveMicros: uint64(now.UnixMicro())}, false)
		lane.maybeRefreshTiming(now, roundTrip)
		assertTransmissionStoreInvariants(t, store)
	}
	if lane.deliveryFeedbackCycleMicros(lane.rttMicros) <= lane.deliveryWindowHorizonMicros() {
		t.Fatal("fixture did not exceed the ordinary timing horizon")
	}
	// This measures refresh requests only. It does not simulate measurement admission, actual pauses, or kernel TCP.
	t.Logf("observed=%t rate=%d operating_rtt=%d minimum_rtt=%d requests=%d cumulative_transport=%d", lane.rateObserved, lane.deliveryRate, lane.rttMicros, lane.minimumRTTMicros, requests, store.transportReported.Load())
	if lane.rateObserved || requests != 0 || lane.rateHistoryCount != 0 || store.deliveryWindow != initialDeliveryWindow {
		t.Fatalf("isolated reverse traffic enabled timing refresh: observed=%t rate=%d requests=%d window=%d", lane.rateObserved, lane.deliveryRate, requests, store.deliveryWindow)
	}
	if store.transportReported.Load() != minimumTransportProofBytes {
		t.Fatal("initial-sample qualification was conflated with cumulative transport proof")
	}
}
