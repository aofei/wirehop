package relay

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestLaneSendDeliveryReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
		parsedAt := time.Now()
		report := protocol.DeliveryReport{LaneID: 2, Generation: 3, DataPackets: 4, PingID: 5}
		synctest.Sleep(10 * time.Millisecond)
		called := false
		if !lane.SendDeliveryReport(report, parsedAt, func() { called = true }) {
			t.Fatal("report was not queued")
		}
		synctest.Sleep(15 * time.Millisecond)
		request := <-lane.control
		frame, err := request.build(lane.clock.NowMicros())
		if err != nil {
			t.Fatal(err)
		}
		got, err := protocol.ParseDeliveryReport(frame)
		want := report
		want.DelayMicros = 25_000
		if err != nil || got != want || called {
			t.Fatalf("report = %+v, error %v, completion %t", got, err, called)
		}
		request.sent()
		if !called {
			t.Fatal("report completion was lost")
		}
		for range cap(lane.control) {
			lane.control <- controlWrite{}
		}
		if lane.SendDeliveryReport(report, parsedAt, nil) {
			t.Fatal("full control queue accepted a report")
		}
	})
}

func TestLanePingExposure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
		for range cap(lane.control) {
			lane.control <- controlWrite{}
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- lane.ping(ctx) }()
		synctest.Sleep(lane.pingInterval)
		synctest.Wait()
		for len(lane.control) > 0 {
			<-lane.control
		}
		if lane.ValidatePingProgress(1) {
			t.Fatal("unqueued ping was exposed")
		}
		synctest.Sleep(3 * lane.pingInterval)
		synctest.Wait()
		request := <-lane.control
		frame, err := request.build(lane.clock.NowMicros())
		if err != nil {
			t.Fatal(err)
		}
		ping, err := protocol.ParseTimingPing(frame)
		if err != nil || ping.ID != 1 || !lane.ValidatePingProgress(1) || lane.ValidatePingProgress(2) {
			t.Fatalf("ping = %+v, error %v", ping, err)
		}
		if !lane.pendingPingAt.IsZero() {
			t.Fatal("parse exposure started the write inactivity timeout")
		}
		cancel()
		<-result
	})
}

func TestLaneEffectiveReportInterval(t *testing.T) {
	lane := &Lane{reportInterval: 25 * time.Millisecond}
	for _, tt := range []struct {
		rtt  uint64
		want time.Duration
	}{
		{want: 25 * time.Millisecond},
		{rtt: 1, want: time.Millisecond},
		{rtt: 40_000, want: 10 * time.Millisecond},
		{rtt: 200_000, want: 25 * time.Millisecond},
	} {
		lane.reportRTTMicros.Store(tt.rtt)
		if got := lane.effectiveReportInterval(); got != tt.want {
			t.Fatalf("RTT %d: interval %v, want %v", tt.rtt, got, tt.want)
		}
	}
}

func TestLaneObserveTiming(t *testing.T) {
	for _, tt := range []struct {
		name   string
		sample clockmap.Sample
		want   uint64
	}{
		{name: "ZeroSpan", want: 1},
		{name: "EqualProcessing", sample: clockmap.Sample{LocalReceiveMicros: 20, RemoteSendMicros: 20}, want: 1},
		{name: "LongerProcessing", sample: clockmap.Sample{LocalReceiveMicros: 20, RemoteSendMicros: 21}, want: 1},
		{name: "PositiveRTT", sample: clockmap.Sample{LocalReceiveMicros: 20, RemoteSendMicros: 15}, want: 5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
			lane.reportInterval = defaultReportInterval
			lane.reportRTTMicros.Store(100_000)
			lane.observeTiming(tt.sample)
			if got := lane.reportRTTMicros.Load(); got != tt.want {
				t.Fatalf("feedback RTT = %d, want %d", got, tt.want)
			}
			if got := lane.effectiveReportInterval(); got != time.Millisecond {
				t.Fatalf("feedback interval = %v, want %v", got, time.Millisecond)
			}
		})
	}
}

func TestSelectCandidatesKeepsCommittedSamePath(t *testing.T) {
	first := schedulerLaneWithLimits(t, 1, 1, 2000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 1 << 20})
	second := schedulerLaneWithLimits(t, 2, 1, 2000, 1_000_000, packetqueue.Limits{Packets: 64, Bytes: 1 << 20})
	lanes := map[protocol.LaneID]*scheduledLane{1: first, 2: second}
	store := first.registration.Store
	t.Cleanup(func() { releaseTransmissions(store.drain()) })
	for id := uint64(1); id <= 16; id++ {
		transmission := schedulerTransmission(id, wgpacket.TransportData, time.Now().Add(time.Second))
		payload := make([]byte, 1400)
		copy(payload, transmission.packet.Payload)
		transmission.packet.Release()
		transmission.packet = datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}
		transmission.size = uint32(dataFrameSize(transmission.data()))
		if err := store.push(transmission); err != nil {
			t.Fatal(err)
		}
		takeOneTransmission(t, store)
	}
	if got := selectCandidates(lanes, 1, 1500, math.MaxUint64); got.lanes[0] != first {
		t.Fatal("unreported committed work caused same-path striping")
	}
	if err := store.push(schedulerTransmission(17, wgpacket.TransportData, time.Now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if got := selectCandidates(lanes, 1, 1500, math.MaxUint64); got.lanes[0] != second {
		t.Fatal("real write backlog did not permit same-path spillover")
	}
	first.degraded = true
	if got := selectCandidates(lanes, 1, 1500, math.MaxUint64); got.lanes[0] != second {
		t.Fatal("degraded preferred lane blocked failover")
	}
	first.degraded = false
	takeOneTransmission(t, store)
	second.registration.PathGroupID = 2
	if got := selectCandidates(lanes, 1, 1500, math.MaxUint64); got.lanes[0] != second {
		t.Fatal("distinct path selection was suppressed")
	}
}

func TestScheduledLaneDeliveryWindow(t *testing.T) {
	lane := schedulerLaneWithLimits(t, 1, 1, 200_000, 1_000_000, packetqueue.Limits{Packets: 65_536, Bytes: 32 << 20})
	lane.rateObserved = true
	if got := lane.deliveryWindowBytes(); got != 904_096 {
		t.Fatalf("initial window = %d", got)
	}
	lane.deliveryRate = 120_000_000
	if got := lane.deliveryWindowBytes(); got != 32<<20 {
		t.Fatalf("high-bandwidth window = %d", got)
	}
	lane.feedbackDelayMicros = math.MaxUint64
	if got := lane.deliveryWindowBytes(); got != 32<<20 {
		t.Fatalf("overflow window = %d", got)
	}
	lane.feedbackDelayMicros = 0
	lane.rttMicros = 1
	lane.deliveryRate = 1
	if got := lane.deliveryWindowBytes(); got != minimumDeliveryWindow {
		t.Fatalf("small-packet floor = %d", got)
	}
	store := lane.registration.Store
	store.backlogBytes.Store(minimumDeliveryWindow - 100)
	if !lane.canAccept(100) || lane.canAccept(101) {
		t.Fatal("estimated window admitted an oversized next frame")
	}
	lane.rateObserved = false
	store.backlogBytes.Store(initialDeliveryWindow - 100)
	if !lane.canAccept(100) || lane.canAccept(101) {
		t.Fatal("startup window is not bounded")
	}
}

func TestScheduledLaneFeedbackDelayIncludesReportWait(t *testing.T) {
	lane := schedulerLaneWithLimits(t, 1, 1, 20_000, 1_000_000, packetqueue.Limits{Packets: 65_536, Bytes: 32 << 20})
	source := schedulerLane(t, 2, 2, 100_000, 1_000_000)
	source.minimumRTTMicros = 100_000
	var scheduler Scheduler
	var preferred protocol.LaneID
	result := make(chan error, 1)
	scheduler.applyEvent(map[protocol.LaneID]*scheduledLane{1: lane, 2: source}, &preferred, schedulerEvent{
		kind: schedulerReport, laneID: 2, generation: 1, report: protocol.DeliveryReport{LaneID: 1, Generation: 1, PingID: 1, DelayMicros: 25_000}, result: result,
	}, time.Now())
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if lane.feedbackDelayMicros != 75_000 {
		t.Fatalf("feedback delay = %d", lane.feedbackDelayMicros)
	}
	if window := lane.registration.Store.deliveryWindow.Load(); window != lane.deliveryWindowBytes() {
		t.Fatalf("sampling window = %d, want %d after feedback", window, lane.deliveryWindowBytes())
	}
	scheduler.applyEvent(map[protocol.LaneID]*scheduledLane{1: lane}, &preferred, schedulerEvent{
		kind: schedulerTiming, laneID: 1, generation: 1,
		timing: clockmap.Sample{LocalSendMicros: 1000, RemoteReceiveMicros: 2000, RemoteSendMicros: 2000, LocalReceiveMicros: 11_000},
	}, time.Now())
	if window := lane.registration.Store.deliveryWindow.Load(); window != lane.deliveryWindowBytes() {
		t.Fatalf("sampling window = %d, want %d after timing", window, lane.deliveryWindowBytes())
	}
	if saturatingAdd(math.MaxUint64, 1) != math.MaxUint64 {
		t.Fatal("feedback delay wrapped")
	}
}

func TestRetainedTransmissionLayout(t *testing.T) {
	type previousTransmission struct {
		packetID     uint64
		wireDeadline uint64
		deadline     time.Time
		migrated     bool
		payloadBytes uint32
		size         int
		budget       *retention.Budget
		packet       datagram.Packet
		delivery     deliverySnapshot
	}
	t.Logf("retained transmission metadata: %d bytes, previous %d bytes", unsafe.Sizeof(retainedTransmission{}), unsafe.Sizeof(previousTransmission{}))
	type previousPacket struct {
		datagram.Packet
		DeadlineMicros uint64
	}
	t.Logf("ingress packet metadata: %d bytes, previous %d bytes", unsafe.Sizeof(Packet{}), unsafe.Sizeof(previousPacket{}))
}

func TestLanePhaseSpreadsCompactIdentifiers(t *testing.T) {
	const spread = 250 * time.Millisecond
	earliest, latest := spread, time.Duration(0)
	for id := protocol.LaneID(1); id <= 16; id++ {
		phase := lanePhase(id, 1, spread)
		if phase < 0 || phase >= spread || phase != lanePhase(id, 1, spread) || phase == lanePhase(id, 2, spread) {
			t.Fatalf("invalid stable phase %v for lane %d", phase, id)
		}
		earliest = min(earliest, phase)
		latest = max(latest, phase)
	}
	if latest-earliest < spread/2 {
		t.Fatalf("compact lane IDs clustered within %v", latest-earliest)
	}
}

func TestReceiverDeliverQuantizedDeadline(t *testing.T) {
	for _, test := range []struct {
		name   string
		offset int64
	}{
		{name: "SameClock"},
		{name: "ReceiverAhead", offset: 10_000},
		{name: "ReceiverBehind", offset: -10_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := new(testClock)
			endpoint := newTestEndpoint()
			receiver, err := NewReceiver(ReceiverConfig{
				Endpoint: endpoint, Clock: clock, DeduplicationSize: 64,
				ClockMapping: clockmap.Mapping{OffsetMicros: test.offset},
			})
			if err != nil {
				t.Fatal(err)
			}
			payload := relayWireGuardPacket(wgpacket.TransportData)
			for phase := range protocol.DeadlineResolutionMicros {
				senderNow := uint64(100_000) + phase
				clock.now = uint64(int64(senderNow) + test.offset)
				frame, err := protocol.MarshalData(protocol.Data{
					PacketID: phase + 1, DeadlineMicros: senderNow + protocol.MaxPacketLifetimeMicros, Payload: payload,
				})
				if err != nil {
					t.Fatalf("phase %d: MarshalData() error = %v", phase, err)
				}
				data, err := protocol.ParseData(frame)
				if err != nil {
					t.Fatalf("phase %d: ParseData() error = %v", phase, err)
				}
				if err := receiver.Deliver(t.Context(), data); err != nil {
					t.Fatalf("phase %d: maximum-lifetime delivery error = %v", phase, err)
				}
				select {
				case got := <-endpoint.writes:
					if string(got) != string(payload) {
						t.Fatalf("phase %d: delivered payload differs", phase)
					}
				default:
					t.Fatalf("phase %d: maximum-lifetime packet was not delivered", phase)
				}
				data.PacketID += protocol.DeadlineResolutionMicros
				data.DeadlineMicros += protocol.DeadlineResolutionMicros
				if err := receiver.Deliver(t.Context(), data); !errors.Is(err, ErrInvalidPacketDeadline) {
					t.Fatalf("phase %d: excessive lifetime error = %v", phase, err)
				}
				if len(endpoint.writes) != 0 {
					t.Fatalf("phase %d: excessive-lifetime packet reached UDP", phase)
				}
			}
		})
	}
}
