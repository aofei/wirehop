package relay

import (
	"math"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestTransportQueueControlDrop(t *testing.T) {
	t.Run("FeedbackBurstAllowance", func(t *testing.T) {
		lane := schedulerLaneWithLimits(t, 1, 1, 300_000, 6_250_000,
			packetqueue.Limits{Packets: 65536, Bytes: 32 << 20})
		now := time.Unix(100, 0)
		control := transportQueueControl{
			lane:       protocol.LaneGeneration{LaneID: 1, Generation: lane.registration.Generation},
			aboveSince: now.Add(-2 * time.Second), lastDequeue: now.Add(-time.Millisecond),
		}
		if control.drop(now, uint64(now.Add(-25*time.Millisecond).UnixMicro()), 2<<20, 1500, lane) ||
			!control.aboveSince.IsZero() {
			t.Fatal("a normal feedback-paced burst inherited persistent congestion")
		}
	})

	t.Run("StartupFlightCanDrain", func(t *testing.T) {
		lane := schedulerLaneWithLimits(t, 1, 1, 300_000, 6_250_000,
			packetqueue.Limits{Packets: 65536, Bytes: 32 << 20})
		var control transportQueueControl
		start := time.Unix(100, 0)
		for step := range 13 {
			now := start.Add(time.Duration(step) * 100 * time.Millisecond)
			if control.drop(now, uint64(start.Add(-20*time.Millisecond).UnixMicro()), 2<<20, 1500, lane) {
				t.Fatal("a startup burst was dropped before the modeled committed flight could drain")
			}
		}
		now := start.Add(1250 * time.Millisecond)
		if control.drop(now, uint64(now.UnixMicro()), 0, 1500, lane) || !control.aboveSince.IsZero() {
			t.Fatal("a drained startup burst left a congestion episode")
		}
	})

	t.Run("PersistentCongestionAndQueueDrain", func(t *testing.T) {
		lane := schedulerLane(t, 1, 1, 10_000, 1_250_000)
		var control transportQueueControl
		start := time.Unix(100, 0)
		for step := range 51 {
			now := start.Add(time.Duration(step) * 10 * time.Millisecond)
			dropped := control.drop(now, uint64(now.Add(-50*time.Millisecond).UnixMicro()), 64*1024, 1500, lane)
			if step < 14 && dropped || step == 14 && !dropped {
				t.Fatalf("step %d dropped = %t", step, dropped)
			}
		}
		if control.count < 3 {
			t.Fatal("persistent congestion did not increase the signaling rate")
		}
		now := start.Add(510 * time.Millisecond)
		if control.drop(now, uint64(start.UnixMicro()), minimumDeliveryWindow, 1500, lane) {
			t.Fatal("queue control consumed the final useful backlog")
		}
		if !control.aboveSince.IsZero() || control.dropping {
			t.Fatal("draining the queue retained congestion state")
		}
		if control.drop(now.Add(time.Millisecond), uint64(start.UnixMicro()), 64*1024, 1500, lane) {
			t.Fatal("a fresh burst inherited the previous congestion episode")
		}
	})

	for _, tt := range []struct {
		name     string
		rtt      uint64
		latest   uint64
		rate     uint64
		frame    uint64
		age      time.Duration
		interval time.Duration
		backlog  int
	}{
		{name: "ShortPath", rtt: 10_000, rate: 1_250_000, frame: 1500, age: 50 * time.Millisecond,
			interval: 140 * time.Millisecond, backlog: 64 * 1024},
		{name: "HighRTT", rtt: 1_200_000, rate: 2_500_000, frame: 1500, age: 2 * time.Second,
			interval: 4900 * time.Millisecond, backlog: 64 * 1024},
		{name: "RecentDelayIncrease", rtt: 10_000, latest: 1_200_000, rate: 2_500_000, frame: 1500,
			age: 50 * time.Millisecond, interval: 2400 * time.Millisecond, backlog: 64 * 1024},
		{name: "Slow32Kbit", rtt: 10_000, rate: 4000, frame: 1500, age: time.Second,
			interval: 1500 * time.Millisecond, backlog: 64 * 1024},
		{name: "JumboSerialization", rtt: 10_000, rate: 4000, frame: 64_000, age: 40 * time.Second,
			interval: 64 * time.Second, backlog: 128 * 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lane := schedulerLane(t, 1, 1, tt.rtt, tt.rate)
			lane.latestRTTMicros = tt.latest
			var control transportQueueControl
			start := time.Unix(100, 0)
			for step := range 100 {
				now := start.Add(tt.interval * time.Duration(step) / 100)
				if control.drop(now, uint64(now.Add(-tt.age).UnixMicro()), tt.backlog, tt.frame, lane) {
					t.Fatalf("transient queue dropped at step %d", step)
				}
			}
			now := start.Add(tt.interval)
			if !control.drop(now, uint64(now.Add(-tt.age).UnixMicro()), tt.backlog, tt.frame, lane) {
				t.Fatal("persistent congestion did not signal at the interval boundary")
			}
		})
	}

	for _, change := range []string{
		"Generation", "Lane", "ServiceInterruption", "FreshHead", "MissingTimestamp", "ReversedTimestamp",
	} {
		t.Run(change, func(t *testing.T) {
			lane := schedulerLane(t, 1, 1, 10_000, 1_250_000)
			var control transportQueueControl
			start := time.Unix(100, 0)
			control.drop(start, uint64(start.Add(-50*time.Millisecond).UnixMicro()), 64*1024, 1500, lane)
			now := start.Add(140 * time.Millisecond)
			queued := uint64(now.Add(-50 * time.Millisecond).UnixMicro())
			switch change {
			case "Generation":
				lane.registration.Generation++
			case "Lane":
				lane.registration.LaneID++
			case "ServiceInterruption":
				now = now.Add(time.Microsecond)
			case "FreshHead":
				queued = uint64(now.UnixMicro())
			case "MissingTimestamp":
				queued = 0
			case "ReversedTimestamp":
				queued = uint64(now.Add(time.Microsecond).UnixMicro())
			}
			if control.drop(now, queued, 64*1024, 1500, lane) {
				t.Fatal("changed service or a fresh head inherited a congestion drop")
			}
		})
	}

	t.Run("SaturatedRTT", func(t *testing.T) {
		lane := schedulerLane(t, 1, 1, math.MaxUint64, 1_250_000)
		var control transportQueueControl
		now := time.Unix(100, 0)
		control.drop(now, uint64(now.Add(-20*time.Millisecond).UnixMicro()), 64*1024, 1500, lane)
		if control.drop(now.Add(time.Hour), uint64(now.UnixMicro()), 64*1024, 1500, lane) {
			t.Fatal("RTT overflow fabricated a short congestion interval")
		}
	})

	for _, scenario := range []string{"RTTIncrease", "CapacityDecrease"} {
		t.Run(scenario, func(t *testing.T) {
			lane := schedulerLane(t, 1, 1, 10_000, 1_250_000)
			var control transportQueueControl
			start := time.Unix(100, 0)
			queued := uint64(start.Add(-2 * time.Second).UnixMicro())
			control.drop(start, queued, 64*1024, 1500, lane)
			interval := 2400 * time.Millisecond
			if scenario == "RTTIncrease" {
				lane.latestRTTMicros = 1_200_000
			} else {
				lane.deliveryRate = 4000
				interval = 1500 * time.Millisecond
			}
			if control.drop(start.Add(100*time.Millisecond), queued, 64*1024, 1500, lane) {
				t.Fatal("changed path used the previous shorter congestion interval")
			}
			if !control.drop(start.Add(interval), queued, 64*1024, 1500, lane) {
				t.Fatal("changed path permanently deferred persistent congestion")
			}
		})
	}
}

func TestSchedulerScheduleTransportQueue(t *testing.T) {
	for _, scenario := range []string{"Congestion", "Control", "InitialKeepalive", "Paused", "NoLaneCredit", "Untimed"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Unix(100, 0)
			budget, err := retention.NewBudget(retention.Limits{Packets: 64, Bytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			queue, err := packetqueue.NewWithBudget[Packet](packetqueue.Limits{Packets: 32, Bytes: 64 * 1024},
				budget, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer queue.Close()
			store, err := NewTransmissionStoreWithBudget(packetqueue.Limits{Packets: 32, Bytes: 64 * 1024},
				budget, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { releaseTransmissions(store.drain()) }()
			lane := &scheduledLane{registration: schedulerRegistration(1, 1, store), rttMicros: 10_000,
				deliveryRate: 1_250_000, rateObserved: true}
			scheduler := Scheduler{ingress: queue, packetID: 7, transportQueue: transportQueueControl{
				lane:       protocol.LaneGeneration{LaneID: 1, Generation: lane.registration.Generation},
				aboveSince: now.Add(-140 * time.Millisecond), lastDequeue: now.Add(-time.Millisecond),
			}}
			kind, size := wgpacket.TransportData, 1400
			preferred := protocol.LaneID(1)
			switch scenario {
			case "Control":
				kind, size = wgpacket.CookieReply, 64
			case "InitialKeepalive":
				size, preferred = wgpacket.TransportKeepaliveLength, 0
			case "Paused":
				scheduler.measurementStarted = now
				store.beginMeasurementPause()
			case "NoLaneCredit":
				store.limits.Packets = 1
				if err := store.push(schedulerTransmission(1, wgpacket.TransportData, now.Add(time.Second))); err != nil {
					t.Fatal(err)
				}
			}
			pending := ingressProbeItem(kind, size, now.Add(5*time.Second))
			pending.Value.queuedMicros = uint64(now.Add(-50 * time.Millisecond).UnixMicro())
			if scenario == "Untimed" {
				pending.Value.queuedMicros = 0
			}
			if err := queue.Push(pending); err != nil {
				t.Fatal(err)
			}
			if err := queue.TryPop(&pending, now); err != nil {
				t.Fatal(err)
			}
			for range 6 {
				if err := queue.Push(ingressProbeItem(wgpacket.TransportData, 1400, now.Add(time.Second))); err != nil {
					t.Fatal(err)
				}
			}
			before := budget.Usage()
			progress, err := scheduler.schedule(map[protocol.LaneID]*scheduledLane{1: lane}, &preferred, &pending, now)
			if err != nil || progress != (scenario != "NoLaneCredit") {
				t.Fatalf("schedule = %t, %v", progress, err)
			}
			if scenario == "Congestion" {
				want := retention.Usage{Packets: before.Packets - 1, Bytes: before.Bytes - size}
				if scheduler.packetID != 7 || budget.Usage() != want {
					t.Fatal("congestion drop consumed an identity or retained the packet's reservation")
				}
			} else if scenario == "NoLaneCredit" {
				if scheduler.packetID != 7 || budget.Usage() != before || len(pending.Value.Payload) == 0 {
					t.Fatal("blocked admission dropped the pending packet")
				}
			} else if scheduler.packetID != 8 {
				t.Fatal("exempt or untimed traffic was dropped")
			}
			pending.Release()
			releaseTransmissions(store.drain())
			queue.Close()
			if budget.Usage() != (retention.Usage{}) {
				t.Fatal("queue congestion handling leaked aggregate ownership")
			}
		})
	}
}

func TestSchedulerUpdateMeasurementDrainResetsCongestion(t *testing.T) {
	now := time.Now()
	lane := schedulerLane(t, 1, 1, 10_000, 1_250_000)
	lane.registration.Store.now = func() time.Time { return now }
	drain := &measurementDrain{ctx: t.Context(), target: protocol.LaneGeneration{
		LaneID: 1, Generation: lane.registration.Generation,
	}, ready: make(chan struct{})}
	scheduler := Scheduler{measurementDrain: drain, transportQueue: transportQueueControl{
		aboveSince: now.Add(-time.Second), dropNext: now, dropping: true, count: 100,
	}}
	scheduler.updateMeasurementDrain(map[protocol.LaneID]*scheduledLane{1: lane})
	if scheduler.measurementTimer != nil {
		scheduler.measurementTimer.Stop()
	}
	if scheduler.measurementStarted.IsZero() || scheduler.transportQueue != (transportQueueControl{}) {
		t.Fatal("starting a timing pause retained congestion from before the pause")
	}
}
