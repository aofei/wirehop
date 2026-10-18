package relay

import (
	"bytes"
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestMeasurementCoordinatorLiveLanes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		targets int
	}{
		{name: "SingleTarget", targets: 1},
		{name: "ConcurrentTargets", targets: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testMeasurementLiveTargets(t, tt.targets)
		})
	}
}

func testMeasurementLiveTargets(t *testing.T, targets int) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var workers []chan struct{}
		defer func() {
			cancel()
			for _, done := range workers {
				<-done
			}
		}()
		start := func(run func() error) {
			done := make(chan struct{})
			workers = append(workers, done)
			go func() {
				defer close(done)
				if err := run(); ctx.Err() == nil {
					t.Errorf("live worker exited before shutdown: %v", err)
				}
			}()
		}
		clock := &measurementLiveClock{origin: time.Now()}
		now := func() time.Time { return time.UnixMicro(int64(clock.NowMicros())) }
		var clientConnections, serverConnections [2]net.Conn
		for index := range clientConnections {
			clientConnections[index], serverConnections[index] = net.Pipe()
			t.Cleanup(func() {
				clientConnections[index].Close()
				serverConnections[index].Close()
			})
		}
		type endpoint struct {
			ingress   *packetqueue.Queue[Packet]
			scheduler *Scheduler
			udp       *testEndpoint
			lanes     [2]*Lane
			stores    [2]*TransmissionStore
			carriers  [2]*measurementLiveCarrier
		}
		makeEndpoint := func(connections [2]net.Conn, client bool) *endpoint {
			e := &endpoint{udp: newTestEndpoint()}
			t.Cleanup(func() { e.udp.Close() })
			var err error
			e.ingress, err = packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 64, Bytes: 2 << 20}, now)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(e.ingress.Close)
			e.scheduler, err = NewScheduler(e.ingress)
			if err != nil {
				t.Fatal(err)
			}
			start(func() error { return e.scheduler.Run(ctx) })
			receiver, err := NewReceiver(ReceiverConfig{Endpoint: e.udp, Clock: clock, DeduplicationSize: 64})
			if err != nil {
				t.Fatal(err)
			}
			initialTiming := clockmap.Sample{LocalReceiveMicros: 100_000}
			for index, connection := range connections {
				e.stores[index], err = newTransmissionStore(packetqueue.Limits{Packets: 64, Bytes: 2 << 20}, now)
				if err != nil {
					t.Fatal(err)
				}
				e.carriers[index] = &measurementLiveCarrier{
					Conn: carrier.NewStreamConn(measurementLiveConnection{Conn: connection}), stores: &e.stores,
				}
				laneContext, stopLane := context.WithCancel(ctx)
				e.lanes[index], err = NewLane(LaneConfig{
					Carrier: e.carriers[index], Receiver: receiver, Store: e.stores[index], Clock: clock,
					Observer: e.scheduler, LaneID: protocol.LaneID(index + 1), Generation: 1,
					ReportInterval: time.Millisecond, PingInterval: time.Hour, PingTimeout: 2 * time.Hour,
				})
				if err != nil {
					stopLane()
					t.Fatal(err)
				}
				lane := e.lanes[index]
				if err := e.scheduler.Register(ctx, LaneRegistration{
					LaneID: lane.laneID, Generation: 1, PathGroupID: 1, Store: lane.store,
					InitialTiming: &initialTiming, Abandon: stopLane, SendControl: lane.SendControl,
					ValidatePingProgress: lane.ValidatePingProgress, SendDeliveryReport: lane.SendDeliveryReport,
					RequestMeasurement: lane.RequestMeasurement, SendMeasurement: lane.SendMeasurement,
					MeasurementInitiator: client,
				}); err != nil {
					stopLane()
					t.Fatal(err)
				}
				start(func() error { return lane.Run(laneContext) })
			}
			return e
		}
		client := makeEndpoint(clientConnections, true)
		server := makeEndpoint(serverConnections, false)
		endpoints := []*endpoint{client, server}
		// Qualify actual rate observations without consuming an ordinary PacketID or retaining a standby prefix.
		for _, e := range endpoints {
			for _, store := range e.stores {
				if err := store.push(retainedTransmission{
					deadlineMicros: clock.NowMicros() + 1_000_000, packet: datagram.Packet{Payload: probePadding[:]},
				}); err != nil {
					t.Fatal(err)
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		var before [2][2]uint64
		for endpointIndex, e := range endpoints {
			for laneIndex, store := range e.stores {
				store.mu.Lock()
				before[endpointIndex][laneIndex] = store.deliveryWindow
				store.mu.Unlock()
				if !store.sentEmpty() || before[endpointIndex][laneIndex] <= initialDeliveryWindow {
					t.Fatal("live padding feedback did not qualify and drain the lane")
				}
			}
		}
		started := time.Now()
		for index := range targets {
			client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: protocol.LaneID(index + 1), Generation: 1})
		}
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		if time.Since(started) >= measurementPauseLimit {
			t.Fatal("live exchange completed only through deadline expiry")
		}
		for endpointIndex, e := range endpoints {
			e.scheduler.measurements.mu.Lock()
			active := e.scheduler.measurements.active
			e.scheduler.measurements.mu.Unlock()
			if active != nil || e.scheduler.pendingMeasurementDrain.Load() {
				t.Fatal("completed live exchange retained coordinator ownership")
			}
			for laneIndex, store := range e.stores {
				if store.measurementDrain.Load() {
					t.Fatal("completed live exchange retained a Data gate")
				}
				store.mu.Lock()
				window := store.deliveryWindow
				store.mu.Unlock()
				if laneIndex < targets && window >= before[endpointIndex][laneIndex] ||
					laneIndex >= targets && window != before[endpointIndex][laneIndex] {
					t.Fatalf("endpoint %d lane %d window = %d, before %d", endpointIndex, laneIndex+1,
						window, before[endpointIndex][laneIndex])
				}
			}
			if phases := e.carriers[1].observed(); targets == 1 && len(phases) != 0 {
				t.Fatalf("empty unrelated standby carried measurement phases: %+v", phases)
			}
		}
		readyCount := 0
		for index := range targets {
			for _, check := range []struct {
				stream *measurementLiveCarrier
				want   []protocol.MeasurementKind
				server bool
			}{
				{stream: client.carriers[index], want: []protocol.MeasurementKind{
					protocol.MeasurementBegin, protocol.MeasurementProbe, protocol.MeasurementResult}},
				{stream: server.carriers[index], want: []protocol.MeasurementKind{protocol.MeasurementReply}, server: true},
			} {
				phases := check.stream.observed()
				var kinds []protocol.MeasurementKind
				for _, phase := range phases {
					if phase.value.ID != 1 || phase.value.Kind != protocol.MeasurementBegin && !phase.paused {
						t.Fatalf("actual phase lost its operation or session hold: %+v", phase)
					}
					if phase.value.Kind == protocol.MeasurementResult && !phase.value.Valid {
						t.Fatal("live client did not publish valid timing evidence")
					}
					if check.server && phase.value.Kind == protocol.MeasurementReady {
						if len(kinds) != 0 || !phase.value.Valid {
							t.Fatal("session readiness followed a reply or lacked prefix proof")
						}
						readyCount++
						continue
					}
					kinds = append(kinds, phase.value.Kind)
				}
				if !slices.Equal(kinds, check.want) {
					t.Fatalf("written phases = %v, want %v", kinds, check.want)
				}
			}
		}
		if readyCount != targets {
			t.Fatalf("session readiness frames = %d, want one acknowledgment for each of %d targets", readyCount, targets)
		}
		for endpointIndex, e := range endpoints {
			payload := append(relayWireGuardPacket(wgpacket.TransportData), make([]byte, 16)...)
			payload[4] = byte(endpointIndex + 1)
			if err := e.ingress.Push(packetqueue.Item[Packet]{
				Value: newPacket(datagram.Packet{Kind: wgpacket.TransportData, Payload: payload}),
				Size:  len(payload), Deadline: now().Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Millisecond)
			synctest.Wait()
			select {
			case received := <-endpoints[1-endpointIndex].udp.writes:
				if !bytes.Equal(received, payload) {
					t.Fatal("ordinary live Data changed after measurement completion")
				}
			default:
				t.Fatal("ordinary live Data did not resume after measurement completion")
			}
		}
	})
}

type measurementLiveClock struct {
	origin time.Time
}

func (c *measurementLiveClock) NowMicros() uint64 {
	return 1_000 + uint64(time.Since(c.origin)/time.Microsecond)
}

type measurementLiveConnection struct {
	net.Conn
}

func (c measurementLiveConnection) Read(buffer []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return c.Conn.Read(buffer)
}

type measurementLivePhase struct {
	value  protocol.Measurement
	paused bool
}

type measurementLiveCarrier struct {
	carrier.Conn
	stores *[2]*TransmissionStore
	mu     sync.Mutex
	phases []measurementLivePhase
}

func (c *measurementLiveCarrier) ReadFrames(ctx context.Context, frames []protocol.Frame) (int, error) {
	return carrier.ReadFrames(ctx, c.Conn, frames)
}

func (c *measurementLiveCarrier) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	paused := true
	for _, store := range c.stores {
		paused = paused && store != nil && store.measurementDrain.Load()
	}
	if err := c.Conn.WriteFrames(ctx, frames); err != nil {
		return err
	}
	for _, frame := range frames {
		if frame.Type != protocol.FrameMeasurement {
			continue
		}
		value, err := protocol.ParseMeasurement(frame)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.phases = append(c.phases, measurementLivePhase{value: value, paused: paused})
		c.mu.Unlock()
	}
	return nil
}

func (c *measurementLiveCarrier) observed() []measurementLivePhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.phases)
}
