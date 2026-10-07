package relay

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestLaneDelayedPongWithReceiveProgress(t *testing.T) {
	for _, name := range []string{"Data", "Ping", "Probe"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				carrier := newTestCarrier()
				endpoint := newTestEndpoint()
				lane := newTestLane(t, carrier, endpoint)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- lane.Run(ctx) }()
				ping := awaitLanePing(t, carrier)
				go func() {
					for {
						select {
						case <-carrier.writes:
						case <-endpoint.writes:
						case <-ctx.Done():
							return
						}
					}
				}()
				for index := range 10 {
					synctest.Sleep(time.Second)
					var frame protocol.Frame
					var err error
					switch name {
					case "Data":
						frame, err = protocol.MarshalData(protocol.Data{
							PacketID: uint64(index + 1), DeadlineMicros: 1000000,
							Payload: relayWireGuardPacket(wgpacket.TransportData),
						})
					case "Ping":
						frame, err = protocol.MarshalTimingPing(protocol.TimingPing{ID: uint64(index + 1), SendMicros: 1000})
					case "Probe":
						frame, err = protocol.MarshalData(protocol.Data{Payload: probePadding[:]})
					}
					if err != nil {
						t.Fatal(err)
					}
					carrier.reads <- frame
					synctest.Wait()
					select {
					case err := <-result:
						t.Fatalf("valid receive progress did not preserve the lane: %v", err)
					default:
					}
				}
				pong, err := protocol.MarshalTimingPong(protocol.TimingPong{
					ID: ping.ID, PingSendMicros: ping.SendMicros, ReceiveMicros: 1000, SendMicros: 1000,
				})
				if err != nil {
					t.Fatal(err)
				}
				carrier.reads <- pong
				synctest.Wait()
				synctest.Sleep(2*lane.pingInterval + lane.pingTimeout)
				if err := <-result; !errors.Is(err, ErrPingTimeout) {
					t.Fatalf("silent peer after delayed pong returned %v, want ping timeout", err)
				}
			})
		})
	}
}

func TestLaneReadPing(t *testing.T) {
	t.Run("FullControlQueue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			carrier := newTestCarrier()
			lane := newTestLane(t, carrier, newTestEndpoint())
			report, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: 1, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			for range cap(lane.control) {
				if !lane.SendControl(report, nil) {
					t.Fatal("failed to fill the control queue")
				}
			}
			ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 1, SendMicros: 500})
			if err != nil {
				t.Fatal(err)
			}
			if err := lane.readPing(ping); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- lane.write(ctx) }()
			synctest.Wait()
			found := false
			for len(carrier.writes) > 0 {
				for _, frame := range <-carrier.writes {
					if frame.Type != protocol.FramePong {
						continue
					}
					pong, err := protocol.ParseTimingPong(frame)
					if err != nil || pong.ID != 1 || pong.PingSendMicros != 500 || pong.ReceiveMicros != 1000 {
						t.Fatalf("pong = %+v, error %v", pong, err)
					}
					found = true
				}
				synctest.Wait()
			}
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("write() error = %v", err)
			}
			if !found {
				t.Fatal("full control queue permanently dropped the timing response")
			}
		})
	})
	t.Run("InvalidRequestPreservesQueuedResponse", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			id   uint64
		}{
			{name: "Duplicate", id: 1},
			{name: "Overlapping", id: 2},
		} {
			t.Run(tt.name, func(t *testing.T) {
				lane := newTestLane(t, newTestCarrier(), newTestEndpoint())
				first, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 1, SendMicros: 500})
				if err != nil {
					t.Fatal(err)
				}
				if err := lane.readPing(first); err != nil {
					t.Fatal(err)
				}
				invalid, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: tt.id, SendMicros: 600})
				if err != nil {
					t.Fatal(err)
				}
				if err := lane.readPing(invalid); !errors.Is(err, protocol.ErrInvalidControlFrame) || !IsProtocolViolation(err) {
					t.Fatalf("invalid request error = %v", err)
				}
				if len(lane.pong) != 1 {
					t.Fatal("invalid request changed the bounded response queue")
				}
				if lane.progress.pingID != 1 {
					t.Fatal("invalid request advanced cumulative parse progress")
				}
				frame, err := (<-lane.pong).build(1000)
				if err != nil {
					t.Fatal(err)
				}
				pong, err := protocol.ParseTimingPong(frame)
				if err != nil || pong.ID != 1 || pong.PingSendMicros != 500 {
					t.Fatalf("queued response = %+v, error %v", pong, err)
				}
			})
		}
	})
	t.Run("NextPingBeforeWriteCompletion", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			carrier := &incompletePongCarrier{testCarrier: newTestCarrier(), complete: make(chan struct{})}
			lane := newTestLane(t, carrier.testCarrier, newTestEndpoint())
			lane.carrier = carrier
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- lane.write(ctx) }()
			for id := uint64(1); id <= 2; id++ {
				ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: id, SendMicros: 500})
				if err != nil {
					t.Fatal(err)
				}
				if err := lane.readPing(ping); err != nil {
					t.Fatalf("ping %d before previous write completion: %v", id, err)
				}
				if id == 2 {
					close(carrier.complete)
				}
				batch := <-carrier.writes
				if len(batch) != 1 || batch[0].Type != protocol.FramePong {
					t.Fatalf("ping %d response = %v", id, batch)
				}
				pong, err := protocol.ParseTimingPong(batch[0])
				if err != nil || pong.ID != id {
					t.Fatalf("pong = %+v, error %v", pong, err)
				}
				synctest.Wait()
			}
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("write() error = %v", err)
			}
		})
	})
}

type incompletePongCarrier struct {
	*testCarrier
	complete chan struct{}
}

func (c *incompletePongCarrier) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	if err := c.testCarrier.WriteFrames(ctx, frames); err != nil {
		return err
	}
	select {
	case <-c.complete:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestLaneWriteControlProgressDuringContinuousPings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		carrier := &sequentialPingCarrier{testCarrier: newTestCarrier()}
		lane := newTestLane(t, carrier.testCarrier, newTestEndpoint())
		lane.carrier = carrier
		carrier.readPing = lane.readPing
		report, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: 1, Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		lane.SendControl(report, nil)
		ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 1, SendMicros: 500})
		if err != nil {
			t.Fatal(err)
		}
		if err := lane.readPing(ping); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- lane.write(ctx) }()
		pongs := 0
		found := false
		for !found {
			for _, frame := range <-carrier.writes {
				if frame.Type == protocol.FramePong {
					pongs++
				} else if frame.Type == protocol.FrameDeliveryReport {
					found = true
					break
				}
			}
		}
		cancel()
		carrier.Close()
		<-result
		if pongs != 1 {
			t.Fatalf("timing responses before queued report = %d, want 1", pongs)
		}
	})
}

type sequentialPingCarrier struct {
	*testCarrier
	readPing func(protocol.Frame) error
}

func (c *sequentialPingCarrier) WriteFrames(ctx context.Context, frames []protocol.Frame) error {
	if err := c.testCarrier.WriteFrames(ctx, frames); err != nil {
		return err
	}
	for _, frame := range frames {
		if frame.Type != protocol.FramePong {
			continue
		}
		pong, err := protocol.ParseTimingPong(frame)
		if err != nil {
			return err
		}
		if pong.ID <= maximumConsecutiveControlFrames {
			ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: pong.ID + 1, SendMicros: 500})
			if err != nil {
				return err
			}
			if err := c.readPing(ping); err != nil {
				return err
			}
		}
	}
	return nil
}
