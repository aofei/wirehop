package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestLaneWriteControlBatch(t *testing.T) {
	t.Run("Fairness", func(t *testing.T) {
		for _, tt := range []struct {
			name         string
			timingFrames int
			reservedPong bool
		}{
			{name: "FullBudget"},
			{name: "RemainingBudget", timingFrames: 3},
			{name: "ReservedPong", timingFrames: 1, reservedPong: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				carrier := newTestCarrier()
				carrier.writes = make(chan []protocol.Frame, 2*maximumConsecutiveControlFrames)
				lane := newTestLane(t, carrier, newTestEndpoint())
				control, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: protocol.LaneID(1), Generation: 1})
				if err != nil {
					t.Fatal(err)
				}
				pong, err := protocol.MarshalTimingPong(protocol.TimingPong{ID: 1})
				if err != nil {
					t.Fatal(err)
				}
				for range tt.timingFrames {
					if tt.reservedPong {
						ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 1, SendMicros: 500})
						if err != nil {
							t.Fatal(err)
						}
						if err := lane.readPing(ping); err != nil {
							t.Fatal(err)
						}
						continue
					}
					if !lane.SendControl(pong, nil) {
						t.Fatal("failed to queue a timing frame")
					}
				}
				for range maximumConsecutiveControlFrames + 1 {
					if !lane.SendControl(control, nil) {
						t.Fatal("SendControl() rejected a bounded test write")
					}
				}
				transmission := schedulerTransmission(1, wgpacket.TransportData, time.Now().Add(time.Second))
				if err := lane.store.push(transmission); err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- lane.write(ctx) }()
				controlFrames := 0
				for {
					select {
					case batch := <-carrier.writes:
						if len(batch) < 1 || len(batch) > maximumConsecutiveControlFrames {
							t.Fatalf("invalid carrier batch size %d", len(batch))
						}
						if batch[0].Type == protocol.FrameData {
							if controlFrames != maximumConsecutiveControlFrames {
								t.Fatalf("control frames before data = %d, want %d", controlFrames,
									maximumConsecutiveControlFrames)
							}
							cancel()
							if err := <-result; !errors.Is(err, context.Canceled) {
								t.Fatalf("write() error = %v, want %v", err, context.Canceled)
							}
							return
						}
						controlFrames += len(batch)
					case <-time.After(time.Second):
						cancel()
						t.Fatal("timed out waiting for a data write")
					}
				}
			})
		}
	})
	t.Run("TimingBoundary", func(t *testing.T) {
		carrier := newTestCarrier()
		lane := newTestLane(t, carrier, newTestEndpoint())
		report, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: 1, Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		ping, err := protocol.MarshalTimingPing(protocol.TimingPing{ID: 1, SendMicros: 1000})
		if err != nil {
			t.Fatal(err)
		}
		lane.SendControl(report, nil)
		lane.SendControl(ping, nil)
		lane.SendControl(report, nil)
		count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
		if err != nil || count != 2 {
			t.Fatalf("count=%d error=%v", count, err)
		}
		batch := <-carrier.writes
		if len(batch) != 2 || batch[1].Type != protocol.FramePing || len(lane.control) != 1 {
			t.Fatal("timing frame was followed by additional queued work")
		}
	})
	t.Run("NoWaitAndSuccessfulCallback", func(t *testing.T) {
		carrier := newTestCarrier()
		lane := newTestLane(t, carrier, newTestEndpoint())
		frame, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: protocol.LaneID(1), Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		called := false
		lane.SendControl(frame, func() { called = true })
		count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
		if err != nil || count != 1 || !called {
			t.Fatalf("count=%d error=%v callback=%t", count, err, called)
		}
	})
	t.Run("FailedWriteSuppressesCallbacks", func(t *testing.T) {
		carrier := newTestCarrier()
		lane := newTestLane(t, carrier, newTestEndpoint())
		frame, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: protocol.LaneID(1), Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		called := 0
		for range 3 {
			lane.SendControl(frame, func() { called++ })
		}
		carrier.writes = make(chan []protocol.Frame)
		carrier.Close()
		count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
		if err == nil || count != 3 || called != 0 {
			t.Fatalf("count=%d error=%v callbacks=%d", count, err, called)
		}
	})
	t.Run("PartialCarrierWrite", func(t *testing.T) {
		writer, reader := net.Pipe()
		defer writer.Close()
		defer reader.Close()
		lane := &Lane{carrier: carrier.NewStreamConn(writer), clock: &testClock{now: 1000},
			control: make(chan controlWrite, maximumConsecutiveControlFrames), writeTimeout: time.Second}
		frame, err := protocol.MarshalDeliveryReport(protocol.DeliveryReport{LaneID: protocol.LaneID(1), Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		expected, err := protocol.MarshalFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		called := 0
		for range 3 {
			if !lane.SendControl(frame, func() { called++ }) {
				t.Fatal("failed to queue a control frame")
			}
		}
		prefix := make([]byte, len(expected))
		readDone := make(chan error, 1)
		go func() {
			_, err := io.ReadFull(reader, prefix)
			reader.Close()
			readDone <- err
		}()
		count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
		if err == nil || count != 3 || called != 0 {
			t.Fatalf("partial write: count %d, callbacks %d, error %v", count, called, err)
		}
		if err := <-readDone; err != nil || !bytes.Equal(prefix, expected) {
			t.Fatalf("partial write did not deliver exactly one frame: %v", err)
		}
		for _, frame := range lane.controlBatch {
			if frame.Payload != nil || frame.Type != 0 {
				t.Fatal("partial write retained frame storage")
			}
		}
	})
	t.Run("Boundaries", func(t *testing.T) {
		for _, boundary := range []protocol.FrameType{protocol.FramePing, protocol.FramePong, protocol.FrameClockSync,
			protocol.FrameSessionClose, protocol.FrameLaneAbandon, protocol.FrameError} {
			for _, preceding := range []bool{false, true} {
				carrier := newTestCarrier()
				lane := newTestLane(t, carrier, newTestEndpoint())
				ordinary := protocol.Frame{Type: protocol.FrameDeliveryReport}
				if preceding {
					lane.SendControl(ordinary, nil)
				}
				lane.SendControl(protocol.Frame{Type: boundary}, nil)
				lane.SendControl(ordinary, nil)
				count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
				want := 1
				if preceding {
					want = 2
				}
				if err != nil || count != want || len(lane.control) != 1 {
					t.Fatalf("boundary %d: count %d, error %v", boundary, count, err)
				}
				frames := <-carrier.writes
				if len(frames) != want || frames[want-1].Type != boundary {
					t.Fatalf("boundary %d: incorrect batch", boundary)
				}
			}
		}
	})
	t.Run("QueueOrderAfterWrite", func(t *testing.T) {
		carrier := newTestCarrier()
		lane := newTestLane(t, carrier, newTestEndpoint())
		var order []int
		for index := range 3 {
			lane.SendControl(protocol.Frame{Type: protocol.FrameDeliveryReport}, func() {
				if len(carrier.writes) != 1 {
					t.Fatal("callback ran before carrier write completion")
				}
				order = append(order, index)
			})
		}
		count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
		for _, frame := range lane.controlBatch {
			if frame.Payload != nil || frame.Type != 0 {
				t.Fatal("completed batch retained frame storage")
			}
		}
		if err != nil || count != 3 || len(order) != 3 {
			t.Fatalf("count %d, order %v, error %v", count, order, err)
		}
		for index, value := range order {
			if value != index {
				t.Fatal("callbacks changed queue order")
			}
		}
	})
	t.Run("BuilderFailure", func(t *testing.T) {
		carrier := newTestCarrier()
		lane := newTestLane(t, carrier, newTestEndpoint())
		called := false
		lane.SendControl(protocol.Frame{Type: protocol.FrameDeliveryReport}, func() { called = true })
		want := errors.New("scripted builder failure")
		lane.control <- controlWrite{build: func(uint64) (protocol.Frame, error) { return protocol.Frame{}, want }}
		count, err := lane.writeControlBatch(context.Background(), <-lane.control, maximumConsecutiveControlFrames)
		for _, frame := range lane.controlBatch {
			if frame.Payload != nil || frame.Type != 0 {
				t.Fatal("failed builder retained frame storage")
			}
		}
		if !errors.Is(err, want) || count != 1 || called || len(carrier.writes) != 0 {
			t.Fatalf("failed batch changed progress: count %d, error %v", count, err)
		}
	})
}
