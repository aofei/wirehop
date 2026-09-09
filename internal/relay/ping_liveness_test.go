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
	for _, frameType := range []protocol.FrameType{protocol.FrameData, protocol.FrameProbe} {
		name := "Data"
		if frameType == protocol.FrameProbe {
			name = "Probe"
		}
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
					if frameType == protocol.FrameData {
						frame, err = protocol.MarshalData(protocol.Data{
							PacketID: uint64(index + 1), DeadlineMicros: 1000000,
							Payload: relayWireGuardPacket(wgpacket.TransportData),
						})
					} else {
						frame, err = protocol.MarshalProbe(protocol.Probe{ID: uint64(index + 1)})
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
