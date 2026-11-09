package relay

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
)

func TestMeasurementCoordinatorPartialCompletion(t *testing.T) {
	for _, tt := range []struct {
		name        string
		loseReply   bool
		finishWrite bool
	}{
		{name: "LostReply", loseReply: true},
		{name: "IncompleteResultWrite"},
		{name: "UndeliveredResult", finishWrite: true},
	} {
		for _, failed := range []protocol.LaneID{1, 2} {
			t.Run(fmt.Sprintf("%sLane%d", tt.name, failed), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client := newMeasurementFixture(t, true, 2)
					server := newMeasurementFixture(t, false, 2)
					assertPaused := func(endpoint *measurementFixture, want bool) {
						t.Helper()
						if (endpoint.scheduler.measurements.active != nil) != want ||
							endpoint.scheduler.pendingMeasurementDrain.Load() != want ||
							(endpoint.scheduler.input.pauseOwner != nil) != want {
							t.Fatal("partial completion left inconsistent session pause ownership")
						}
						for _, lane := range endpoint.lanes {
							store := lane.registration.Store
							if store.measurementDrain.Load() != want || store.measurementSupplyPaused != want {
								t.Fatal("a carrier retained or retracted the wrong Data gate")
							}
						}
					}
					for id := protocol.LaneID(1); id <= 2; id++ {
						client.scheduler.RequestMeasurement(protocol.LaneGeneration{LaneID: id, Generation: 1})
					}
					client.apply(t)
					for range 2 {
						client.write(t, 1).deliver(t, server, 1)
						server.apply(t)
					}
					for range 2 {
						server.write(t, 1).deliver(t, client, 1)
					}
					for range 2 {
						client.write(t, 100).deliver(t, server, 200)
					}
					clientDeadline := client.scheduler.measurementDeadline
					serverDeadline := server.scheduler.measurementDeadline
					var late measurementTransmission
					for range 2 {
						reply := server.write(t, 250)
						if tt.loseReply && reply.source.LaneID == failed {
							late = reply
							continue
						}
						reply.deliver(t, client, 400)
					}
					client.apply(t)
					results := 2
					if tt.loseReply {
						results--
					}
					for range results {
						result := client.write(t, 500)
						if !result.value.Valid {
							t.Fatal("independent completed timing was rejected")
						}
						if result.source.LaneID == failed {
							late = result
							if tt.finishWrite {
								result.sent()
							}
							continue
						}
						result.sent()
						result.deliver(t, server, 600)
					}
					client.apply(t)
					server.apply(t)
					assertPaused(client, !tt.finishWrite)
					assertPaused(server, true)
					if client.scheduler.measurementDeadline != clientDeadline ||
						server.scheduler.measurementDeadline != serverDeadline {
						t.Fatal("partial completion released or extended an outstanding quiet promise")
					}
					for id := protocol.LaneID(1); id <= 2; id++ {
						wantClient, wantServer := uint64(250), uint64(250)
						if id == failed {
							wantServer = 10_000
							if tt.loseReply {
								wantClient = 10_000
							}
						}
						if client.lanes[id].minimumRTTMicros != wantClient || server.lanes[id].minimumRTTMicros != wantServer {
							t.Fatal("timing publication crossed an incomplete carrier exchange")
						}
					}
					time.Sleep(time.Until(clientDeadline) - time.Nanosecond)
					client.apply(t)
					server.apply(t)
					assertPaused(client, !tt.finishWrite)
					assertPaused(server, true)
					time.Sleep(time.Nanosecond)
					client.apply(t)
					assertPaused(client, false)
					assertPaused(server, true)
					time.Sleep(time.Until(serverDeadline) - time.Nanosecond)
					server.apply(t)
					assertPaused(server, true)
					time.Sleep(time.Nanosecond)
					server.apply(t)
					assertPaused(server, false)
					failedClientRTT := client.lanes[failed].minimumRTTMicros
					late.sent()
					destination := server
					if tt.loseReply {
						destination = client
					}
					late.deliver(t, destination, 700)
					client.apply(t)
					server.apply(t)
					assertPaused(client, false)
					assertPaused(server, false)
					if client.lanes[failed].minimumRTTMicros != failedClientRTT ||
						server.lanes[failed].minimumRTTMicros != 10_000 {
						t.Fatal("a late phase published incomplete timing")
					}
				})
			})
		}
	}
}
