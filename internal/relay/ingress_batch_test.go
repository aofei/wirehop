package relay

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/monotime"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestIngressRunBatch(t *testing.T) {
	for _, tt := range []struct {
		name       string
		limits     packetqueue.Limits
		budget     retention.Limits
		queueDelay time.Duration
		closed     bool
		wantIDs    []byte
		wantErr    error
	}{
		{
			name: "AggregatePackets", limits: packetqueue.Limits{Packets: 8, Bytes: 512, ControlPreemption: true},
			budget: retention.Limits{Packets: 4, Bytes: 512}, wantIDs: []byte{15, 1, 2, 3}, wantErr: io.EOF,
		},
		{
			name: "AggregateBytes", limits: packetqueue.Limits{Packets: 8, Bytes: 512, ControlPreemption: true},
			budget: retention.Limits{Packets: 16, Bytes: 128}, wantIDs: []byte{15, 2, 3}, wantErr: io.EOF,
		},
		{
			name: "QueueBytes", limits: packetqueue.Limits{Packets: 8, Bytes: 128, ControlPreemption: true},
			budget: retention.Limits{Packets: 16, Bytes: 512}, wantIDs: []byte{15, 2, 3}, wantErr: io.EOF,
		},
		{
			name: "NoPreemption", limits: packetqueue.Limits{Packets: 8, Bytes: 512},
			budget: retention.Limits{Packets: 4, Bytes: 512}, wantIDs: []byte{0, 1, 2, 3}, wantErr: io.EOF,
		},
		{
			name: "ExpiredNormals", limits: packetqueue.Limits{Packets: 8, Bytes: 512, ControlPreemption: true},
			budget: retention.Limits{Packets: 16, Bytes: 512}, queueDelay: 2 * time.Millisecond,
			wantIDs: []byte{15}, wantErr: io.EOF,
		},
		{
			name: "Closed", limits: packetqueue.Limits{Packets: 8, Bytes: 512, ControlPreemption: true},
			budget: retention.Limits{Packets: 16, Bytes: 512}, closed: true, wantErr: packetqueue.ErrClosed,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &ingressBatchEndpoint{testEndpoint: newTestEndpoint()}
			for index := range 16 {
				kind := wgpacket.TransportData
				if index == 15 {
					kind = wgpacket.CookieReply
				}
				payload := relayWireGuardPacket(kind)
				payload[4] = byte(index)
				endpoint.packets = append(endpoint.packets, datagram.Packet{Kind: kind, Payload: payload})
			}
			budget, err := retention.NewBudget(tt.budget)
			if err != nil {
				t.Fatal(err)
			}
			clock := &testClock{now: 1000}
			queue, err := packetqueue.NewWithBudget[Packet](tt.limits, budget, func() time.Time {
				return monotime.Time(clock.NowMicros()).Add(tt.queueDelay)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(queue.Close)
			if tt.closed {
				queue.Close()
			}
			policy := DeadlinePolicy{Control: time.Second, Transport: time.Millisecond}
			ingress, err := NewIngress(endpoint, queue, clock, policy)
			if err != nil {
				t.Fatal(err)
			}
			err = ingress.Run(t.Context())
			if !errors.Is(err, tt.wantErr) || errors.Is(err, ErrEndpointFailure) != errors.Is(tt.wantErr, io.EOF) {
				t.Fatalf("Run() = %v, want %v with the matching endpoint failure classification", err, tt.wantErr)
			}
			wantUsage := retention.Usage{Packets: len(tt.wantIDs)}
			for _, id := range tt.wantIDs {
				wantUsage.Bytes += len(endpoint.packets[id].Payload)
			}
			if queue.Len() != wantUsage.Packets || queue.Bytes() != wantUsage.Bytes || budget.Usage() != wantUsage {
				t.Fatalf("partial batch retained %d packets and %d bytes with usage %+v, want %+v",
					queue.Len(), queue.Bytes(), budget.Usage(), wantUsage)
			}
			for _, id := range tt.wantIDs {
				var item packetqueue.Item[Packet]
				if err := queue.TryPop(&item, queue.Now()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(item.Release)
				packet := endpoint.packets[id]
				if item.Value.Kind != packet.Kind || len(item.Value.Payload) != len(packet.Payload) ||
					item.Value.Payload[4] != id || &item.Value.Payload[0] != &packet.Payload[0] ||
					item.Value.queuedMicros != clock.NowMicros() ||
					item.Deadline != monotime.Time(clock.NowMicros()).Add(policy.Lifetime(packet.Kind)) {
					t.Fatalf("dequeued packet %d lost its payload, order, classification, or deadline", id)
				}
				if budget.Usage() != wantUsage {
					t.Fatal("dequeue released the caller's retained capacity")
				}
				item.Release()
				wantUsage.Packets--
				wantUsage.Bytes -= len(packet.Payload)
				if budget.Usage() != wantUsage {
					t.Fatal("release did not return the packet's exact charge")
				}
			}
			queue.Close()
			if queue.Len() != 0 || queue.Bytes() != 0 || budget.Usage() != (retention.Usage{}) {
				t.Fatal("ingress batch retained capacity after cleanup")
			}
		})
	}
}
