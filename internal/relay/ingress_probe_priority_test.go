package relay

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/datagram"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerRunUsefulIngressBeforePadding(t *testing.T) {
	t.Run("ReadyBurst", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fixture := newIngressProbeFixture(t)
			fixture.register(t, 1, packetqueue.Limits{Packets: 128, Bytes: 256 * 1024})
			fixture.register(t, 2, packetqueue.Limits{Packets: 128, Bytes: 256 * 1024})
			synctest.Wait()
			if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 0 {
				t.Fatal("initial primary discovery did not supply padding")
			}
			deadline := time.Now().Add(5 * time.Second)
			burst := make([]packetqueue.Item[Packet], 8)
			for index := range burst {
				burst[index] = ingressProbeItem(wgpacket.TransportData, 1452, deadline)
			}
			// The callback supplies the whole burst while Run owns the report event.
			// The real first report then measures the primary and frees its initial padding.
			fixture.beforeReport <- func() error {
				written, err := fixture.ingress.PushBatch(burst)
				if err == nil && written != len(burst) {
					return errors.New("incomplete ready-burst admission")
				}
				return err
			}
			time.Sleep(10 * time.Millisecond)
			fixture.report(t, 1)
			synctest.Wait()
			for index := range burst {
				data := takeOneTransmission(t, fixture.stores[0])
				if data.PacketID != uint64(index+1) {
					t.Fatalf("ready useful packet %d was preceded by newly admitted padding: packet ID %d", index+1, data.PacketID)
				}
				if data.DeadlineMicros != uint64(deadline.UnixMicro()) {
					t.Fatal("burst scheduling changed a useful packet deadline")
				}
			}
			if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 0 {
				t.Fatal("discovery did not resume after draining the ready useful burst")
			}
		})
	})

	t.Run("BlockedPrimaryStillDiscoversAlternate", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fixture := newIngressProbeFixture(t)
			deadline := time.Now().Add(5 * time.Second)
			for range 3 {
				if err := fixture.ingress.Push(ingressProbeItem(wgpacket.TransportData, 1452, deadline)); err != nil {
					t.Fatal(err)
				}
			}
			// The first real packet establishes a preferred lane and fills its only slot.
			fixture.register(t, 1, packetqueue.Limits{Packets: 1, Bytes: 4096})
			synctest.Wait()
			if packets, _ := fixture.stores[0].backlog(); packets != 1 || fixture.ingress.Len() != 1 {
				t.Fatalf("full-primary setup retained %d packets with %d waiting ingress packets", packets, fixture.ingress.Len())
			}
			fixture.register(t, 2, packetqueue.Limits{Packets: 16, Bytes: 64 * 1024})
			synctest.Wait()
			if packets, _ := fixture.stores[1].backlog(); packets == 0 {
				t.Fatal("blocked preferred lane and waiting ingress starved alternate discovery")
			}
			if data := takeOneTransmission(t, fixture.stores[1]); data.PacketID != 0 {
				t.Fatal("unknown alternate received useful spillover instead of bounded discovery")
			}
			if fixture.ingress.Len() != 1 {
				t.Fatal("alternate discovery consumed waiting ingress")
			}
			if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 1 || data.DeadlineMicros != uint64(deadline.UnixMicro()) {
				t.Fatal("blocked-primary discovery changed the original useful assignment or deadline")
			}
		})
	})

	t.Run("PreemptedNormalBeforeNewPadding", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fixture := newIngressProbeFixture(t)
			fixture.register(t, 1, packetqueue.Limits{Packets: 128, Bytes: 256 * 1024})
			fixture.register(t, 2, packetqueue.Limits{Packets: 128, Bytes: 256 * 1024})
			synctest.Wait()
			if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 0 {
				t.Fatal("initial primary discovery did not supply padding")
			}
			time.Sleep(10 * time.Millisecond)
			fixture.report(t, 1)
			synctest.Wait()
			// Keep the discovered prefix until feedback. Constrain hard packet capacity to
			// that retained prefix so the normal item becomes scheduler-pending.
			packets, bytes := fixture.stores[0].backlog()
			if packets < 2 || bytes <= fixture.stores[0].deliveryWindow {
				t.Fatalf("measured discovery did not fill useful admission credit: packets %d, bytes %d, window %d", packets, bytes, fixture.stores[0].deliveryWindow)
			}
			fixture.stores[0].limits.Packets = packets
			for range packets {
				if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 0 {
					t.Fatal("setup committed a non-padding frame")
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			if err := fixture.ingress.Push(ingressProbeItem(wgpacket.TransportData, 5000, deadline)); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if fixture.ingress.Len() != 0 {
				t.Fatal("normal item did not become the held pending item")
			}
			fixture.beforeReport <- func() error {
				fixture.stores[0].limits.Packets = 128
				return fixture.ingress.Push(ingressProbeItem(wgpacket.HandshakeInitiation, 0, deadline))
			}
			time.Sleep(10 * time.Millisecond)
			fixture.report(t, uint64(packets+1))
			synctest.Wait()
			if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 1 || wgpacket.Classify(data.Payload) != wgpacket.HandshakeInitiation {
				t.Fatal("control did not preempt the held normal item")
			}
			if data := takeOneTransmission(t, fixture.stores[0]); data.PacketID != 2 || data.DeadlineMicros != uint64(deadline.UnixMicro()) {
				t.Fatalf("new padding preceded the preempted normal item: packet ID %d", data.PacketID)
			}
		})
	})
}

type ingressProbeFixture struct {
	scheduler    *Scheduler
	ingress      *packetqueue.Queue[Packet]
	stores       [2]*TransmissionStore
	ctx          context.Context
	beforeReport chan func() error
	callbackErr  chan error
}

func newIngressProbeFixture(t *testing.T) *ingressProbeFixture {
	t.Helper()
	ingress, err := packetqueue.NewWithClock[Packet](packetqueue.Limits{Packets: 32, Bytes: 128 * 1024}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(ingress)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("scheduler exit = %v", err)
		}
		ingress.Close()
	})
	return &ingressProbeFixture{
		scheduler: scheduler, ingress: ingress, ctx: ctx,
		beforeReport: make(chan func() error, 1), callbackErr: make(chan error, 1),
	}
}

func (f *ingressProbeFixture) register(t *testing.T, id byte, limits packetqueue.Limits) {
	t.Helper()
	store, err := NewTransmissionStore(limits)
	if err != nil {
		t.Fatal(err)
	}
	f.stores[id-1] = store
	registration := schedulerRegistration(id, id, store)
	registration.InitialTiming = &clockmap.Sample{
		LocalSendMicros: 1000, RemoteReceiveMicros: 1000,
		RemoteSendMicros: 1000, LocalReceiveMicros: 1000 + uint64(id)*10_000,
	}
	if id == 1 {
		registration.ValidatePingProgress = func(uint64) bool {
			select {
			case callback := <-f.beforeReport:
				if err := callback(); err != nil {
					f.callbackErr <- err
					return false
				}
			default:
			}
			return true
		}
	}
	if err := f.scheduler.Register(f.ctx, registration); err != nil {
		t.Fatal(err)
	}
}

func (f *ingressProbeFixture) report(t *testing.T, packets uint64) {
	t.Helper()
	err := f.scheduler.ObserveDeliveryReport(f.ctx, protocol.LaneGeneration{LaneID: 1, Generation: 1},
		protocol.DeliveryReport{LaneID: 1, Generation: 1, DataPackets: packets}, uint64(time.Now().UnixMicro()), make(chan error, 1))
	select {
	case callbackErr := <-f.callbackErr:
		t.Fatalf("report callback: %v", callbackErr)
	default:
	}
	if err != nil {
		t.Fatal(err)
	}
}

func ingressProbeItem(kind wgpacket.Kind, size int, deadline time.Time) packetqueue.Item[Packet] {
	payload := relayWireGuardPacket(kind)
	if size > len(payload) {
		larger := make([]byte, size)
		copy(larger, payload)
		payload = larger
	}
	priority := packetqueue.PriorityNormal
	if kind.Control() {
		priority = packetqueue.PriorityControl
	}
	return packetqueue.Item[Packet]{Value: newPacket(datagram.Packet{Kind: kind, Payload: payload}),
		Size: len(payload), Priority: priority, Deadline: deadline}
}
