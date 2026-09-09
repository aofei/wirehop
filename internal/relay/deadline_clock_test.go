package relay

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/wgpacket"
)

func TestSchedulerExpiryUsesIngressClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		origin := time.Now()
		now := func() time.Time { return time.Unix(123, 0).Add(time.Since(origin)) }
		limits := packetqueue.Limits{Packets: 2, Bytes: 4096}
		ingress, err := packetqueue.NewWithClock[Packet](limits, now)
		if err != nil {
			t.Fatal(err)
		}
		defer ingress.Close()
		scheduler, err := NewScheduler(ingress)
		if err != nil {
			t.Fatal(err)
		}
		store, err := newTransmissionStore(limits, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.push(schedulerTransmission(1, wgpacket.TransportData, now().Add(100*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- scheduler.Run(ctx) }()
		if err := scheduler.Register(ctx, schedulerRegistration(1, 1, store)); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(25 * time.Millisecond)
		synctest.Wait()
		if packets, _ := store.backlog(); packets != 1 {
			t.Fatal("the timer's wall epoch prematurely expired a fresh packet")
		}
		synctest.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if packets, _ := store.backlog(); packets != 0 {
			t.Fatal("the protocol deadline did not release expired queued work")
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() = %v, want cancellation", err)
		}
	})
}
