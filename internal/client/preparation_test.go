package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/monotime"
)

type preparationClock struct {
	now atomic.Uint64
}

func (c *preparationClock) NowMicros() uint64 {
	return c.now.Load()
}

func TestClientPreparationContext(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		name := "OrdinaryElapsed"
		if resumed {
			name = "SystemResume"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clock := &preparationClock{}
				instance := &Client{config: Config{Clock: clock}}
				ctx, cancel := instance.preparationContext(t.Context())
				defer cancel()
				synctest.Wait()
				elapsed := uint64(1_000_000)
				if resumed {
					elapsed = 60_000_000
				}
				clock.now.Store(elapsed)
				synctest.Sleep(time.Second)
				synctest.Wait()
				if resumed {
					if !errors.Is(context.Cause(ctx), monotime.ErrResumed) {
						t.Fatalf("preparation cause = %v", context.Cause(ctx))
					}
				} else if ctx.Err() != nil {
					t.Fatalf("ordinary preparation canceled: %v", context.Cause(ctx))
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}
