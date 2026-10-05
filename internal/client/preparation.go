package client

import (
	"context"
	"time"

	"github.com/aofei/wirehop/internal/monotime"
)

// preparationContext cancels an outstanding carrier preparation after system resume. Its temporary checks run only
// during dialing and admission, never while an established lane is idle.
func (c *Client) preparationContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	resume := monotime.NewResumeDetector(c.config.Clock.NowMicros)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if resume.Resumed() {
					cancel(monotime.ErrResumed)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled) }
}
