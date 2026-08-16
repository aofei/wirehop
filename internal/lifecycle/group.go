// Package lifecycle coordinates cancellable long-running WireHop workers.
package lifecycle

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrWorkerStopped indicates that a long-running worker returned without an error.
	ErrWorkerStopped = errors.New("worker stopped unexpectedly")
)

// Group cancels sibling workers when the first worker returns.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	wait   sync.WaitGroup
	err    error
}

// NewGroup returns an empty worker group derived from parent.
func NewGroup(parent context.Context) *Group {
	ctx, cancel := context.WithCancel(parent)
	return &Group{ctx: ctx, cancel: cancel}
}

// Go starts one long-running worker.
func (g *Group) Go(run func(context.Context) error) {
	g.wait.Go(func() {
		err := run(g.ctx)
		if err == nil {
			err = ErrWorkerStopped
		}
		g.once.Do(func() {
			g.err = err
			g.cancel()
		})
	})
}

// Wait waits for every worker and returns the first worker result.
func (g *Group) Wait() error {
	g.wait.Wait()
	if g.err != nil {
		return g.err
	}
	return g.ctx.Err()
}
