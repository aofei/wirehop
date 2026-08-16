package lifecycle

import (
	"context"
	"errors"
	"testing"
)

func TestGroup(t *testing.T) {
	want := errors.New("worker failure")
	group := NewGroup(context.Background())
	siblingCanceled := make(chan struct{})
	group.Go(func(context.Context) error { return want })
	group.Go(func(ctx context.Context) error {
		<-ctx.Done()
		close(siblingCanceled)
		return ctx.Err()
	})
	if err := group.Wait(); !errors.Is(err, want) {
		t.Fatalf("Wait() error = %v, want %v", err, want)
	}
	<-siblingCanceled
}
