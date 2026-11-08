//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestWaitForFlow(t *testing.T) {
	for _, tt := range []struct {
		name                        string
		future, cancelled, existing bool
	}{
		{name: "Started"},
		{name: "FutureDeadline", future: true},
		{name: "CancelledBeforeStart", cancelled: true},
		{name: "CancelledAfterStart", cancelled: true, existing: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "client-flow-start.txt")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, err := bootSeconds()
			if err != nil {
				t.Fatal(err)
			}
			offset := 0.0
			if tt.future {
				offset = 0.01
			}
			if !tt.cancelled || tt.existing {
				if err := os.WriteFile(path, fmt.Appendf(nil, "%.9f\n", started), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tt.cancelled {
				cancel()
			}
			for range 32 {
				err = waitForFlow(ctx, path, offset)
				if tt.cancelled && !errors.Is(err, context.Canceled) {
					t.Fatalf("waitForFlow() = %v, want cancellation", err)
				}
				if !tt.cancelled && err != nil {
					t.Fatal(err)
				}
			}
			if tt.cancelled {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			now, err := bootSeconds()
			if err != nil || now < started+offset {
				t.Fatalf("fault released before data-relative deadline: now=%f start=%f error=%v", now, started, err)
			}
		})
	}
}

func TestWaitForFlowInvalidMarker(t *testing.T) {
	for _, tt := range []struct{ name, marker string }{
		{name: "NotANumber", marker: "NaN"},
		{name: "Infinity", marker: "+Inf"},
		{name: "Zero", marker: "0"},
		{name: "Negative", marker: "-1"},
		{name: "Malformed", marker: "incomplete"},
		{name: "Future", marker: "1e300"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "start.txt")
			if err := os.WriteFile(path, []byte(tt.marker), 0600); err != nil {
				t.Fatal(err)
			}
			if err := waitForFlow(context.Background(), path, 0); err == nil {
				t.Fatal("accepted an invalid flow marker")
			}
		})
	}
}

func TestWaitForFlowInvalidOffset(t *testing.T) {
	for _, tt := range []struct {
		name    string
		seconds float64
	}{
		{name: "Negative", seconds: -1},
		{name: "NotANumber", seconds: math.NaN()},
		{name: "Infinity", seconds: math.Inf(1)},
		{name: "Overflow", seconds: math.MaxFloat64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := waitForFlow(context.Background(), filepath.Join(t.TempDir(), "absent"), tt.seconds); err == nil {
				t.Fatal("accepted an invalid wait offset")
			}
		})
	}
}
