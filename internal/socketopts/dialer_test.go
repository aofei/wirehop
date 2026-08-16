package socketopts

import (
	"context"
	"errors"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestNewDialer(t *testing.T) {
	base := &net.Dialer{Timeout: time.Second}
	dialer, err := NewDialer(base, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dialer == base || dialer.Timeout != base.Timeout || dialer.Control != nil {
		t.Fatalf("NewDialer() = %+v", dialer)
	}
	marked, err := NewDialer(base, 1)
	if runtime.GOOS == "linux" {
		if err != nil || marked.Control == nil || marked.Resolver == nil || !marked.Resolver.PreferGo ||
			marked.Resolver.Dial == nil {
			t.Fatalf("NewDialer(mark) = %+v, %v", marked, err)
		}
	} else if !errors.Is(err, ErrUnsupportedMark) {
		t.Fatalf("NewDialer(mark) error = %v, want %v", err, ErrUnsupportedMark)
	}
}

func TestNewDialerComposesControlContext(t *testing.T) {
	previousError := errors.New("previous control context")
	base := &net.Dialer{ControlContext: func(context.Context, string, string, syscall.RawConn) error {
		return previousError
	}}
	dialer, err := NewDialer(base, 1)
	if runtime.GOOS != "linux" {
		if !errors.Is(err, ErrUnsupportedMark) {
			t.Fatalf("NewDialer(mark) error = %v, want %v", err, ErrUnsupportedMark)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if dialer.Control != nil || dialer.ControlContext == nil {
		t.Fatalf("NewDialer(mark) hook presence = %t, %t", dialer.Control != nil, dialer.ControlContext != nil)
	}
	if err := dialer.ControlContext(context.Background(), "tcp", "127.0.0.1:1", nil); !errors.Is(err, previousError) {
		t.Fatalf("ControlContext() error = %v, want %v", err, previousError)
	}
}
