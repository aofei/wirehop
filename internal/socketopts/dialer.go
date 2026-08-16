// Package socketopts configures carrier sockets before connection establishment.
package socketopts

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
)

var (
	// ErrUnsupportedMark indicates a nonzero firewall mark on an unsupported platform.
	ErrUnsupportedMark = errors.New("socket firewall mark is unsupported")
)

// NewDialer clones base and applies mark before every carrier and resolver socket connection.
func NewDialer(base *net.Dialer, mark uint32) (*net.Dialer, error) {
	var dialer net.Dialer
	if base != nil {
		dialer = *base
	}
	if mark == 0 {
		return &dialer, nil
	}
	if !markSupported() {
		return nil, ErrUnsupportedMark
	}
	if dialer.ControlContext != nil {
		dialer.ControlContext = markedControlContext(dialer.ControlContext, mark)
	} else {
		dialer.Control = markedControl(dialer.Control, mark)
	}
	resolverDialer := &net.Dialer{Control: markedControl(nil, mark)}
	dialer.Resolver = &net.Resolver{
		PreferGo: true,
		Dial:     resolverDialer.DialContext,
	}
	return &dialer, nil
}

// markedControl composes an existing socket control hook with one SO_MARK operation.
func markedControl(previous func(string, string, syscall.RawConn) error,
	mark uint32) func(string, string, syscall.RawConn) error {
	return func(network, address string, raw syscall.RawConn) error {
		if previous != nil {
			if err := previous(network, address, raw); err != nil {
				return err
			}
		}
		return applyMark(raw, mark)
	}
}

// markedControlContext composes an existing context-aware socket hook with one SO_MARK operation.
func markedControlContext(previous func(context.Context, string, string, syscall.RawConn) error,
	mark uint32) func(context.Context, string, string, syscall.RawConn) error {
	return func(ctx context.Context, network, address string, raw syscall.RawConn) error {
		if err := previous(ctx, network, address, raw); err != nil {
			return err
		}
		return applyMark(raw, mark)
	}
}

// applyMark applies one firewall mark with a stable operation diagnostic.
func applyMark(raw syscall.RawConn, mark uint32) error {
	if err := setMark(raw, mark); err != nil {
		return fmt.Errorf("set socket firewall mark %d: %w", mark, err)
	}
	return nil
}
