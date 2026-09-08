package netsetup

import (
	"log/slog"
	"time"
)

// RetryNotice reports prolonged failed attempts without logging every retry. It is owned by one recovery loop.
type RetryNotice struct {
	logger    *slog.Logger
	operation string
	next      time.Time
	warned    bool
}

// NewRetryNotice starts a quiet 30-second preparation window before failed attempts can produce warnings.
func NewRetryNotice(logger *slog.Logger, operation string) *RetryNotice {
	return &RetryNotice{logger: logger, operation: operation, next: time.Now().Add(30 * time.Second)}
}

// Failed emits at most one warning per minute after the initial quiet window.
func (n *RetryNotice) Failed(attributes ...any) {
	if n.logger == nil || time.Now().Before(n.next) {
		return
	}
	n.logger.Warn(n.operation+" unavailable, retrying", attributes...)
	n.next = time.Now().Add(time.Minute)
	n.warned = true
}

// Recovered reports recovery only after a warning and resets the quiet window for the next outage.
func (n *RetryNotice) Recovered() {
	if n.warned {
		n.logger.Info(n.operation + " recovered")
	}
	n.next = time.Now().Add(30 * time.Second)
	n.warned = false
}
