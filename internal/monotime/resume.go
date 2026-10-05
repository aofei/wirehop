package monotime

import (
	"errors"
	"time"
)

// ErrResumed indicates system suspend detected by a gap between protocol and runtime elapsed time.
var ErrResumed = errors.New("system resumed after suspend")

// resumeClockTolerance allows ordinary sampling variation without hiding a meaningful system suspend.
const resumeClockTolerance = time.Second

// ResumeDetector compares a suspend-aware protocol clock with the Go runtime's elapsed-time clock. Platforms where
// both clocks include suspend rely on ordinary expired budgets instead of this additional signal.
type ResumeDetector struct {
	now         func() uint64
	runtimeTime time.Time
	clockMicros uint64
}

// NewResumeDetector captures the starting clock pair for resume detection.
func NewResumeDetector(now func() uint64) ResumeDetector {
	before := time.Now()
	return ResumeDetector{now: now, runtimeTime: before, clockMicros: now()}
}

// Resumed updates the clock pair and reports excess protocol elapsed time beyond the sampling tolerance.
func (d *ResumeDetector) Resumed() bool {
	before := time.Now()
	nowMicros := d.now()
	after := time.Now()
	// Bound ordinary elapsed time from the previous pre-sample reading through this post-sample reading. Scheduling
	// delays inside a clock sample must not be mistaken for suspend.
	elapsed := after.Sub(d.runtimeTime)
	resumed := nowMicros-d.clockMicros > uint64((elapsed+resumeClockTolerance)/time.Microsecond)
	d.runtimeTime = before
	d.clockMicros = nowMicros
	return resumed
}
