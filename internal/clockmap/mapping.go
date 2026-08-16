// Package clockmap maps monotonic timestamps between WireHop peers.
package clockmap

import (
	"errors"
	"math"
)

var (
	// ErrInvalidSample indicates an impossible four-timestamp clock sample.
	ErrInvalidSample = errors.New("invalid clock sample")
	// ErrTimestampOverflow indicates a timestamp outside safe signed arithmetic.
	ErrTimestampOverflow = errors.New("timestamp overflow")
)

// Sample is a four-timestamp exchange from one local clock to one remote clock.
type Sample struct {
	LocalSendMicros     uint64
	RemoteReceiveMicros uint64
	RemoteSendMicros    uint64
	LocalReceiveMicros  uint64
}

// Mapping translates local monotonic timestamps into remote monotonic timestamps.
type Mapping struct {
	OffsetMicros      int64
	UncertaintyMicros uint64
}

// Estimate derives a remote-minus-local mapping and uncertainty from sample.
func Estimate(sample Sample) (Mapping, error) {
	if sample.LocalReceiveMicros < sample.LocalSendMicros ||
		sample.RemoteSendMicros < sample.RemoteReceiveMicros {
		return Mapping{}, ErrInvalidSample
	}
	if sample.LocalSendMicros > math.MaxInt64 || sample.LocalReceiveMicros > math.MaxInt64 ||
		sample.RemoteReceiveMicros > math.MaxInt64 || sample.RemoteSendMicros > math.MaxInt64 {
		return Mapping{}, ErrTimestampOverflow
	}

	localSpan := sample.LocalReceiveMicros - sample.LocalSendMicros
	remoteProcessing := sample.RemoteSendMicros - sample.RemoteReceiveMicros
	var offsetSpan uint64
	if localSpan >= remoteProcessing {
		offsetSpan = localSpan - remoteProcessing
	} else {
		offsetSpan = remoteProcessing - localSpan
	}
	firstOffset := int64(sample.RemoteReceiveMicros) - int64(sample.LocalSendMicros)
	secondOffset := int64(sample.RemoteSendMicros) - int64(sample.LocalReceiveMicros)
	return Mapping{
		OffsetMicros:      average(firstOffset, secondOffset),
		UncertaintyMicros: offsetSpan/2 + offsetSpan%2,
	}, nil
}

// average returns the truncated signed average without overflowing the sum.
func average(left, right int64) int64 {
	quotient := left/2 + right/2
	remainder := left%2 + right%2
	switch {
	case remainder == 2:
		return quotient + 1
	case remainder == -2:
		return quotient - 1
	case remainder == 1 && quotient < 0:
		return quotient + 1
	case remainder == -1 && quotient > 0:
		return quotient - 1
	default:
		return quotient
	}
}

// Inverse returns the mapping from the former remote clock back to the local clock.
func (m Mapping) Inverse() Mapping {
	return Mapping{OffsetMicros: -m.OffsetMicros, UncertaintyMicros: m.UncertaintyMicros}
}

// Translate maps a local monotonic timestamp into the remote clock domain.
func (m Mapping) Translate(localMicros uint64) (uint64, error) {
	if m.OffsetMicros >= 0 {
		offset := uint64(m.OffsetMicros)
		if localMicros > math.MaxUint64-offset {
			return 0, ErrTimestampOverflow
		}
		return localMicros + offset, nil
	}
	offset := uint64(-(m.OffsetMicros + 1)) + 1
	if localMicros < offset {
		return 0, ErrTimestampOverflow
	}
	return localMicros - offset, nil
}

// DeadlineBounds returns the earliest and latest remote times supported for a local deadline.
func (m Mapping) DeadlineBounds(localDeadlineMicros uint64) (uint64, uint64, error) {
	deadlineRemote, err := m.Translate(localDeadlineMicros)
	if err != nil {
		return 0, 0, err
	}
	earliestDeadline := uint64(0)
	if deadlineRemote > m.UncertaintyMicros {
		earliestDeadline = deadlineRemote - m.UncertaintyMicros
	}
	latestDeadline := uint64(math.MaxUint64)
	if deadlineRemote <= math.MaxUint64-m.UncertaintyMicros {
		latestDeadline = deadlineRemote + m.UncertaintyMicros
	}
	return earliestDeadline, latestDeadline, nil
}
