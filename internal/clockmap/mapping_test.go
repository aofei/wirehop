package clockmap

import (
	"errors"
	"math"
	"testing"
)

func TestEstimate(t *testing.T) {
	for _, tt := range []struct {
		name   string
		sample Sample
		want   Mapping
	}{
		{name: "Symmetric", sample: Sample{
			LocalSendMicros: 100, RemoteReceiveMicros: 160, RemoteSendMicros: 170, LocalReceiveMicros: 230,
		}, want: Mapping{OffsetMicros: 0, UncertaintyMicros: 60}},
		{name: "RemoteAhead", sample: Sample{
			LocalSendMicros: 100, RemoteReceiveMicros: 1150, RemoteSendMicros: 1170, LocalReceiveMicros: 220,
		}, want: Mapping{OffsetMicros: 1000, UncertaintyMicros: 50}},
		{name: "RemoteBehind", sample: Sample{
			LocalSendMicros: 1100, RemoteReceiveMicros: 150, RemoteSendMicros: 170, LocalReceiveMicros: 1220,
		}, want: Mapping{OffsetMicros: -1000, UncertaintyMicros: 50}},
		{name: "AsymmetricOddPositive", sample: Sample{
			LocalSendMicros: 100, RemoteReceiveMicros: 102, RemoteSendMicros: 102, LocalReceiveMicros: 103,
		}, want: Mapping{OffsetMicros: 0, UncertaintyMicros: 2}},
		{name: "AsymmetricOddNegative", sample: Sample{
			LocalSendMicros: 100, RemoteReceiveMicros: 101, RemoteSendMicros: 101, LocalReceiveMicros: 103,
		}, want: Mapping{OffsetMicros: 0, UncertaintyMicros: 2}},
		{name: "RemoteSpanLarger", sample: Sample{
			LocalSendMicros: 100, RemoteReceiveMicros: 1000, RemoteSendMicros: 1002, LocalReceiveMicros: 101,
		}, want: Mapping{OffsetMicros: 900, UncertaintyMicros: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Estimate(tt.sample)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("Estimate() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestEstimateErrors(t *testing.T) {
	for _, tt := range []struct {
		name   string
		sample Sample
		want   error
	}{
		{name: "LocalReversed", sample: Sample{LocalSendMicros: 2, LocalReceiveMicros: 1}, want: ErrInvalidSample},
		{name: "RemoteReversed", sample: Sample{RemoteReceiveMicros: 2, RemoteSendMicros: 1}, want: ErrInvalidSample},
		{name: "Overflow", sample: Sample{LocalSendMicros: math.MaxInt64 + 1, LocalReceiveMicros: math.MaxInt64 + 1}, want: ErrTimestampOverflow},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Estimate(tt.sample)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Estimate() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestAverage(t *testing.T) {
	for _, tt := range []struct {
		name        string
		left, right int64
		want        int64
	}{
		{name: "PositiveOdd", left: 1, right: 2, want: 1},
		{name: "NegativeOdd", left: -1, right: -2, want: -1},
		{name: "MixedNegative", left: 1, right: -2},
		{name: "MixedPositive", left: 2, right: -1},
		{name: "Maximum", left: math.MaxInt64, right: math.MaxInt64, want: math.MaxInt64},
		{name: "Minimum", left: math.MinInt64, right: math.MinInt64, want: math.MinInt64},
		{name: "OppositeLimits", left: math.MaxInt64, right: math.MinInt64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := average(tt.left, tt.right); got != tt.want {
				t.Fatalf("average(%d, %d) = %d, want %d", tt.left, tt.right, got, tt.want)
			}
		})
	}
}

func TestMapping(t *testing.T) {
	mapping := Mapping{OffsetMicros: 1000, UncertaintyMicros: 100}
	translated, err := mapping.Translate(500)
	if err != nil || translated != 1500 {
		t.Fatalf("Translate() = %d, %v", translated, err)
	}
	restored, err := mapping.Inverse().Translate(translated)
	if err != nil || restored != 500 {
		t.Fatalf("inverse Translate() = %d, %v", restored, err)
	}
	earliest, latest, err := mapping.DeadlineBounds(2000)
	if err != nil || earliest != 2900 || latest != 3100 {
		t.Fatalf("DeadlineBounds() = %d, %d, %v", earliest, latest, err)
	}
	earliest, latest, err = (Mapping{UncertaintyMicros: 100}).DeadlineBounds(50)
	if err != nil || earliest != 0 || latest != 150 {
		t.Fatalf("lower saturated DeadlineBounds() = %d, %d, %v", earliest, latest, err)
	}
	earliest, latest, err = (Mapping{UncertaintyMicros: 100}).DeadlineBounds(math.MaxUint64 - 50)
	if err != nil || earliest != math.MaxUint64-150 || latest != math.MaxUint64 {
		t.Fatalf("upper saturated DeadlineBounds() = %d, %d, %v", earliest, latest, err)
	}
}

func TestMappingOverflow(t *testing.T) {
	mapping := Mapping{OffsetMicros: -10}
	if _, err := mapping.Translate(9); !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("Translate() error = %v", err)
	}
	if _, _, err := mapping.DeadlineBounds(9); !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("DeadlineBounds() error = %v", err)
	}
	if _, _, err := (Mapping{OffsetMicros: 10}).DeadlineBounds(math.MaxUint64); !errors.Is(err,
		ErrTimestampOverflow) {
		t.Fatalf("DeadlineBounds() error = %v", err)
	}
}
