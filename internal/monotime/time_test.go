package monotime

import (
	"math"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	for _, micros := range []uint64{0, 1, 999_999, 1_000_000, uint64(math.MaxInt64), math.MaxUint64} {
		value := Time(micros)
		if got := uint64(value.Unix())*1_000_000 + uint64(value.Nanosecond()/1000); got != micros {
			t.Fatalf("Time(%d) lost precision: %d", micros, got)
		}
		if value.IsZero() || value != value.Round(0) {
			t.Fatalf("Time(%d) has an invalid epoch or a second clock component", micros)
		}
		if micros < math.MaxUint64 && Time(micros+1).Sub(value) != time.Microsecond {
			t.Fatalf("Time(%d) did not advance by one microsecond", micros)
		}
	}
}
