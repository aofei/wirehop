package dedup

import (
	"errors"
	"math"
	"testing"
)

func TestWindow(t *testing.T) {
	window, err := NewWindow(4)
	if err != nil {
		t.Fatal(err)
	}
	if got := window.Classify(1); got != New || window.Highest() != 0 {
		t.Fatalf("Classify(1) = %d, highest = %d, want %d and 0", got, window.Highest(), New)
	}
	for _, tt := range []struct {
		sequence uint64
		want     Result
	}{
		{sequence: 1, want: New},
		{sequence: 2, want: New},
		{sequence: 1, want: Duplicate},
		{sequence: 4, want: New},
		{sequence: 3, want: New},
		{sequence: 2, want: Duplicate},
		{sequence: 6, want: New},
		{sequence: 2, want: TooOld},
		{sequence: 100, want: New},
		{sequence: 99, want: New},
		{sequence: 6, want: TooOld},
		{sequence: 0, want: TooOld},
	} {
		if got := window.Observe(tt.sequence); got != tt.want {
			t.Fatalf("Observe(%d) = %d, want %d", tt.sequence, got, tt.want)
		}
	}
	if got := window.Highest(); got != 100 {
		t.Fatalf("Highest() = %d, want 100", got)
	}
}

func TestNewWindow(t *testing.T) {
	for _, capacity := range []int{-1, 0} {
		if _, err := NewWindow(capacity); !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("NewWindow(%d) error = %v", capacity, err)
		}
	}
	window, err := NewWindow(65_536)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(window.bits); got != 1024 {
		t.Fatalf("storage words = %d, want 1024", got)
	}
}

func TestWindowSequenceLimit(t *testing.T) {
	window, err := NewWindow(4)
	if err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{math.MaxUint64 - 1, math.MaxUint64} {
		if got := window.Observe(sequence); got != New {
			t.Fatalf("Observe(%d) = %d, want %d", sequence, got, New)
		}
	}
	if got := window.Observe(math.MaxUint64); got != Duplicate {
		t.Fatalf("Observe(MaxUint64) = %d, want %d", got, Duplicate)
	}
	if got := window.Observe(math.MaxUint64 - 4); got != TooOld {
		t.Fatalf("Observe(MaxUint64 - 4) = %d, want %d", got, TooOld)
	}
	if got := window.Observe(math.MaxUint64 - 3); got != New {
		t.Fatalf("Observe(MaxUint64 - 3) = %d, want %d", got, New)
	}
}

func TestWindowAdvanceAcrossPartialWordBoundary(t *testing.T) {
	window, err := NewWindow(65)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 65; sequence++ {
		if got := window.Observe(sequence); got != New {
			t.Fatalf("Observe(%d) = %d, want %d", sequence, got, New)
		}
	}
	if got := window.Observe(129); got != New {
		t.Fatalf("Observe(129) = %d, want %d", got, New)
	}
	for _, tt := range []struct {
		sequence uint64
		want     Result
	}{
		{sequence: 64, want: TooOld},
		{sequence: 65, want: Duplicate},
		{sequence: 66, want: New},
		{sequence: 128, want: New},
		{sequence: 129, want: Duplicate},
	} {
		if got := window.Classify(tt.sequence); got != tt.want {
			t.Fatalf("Classify(%d) = %d, want %d", tt.sequence, got, tt.want)
		}
	}
	if got := window.Observe(130); got != New || window.Classify(65) != TooOld {
		t.Fatalf("Observe(130) = %d, Classify(65) = %d", got, window.Classify(65))
	}
}

func TestWindowMatchesReferenceAcrossCapacities(t *testing.T) {
	for capacity := 1; capacity <= 257; capacity++ {
		window, err := NewWindow(capacity)
		if err != nil {
			t.Fatal(err)
		}
		observed := make(map[uint64]bool)
		var highest uint64
		state := uint64(capacity)
		for range 5000 {
			state = state*6364136223846793005 + 1442695040888963407
			var sequence uint64
			if state&1 == 0 {
				sequence = highest + 1 + state%uint64(capacity+17)
			} else if highest > 0 {
				span := uint64(capacity*2 + 17)
				offset := state % span
				if offset < highest {
					sequence = highest - offset
				}
			}
			want := referenceResult(sequence, highest, uint64(capacity), observed)
			if got := window.Observe(sequence); got != want {
				t.Fatalf("capacity %d Observe(%d) = %d, want %d at highest %d", capacity, sequence, got, want,
					highest)
			}
			if want == New {
				observed[sequence] = true
				if sequence > highest {
					highest = sequence
				}
			}
		}
	}
}

func referenceResult(sequence, highest, capacity uint64, observed map[uint64]bool) Result {
	if sequence == 0 || sequence <= highest && highest-sequence >= capacity {
		return TooOld
	}
	if sequence <= highest && observed[sequence] {
		return Duplicate
	}
	return New
}

func BenchmarkWindowLargeAdvance(b *testing.B) {
	const capacity = 1_048_576
	window, err := NewWindow(capacity)
	if err != nil {
		b.Fatal(err)
	}
	sequence := uint64(1)
	window.Observe(sequence)
	b.ReportAllocs()
	for b.Loop() {
		sequence += capacity - 1
		window.Observe(sequence)
	}
}
