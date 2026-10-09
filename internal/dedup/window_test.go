package dedup

import (
	"encoding/binary"
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
	if window.bits != nil {
		t.Fatal("empty window allocated a bitmap")
	}
	if got := window.Observe(0); got != TooOld || window.bits != nil {
		t.Fatalf("Observe(0) = %d, allocated = %t, want %d and false", got, window.bits != nil, TooOld)
	}
	if got := window.Classify(1); got != New || window.bits != nil || window.Highest() != 0 {
		t.Fatalf("Classify(1) = %d, allocated = %t, highest = %d", got, window.bits != nil, window.Highest())
	}
	if got := window.Observe(1); got != New || len(window.bits) != 1 {
		t.Fatalf("Observe(1) = %d, storage words = %d, want %d and 1", got, len(window.bits), New)
	}
	if got := window.Observe(63); got != New || len(window.bits) != 1 {
		t.Fatalf("Observe(63) = %d, storage words = %d, want %d and 1", got, len(window.bits), New)
	}
	if got := window.Observe(64); got != New || len(window.bits) != 2 {
		t.Fatalf("Observe(64) = %d, storage words = %d, want %d and 2", got, len(window.bits), New)
	}
}

func TestWindowSparseStorage(t *testing.T) {
	for _, capacity := range []int{1, 63, 64, 65, 257, 65_536, 1_048_576} {
		window, err := NewWindow(capacity)
		if err != nil {
			t.Fatal(err)
		}
		observed := make(map[uint64]bool)
		var highest uint64
		for _, sequence := range []uint64{
			uint64(capacity), uint64(capacity) - 1, 1, 63, 64, 65, uint64(capacity) + 1,
			2*uint64(capacity) - 1, 2 * uint64(capacity), 2*uint64(capacity) + 1,
			math.MaxUint64 - 3, math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64 - uint64(capacity),
		} {
			words := len(window.bits)
			want := referenceResult(sequence, highest, uint64(capacity), observed)
			if got := window.Classify(sequence); got != want || len(window.bits) != words {
				t.Fatalf("capacity %d Classify(%d) = %d with %d words, want %d with %d words", capacity, sequence,
					got, len(window.bits), want, words)
			}
			if got := window.Observe(sequence); got != want {
				t.Fatalf("capacity %d Observe(%d) = %d, want %d", capacity, sequence, got, want)
			}
			if want == New {
				observed[sequence] = true
				highest = max(highest, sequence)
			}
			if got := window.Highest(); got != highest {
				t.Fatalf("capacity %d Highest() = %d, want %d", capacity, got, highest)
			}
			if len(window.bits) > (capacity-1)/64+1 {
				t.Fatalf("capacity %d allocated %d words beyond its ring", capacity, len(window.bits))
			}
		}
	}
}

func TestWindowSequenceLimit(t *testing.T) {
	for _, tt := range []struct {
		name     string
		capacity int
	}{{name: "SmallCapacity", capacity: 4}, {name: "MaximumCapacity", capacity: math.MaxInt}} {
		t.Run(tt.name, func(t *testing.T) {
			window, err := NewWindow(tt.capacity)
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
			if got := window.Observe(math.MaxUint64 - uint64(tt.capacity)); got != TooOld {
				t.Fatalf("Observe(MaxUint64 - capacity) = %d, want %d", got, TooOld)
			}
			if got := window.Observe(math.MaxUint64 - uint64(tt.capacity) + 1); got != New {
				t.Fatalf("Observe(MaxUint64 - capacity + 1) = %d, want %d", got, New)
			}
			if tt.capacity == math.MaxInt {
				if got := window.Classify(math.MaxUint64 - 63); got != New || len(window.bits) != 1 {
					t.Fatalf("virtual classification = %d with %d words, want %d with one word", got, len(window.bits), New)
				}
			}
		})
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
	capacities := make([]int, 0, 260)
	for capacity := 1; capacity <= 257; capacity++ {
		capacities = append(capacities, capacity)
	}
	capacities = append(capacities, 1024, 65_536, 1_048_576)
	for _, capacity := range capacities {
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
			words := len(window.bits)
			if got := window.Classify(sequence); got != want || len(window.bits) != words {
				t.Fatalf("capacity %d Classify(%d) = %d with %d words, want %d with %d words", capacity, sequence,
					got, len(window.bits), want, words)
			}
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

func FuzzWindow(f *testing.F) {
	for _, capacity := range []uint32{1, 63, 64, 65, 257, 65_536, 1_048_576} {
		var encoded []byte
		for _, sequence := range []uint64{0, 1, 63, 64, uint64(capacity), uint64(capacity) - 1,
			uint64(capacity) + 1, 2 * uint64(capacity), math.MaxUint64 - 3, math.MaxUint64, math.MaxUint64 - 1} {
			encoded = binary.LittleEndian.AppendUint64(encoded, sequence)
		}
		f.Add(capacity-1, encoded)
	}
	f.Fuzz(func(t *testing.T, selector uint32, encoded []byte) {
		capacity := uint64(selector%1_048_576) + 1
		window, err := NewWindow(int(capacity))
		if err != nil {
			t.Fatal(err)
		}
		encoded = encoded[:min(len(encoded), 1024)]
		observed := make(map[uint64]bool)
		var highest uint64
		for len(encoded) >= 8 {
			sequence := binary.LittleEndian.Uint64(encoded)
			encoded = encoded[8:]
			want := referenceResult(sequence, highest, capacity, observed)
			words := len(window.bits)
			if got := window.Classify(sequence); got != want || len(window.bits) != words {
				t.Fatalf("Classify(%d) = %d with %d words, want %d with %d words", sequence, got,
					len(window.bits), want, words)
			}
			if got := window.Observe(sequence); got != want {
				t.Fatalf("Observe(%d) = %d, want %d at highest %d and capacity %d", sequence, got, want, highest, capacity)
			}
			if want == New {
				observed[sequence] = true
				highest = max(highest, sequence)
			}
			if window.Highest() != highest {
				t.Fatalf("Highest() = %d, want %d", window.Highest(), highest)
			}
		}
	})
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
