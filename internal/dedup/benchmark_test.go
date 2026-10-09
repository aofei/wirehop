package dedup

import "testing"

// benchmarkWindowSink keeps newly created windows live beyond each measured operation.
var benchmarkWindowSink *Window

func BenchmarkNewWindow(b *testing.B) {
	for _, tt := range []struct {
		name    string
		observe bool
	}{{name: "Empty"}, {name: "FirstObservation", observe: true}} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				window, err := NewWindow(1_048_576)
				if err != nil {
					b.Fatal(err)
				}
				if tt.observe {
					window.Observe(1)
				}
				benchmarkWindowSink = window
			}
		})
	}
}

func BenchmarkWindowObserve(b *testing.B) {
	for _, tt := range []struct {
		name      string
		duplicate bool
	}{{name: "Sequential"}, {name: "Duplicate", duplicate: true}} {
		b.Run(tt.name, func(b *testing.B) {
			window, err := NewWindow(1_048_576)
			if err != nil {
				b.Fatal(err)
			}
			window.Observe(1_048_575)
			sequence := uint64(1_048_575)
			b.ReportAllocs()
			for b.Loop() {
				sequence++
				window.Observe(sequence)
				if tt.duplicate {
					window.Observe(sequence - 1)
				}
			}
		})
	}
}

func BenchmarkWindowFill(b *testing.B) {
	const capacity = 1_048_576
	b.ReportAllocs()
	for b.Loop() {
		window, err := NewWindow(capacity)
		if err != nil {
			b.Fatal(err)
		}
		for sequence := uint64(1); sequence <= capacity; sequence++ {
			window.Observe(sequence)
		}
		benchmarkWindowSink = window
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*capacity), "ns/observation")
}
