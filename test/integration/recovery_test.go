package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMeasureThroughput(t *testing.T) {
	for _, tt := range []struct {
		name       string
		samples    []flowInterval
		windows    []throughputWindow
		wantBytes  float64
		wantStatus string
	}{
		{name: "PartialBoundaryIntervals", samples: []flowInterval{
			{Start: 0, End: 1, Bytes: 1000},
			{Start: 1, End: 3, Bytes: 4000},
			{Start: 3, End: 4, Bytes: 1000},
		}, windows: []throughputWindow{
			{name: "baseline", start: 0, end: 4},
			{name: "clipped", start: 0.5, end: 3.5},
		}, wantBytes: 5000, wantStatus: "pass"},
		{name: "MissingInteriorInterval", samples: []flowInterval{
			{Start: 0, End: 1, Bytes: 1000},
			{Start: 2, End: 4, Bytes: 2000},
		}, windows: []throughputWindow{{name: "baseline", start: 0, end: 4}}, wantBytes: 3000, wantStatus: "invalid"},
		{name: "ZeroDeliveryIsMeasured", samples: []flowInterval{
			{Start: 0, End: 4, Bytes: 4000},
			{Start: 4, End: 9},
		}, windows: []throughputWindow{
			{name: "baseline", start: 0, end: 4},
			{name: "recovered", start: 4, end: 9},
		}, wantStatus: "below-threshold"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			measurements, err := measureThroughput(tt.samples, tt.windows, 0)
			if (err != nil) != (tt.wantStatus != "pass") {
				t.Fatalf("measureThroughput() error = %v", err)
			}
			last := measurements[len(measurements)-1]
			if last.Bytes != tt.wantBytes || last.Status != tt.wantStatus {
				t.Fatalf("measurement = %+v, want bytes=%f status=%s", last, tt.wantBytes, tt.wantStatus)
			}
			if tt.wantStatus == "pass" && last.BitsPerSecond != tt.wantBytes*8/(last.End-last.Start) {
				t.Fatalf("rate did not use the full window: %+v", last)
			}
		})
	}
}

func TestMeasureThroughputInvalidBaseline(t *testing.T) {
	measurements, err := measureThroughput([]flowInterval{
		{Start: 0, End: 4, Bytes: 4000},
		{Start: 4, End: 9, Bytes: 5000},
	}, []throughputWindow{
		{name: "baseline", start: 0, end: 4},
		{name: "recovered", start: 4, end: 9},
	}, 16_000)
	if err == nil {
		t.Fatal("accepted a baseline below the required rate")
	}
	for _, measurement := range measurements {
		if measurement.Status != "invalid" {
			t.Fatalf("unusable baseline produced a valid recovery status: %+v", measurement)
		}
	}
}

func TestMeasureThroughputJitter(t *testing.T) {
	for _, tt := range []struct {
		name       string
		bytes      uint64
		wantStatus string
	}{
		{name: "ExactlyThreshold", bytes: 50_000, wantStatus: "pass"},
		{name: "AboveThreshold", bytes: 52_000, wantStatus: "pass"},
		{name: "BelowThreshold", bytes: 49_999, wantStatus: "below-threshold"},
		{name: "Stalled", wantStatus: "below-threshold"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for microseconds := -1000; microseconds <= 1000; microseconds++ {
				offset := float64(microseconds) / 1e6
				samples := make([]flowInterval, 300)
				for index := range samples {
					bytes := tt.bytes
					if index < 50 {
						bytes = 200_000
					}
					samples[index] = flowInterval{
						Start: float64(index)/5 + offset,
						End:   float64(index+1)/5 + offset,
						Bytes: bytes,
					}
				}
				measurements, err := measureThroughput(samples, []throughputWindow{
					{name: "baseline", start: 2, end: 8},
					{name: "raised", start: 28, end: 38},
					{name: "restored", start: 50, end: 58},
				}, 0)
				if (err != nil) != (tt.wantStatus != "pass") {
					t.Fatalf("offset %d microseconds: unexpected error %v", microseconds, err)
				}
				for _, measurement := range measurements[1:] {
					if measurement.Status != tt.wantStatus || math.Abs(*measurement.Fraction-float64(tt.bytes)/200_000) > 1e-12 {
						t.Fatalf("offset %d microseconds: unstable measurement %+v", microseconds, measurement)
					}
				}
			}
		})
	}
}

func TestMeasureThroughputBaselineBoundary(t *testing.T) {
	for _, tt := range []struct {
		name    string
		bytes   uint64
		wantErr bool
	}{
		{name: "ExactlyMinimum", bytes: 125_000},
		{name: "BelowMinimum", bytes: 124_999, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for microseconds := -1000; microseconds <= 1000; microseconds++ {
				offset := float64(microseconds) / 1e6
				samples := make([]flowInterval, 300)
				for index := range samples {
					samples[index] = flowInterval{Start: float64(index)/5 + offset, End: float64(index+1)/5 + offset, Bytes: tt.bytes}
				}
				_, err := measureThroughput(samples, []throughputWindow{{name: "baseline", start: 2, end: 8}}, 5e6)
				if (err != nil) != tt.wantErr {
					t.Fatalf("offset %d microseconds: minimum-baseline error = %v, want error %v", microseconds, err, tt.wantErr)
				}
			}
		})
	}
}

func TestVerifyFaultTimes(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		dropped, recovered        float64
		wantDropped, wantRestored float64
		wantErr                   bool
	}{
		{name: "Stall", dropped: 5.01, recovered: 9.02, wantDropped: 5, wantRestored: 9},
		{name: "ShortStall", dropped: 6, recovered: 8, wantDropped: 5, wantRestored: 9, wantErr: true},
		{name: "ShortOutage", dropped: 6, recovered: 16, wantDropped: 5, wantRestored: 17, wantErr: true},
		{name: "ReversedRouteEvents", dropped: 5.9, recovered: 5.1, wantDropped: 5, wantRestored: 6, wantErr: true},
		{name: "ImmediateRoam", dropped: 5.01, recovered: 5.02, wantDropped: 5, wantRestored: 5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			for name, at := range map[string]float64{"dropped": tt.dropped, "recovered": tt.recovered} {
				if err := os.WriteFile(filepath.Join(directory, "path-"+name+".txt"),
					fmt.Appendf(nil, "%.9f\n", 100+at), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := verifyFaultTimes(directory, 100, "path", tt.wantDropped, tt.wantRestored)
			if (err != nil) != tt.wantErr {
				t.Fatalf("verifyFaultTimes() = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyRTTStepFaultDuration(t *testing.T) {
	directory := t.TempDir()
	writeRTTStepEvidence(t, directory)
	for name, value := range map[string]string{
		"client-delay-raised.txt":   "1700000011\n",
		"client-delay-restored.txt": "1700000039\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, err := verifyRTTStep(directory, "tcp-rtt-step-bidir", rttStepDirectionalFlow())
	if err == nil || !strings.Contains(err.Error(), "RTT fault lasted") {
		t.Fatalf("accepted a shortened RTT fault: %v", err)
	}
}

func TestVerifyRTTStepBoundaryJitter(t *testing.T) {
	for _, tt := range []struct {
		name   string
		offset float64
	}{
		{name: "EarlierMillisecond", offset: -0.001},
		{name: "Earlier43Microseconds", offset: -0.000043},
		{name: "Original"},
		{name: "Later43Microseconds", offset: 0.000043},
		{name: "LaterMillisecond", offset: 0.001},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			flow := rttStepDirectionalFlow()
			for index := 40; index < 60; index++ {
				flow.ServerOutput.Intervals[index].Sum.Bytes = 240_000
			}
			flow.ServerOutput.Intervals[57].Sum.Bytes = 400_000
			shiftReceiverIntervals(flow.ServerOutput, tt.offset)
			measurements, err := verifyRTTStep(directory, "tcp-rtt-step", flow)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(*measurements[2].Fraction-0.26) > 0.001 {
				t.Fatalf("boundary jitter changed the measured recovery: %+v", measurements[2])
			}
		})
	}
}

func TestVerifyRTTStepHistoricalRun77(t *testing.T) {
	data, err := os.ReadFile("testdata/rtt-step-run77-forward.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		offset float64
	}{
		{name: "EarlierMillisecond", offset: -0.001},
		{name: "Earlier43Microseconds", offset: -0.000043},
		{name: "Original"},
		{name: "Later43Microseconds", offset: 0.000043},
		{name: "LaterMillisecond", offset: 0.001},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			receiver := flowResult{Started: 1_700_000_000}
			if err := json.Unmarshal(data, &receiver); err != nil {
				t.Fatal(err)
			}
			shiftReceiverIntervals(&receiver, tt.offset)
			measurements, err := verifyRTTStep(directory, "tcp-multipath-rtt-step", flowResult{
				Started: receiver.Started, ServerOutput: &receiver,
			})
			if err == nil || !strings.Contains(err.Error(), "require at least 25 percent") {
				t.Fatalf("historical failure was incorrectly accepted: %v", err)
			}
			if len(measurements) != 3 || measurements[2].Status != "below-threshold" ||
				math.Abs(*measurements[2].Fraction-0.21799306198233836) > 0.001 {
				t.Fatalf("incorrect historical recovery measurements: %+v", measurements)
			}
		})
	}
}

func shiftReceiverIntervals(receiver *flowResult, offset float64) {
	for index := range receiver.Intervals {
		sample := &receiver.Intervals[index].Sum
		if index > 0 {
			sample.Start += offset
		}
		sample.End += offset
	}
}

func TestVerifyRecoveryDirections(t *testing.T) {
	for _, tt := range []struct {
		name, scenario, wantErr string
		mutate                  func(*flowResult)
	}{
		{name: "Forward", scenario: "tcp-asymmetric-stall"},
		{name: "Reverse", scenario: "tcp-asymmetric-stall-reverse",
			mutate: func(flow *flowResult) {
				for index := range flow.Intervals {
					flow.Intervals[index].Sum = flow.Intervals[index].Reverse
				}
				flow.ServerOutput = nil
			}},
		{name: "Bidirectional", scenario: "tcp-asymmetric-stall-bidir"},
		{name: "ForwardSenderOnly", scenario: "tcp-asymmetric-stall", wantErr: "missing completed receiver",
			mutate: func(flow *flowResult) { flow.ServerOutput = nil }},
		{name: "ReverseSenderOnly", scenario: "tcp-asymmetric-stall-reverse", wantErr: "invalid receiver interval"},
		{name: "BidirectionalReverseStalled", scenario: "tcp-asymmetric-stall-bidir", wantErr: "reverse direction",
			mutate: func(flow *flowResult) {
				for index := 15; index < 20; index++ {
					flow.Intervals[index].Reverse.Bytes = 0
				}
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flow := rttStepDirectionalFlow()
			flow.Intervals = flow.Intervals[:20]
			flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:20]
			if tt.mutate != nil {
				tt.mutate(&flow)
			}
			_, err := verifyRecovery(tt.scenario, flow, 20, 0)
			if tt.wantErr == "" && err != nil || tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("verifyRecovery() = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyScenarioBufferedSender(t *testing.T) {
	for _, tt := range []struct{ name, scenario string }{
		{name: "Forward", scenario: "tcp-asymmetric-stall"},
		{name: "Bidirectional", scenario: "tcp-asymmetric-stall-bidir"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			flow := rttStepDirectionalFlow()
			flow.Intervals = flow.Intervals[:20]
			flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:20]
			flow.End.Received.Bytes, flow.End.ReverseReceived.Bytes = 15_000_000, 20_000_000
			for index := 15; index < 20; index++ {
				flow.ServerOutput.Intervals[index].Sum.Bytes = 0
			}
			for name, contents := range map[string]string{
				"path-dropped.txt":       "1700000005\n",
				"path-recovered.txt":     "1700000009\n",
				"client-tcp-sockets.txt": "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n",
			} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			measurements, err := verifyScenario(directory, tt.scenario, flow)
			if err == nil || !strings.Contains(err.Error(), "forward direction") {
				t.Fatalf("sender buffering satisfied recovery: %v", err)
			}
			if measurements[1].Bytes != 0 || measurements[1].Status != "below-threshold" {
				t.Fatalf("zero receiver progress was not reported: %+v", measurements)
			}
		})
	}
}

func TestVerifyScenarioIndependentFailures(t *testing.T) {
	directory := t.TempDir()
	writeRTTStepEvidence(t, directory)
	flow := rttStepDirectionalFlow()
	flow.End.Received.Bytes, flow.End.ReverseReceived.Bytes = 1, 1
	for index := 40; index < 60; index++ {
		flow.ServerOutput.Intervals[index].Sum.Bytes = 0
	}
	for index := 10; index < 40; index++ {
		flow.Intervals[index].Reverse.Bytes = 0
	}
	for name, contents := range map[string]string{
		"client-tcp-sockets.txt":       "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n",
		"client-tcp-sockets-after.txt": "0 0 local:3 remote:51822\n0 0 local:2 remote:51822\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	measurements, err := verifyScenario(directory, "tcp-multipath-rtt-step-bidir", flow)
	for _, want := range []string{"forward direction", "reverse direction", "carrier socket identities changed"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("missing independent failure %q: %v", want, err)
		}
	}
	if len(measurements) != 6 {
		t.Fatalf("reported %d windows, want all six", len(measurements))
	}
}

func TestReadTimestamp(t *testing.T) {
	for _, tt := range []struct{ name, value string }{
		{name: "NotANumber", value: "NaN"},
		{name: "PositiveInfinity", value: "+Inf"},
		{name: "NegativeInfinity", value: "-Inf"},
		{name: "Zero", value: "0"},
		{name: "Negative", value: "-1"},
		{name: "Invalid", value: "invalid"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "event.txt")
			if err := os.WriteFile(path, []byte(tt.value+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readTimestamp(path); err == nil {
				t.Fatalf("accepted invalid timestamp %q", tt.value)
			}
		})
	}
}
