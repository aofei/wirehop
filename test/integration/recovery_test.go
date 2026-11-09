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
			attachReceiverCounters(&flow, "tcp-rtt-step")
			measurements, err := verifyRTTStep(directory, "tcp-rtt-step", flow)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(*measurements[2].LowerFraction-0.26) > 0.001 {
				t.Fatalf("boundary jitter changed the measured recovery: %+v", measurements[2])
			}
		})
	}
}

func TestVerifyRTTStepHistoricalRun77NeedsCounterEvidence(t *testing.T) {
	data, err := os.ReadFile("testdata/rtt-step-run77-forward.json")
	if err != nil {
		t.Fatal(err)
	}
	var receiver flowResult
	if err := json.Unmarshal(data, &receiver); err != nil {
		t.Fatal(err)
	}
	receiver.Started = 1_700_000_000
	directory := t.TempDir()
	writeRTTStepEvidence(t, directory)
	_, err = verifyRTTStep(directory, "tcp-multipath-rtt-step", flowResult{Started: receiver.Started, ServerOutput: &receiver})
	if err == nil || !strings.Contains(err.Error(), "counter evidence") {
		t.Fatalf("accepted historical intervals as exact counter evidence: %v", err)
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
		{name: "ReverseSenderOnly", scenario: "tcp-asymmetric-stall-reverse", wantErr: "missing completed receiver"},
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
			attachReceiverCounters(&flow, tt.scenario)
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
			attachReceiverCounters(&flow, tt.scenario)
			measurements, err := verifyScenario(directory, tt.scenario, flow)
			if err == nil || !strings.Contains(err.Error(), "forward direction") {
				t.Fatalf("sender buffering satisfied recovery: %v", err)
			}
			if measurements[1].LowerBytes != 0 || measurements[1].Status != "below-threshold" {
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
	attachReceiverCounters(&flow, "tcp-multipath-rtt-step-bidir")
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
