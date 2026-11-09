package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFlowOrigin(t *testing.T) {
	for _, tt := range []struct {
		name, data string
		wantErr    bool
	}{
		{name: "ProducerOrigin", data: `{"started_boottime":100,"start":{"started_boottime":100}}`},
		{name: "MissingProducerOrigin", data: `{"started_boottime":100,"start":{}}`, wantErr: true},
		{name: "CollectorReceipt", data: `{"started_boottime":103,"start":{"started_boottime":100}}`, wantErr: true},
		{name: "MissingOrigins", data: `{}`, wantErr: true},
		{name: "ProducerFailureBeforeStart", data: `{"error":"original connection failure"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "flow.json")
			if err := os.WriteFile(path, []byte(tt.data), 0600); err != nil {
				t.Fatal(err)
			}
			flow, err := loadFlow(path)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "flow.json") || !strings.Contains(err.Error(), "producer measurement origin") {
					t.Fatalf("accepted unsupported measurement provenance: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if tt.name == "ProducerFailureBeforeStart" && flow.Error != "original connection failure" {
				t.Fatalf("lost original producer error: %+v", flow)
			}
		})
	}
}

func TestVerifyRecoveryOrigins(t *testing.T) {
	for _, tt := range []struct {
		name, scenario string
		offset, stop   int
		wantErr        string
		wantFraction   float64
	}{
		{name: "EarlierReceiver", offset: -3, stop: 20, wantFraction: 1},
		{name: "EarlierReceiverStalled", offset: -3, stop: 15, wantErr: "forward direction:"},
		{name: "BidirectionalEarlierReceiverStalled", scenario: "tcp-asymmetric-stall-bidir", offset: -3, stop: 15, wantErr: "forward direction:"},
		{name: "ReverseUsesClientOrigin", scenario: "tcp-asymmetric-stall-reverse", offset: -3, stop: 15, wantFraction: 1},
		{name: "LaterReceiver", offset: 3, stop: 20, wantFraction: 4},
		{name: "ReceiverAfterBaseline", offset: 4, stop: 20, wantErr: "baseline receiver throughput"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flow := originFlow(tt.offset, tt.stop)
			scenario := tt.scenario
			if scenario == "" {
				scenario = "tcp-asymmetric-stall"
			}
			if strings.HasSuffix(scenario, "-reverse") {
				for index := range flow.Intervals {
					flow.Intervals[index].Sum = flow.Intervals[index].Reverse
				}
			}
			attachReceiverCounters(&flow, scenario)
			measurements, err := verifyRecovery(scenario, flow, 20, 0)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("verifyRecovery() = %+v, %v, want %q", measurements, err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatal(err)
			} else if len(measurements) != 2 || measurements[1].LowerFraction == nil ||
				math.Abs(*measurements[1].LowerFraction-tt.wantFraction) > 1e-12 {
				t.Fatalf("wrong common-clock recovery: %+v", measurements)
			}
		})
	}
}

func TestVerifyProgressOrigins(t *testing.T) {
	for _, tt := range []struct {
		name, scenario string
		offset, stop   int
		wantErr        bool
	}{
		{name: "EarlierReceiver", offset: -3, stop: 20},
		{name: "EarlierReceiverStalled", offset: -3, stop: 17, wantErr: true},
		{name: "BidirectionalEarlierReceiverStalled", scenario: "tcp-stall-bidir", offset: -3, stop: 17, wantErr: true},
		{name: "ReverseUsesClientOrigin", scenario: "tcp-stall-reverse", offset: -3, stop: 17},
		{name: "LaterReceiver", offset: 3, stop: 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flow := originFlow(tt.offset, tt.stop)
			scenario := tt.scenario
			if scenario == "" {
				scenario = "tcp-stall"
			}
			if strings.HasSuffix(scenario, "-reverse") {
				for index := range flow.Intervals {
					flow.Intervals[index].Sum = flow.Intervals[index].Reverse
				}
			}
			attachReceiverCounters(&flow, scenario)
			measurements, err := verifyProgress(scenario, flow, 20)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "no receiver progress") {
					t.Fatalf("verifyProgress() = %+v, %v, want no progress", measurements, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVerifyRTTStepOrigins(t *testing.T) {
	for _, tt := range []struct {
		name         string
		offset       int
		wantFraction float64
	}{
		{name: "EarlierReceiver", offset: -3, wantFraction: 0.25},
		{name: "LaterReceiver", offset: 3, wantFraction: 0.3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			flow := rttStepDirectionalFlow()
			flow.ServerOutput.Started += float64(tt.offset)
			flow.ServerOutput.Intervals = nil
			for index := range 60 - tt.offset {
				var bytes uint64
				if index+tt.offset >= 0 {
					bytes = 1_000_000
					if index+tt.offset >= 10 {
						bytes = 250_000
					}
				}
				flow.ServerOutput.Intervals = append(flow.ServerOutput.Intervals, flowIntervalResult{
					Sum: flowInterval{Start: float64(index), End: float64(index + 1), Bytes: bytes, Sender: new(bool)},
				})
			}
			for index := 10; index < len(flow.Intervals); index++ {
				flow.Intervals[index].Reverse.Bytes = 250_000
			}
			attachReceiverCounters(&flow, "tcp-rtt-step-bidir")
			measurements, err := verifyRTTStep(directory, "tcp-rtt-step-bidir", flow)
			if err != nil {
				t.Fatal(err)
			}
			for _, measurement := range measurements {
				if measurement.Phase != "baseline" {
					want := tt.wantFraction
					if measurement.Direction == "reverse" {
						want = 0.25
					}
					if measurement.LowerFraction == nil || math.Abs(*measurement.LowerFraction-want) > 1e-12 {
						t.Fatalf("wrong common-clock RTT measurement: %+v", measurement)
					}
				}
			}
			if measurements[0].Start != measurements[3].Start || measurements[2].End != 58 || measurements[5].End != 58 {
				t.Fatalf("directions used different phase clocks: %+v", measurements)
			}
		})
	}
}

func TestVerifyRecoveryOriginTimestamp(t *testing.T) {
	flow := originFlow(-3, 20)
	flow.Started = 0
	attachReceiverCounters(&flow, "tcp-asymmetric-stall")
	if _, err := verifyRecovery("tcp-asymmetric-stall", flow, 20, 0); err == nil {
		t.Fatal("accepted receiver data without a common origin")
	}
}

func TestVerifyRecoveryOriginRounding(t *testing.T) {
	flow := originFlow(0, 20)
	flow.ServerOutput.Started += 0.00002
	flow.ServerOutput.Intervals[len(flow.ServerOutput.Intervals)-1].Sum.End -= 0.00002
	attachReceiverCounters(&flow, "tcp-asymmetric-stall")
	measurements, err := verifyRecovery("tcp-asymmetric-stall", flow, 20, 0)
	if err != nil || math.Abs(measurements[0].Covered-4) > 1e-12 || math.Abs(measurements[1].Covered-5) > 1e-12 {
		t.Fatalf("small initialization skew invalidated measured coverage: %+v, %v", measurements, err)
	}
	// Complete reporting intervals cannot replace missing counter evidence.
	flow.ServerOutput.Receiver = nil
	if _, err := verifyRecovery("tcp-asymmetric-stall", flow, 20, 0); err == nil {
		t.Fatal("accepted reporting intervals without receiver counter evidence")
	}
}

func originFlow(offset, stopped int) flowResult {
	flow := flowResult{Started: 100, ServerOutput: &flowResult{Started: 100 + float64(offset)}}
	sender, receiver := true, false
	for index := range 20 {
		flow.Intervals = append(flow.Intervals, flowIntervalResult{
			Sum:     flowInterval{Start: float64(index), End: float64(index + 1), Bytes: 1000, Sender: &sender},
			Reverse: flowInterval{Start: float64(index), End: float64(index + 1), Bytes: 1000, Sender: &receiver},
		})
	}
	for index := range 20 - offset {
		var bytes uint64
		if index+offset >= 0 && index+offset < stopped {
			bytes = 1000
		}
		flow.ServerOutput.Intervals = append(flow.ServerOutput.Intervals, flowIntervalResult{
			Sum: flowInterval{Start: float64(index), End: float64(index + 1), Bytes: bytes, Sender: &receiver},
		})
	}
	return flow
}
