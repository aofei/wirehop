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

// attachReceiverCounters makes discrete deliveries at interval midpoints for synthetic scenario fixtures.
// These fixtures exercise direction and phase selection. Boundary uncertainty has separate event-level tests.
func attachReceiverCounters(flow *flowResult, scenario string) {
	for _, direction := range receiverDirections(scenario, flow) {
		receiver := direction.receiver
		if receiver == nil {
			continue
		}
		receiver.Receiver = nil
		trace := &receiverTrace{Started: int64(math.Round(receiver.Started * 1e9))}
		for _, interval := range receiver.Intervals {
			sample := interval.Sum
			if direction.name == "reverse" && strings.HasSuffix(scenario, "-bidir") {
				sample = interval.Reverse
			}
			if sample.Sender == nil || *sample.Sender {
				trace = nil
				break
			}
			trace.Ended = trace.Started + int64(math.Round(sample.End*1e9))
			if sample.Bytes > 0 {
				at := trace.Started + int64(math.Round((sample.Start+sample.End)/2*1e9))
				trace.Events = append(trace.Events, receiverEvent{Began: at, Ended: at, Bytes: sample.Bytes})
				trace.Total += sample.Bytes
			}
		}
		receiver.Receiver = trace
	}
}

func writeReceiverTrace(t *testing.T, directory, role string, trace *receiverTrace) {
	t.Helper()
	trace.Type, trace.Stream, trace.Count = "receiver_reads", 1, len(trace.Events)
	if trace.Error == nil {
		trace.Error = new(false)
	}
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	for _, event := range trace.Events {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		data = append(append(data, encoded...), '\n')
	}
	if err := os.WriteFile(filepath.Join(directory, role+"-receiver-1.jsonl"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestMeasureWindowCounterBounds(t *testing.T) {
	trace := &receiverTrace{Started: 100e9, Ended: 110e9, Events: []receiverEvent{
		{Began: 100e9, Ended: 100e9, Bytes: 1},
		{Began: 101e9 - 1, Ended: 101e9 + 1, Bytes: 10},
		{Began: 102e9, Ended: 102e9, Bytes: 100},
		{Began: 103e9 - 1, Ended: 103e9, Bytes: 1000},
		{Began: 103e9, Ended: 103e9, Bytes: 10000},
	}}
	for _, tt := range []struct {
		name         string
		originOffset float64
		start, end   float64
		lower, upper uint64
		wantErr      bool
	}{
		{name: "BracketsBothCuts", start: 1, end: 3, lower: 100, upper: 1110},
		{name: "ExactStartIncluded", start: 2, end: 3, lower: 100, upper: 1100},
		{name: "ExactEndExcluded", start: 0, end: 1, lower: 1, upper: 11},
		{name: "ZeroDelivery", start: 4, end: 9},
		{name: "ShortWindow", start: 0, end: 0.5, lower: 1, upper: 1},
		{name: "KnownZeroBeforeReceiver", originOffset: -1, end: 0.5},
		{name: "BeforeInitializationEndsAtFirstUpdate", originOffset: -1, end: 1},
		{name: "MissingTail", start: 9, end: 11, wantErr: true},
		{name: "ReversedWindow", start: 2, end: 1, wantErr: true},
		{name: "NegativeWindow", start: -1, end: 1, wantErr: true},
		{name: "NotFinite", end: math.NaN(), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			measurement, err := measureWindow(trace, 100+tt.originOffset, throughputWindow{name: tt.name, start: tt.start, end: tt.end})
			if (err != nil) != tt.wantErr || measurement.LowerBytes != tt.lower || measurement.UpperBytes != tt.upper {
				t.Fatalf("measureWindow() = %+v, %v", measurement, err)
			}
		})
	}
}

func TestMeasureThroughputCounterThreshold(t *testing.T) {
	for _, tt := range []struct {
		name      string
		bytes     uint64
		uncertain bool
		want      string
	}{
		{name: "ExactlyThreshold", bytes: 25, want: "pass"},
		{name: "AboveThreshold", bytes: 26, want: "pass"},
		{name: "BelowThreshold", bytes: 24, want: "below-threshold"},
		{name: "NoDelivery", want: "below-threshold"},
		{name: "UncertainCrossing", bytes: 25, uncertain: true, want: "inconclusive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for shift := -1000; shift <= 1000; shift++ {
				trace := &receiverTrace{Ended: 110e9, Events: []receiverEvent{
					{Began: 101e9, Ended: 101e9, Bytes: 100},
					{Began: 106e9, Ended: 106e9, Bytes: tt.bytes},
				}}
				if tt.uncertain {
					trace.Events[1].Began = 105e9 - 2e6
					trace.Events[1].Ended = 105e9 + 2e6
				}
				offset := float64(shift) / 1e6
				measured, err := measureThroughput(trace, 100, []throughputWindow{
					{name: "baseline", start: 0.5 + offset, end: 4.5 + offset},
					{name: "recovered", start: 5 + offset, end: 9 + offset},
				}, 0)
				if (err != nil) != (tt.want != "pass") || measured[1].Status != tt.want {
					t.Fatalf("shift=%d: measurements=%+v, error=%v", shift, measured, err)
				}
			}
		})
	}
}

func TestLoadReceiverTrace(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*receiverTrace, *flowResult)
		wantErr bool
	}{
		{name: "Conserved"},
		{name: "BidirectionalPeerDiffers", mutate: func(_ *receiverTrace, flow *flowResult) {
			flow.End.Received.Sender = new(true)
			flow.End.Received.Bytes = 9999
			flow.End.ReverseReceived.Sender = new(false)
			flow.End.ReverseReceived.Bytes = 100
		}},
		{name: "LostIntervalBytes", wantErr: true, mutate: func(_ *receiverTrace, flow *flowResult) { flow.Intervals[0].Sum.Bytes-- }},
		{name: "WrongLocalTotal", wantErr: true, mutate: func(_ *receiverTrace, flow *flowResult) { flow.End.Received.Bytes-- }},
		{name: "TraceTotalMismatch", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Total-- }},
		{name: "UnknownLocalDirection", wantErr: true, mutate: func(_ *receiverTrace, flow *flowResult) { flow.End.Received.Sender = nil }},
		{name: "UnknownIntervalDirection", wantErr: true, mutate: func(_ *receiverTrace, flow *flowResult) { flow.Intervals[0].Sum.Sender = nil }},
		{name: "Nonmonotonic", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Events[1].Began = 100e9 }},
		{name: "AfterFinalSnapshot", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Ended = 101e9 }},
		{name: "WrongOrigin", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Started += 10000 }},
		{name: "OverflowedTrace", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Error = new(true) }},
		{name: "ZeroByteRecord", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Events[0].Bytes = 0 }},
		{name: "CounterOverflow", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Events[1].Bytes = math.MaxUint64 }},
		{name: "BeforeInitialization", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Events[0].Began = trace.Started - 1 }},
		{name: "ReversedRecord", wantErr: true, mutate: func(trace *receiverTrace, _ *flowResult) { trace.Events[0].Ended = trace.Events[0].Began - 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			trace := &receiverTrace{Started: 100e9, Ended: 104e9, Total: 100, Events: []receiverEvent{
				{Began: 101e9, Ended: 101e9 + 1, Bytes: 40}, {Began: 102e9, Ended: 102e9 + 1, Bytes: 60},
			}}
			flow := &flowResult{Started: 100, Intervals: []flowIntervalResult{{Sum: flowInterval{Bytes: 100, Sender: new(false)}}}}
			flow.End.Received.Bytes, flow.End.Received.Sender = 100, new(false)
			if tt.mutate != nil {
				tt.mutate(trace, flow)
			}
			writeReceiverTrace(t, directory, "client", trace)
			_, err := loadReceiverTrace(directory, "client", flow)
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadReceiverTrace() = %v", err)
			}
		})
	}
}

func TestLoadReceiverTraceMissingOrMalformed(t *testing.T) {
	for _, data := range []string{"", "{}\n", "invalid\n", `{"type":"receiver_reads","stream_id":1,"started_boottime_ns":100000000000,"ended_boottime_ns":104000000000,"count":1,"error":false}` + "\n"} {
		t.Run(fmt.Sprintf("Length%d", len(data)), func(t *testing.T) {
			directory := t.TempDir()
			flow := &flowResult{Started: 100}
			if _, err := loadReceiverTrace(directory, "client", flow); err == nil {
				t.Fatal("accepted missing trace")
			}
			if err := os.WriteFile(filepath.Join(directory, "client-receiver-1.jsonl"), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadReceiverTrace(directory, "client", flow); err == nil {
				t.Fatal("accepted malformed or incomplete trace")
			}
		})
	}
}

func TestMeasureThroughputFalsePassRegression(t *testing.T) {
	flow, err := loadFlow("testdata/receiver-rtt-flow.json")
	if err != nil {
		t.Fatal(err)
	}
	trace, err := loadReceiverTrace("testdata", "receiver-rtt-client", &flow)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/receiver-rtt-windows.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded []struct {
		Name  string  `json:"phase"`
		Start float64 `json:"start_seconds"`
		End   float64 `json:"end_seconds"`
	}
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	var windows []throughputWindow
	var formerRates []float64
	for _, window := range recorded {
		windows = append(windows, throughputWindow{name: window.Name, start: window.Start, end: window.End})
		var bytes float64
		for _, interval := range flow.Intervals {
			sample := interval.Sum
			overlap := min(window.End, sample.End) - max(window.Start, sample.Start)
			if overlap > 0 {
				bytes += float64(sample.Bytes) * overlap / (sample.End - sample.Start)
			}
		}
		formerRates = append(formerRates, bytes*8/(window.End-window.Start))
	}
	if formerRates[1]/formerRates[0] < 0.25 {
		t.Fatal("fixture no longer demonstrates the former interpolation false pass")
	}
	measured, err := measureThroughput(trace, flow.Started, windows, 5e6)
	if err == nil || measured[1].Status != "below-threshold" || measured[2].Status != "pass" {
		t.Fatalf("failed to reject a real false pass: %+v, %v", measured, err)
	}
	if measured[0].LowerBytes != 13238272 || measured[1].LowerBytes != 5505024 ||
		measured[1].LowerBytes != measured[1].UpperBytes || math.Abs(*measured[1].LowerFraction-0.24908117503944321) > 1e-12 {
		t.Fatalf("counter reconstruction differs from the original application records: %+v", measured)
	}
}

func TestLoadReceiverTraceRecordIntegrity(t *testing.T) {
	for _, tt := range []struct {
		name     string
		key      string
		value    any
		truncate bool
		extra    string
	}{
		{name: "TruncatedRecords", truncate: true},
		{name: "ExtraRecord", extra: `{"began_boottime_ns":103000000000,"ended_boottime_ns":103000000000,"bytes":1}`},
		{name: "MalformedTail", extra: "{"},
		{name: "CountTooSmall", key: "count", value: 1},
		{name: "CountTooLarge", key: "count", value: 1048577},
		{name: "NegativeCount", key: "count", value: -1},
		{name: "MissingErrorFlag", key: "error", value: nil},
		{name: "WrongType", key: "type", value: "other"},
		{name: "InvalidStream", key: "stream_id", value: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			trace := &receiverTrace{Started: 100e9, Ended: 104e9, Total: 100, Events: []receiverEvent{
				{Began: 101e9, Ended: 101e9, Bytes: 40}, {Began: 102e9, Ended: 102e9, Bytes: 60},
			}}
			flow := &flowResult{Started: 100, Intervals: []flowIntervalResult{{Sum: flowInterval{Start: 0, End: 4, Bytes: 100, Sender: new(false)}}}}
			flow.End.Received.Bytes, flow.End.Received.Sender = 100, new(false)
			writeReceiverTrace(t, directory, "client", trace)
			if _, err := loadReceiverTrace(directory, "client", flow); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, "client-receiver-1.jsonl")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			if tt.key != "" {
				var header map[string]any
				if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
					t.Fatal(err)
				}
				header[tt.key] = tt.value
				encoded, err := json.Marshal(header)
				if err != nil {
					t.Fatal(err)
				}
				lines[0] = string(encoded)
			}
			if tt.truncate {
				lines = lines[:len(lines)-1]
			}
			if tt.extra != "" {
				lines = append(lines, tt.extra)
			}
			if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadReceiverTrace(directory, "client", flow); err == nil {
				t.Fatal("accepted incomplete or corrupt counter records")
			}
		})
	}
}

func TestVerifyProgressCounterBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name  string
		event receiverEvent
		want  string
	}{
		{name: "DefiniteDelivery", event: receiverEvent{Began: 119e9, Ended: 119e9 + 1, Bytes: 1}, want: "pass"},
		{name: "BeforeWindow", event: receiverEvent{Began: 118e9, Ended: 118e9 + 1, Bytes: 1}, want: "no-progress"},
		{name: "AfterWindow", event: receiverEvent{Began: 120e9, Ended: 120e9, Bytes: 1}, want: "no-progress"},
		{name: "UncertainStart", event: receiverEvent{Began: 119e9 - 1, Ended: 119e9 + 1, Bytes: 1}, want: "inconclusive"},
		{name: "UncertainEnd", event: receiverEvent{Began: 120e9 - 1, Ended: 120e9 + 1, Bytes: 1}, want: "inconclusive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flow := flowResult{Started: 100, ServerOutput: &flowResult{Started: 100, Receiver: &receiverTrace{
				Started: 100e9, Ended: 121e9, Events: []receiverEvent{tt.event},
			}}}
			measured, err := verifyProgress("tcp-stall", flow, 20)
			if (err != nil) != (tt.want != "pass") || len(measured) != 1 || measured[0].Status != tt.want {
				t.Fatalf("verifyProgress() = %+v, %v", measured, err)
			}
		})
	}
}
