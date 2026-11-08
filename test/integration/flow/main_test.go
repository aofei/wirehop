package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCollect(t *testing.T) {
	const start = `{"event":"start","data":{"started_boottime":100,"timestamp":{"timesecs":1700000000},"test_start":{"duration":20}}}`
	const interval = `{"event":"interval","data":{"sum":{"start":0,"end":0.2,"bytes":100,"sender":false}}}`
	const end = `{"event":"end","data":{"sum_received":{"bytes":100}}}`
	for _, tt := range []struct {
		name, role, input, wantErr string
		wantNoMarker               bool
	}{
		{name: "Client", role: "client", input: start + interval + end},
		{name: "Server", role: "server", input: start + interval + end},
		{name: "MissingStart", input: interval + end, wantErr: "outside an active"},
		{name: "NullStart", input: `{"event":"start","data":null}`, wantErr: "invalid iperf3 start"},
		{name: "MissingStartData", input: `{"event":"start"}`, wantErr: "invalid iperf3 start"},
		{name: "EmptyStart", input: `{"event":"start","data":{}}`, wantErr: "invalid iperf3 start"},
		{name: "MalformedTestStart", input: `{"event":"start","data":{"test_start":[]}}`, wantErr: "invalid iperf3 start"},
		{name: "NullInterval", input: start + `{"event":"interval","data":null}`, wantErr: "invalid iperf3 interval"},
		{name: "EmptyInterval", input: start + `{"event":"interval","data":{}}`, wantErr: "invalid iperf3 interval"},
		{name: "EmptyEnd", input: start + interval + `{"event":"end","data":{}}`, wantErr: "invalid iperf3 end"},
		{name: "EmptyError", input: start + interval + `{"event":"error","data":""}` + end, wantErr: "invalid iperf3 error"},
		{name: "NullError", input: start + interval + `{"event":"error","data":null}` + end, wantErr: "invalid iperf3 error"},
		{name: "MissingEnd", input: start + interval, wantErr: "missing completed"},
		{name: "MissingIntervals", input: start + end, wantErr: "missing completed"},
		{name: "DuplicateStart", input: start + start, wantErr: "multiple iperf3"},
		{name: "DuplicateEnd", input: start + interval + end + end, wantErr: "invalid iperf3 end"},
		{name: "IntervalAfterEnd", input: start + interval + end + interval, wantErr: "outside an active"},
		{name: "UnknownEvent", input: `{"event":"unexpected","data":{}}`, wantErr: "unexpected iperf3 event"},
		{name: "MalformedEvent", input: start + `{"event":`, wantErr: "unexpected EOF"},
		{name: "ProcessError", input: `{"event":"error","data":"control connection failed"}`, wantErr: "control connection failed"},
		{name: "ErrorBeforeStart", input: `{"event":"error","data":"control connection failed"}` + start + interval + end, wantErr: "control connection failed", wantNoMarker: true},
		{name: "ProcessErrorWithEmptyEnd", input: start + interval + `{"event":"error","data":"control connection failed"}{"event":"end","data":{}}`, wantErr: "control connection failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			role := tt.role
			if role == "" {
				role = "client"
			}
			now := 100.0
			result, err := collect(strings.NewReader(tt.input), directory, role, func() (float64, error) {
				at := now
				now += 0.2
				return at, nil
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("collect() error = %v, want %q", err, tt.wantErr)
				}
				if tt.wantNoMarker {
					if _, err := os.Stat(filepath.Join(directory, role+"-flow-start.txt")); !os.IsNotExist(err) {
						t.Fatalf("published a flow marker after an error: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Started != 100 || len(result.Intervals) != 1 || result.End == nil {
				t.Fatalf("incorrect collected result: %+v", result)
			}
			marker, err := os.ReadFile(filepath.Join(directory, role+"-flow-start.txt"))
			if err != nil || string(marker) != "100.000000000\n" {
				t.Fatalf("flow start = %q, %v", marker, err)
			}
			_, err = os.Stat(filepath.Join(directory, "flow-start.txt"))
			if role == "client" && err != nil || role == "server" && !os.IsNotExist(err) {
				t.Fatalf("client wall-clock marker existence: %v", err)
			}
			trace, err := os.ReadFile(filepath.Join(directory, role+"-iperf-events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(trace)), "\n")
			if len(lines) != 3 {
				t.Fatalf("trace has %d events, want 3", len(lines))
			}
			var event struct {
				Received float64 `json:"received_boottime"`
				Event    string  `json:"event"`
			}
			if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
				t.Fatal(err)
			}
			if event.Received != 100 || event.Event != "start" {
				t.Fatalf("incorrect event clock: %+v", event)
			}
		})
	}

	t.Run("DelayedStart", func(t *testing.T) {
		directory := t.TempDir()
		reader, writer := io.Pipe()
		defer writer.Close()
		completed := make(chan error, 1)
		go func() {
			defer reader.Close()
			_, err := collect(reader, directory, "client", func() (float64, error) { return 100, nil })
			completed <- err
		}()
		// Synchronize with the waiting decoder without publishing a data-start event.
		if _, err := io.WriteString(writer, " "); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"client-flow-start.txt", "flow-start.txt"} {
			if _, err := os.Stat(filepath.Join(directory, name)); !os.IsNotExist(err) {
				t.Fatalf("published %s before data started: %v", name, err)
			}
		}
		if _, err := io.WriteString(writer, start+interval+end); err != nil {
			t.Fatal(err)
		}
		writer.Close()
		if err := <-completed; err != nil {
			t.Fatal(err)
		}
		marker, err := os.ReadFile(filepath.Join(directory, "client-flow-start.txt"))
		if err != nil || string(marker) != "100.000000000\n" {
			t.Fatalf("flow start = %q, %v", marker, err)
		}
	})
}

func TestCollectMeasurementClock(t *testing.T) {
	for _, tt := range []struct {
		name, data string
		invalid    bool
	}{
		{name: "DelayedDelivery", data: `"started_boottime":100,"test_start":{}`},
		{name: "MissingTimestamp", data: `"test_start":{}`, invalid: true},
		{name: "ZeroTimestamp", data: `"started_boottime":0,"test_start":{}`, invalid: true},
		{name: "NegativeTimestamp", data: `"started_boottime":-1,"test_start":{}`, invalid: true},
		{name: "FutureTimestamp", data: `"started_boottime":104,"test_start":{}`, invalid: true},
		{name: "WrongTimestampType", data: `"started_boottime":"100","test_start":{}`, invalid: true},
		{name: "OmittedWarmup", data: `"started_boottime":100,"test_start":{"omit":1}`, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			input := `{"event":"start","data":{` + tt.data + `}}` +
				`{"event":"interval","data":{"sum":{"start":0,"end":0.2,"bytes":100,"sender":false}}}` +
				`{"event":"end","data":{"sum_received":{"bytes":100}}}`
			before := time.Now().Add(-3 * time.Second).Unix()
			result, err := collect(strings.NewReader(input), directory, "client", func() (float64, error) { return 103, nil })
			if tt.invalid {
				if err == nil {
					t.Fatal("accepted an invalid measurement clock")
				}
				if _, err := os.Stat(filepath.Join(directory, "client-flow-start.txt")); !os.IsNotExist(err) {
					t.Fatalf("published an invalid clock: %v", err)
				}
				return
			}
			if err != nil || result.Started != 100 {
				t.Fatalf("collector latency changed the measurement origin: %+v, %v", result, err)
			}
			data, err := os.ReadFile(filepath.Join(directory, "flow-start.txt"))
			if err != nil {
				t.Fatal(err)
			}
			wall, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil || wall < before || wall > time.Now().Add(-3*time.Second).Unix() {
				t.Fatalf("wall-clock marker retained the delivery delay: %q, %v", data, err)
			}
		})
	}
}

func TestCollectRetainsProducerError(t *testing.T) {
	const producerError = "control connection refused"
	for _, tt := range []struct{ name, suffix, detail string }{
		{name: "EndBeforeStart", suffix: `{"event":"end","data":{}}`},
		{name: "InvalidEnd", suffix: `{"event":"end","data":null}`, detail: "invalid iperf3 end"},
		{name: "MalformedJSON", suffix: `{"event":`, detail: "unexpected EOF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			input := `{"event":"error","data":"` + producerError + `"}` + tt.suffix
			result, err := collect(strings.NewReader(input), t.TempDir(), "client", func() (float64, error) { return 100, nil })
			if result.Error != producerError || err == nil || !strings.Contains(err.Error(), producerError) {
				t.Fatalf("lost producer error: result=%+v, err=%v", result, err)
			}
			if tt.detail != "" && !strings.Contains(err.Error(), tt.detail) {
				t.Fatalf("lost independent stream error %q: %v", tt.detail, err)
			}
		})
	}
}
