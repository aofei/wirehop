//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestIPerfReceiverAccounting(t *testing.T) {
	if os.Getenv("WIREHOP_TEST_TOOLS") != "1" {
		t.Skip("requires the kernel test image's iperf3 and reporter delay fixture")
	}
	// Leave workers running while the reporter queries TCP_INFO. Read-then-reset counters lose these bytes.
	t.Setenv("LD_PRELOAD", "/usr/local/lib/iperf-stats-delay.so")
	t.Setenv("WIREHOP_TEST_STATS_DELAY_NS", "100000000")
	for _, tt := range []struct {
		name                           string
		arguments                      []string
		clientReceives, serverReceives bool
	}{
		{name: "Forward", serverReceives: true},
		{name: "Reverse", arguments: []string{"-R"}, clientReceives: true},
		{name: "Bidirectional", arguments: []string{"--bidir"}, clientReceives: true, serverReceives: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server, directory := runIPerfIntervals(t, append([]string{"-b", "20M", "-l", "16K"}, tt.arguments...)...)
			for _, endpoint := range []struct {
				name, role string
				result     streamResult
				receives   bool
			}{
				{name: "Client", role: "client", result: client, receives: tt.clientReceives},
				{name: "Server", role: "server", result: server, receives: tt.serverReceives},
			} {
				t.Run(endpoint.name, func(t *testing.T) {
					if err := checkReceiverAccounting(endpoint.result, directory, endpoint.role, endpoint.receives); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestCheckReceiverAccounting(t *testing.T) {
	const received = `{"sum_received":{"bytes":100,"sender":false}}`
	const sent = `{"sum_received":{"bytes":100,"sender":true}}`
	const receivingInterval = `{"sum":{"bytes":100,"sender":false}}`
	const sendingInterval = `{"sum":{"bytes":100,"sender":true}}`
	const trace = `{"started_boottime_ns":100000000000,"ended_boottime_ns":104000000000,"total_bytes":100,"count":2,"error":false}` + "\n" +
		`{"began_boottime_ns":101000000000,"ended_boottime_ns":101000000001,"bytes":40}` + "\n" +
		`{"began_boottime_ns":102000000000,"ended_boottime_ns":102000000001,"bytes":60}` + "\n"
	for _, tt := range []struct {
		name, end, interval string
		receives, trace     bool
		wantError           bool
	}{
		{name: "Receiver", end: received, interval: receivingInterval, receives: true, trace: true},
		{name: "Sender", end: sent, interval: sendingInterval},
		{name: "BidirectionalReceiver", receives: true, trace: true,
			end:      `{"sum_received":{"bytes":9999,"sender":true},"sum_received_bidir_reverse":{"bytes":100,"sender":false}}`,
			interval: `{"sum":{"bytes":9999,"sender":true},"sum_bidir_reverse":{"bytes":100,"sender":false}}`},
		{name: "MissingReceiverIdentity", end: `{"sum_received":{"bytes":100}}`, interval: receivingInterval, receives: true, wantError: true},
		{name: "IncorrectReceiverIdentity", end: sent, interval: receivingInterval, receives: true, wantError: true},
		{name: "UnexpectedLocalReceiver", end: received, interval: receivingInterval, trace: true, wantError: true},
		{name: "UnexpectedReceiverTrace", end: sent, interval: sendingInterval, trace: true, wantError: true},
		{name: "MissingReceiverTrace", end: received, interval: receivingInterval, receives: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			if tt.trace {
				if err := os.WriteFile(filepath.Join(directory, "server-receiver-1.jsonl"), []byte(trace), 0600); err != nil {
					t.Fatal(err)
				}
			}
			result := streamResult{End: json.RawMessage(tt.end), Intervals: []json.RawMessage{json.RawMessage(tt.interval)}}
			err := checkReceiverAccounting(result, directory, "server", tt.receives)
			if (err != nil) != tt.wantError {
				t.Fatalf("receiver accounting = %v, want error %t", err, tt.wantError)
			}
		})
	}
}

// checkReceiverAccounting checks the local receiver totals and their independent counter trace.
func checkReceiverAccounting(result streamResult, directory, role string, receives bool) error {
	var end map[string]json.RawMessage
	if err := json.Unmarshal(result.End, &end); err != nil {
		return err
	}
	var local []uint64
	for _, name := range []string{"sum_received", "sum_received_bidir_reverse"} {
		var sum struct {
			Bytes  uint64
			Sender *bool
		}
		if raw := end[name]; len(raw) > 0 {
			if err := json.Unmarshal(raw, &sum); err != nil {
				return err
			}
		}
		if sum.Sender != nil && !*sum.Sender {
			local = append(local, sum.Bytes)
		}
	}
	paths, err := filepath.Glob(filepath.Join(directory, role+"-receiver-*.jsonl"))
	if err != nil {
		return err
	}
	if !receives {
		if len(local) != 0 || len(paths) != 0 {
			return fmt.Errorf("%s: send-only endpoint has %d local receiver totals and %d traces", role, len(local), len(paths))
		}
		return nil
	}
	if len(local) != 1 || local[0] == 0 {
		return fmt.Errorf("invalid local receiver totals: %v", local)
	}
	var intervalBytes uint64
	for _, raw := range result.Intervals {
		var interval struct {
			Sum struct {
				Bytes  uint64
				Sender bool
			}
			Reverse *struct {
				Bytes  uint64
				Sender bool
			} `json:"sum_bidir_reverse"`
		}
		if err := json.Unmarshal(raw, &interval); err != nil {
			return err
		}
		if !interval.Sum.Sender {
			intervalBytes += interval.Sum.Bytes
		}
		if interval.Reverse != nil && !interval.Reverse.Sender {
			intervalBytes += interval.Reverse.Bytes
		}
	}
	if len(paths) != 1 {
		return fmt.Errorf("receiver trace paths %v: require exactly one trace", paths)
	}
	file, err := os.Open(paths[0])
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var header struct {
		Started int64  `json:"started_boottime_ns"`
		Ended   int64  `json:"ended_boottime_ns"`
		Total   uint64 `json:"total_bytes"`
		Count   int
		Error   bool
	}
	if err := decoder.Decode(&header); err != nil {
		return err
	}
	if header.Error || header.Count == 0 {
		return fmt.Errorf("invalid receiver header: %+v", header)
	}
	var eventBytes uint64
	previous := header.Started
	for range header.Count {
		var event struct {
			Began int64 `json:"began_boottime_ns"`
			Ended int64 `json:"ended_boottime_ns"`
			Bytes uint64
		}
		if err := decoder.Decode(&event); err != nil {
			return err
		}
		if event.Began < previous || event.Ended < event.Began || event.Ended > header.Ended {
			return fmt.Errorf("receiver counter event outside coverage: %+v", event)
		}
		previous = event.Ended
		eventBytes += event.Bytes
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("extra receiver trace data: %v", err)
	}
	if intervalBytes != local[0] || eventBytes != local[0] || header.Total != local[0] {
		return fmt.Errorf("nonconserving receive accounting: intervals=%d events=%d header=%d local=%d", intervalBytes, eventBytes, header.Total, local[0])
	}
	return nil
}
