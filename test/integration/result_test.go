package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyResult(t *testing.T) {
	for _, tt := range []struct {
		name, scenario string
		replace        map[string]string
		remove         string
		missingEnd     bool
		want           []string
	}{
		{name: "Complete"},
		{name: "ClientExit", replace: map[string]string{"client-exit.txt": "7"}, want: []string{"client-exit.txt records process exit 7"}},
		{name: "ServerExit", replace: map[string]string{"server-flow-exit.txt": "7"}, want: []string{"server-flow-exit.txt records process exit 7"}},
		{name: "MissingClientExit", remove: "client-exit.txt", want: []string{"client-exit.txt"}},
		{name: "MissingServer", remove: "server-flow.json", want: []string{"server-flow.json", "forward direction"}},
		{name: "IncompleteServer", replace: map[string]string{"server-flow.json": `{"started_boottime":1700000000,"start":{"started_boottime":1700000000}}`}, want: []string{"server-flow.json: transfer did not complete"}},
		{name: "IntervalsWithoutServerCompletion", missingEnd: true, want: []string{"server-flow.json: transfer did not complete"}},
		{name: "MalformedClientAndChurn", replace: map[string]string{
			"flow.json":                    "{invalid",
			"client-tcp-sockets-after.txt": "0 0 local:3 remote:51822\n0 0 local:2 remote:51822\n",
		}, want: []string{"flow.json", "carrier socket identities changed"}},
		{name: "MissingProducerOriginAndChurn", replace: map[string]string{
			"flow.json":                    `{"started_boottime":1700000000}`,
			"client-tcp-sockets-after.txt": "0 0 local:3 remote:51822\n0 0 local:2 remote:51822\n",
		}, want: []string{"producer measurement origin", "carrier socket identities changed"}},
		{name: "HarnessFailure", replace: map[string]string{"harness-error.txt": "server observations did not complete"}, want: []string{"test harness: server observations"}},
		{name: "Reverse", scenario: "tcp-multipath-rtt-step-reverse"},
		{name: "ReverseMissingServer", scenario: "tcp-multipath-rtt-step-reverse", remove: "server-flow.json", want: []string{"server-flow.json"}},
		{name: "ReverseServerError", scenario: "tcp-multipath-rtt-step-reverse", replace: map[string]string{
			"server-flow.json": `{"started_boottime":1700000000,"error":"server interrupted","end":{"sum_sent":{"bytes":1}}}`,
		}, want: []string{"server interrupted"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			flow := rttStepDirectionalFlow()
			flow.End.Received.Bytes, flow.End.ReverseReceived.Bytes = 60_000_000, 60_000_000
			flow.ServerOutput.End.Received.Bytes, flow.ServerOutput.End.Sent.Bytes = 60_000_000, 60_000_000
			scenario := tt.scenario
			if scenario == "" {
				scenario = "tcp-multipath-rtt-step-bidir"
			}
			after := "TcpActiveOpens 6\n"
			if strings.HasSuffix(scenario, "-reverse") {
				for index := range flow.Intervals {
					flow.Intervals[index].Sum = flow.Intervals[index].Reverse
					flow.ServerOutput.Intervals[index].Sum = flow.ServerOutput.Intervals[index].Reverse
				}
				flow.ServerOutput.End.Received.Bytes = 0
				after = "TcpActiveOpens 5\n"
			}
			if tt.missingEnd {
				flow.ServerOutput.End.Received.Bytes = 0
				flow.ServerOutput.End.Sent.Bytes = 0
			}
			for name, result := range map[string]*flowResult{"flow.json": &flow, "server-flow.json": flow.ServerOutput} {
				data, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for name, data := range map[string]string{
				"client-exit.txt": "0", "server-flow-exit.txt": "0",
				"client-tcp-sockets.txt":       "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n",
				"client-tcp-sockets-after.txt": "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n",
				"client-before.txt":            "TcpActiveOpens 3\n", "client-after.txt": after,
			} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for name, data := range tt.replace {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tt.remove != "" {
				if err := os.Remove(filepath.Join(directory, tt.remove)); err != nil {
					t.Fatal(err)
				}
			}
			_, measurements, err := verifyResult(directory, scenario)
			if len(tt.want) == 0 && err != nil {
				t.Fatal(err)
			}
			for _, want := range tt.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("verifyResult() = %v, want %q", err, want)
				}
			}
			if tt.name == "MalformedClientAndChurn" && len(measurements) != 0 {
				t.Fatal("calculated throughput from malformed input")
			}
		})
	}
}
