//go:build linux

package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestIPerfMeasurementClock(t *testing.T) {
	if os.Getenv("WIREHOP_TEST_TOOLS") != "1" {
		t.Skip("requires the kernel test image's tools")
	}
	flow, err := exec.LookPath("wirehop-test-flow")
	if err != nil {
		t.Fatal(err)
	}
	iperf, err := exec.LookPath("iperf3")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ name, role string }{
		{name: "Client", role: "client"},
		{name: "Server", role: "server"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			role := tt.role
			directory := t.TempDir()
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			wrapper := `#!/bin/sh
set -eu
kill -STOP "$PPID"
"$WIREHOP_FLOW_TOOL" now > "$WIREHOP_PAUSE_DIRECTORY/producer-launched.txt"
exec "$WIREHOP_REAL_IPERF3" "$@"
`
			if err := os.WriteFile(filepath.Join(directory, "iperf3"), []byte(wrapper), 0755); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "sh", "-c", measurementClockScript, "clock-test", directory, flow, iperf, strconv.Itoa(port), role)
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			command.Cancel = func() error {
				syscall.Kill(-command.Process.Pid, syscall.SIGCONT)
				return syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
			}
			command.WaitDelay = 2 * time.Second
			if output, err := command.CombinedOutput(); err != nil {
				for _, endpoint := range []string{"client", "server"} {
					data, _ := os.ReadFile(filepath.Join(directory, endpoint+"-collector.log"))
					t.Logf("%s: %s", endpoint, data)
				}
				t.Fatalf("clock fixture: %v: %s", err, output)
			}
			readTime := func(name string) float64 {
				t.Helper()
				data, err := os.ReadFile(filepath.Join(directory, name))
				if err != nil {
					t.Fatal(err)
				}
				at, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
				if err != nil {
					t.Fatal(err)
				}
				return at
			}
			lower, upper := readTime("producer-launched.txt"), readTime("progress-before-resume.txt")
			for endpoint, name := range map[string]string{"client": "flow.json", "server": "server-flow.json"} {
				data, err := os.ReadFile(filepath.Join(directory, name))
				if err != nil {
					t.Fatal(err)
				}
				var result streamResult
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatal(err)
				}
				var start struct {
					Started float64 `json:"started_boottime"`
				}
				if err := json.Unmarshal(result.Start, &start); err != nil {
					t.Fatal(err)
				}
				if result.Error != "" || result.Started != start.Started || result.Started < lower-1e-6 || result.Started > upper {
					t.Fatalf("%s measurement origin moved: %+v, bounds [%f,%f]", endpoint, result, lower, upper)
				}
				if endpoint == role {
					trace, err := os.ReadFile(filepath.Join(directory, endpoint+"-iperf-events.jsonl"))
					if err != nil {
						t.Fatal(err)
					}
					var first struct {
						Received float64 `json:"received_boottime"`
					}
					if err := json.Unmarshal([]byte(strings.SplitN(string(trace), "\n", 2)[0]), &first); err != nil {
						t.Fatal(err)
					}
					if first.Received-result.Started < 3-1e-6 {
						t.Fatal("fixture did not delay collection for three seconds")
					}
				}
			}
			offset := readTime("fault.txt") - readTime("client-flow-start.txt")
			planned := readTime("planned-offset.txt")
			if offset < planned-1e-6 || offset > planned+1 {
				t.Fatalf("scheduled event moved from the measurement clock: %f seconds", offset)
			}
		})
	}
}

const measurementClockScript = `set -eu
directory=$1
flow=$2
export WIREHOP_FLOW_TOOL=$flow
export WIREHOP_REAL_IPERF3=$3
export WIREHOP_PAUSE_DIRECTORY=$directory
port=$4
role=$5
client=
server=
cleanup() {
  for process in "$client" "$server"; do
    if [ -n "$process" ]; then
      kill -CONT "$process" 2>/dev/null || true
      kill "$process" 2>/dev/null || true
      wait "$process" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
if [ "$role" = server ]; then
  PATH=$directory:$PATH "$flow" run "$directory" server -s -1 -p "$port" > "$directory/server-flow.json" 2> "$directory/server-collector.log" &
else
  "$flow" run "$directory" server -s -1 -p "$port" > "$directory/server-flow.json" 2> "$directory/server-collector.log" &
fi
server=$!
while [ -z "$(ss -ltnH "sport = :$port")" ]; do sleep 0.02; done
set --
if [ "$role" = server ]; then set -- -R; fi
if [ "$role" = client ]; then
  PATH=$directory:$PATH "$flow" run "$directory" client -c 127.0.0.1 -p "$port" -t 7 -i 1 -b 1M -l 16K "$@" > "$directory/flow.json" 2> "$directory/client-collector.log" &
else
  "$flow" run "$directory" client -c 127.0.0.1 -p "$port" -t 7 -i 1 -b 1M -l 16K "$@" > "$directory/flow.json" 2> "$directory/client-collector.log" &
fi
client=$!
peer=server
paused=$client
if [ "$role" = server ]; then peer=client; paused=$server; fi
while ! grep -Eq '"event":"interval".*"bytes":[1-9]' "$directory/$peer-iperf-events.jsonl" 2>/dev/null; do sleep 0.02; done
"$flow" now > "$directory/progress-before-resume.txt"
sleep 3
kill -CONT "$paused"
"$flow" wait "$directory/client-flow-start.txt" 0
"$flow" now > "$directory/scheduling-start.txt"
offset=$(awk 'NR == 1 { start = $1; next } { offset = $1 - start + 1; if (offset < 5) offset = 5; printf "%.6f\n", offset }' "$directory/client-flow-start.txt" "$directory/scheduling-start.txt")
printf '%s\n' "$offset" > "$directory/planned-offset.txt"
"$flow" wait "$directory/client-flow-start.txt" "$offset"
"$flow" now > "$directory/fault.txt"
wait "$client"
wait "$server"
client=
server=
`
