//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestIPerfIntervalCoverage(t *testing.T) {
	// A one-bit/s sender cannot complete a second byte before the one-second deadline.
	// The reporter is scheduled at 0.995 seconds, allowing a short zero-delivery tail before completion.
	client, server, _ := runIPerfIntervals(t, "-b", "1", "-l", "1")
	for _, tt := range []struct {
		name, role string
		result     streamResult
	}{
		{name: "Client", role: "client", result: client},
		{name: "Server", role: "server", result: server},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var end struct {
				Sent struct {
					End float64
				} `json:"sum_sent"`
				Received struct {
					End float64
				} `json:"sum_received"`
			}
			if err := json.Unmarshal(tt.result.End, &end); err != nil {
				t.Fatal(err)
			}
			var last struct {
				Sum struct{ Start, End float64 }
			}
			if err := json.Unmarshal(tt.result.Intervals[len(tt.result.Intervals)-1], &last); err != nil {
				t.Fatal(err)
			}
			completed := end.Received.End
			if tt.role == "client" {
				completed = end.Sent.End
			}
			if last.Sum.End+1e-6 < completed {
				t.Fatalf("lost final interval: reports end at %.6f, local statistics end at %.6f", last.Sum.End, completed)
			}
		})
	}
}

func TestIPerfIntervalDirections(t *testing.T) {
	type interval struct {
		Start, End, Seconds float64
		Bytes               uint64
		Sender              bool
	}
	for _, tt := range []struct {
		name      string
		arguments []string
	}{
		{name: "Forward"},
		{name: "Reverse", arguments: []string{"-R"}},
		{name: "Bidirectional", arguments: []string{"--bidir"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server, _ := runIPerfIntervals(t, append([]string{"-b", "1M", "-l", "1024"}, tt.arguments...)...)
			for _, endpoint := range []struct {
				name   string
				result streamResult
			}{
				{name: "Client", result: client},
				{name: "Server", result: server},
			} {
				t.Run(endpoint.name, func(t *testing.T) {
					for index, raw := range endpoint.result.Intervals {
						var data struct {
							Streams []interval
							Sum     interval
							Reverse *interval `json:"sum_bidir_reverse"`
						}
						if err := json.Unmarshal(raw, &data); err != nil {
							t.Fatal(err)
						}
						sums := []interval{data.Sum}
						if tt.name == "Bidirectional" {
							if data.Reverse == nil {
								t.Fatalf("interval %d: missing reverse sum", index)
							}
							sums = append(sums, *data.Reverse)
						}
						for _, sum := range sums {
							matches := 0
							for _, stream := range data.Streams {
								if stream.Sender != sum.Sender {
									continue
								}
								matches++
								if sum != stream {
									t.Errorf("interval %d: sum %+v differs from its direction's stream %+v", index, sum, stream)
								}
							}
							if matches != 1 {
								t.Errorf("interval %d: found %d streams for sender=%t, want one", index, matches, sum.Sender)
							}
						}
					}
				})
			}
		})
	}
}

func runIPerfIntervals(t *testing.T, arguments ...string) (streamResult, streamResult, string) {
	t.Helper()
	if os.Getenv("WIREHOP_TEST_TOOLS") != "1" {
		t.Skip("requires the kernel test image's iperf3")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var serverOutput, serverError bytes.Buffer
	directory := t.TempDir()
	server := exec.CommandContext(ctx, "iperf3", "--json-stream", "--forceflush", "-s", "-1", "-B", "127.0.0.1", "-p", strconv.Itoa(port), "-i", "0.199")
	server.Env = append(os.Environ(), "WIREHOP_TEST_RECEIVER_TRACE="+filepath.Join(directory, "server-receiver"))
	server.Stdout, server.Stderr = &serverOutput, &serverError
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if server.ProcessState == nil {
			server.Process.Kill()
			server.Wait()
		}
		if t.Failed() {
			t.Logf("iperf3 server: %s", serverError.String())
		}
	}()
	waitForIPerfListener(t, ctx, port)
	clientArguments := []string{"--json-stream", "--forceflush", "-c", "127.0.0.1", "-p", strconv.Itoa(port), "-t", "1", "-i", "0.199"}
	client := exec.CommandContext(ctx, "iperf3", append(clientArguments, arguments...)...)
	client.Env = append(os.Environ(), "WIREHOP_TEST_RECEIVER_TRACE="+filepath.Join(directory, "client-receiver"))
	clientOutput, err := client.CombinedOutput()
	if err != nil {
		t.Fatalf("iperf3 client: %v: %s", err, clientOutput)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("iperf3 server: %v: %s", err, serverError.String())
	}
	clientResult, err := collect(bytes.NewReader(clientOutput), t.TempDir(), "client", bootSeconds)
	if err != nil {
		t.Fatal(err)
	}
	serverResult, err := collect(bytes.NewReader(serverOutput.Bytes()), t.TempDir(), "server", bootSeconds)
	if err != nil {
		t.Fatal(err)
	}
	return clientResult, serverResult, directory
}
