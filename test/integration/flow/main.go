// Command flow records live iperf3 events and schedules faults against the producer's measurement start.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// streamEvent is one flushed iperf3 JSON-stream event.
type streamEvent struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

// streamResult retains ordinary iperf3 JSON fields with the producer's measurement-start boot-clock timestamp.
type streamResult struct {
	Started   float64           `json:"started_boottime"`
	Start     json.RawMessage   `json:"start,omitempty"`
	Intervals []json.RawMessage `json:"intervals"`
	End       json.RawMessage   `json:"end,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// main runs iperf3, reads the Linux boot clock, or waits for a flow-relative fault deadline.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch {
	case len(os.Args) == 2 && os.Args[1] == "now":
		var at float64
		at, err = bootSeconds()
		if err == nil {
			_, err = fmt.Printf("%.9f\n", at)
		}
	case len(os.Args) == 4 && os.Args[1] == "wait":
		var seconds float64
		seconds, err = strconv.ParseFloat(os.Args[3], 64)
		if err == nil {
			err = waitForFlow(ctx, os.Args[2], seconds)
		}
	case len(os.Args) >= 5 && os.Args[1] == "run":
		err = runFlow(ctx, os.Args[2], os.Args[3], os.Args[4:])
	default:
		err = errors.New("usage: wirehop-test-flow now | wait START_FILE SECONDS | run RESULTS_DIRECTORY ROLE IPERF_ARGUMENT...")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// runFlow saves a complete event trace, reaps iperf3, and emits its collected result even on failure.
func runFlow(ctx context.Context, directory, role string, arguments []string) error {
	if role != "client" && role != "server" {
		return errors.New("role must be client or server")
	}
	stderr, err := os.Create(filepath.Join(directory, role+"-iperf-stderr.log"))
	if err != nil {
		return err
	}
	defer stderr.Close()
	command := exec.CommandContext(ctx, "iperf3", append([]string{"--json-stream", "--forceflush"}, arguments...)...)
	command.Stderr = stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	result, readError := collect(stdout, directory, role, bootSeconds)
	if readError != nil {
		command.Process.Kill()
	}
	waitError := command.Wait()
	runError := errors.Join(readError, waitError)
	if runError != nil {
		result.Error = runError.Error()
	}
	writeError := json.NewEncoder(os.Stdout).Encode(result)
	return errors.Join(runError, writeError)
}

// collect records event receipt separately from the producer's measurement origin.
// Producer errors remain visible when subsequent event decoding or validation also fails.
func collect(reader io.Reader, directory, role string, now func() (float64, error)) (result streamResult, failure error) {
	defer func() {
		if result.Error != "" {
			failure = errors.Join(fmt.Errorf("iperf3: %s", result.Error), failure)
		}
	}()
	trace, err := os.Create(filepath.Join(directory, role+"-iperf-events.jsonl"))
	if err != nil {
		return streamResult{}, err
	}
	defer trace.Close()
	decoder := json.NewDecoder(reader)
	encoder := json.NewEncoder(trace)
	for {
		var event streamEvent
		if err := decoder.Decode(&event); err != nil {
			if !errors.Is(err, io.EOF) {
				return result, err
			}
			break
		}
		at, err := now()
		if err != nil {
			return result, err
		}
		var wallObserved time.Time
		if event.Event == "start" && role == "client" {
			wallObserved = time.Now()
		}
		if err := encoder.Encode(struct {
			Received float64 `json:"received_boottime"`
			streamEvent
		}{Received: at, streamEvent: event}); err != nil {
			return result, err
		}
		data := bytes.TrimSpace(event.Data)
		if event.Event != "error" && (len(data) == 0 || data[0] != '{') {
			return result, fmt.Errorf("invalid iperf3 %s data", event.Event)
		}
		switch event.Event {
		case "start":
			if result.Error != "" {
				return result, nil
			}
			if result.Start != nil {
				return result, errors.New("multiple iperf3 tests in one result")
			}
			var start struct {
				Started   float64 `json:"started_boottime"`
				TestStart *struct {
					Omit int `json:"omit"`
				} `json:"test_start"`
			}
			if err := json.Unmarshal(data, &start); err != nil || start.TestStart == nil {
				return result, errors.New("invalid iperf3 start data")
			}
			if start.Started <= 0 || start.Started > at {
				return result, errors.New("invalid iperf3 start timestamp: require producer started_boottime")
			}
			if start.TestStart.Omit != 0 {
				return result, errors.New("iperf3 warmup omission changes the measurement origin")
			}
			result.Start, result.Started = event.Data, start.Started
			if err := writeMarker(filepath.Join(directory, role+"-flow-start.txt"), fmt.Sprintf("%.9f\n", result.Started)); err != nil {
				return result, err
			}
			if role == "client" {
				wallStarted := wallObserved.Add(time.Duration((result.Started - at) * float64(time.Second)))
				if err := writeMarker(filepath.Join(directory, "flow-start.txt"), fmt.Sprintf("%d\n", wallStarted.Unix())); err != nil {
					return result, err
				}
			}
		case "interval":
			if result.Start == nil || result.End != nil {
				return result, errors.New("interval outside an active iperf3 test")
			}
			var interval struct {
				Sum *struct{} `json:"sum"`
			}
			if err := json.Unmarshal(data, &interval); err != nil || interval.Sum == nil {
				return result, errors.New("invalid iperf3 interval data")
			}
			result.Intervals = append(result.Intervals, event.Data)
		case "end":
			if result.Start == nil && result.Error == "" || result.End != nil {
				return result, errors.New("invalid iperf3 end event")
			}
			if result.Error == "" {
				var end struct {
					Received *struct{} `json:"sum_received"`
				}
				if err := json.Unmarshal(data, &end); err != nil || end.Received == nil {
					return result, errors.New("invalid iperf3 end data")
				}
			}
			result.End = event.Data
		case "error":
			var message string
			if err := json.Unmarshal(event.Data, &message); err != nil || message == "" {
				return result, errors.New("invalid iperf3 error event")
			}
			result.Error = message
		default:
			return result, fmt.Errorf("unexpected iperf3 event %q", event.Event)
		}
	}
	if result.Error == "" && (result.Start == nil || result.End == nil || len(result.Intervals) == 0) {
		return result, errors.New("missing completed iperf3 event evidence")
	}
	return result, nil
}

// writeMarker atomically publishes a timestamp so another process cannot read a partial value.
func writeMarker(path, contents string) error {
	if err := os.WriteFile(path+".tmp", []byte(contents), 0644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// waitForFlow waits for a data-start marker and then for its absolute boot-clock deadline.
func waitForFlow(ctx context.Context, path string, seconds float64) error {
	maximum := float64((time.Duration(1<<63-1) - time.Minute) / time.Second)
	if seconds < 0 || seconds > maximum || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return errors.New("invalid wait offset: require finite, nonnegative, representable seconds")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute+time.Duration(seconds*float64(time.Second)))
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err == nil {
			started, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
			if err != nil || started <= 0 || math.IsNaN(started) || math.IsInf(started, 0) {
				return fmt.Errorf("invalid flow-start marker %s", path)
			}
			now, err := bootSeconds()
			if err != nil {
				return err
			}
			if started > now {
				return fmt.Errorf("invalid flow-start marker %s: timestamp is in the future", path)
			}
			remaining := time.Duration(max(0, started+seconds-now) * float64(time.Second))
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return ctx.Err()
			}
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
