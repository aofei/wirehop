//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	mode := os.Getenv("WIREHOP_FLOW_PROCESS_FIXTURE")
	if mode != "" {
		if mode == "connect-error" {
			fmt.Println(`{"event":"error","data":"control connection refused"}`)
			fmt.Println(`{"event":"end","data":{}}`)
			os.Exit(1)
		}
		if mode == "cancel-delayed" {
			time.Sleep(200 * time.Millisecond)
		}
		started, err := bootSeconds()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Printf("{\"event\":\"start\",\"data\":{\"started_boottime\":%.9f,\"test_start\":{\"duration\":1}}}\n", started)
		if strings.HasPrefix(mode, "cancel") {
			time.Sleep(time.Hour)
		}
		fmt.Println(`{"event":"interval","data":{"sum":{"start":0,"end":1,"bytes":100,"sender":false}}}`)
		fmt.Println(`{"event":"end","data":{"sum_received":{"bytes":100}}}`)
		if mode == "exit7" {
			os.Exit(7)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestRunFlow(t *testing.T) {
	for _, tt := range []struct {
		name, mode, wantErr string
		outputFailure       bool
	}{
		{name: "Complete", mode: "complete"},
		{name: "CompletedWithProcessFailure", mode: "exit7", wantErr: "exit status 7"},
		{name: "FailedBeforeStart", mode: "connect-error", wantErr: "control connection refused"},
		{name: "Cancelled", mode: "cancel", wantErr: "missing completed"},
		{name: "CancelledAfterDelayedStart", mode: "cancel-delayed", wantErr: "missing completed"},
		{name: "OutputFailure", mode: "complete", outputFailure: true},
		{name: "ProcessAndOutputFailure", mode: "exit7", wantErr: "exit status 7", outputFailure: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(binary, filepath.Join(directory, "iperf3")); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("WIREHOP_FLOW_PROCESS_FIXTURE", tt.mode)
			output, err := os.Create(filepath.Join(directory, "output.json"))
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := make(chan struct{})
			if strings.HasPrefix(tt.mode, "cancel") {
				ctx, cancel = context.WithCancel(ctx)
				watcherDone := make(chan struct{})
				go func() {
					defer close(watcherDone)
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							if _, err := os.Stat(filepath.Join(directory, "client-flow-start.txt")); err == nil {
								close(started)
								cancel()
								return
							}
						}
					}
				}()
				defer func() {
					cancel()
					<-watcherDone
				}()
			}
			if tt.outputFailure {
				if err := output.Close(); err != nil {
					t.Fatal(err)
				}
			}
			func() {
				original := os.Stdout
				os.Stdout = output
				defer func() { os.Stdout = original }()
				err = runFlow(ctx, directory, "client", []string{"-c", "fixture"})
			}()
			if tt.wantErr == "" && !tt.outputFailure && err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("runFlow() = %v, want %q", err, tt.wantErr)
			}
			if tt.mode == "connect-error" {
				if _, err := os.Stat(filepath.Join(directory, "client-flow-start.txt")); !os.IsNotExist(err) {
					t.Fatalf("published a start marker for a failed setup: %v", err)
				}
			}
			if strings.HasPrefix(tt.mode, "cancel") {
				select {
				case <-started:
				default:
					t.Fatal("canceled before exercising an active producer")
				}
			}
			if tt.outputFailure {
				if !errors.Is(err, os.ErrClosed) {
					t.Fatalf("lost result-write failure: %v", err)
				}
				return
			}
			if _, err := output.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			var result streamResult
			if err := json.NewDecoder(output).Decode(&result); err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" && !strings.Contains(result.Error, tt.wantErr) {
				t.Fatalf("lost process failure in collected JSON: %+v", result)
			}
		})
	}
}
