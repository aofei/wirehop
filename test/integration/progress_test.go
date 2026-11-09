package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyScenarioOrdinaryReceiverProgress(t *testing.T) {
	for _, tt := range []struct {
		name, scenario string
		duration       int
		prefix         string
		recovered      int
		opens          int
	}{
		{name: "TCPStall", scenario: "tcp-stall", duration: 20, prefix: "path", recovered: 9, opens: 3},
		{name: "TCPOutage", scenario: "tcp-outage", duration: 40, prefix: "path", recovered: 17, opens: 3},
		{name: "TCPRoam", scenario: "tcp-roam", duration: 40, prefix: "path", recovered: 5, opens: 3},
		{name: "ForwardProhibit", scenario: "forward-prohibit", duration: 20, prefix: "route", recovered: 6, opens: 2},
		{name: "TCPBlackhole", scenario: "tcp-blackhole", duration: 20, prefix: "route", recovered: 6, opens: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, delivery := range []struct {
				name    string
				stalled bool
			}{{name: "Delivered"}, {name: "BufferedSender", stalled: true}} {
				t.Run(delivery.name, func(t *testing.T) {
					directory := t.TempDir()
					flow := rttStepDirectionalFlow()
					flow.Intervals = flow.Intervals[:tt.duration]
					flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:tt.duration]
					flow.End.Received.Bytes = 20_000_000
					flow.ServerOutput.End.Received.Bytes = 20_000_000
					if delivery.stalled {
						flow.ServerOutput.Intervals[tt.duration-1].Sum.Bytes = 0
					}
					for name, data := range map[string]string{
						tt.prefix + "-dropped.txt":   "1700000005\n",
						tt.prefix + "-recovered.txt": fmt.Sprintf("%d\n", 1_700_000_000+tt.recovered),
						"client-after.txt":           fmt.Sprintf("TcpActiveOpens %d\n", tt.opens),
					} {
						if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0600); err != nil {
							t.Fatal(err)
						}
					}
					attachReceiverCounters(&flow, tt.scenario)
					_, err := verifyScenario(directory, tt.scenario, flow)
					if delivery.stalled {
						if err == nil || !strings.Contains(err.Error(), "receiver") {
							t.Fatalf("sender buffering satisfied late progress: %v", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestVerifyProgress(t *testing.T) {
	for _, tt := range []struct {
		name, scenario, wantStatus string
		mutate                     func(*flowResult)
	}{
		{name: "Forward", scenario: "tcp-stall", wantStatus: "pass"},
		{name: "Reverse", scenario: "tcp-stall-reverse", wantStatus: "pass", mutate: func(flow *flowResult) {
			for i := range flow.Intervals {
				flow.Intervals[i].Sum = flow.Intervals[i].Reverse
			}
			flow.ServerOutput = nil
		}},
		{name: "Bidirectional", scenario: "tcp-stall-bidir", wantStatus: "pass"},
		{name: "DeliveryBeforeFinalWindow", scenario: "tcp-stall", wantStatus: "no-progress", mutate: func(flow *flowResult) {
			flow.ServerOutput.Intervals[18].Sum.End = 19.8
			flow.ServerOutput.Intervals[19].Sum.Start = 19.8
			flow.ServerOutput.Intervals[19].Sum.End = 20.0001
			flow.ServerOutput.Intervals[19].Sum.Bytes = 0
		}},
		{name: "MissingTail", scenario: "tcp-stall", wantStatus: "invalid", mutate: func(flow *flowResult) { flow.ServerOutput.Intervals[19].Sum.End = 19.9 }},
		{name: "ReverseStalled", scenario: "tcp-stall-bidir", wantStatus: "no-progress", mutate: func(flow *flowResult) { flow.Intervals[19].Reverse.Bytes = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flow := rttStepDirectionalFlow()
			flow.Intervals = flow.Intervals[:20]
			flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:20]
			if tt.mutate != nil {
				tt.mutate(&flow)
			}
			attachReceiverCounters(&flow, tt.scenario)
			measurements, err := verifyProgress(tt.scenario, flow, 20)
			if (err != nil) != (tt.wantStatus != "pass") {
				t.Fatalf("verifyProgress() = %v", err)
			}
			last := measurements[len(measurements)-1]
			if last.Status != tt.wantStatus || last.LowerFraction != nil {
				t.Fatalf("incorrect progress measurement: %+v", last)
			}
		})
	}
}
