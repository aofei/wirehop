package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyRTTStep(t *testing.T) {
	for _, tt := range []struct {
		name     string
		raised   uint64
		restored uint64
		mutate   func(*testing.T, string, *flowResult)
		wantErr  bool
	}{
		{name: "DeliveredThroughout", raised: 1_000_000, restored: 1_000_000},
		{name: "BaselineFloor", raised: 1_000_000, restored: 1_000_000,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				for index := range 10 {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 625_000
				}
			}},
		{name: "BaselineBelowFloor", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				for index := range 10 {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 624_999
				}
			}},
		{name: "ThresholdBoundary", raised: 250_000, restored: 250_000},
		{name: "RaisedRTTBelowThreshold", raised: 249_000, restored: 1_000_000, wantErr: true},
		{name: "RestoredRTTBelowThreshold", raised: 1_000_000, restored: 249_000, wantErr: true},
		{name: "NoDeliveryDuringRaisedRTT", restored: 1_000_000, wantErr: true},
		{name: "NoDeliveryAfterRestoration", raised: 1_000_000, wantErr: true},
		{name: "SenderOnly", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				flow.Intervals = flow.ServerOutput.Intervals
				for index := range flow.Intervals {
					*flow.Intervals[index].Sum.Sender = true
				}
				flow.ServerOutput = nil
			}},
		{name: "MissingBaseline", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				for index := range 10 {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 0
				}
			}},
		{name: "ReceiverError", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) { flow.ServerOutput.Error = "interrupted" }},
		{name: "UnknownReceiverStart", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) { flow.ServerOutput.Start.Timestamp.Seconds = 0 }},
		{name: "UnknownIntervalDirection", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) { flow.ServerOutput.Intervals[30].Sum.Sender = nil }},
		{name: "ServerSenderIntervals", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) { *flow.ServerOutput.Intervals[30].Sum.Sender = true }},
		{name: "TruncatedReceiver", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:53]
			}},
		{name: "MissingRaisedRTTIntervals", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				flow.ServerOutput.Intervals = append(flow.ServerOutput.Intervals[:28], flow.ServerOutput.Intervals[35:]...)
			}},
		{name: "DuplicatedIntervals", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				flow.ServerOutput.Intervals[31] = flow.ServerOutput.Intervals[30]
			}},
		{name: "ZeroDuration", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				flow.ServerOutput.Intervals[30].Sum.End = flow.ServerOutput.Intervals[30].Sum.Start
			}},
		{name: "IntervalTimerJitter", raised: 1_000_000, restored: 1_000_000,
			mutate: func(_ *testing.T, _ string, flow *flowResult) {
				for index := range flow.ServerOutput.Intervals {
					if index > 0 {
						flow.ServerOutput.Intervals[index].Sum.Start += 0.000043
					}
					flow.ServerOutput.Intervals[index].Sum.End += 0.000043
				}
			}},
		{name: "MissingChangeMarker", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(t *testing.T, directory string, _ *flowResult) {
				if err := os.Remove(filepath.Join(directory, "server-delay-raised.txt")); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "DelayedRestoration", raised: 1_000_000, restored: 1_000_000, wantErr: true,
			mutate: func(t *testing.T, directory string, _ *flowResult) {
				if err := os.WriteFile(filepath.Join(directory, "server-delay-restored.txt"), []byte("1700000048\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "StaggeredChanges", raised: 1_000_000, restored: 1_000_000,
			mutate: func(t *testing.T, directory string, _ *flowResult) {
				for name, contents := range map[string]string{
					"server-delay-raised.txt":   "1700000011\n",
					"server-delay-restored.txt": "1700000041\n",
				} {
					if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			var intervals []string
			for index := range 60 {
				bytes := uint64(1_000_000)
				if index >= 40 {
					bytes = tt.restored
				} else if index >= 10 {
					bytes = tt.raised
				}
				intervals = append(intervals, fmt.Sprintf(`{"sum":{"start":%d,"end":%d,"bytes":%d,"sender":false}}`,
					index, index+1, bytes))
			}
			encoded := fmt.Sprintf(`{"server_output_json":{"start":{"timestamp":{"timesecs":1700000000}},"intervals":[%s]}}`,
				strings.Join(intervals, ","))
			var flow flowResult
			if err := json.Unmarshal([]byte(encoded), &flow); err != nil {
				t.Fatal(err)
			}
			if tt.mutate != nil {
				tt.mutate(t, directory, &flow)
			}
			if err := verifyRTTStep(directory, "tcp-rtt-step", flow); (err != nil) != tt.wantErr {
				t.Fatalf("verifyRTTStep() = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyRTTStepDirections(t *testing.T) {
	for _, tt := range []struct {
		name, scenario string
		mutate         func(*flowResult)
		wantErr        bool
	}{
		{name: "Forward", scenario: "tcp-rtt-step"},
		{name: "Reverse", scenario: "tcp-rtt-step-reverse"},
		{name: "Bidirectional", scenario: "tcp-rtt-step-bidir"},
		{name: "SameGroupForward", scenario: "tcp-multipath-rtt-step"},
		{name: "SameGroupReverse", scenario: "tcp-multipath-rtt-step-reverse"},
		{name: "SameGroupBidirectional", scenario: "tcp-multipath-rtt-step-bidir"},
		{name: "ReverseSenderEvidence", scenario: "tcp-rtt-step-reverse", wantErr: true,
			mutate: func(flow *flowResult) { flow.Intervals = flow.ServerOutput.Intervals }},
		{name: "ForwardRaisedRTTStalled", scenario: "tcp-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) {
				for index := 10; index < 40; index++ {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 0
				}
			}},
		{name: "ReverseRaisedRTTStalled", scenario: "tcp-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) {
				for index := 10; index < 40; index++ {
					flow.Intervals[index].Reverse.Bytes = 0
				}
			}},
		{name: "ReverseRestorationStalled", scenario: "tcp-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) {
				for index := 40; index < 60; index++ {
					flow.Intervals[index].Reverse.Bytes = 0
				}
			}},
		{name: "ReverseBaselineBelowFloor", scenario: "tcp-multipath-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) {
				for index := range 10 {
					flow.Intervals[index].Reverse.Bytes = 624_999
				}
			}},
		{name: "ReverseDirectionUnknown", scenario: "tcp-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) { flow.Intervals[30].Reverse.Sender = nil }},
		{name: "ReverseIntervalsTruncated", scenario: "tcp-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) { flow.Intervals = flow.Intervals[:53] }},
		{name: "SeparateReceiverEpochs", scenario: "tcp-rtt-step-bidir",
			mutate: func(flow *flowResult) { flow.Start.Timestamp.Seconds-- }},
		{name: "InvalidReverseReceiverEpoch", scenario: "tcp-rtt-step-bidir", wantErr: true,
			mutate: func(flow *flowResult) { flow.Start.Timestamp.Seconds -= 5 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			flow := rttStepDirectionalFlow()
			if strings.HasSuffix(tt.scenario, "-reverse") {
				for index := range flow.Intervals {
					flow.Intervals[index].Sum = flow.Intervals[index].Reverse
					flow.Intervals[index].Reverse = flowInterval{}
					flow.ServerOutput.Intervals[index].Sum = flow.ServerOutput.Intervals[index].Reverse
					flow.ServerOutput.Intervals[index].Reverse = flowInterval{}
				}
			}
			if tt.mutate != nil {
				tt.mutate(&flow)
			}
			if err := verifyRTTStep(directory, tt.scenario, flow); (err != nil) != tt.wantErr {
				t.Fatalf("verifyRTTStep(%q) = %v, want error %t", tt.scenario, err, tt.wantErr)
			}
		})
	}
}

func rttStepDirectionalFlow() flowResult {
	var flow flowResult
	flow.Start.Timestamp.Seconds = 1_700_000_000
	flow.ServerOutput = new(flowResult)
	flow.ServerOutput.Start.Timestamp.Seconds = flow.Start.Timestamp.Seconds
	sender, receiver := true, false
	for index := range 60 {
		sent := flowInterval{Start: float64(index), End: float64(index + 1), Bytes: 1_000_000, Sender: &sender}
		received := sent
		received.Sender = &receiver
		flow.Intervals = append(flow.Intervals, flowIntervalResult{Sum: sent, Reverse: received})
		flow.ServerOutput.Intervals = append(flow.ServerOutput.Intervals, flowIntervalResult{Sum: received, Reverse: sent})
	}
	return flow
}

func TestVerifyRTTStepQdiscs(t *testing.T) {
	for _, tt := range []struct {
		name, phase, original, replacement string
	}{
		{name: "UnchangedDelay", phase: "raised", original: "delay 600ms", replacement: "delay 5ms"},
		{name: "ChangedCapacity", phase: "restored", original: "rate 20Mbit", replacement: "rate 40Mbit"},
		{name: "ChangedQueue", phase: "raised", original: "limit 10000", replacement: "limit 1000"},
		{name: "Drops", phase: "final", original: "dropped 0", replacement: "dropped 1"},
		{name: "CounterReset", phase: "restored", original: "30000 bytes", replacement: "10000 bytes"},
		{name: "NoRestoredTraffic", phase: "final", original: "40000 bytes", replacement: "30000 bytes"},
		{name: "BypassedRoot", phase: "raised", original: "root", replacement: "parent 1:1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeRTTStepEvidence(t, directory)
			path := filepath.Join(directory, "client-qdisc-"+tt.phase+".txt")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data = []byte(strings.Replace(string(data), tt.original, tt.replacement, 1))
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := verifyRTTStepQdiscs(directory, "client"); err == nil {
				t.Fatal("verifyRTTStepQdiscs() accepted incorrect impairment evidence")
			}
		})
	}
}

func writeRTTStepEvidence(t *testing.T, directory string) {
	t.Helper()
	for _, role := range []string{"client", "server"} {
		for name, contents := range map[string]string{
			"delay-raised.txt":   "1700000010\n",
			"delay-restored.txt": "1700000040\n",
		} {
			if err := os.WriteFile(filepath.Join(directory, role+"-"+name), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		for index, phase := range []struct{ name, delay string }{
			{name: "initial", delay: "5ms"},
			{name: "raised", delay: "600ms"},
			{name: "restored", delay: "5ms"},
			{name: "final", delay: "5ms"},
		} {
			contents := fmt.Sprintf("qdisc netem 10: root refcnt 2 limit 10000 delay %s rate 20Mbit\n"+
				" Sent %d bytes 100 pkt (dropped 0, overlimits 0 requeues 0)\n backlog 0b 0p requeues 0\n",
				phase.delay, (index+1)*10000)
			if err := os.WriteFile(filepath.Join(directory, role+"-qdisc-"+phase.name+".txt"), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestVerifyCapacityChangeFault(t *testing.T) {
	for _, tt := range []struct {
		name, role, dropped, restored, missing, wantErr string
		missingEpoch                                    bool
	}{
		{name: "Scheduled"},
		{name: "EarliestRoundedMarkers", dropped: "1700000004", restored: "1700000011"},
		{name: "LatestRoundedMarkers", dropped: "1700000007", restored: "1700000014"},
		{name: "RoundedShortDuration", restored: "1700000011"},
		{name: "RoundedLongDuration", dropped: "1700000004"},
		{name: "MissingEpoch", missingEpoch: true, wantErr: "missing flow timestamp"},
		{name: "MissingClientMarker", missing: "client-rate-dropped.txt", wantErr: "client-rate-dropped.txt"},
		{name: "MissingServerMarker", missing: "server-rate-restored.txt", wantErr: "server-rate-restored.txt"},
		{name: "InvalidMarker", dropped: "not a timestamp", wantErr: "invalid syntax"},
		{name: "FaultTooEarly", dropped: "1700000003", wantErr: "changed at 3 seconds"},
		{name: "FaultTooLate", dropped: "1700000008", wantErr: "changed at 8 seconds"},
		{name: "ClientRestoredTooLate", restored: "1700000038", wantErr: "changed at 38 seconds"},
		{name: "ServerRestoredTooLate", role: "server", restored: "1700000038", wantErr: "server-rate-restored.txt"},
		{name: "FaultTooShort", dropped: "1700000007", restored: "1700000011", wantErr: "fault lasted 4 seconds"},
		{name: "FaultTooLong", dropped: "1700000004", restored: "1700000014", wantErr: "fault lasted 10 seconds"},
		{name: "MissingQueueEvidence", missing: "server-qdisc-restored.txt", wantErr: "server-qdisc-restored.txt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeCapacityChangeEvidence(t, directory)
			role := tt.role
			if role == "" {
				role = "client"
			}
			for change, contents := range map[string]string{"dropped": tt.dropped, "restored": tt.restored} {
				if contents != "" {
					if err := os.WriteFile(filepath.Join(directory, role+"-rate-"+change+".txt"), []byte(contents+"\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tt.missing != "" {
				if err := os.Remove(filepath.Join(directory, tt.missing)); err != nil {
					t.Fatal(err)
				}
			}
			started := int64(1700000000)
			if tt.missingEpoch {
				started = 0
			}
			err := verifyCapacityChangeFault(directory, started)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("verifyCapacityChangeFault() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyCapacityChangeQdiscs(t *testing.T) {
	for _, tt := range []struct {
		name, phase, old, replacement, wantErr string
	}{
		{name: "RetainedQueues"},
		{name: "LimitedWhitespace", phase: "limited", old: "100ms 20ms", replacement: "100ms  20ms"},
		{name: "InitialRate", phase: "initial", old: "rate 1Gbit", replacement: "rate 1Mbit", wantErr: "expected rate 1Gbit"},
		{name: "LimitedRate", phase: "limited", old: "rate 32Kbit", replacement: "rate 64Kbit", wantErr: "expected rate 32Kbit"},
		{name: "LimitedBurst", phase: "limited", old: "burst 4Kb", replacement: "burst 64Kb", wantErr: "expected burst 4Kb"},
		{name: "LimitedLatency", phase: "limited", old: "lat 1s", replacement: "lat 2s", wantErr: "expected lat 1s"},
		{name: "MissingRoot", phase: "limited", old: "qdisc tbf 1:", replacement: "qdisc tbf 2:", wantErr: "queue 0 evidence"},
		{name: "MissingChild", phase: "limited", old: "parent 1:1", replacement: "parent 2:1", wantErr: "queue 1 evidence"},
		{name: "InitialQueueLimit", phase: "initial", old: "limit 10000", replacement: "limit 8", wantErr: "expected limit 10000"},
		{name: "LimitedQueueLimit", phase: "limited", old: "limit 8", replacement: "limit 10000", wantErr: "expected limit 8"},
		{name: "LimitedDelay", phase: "limited", old: "delay 100ms 20ms", replacement: "delay 100ms", wantErr: "expected delay"},
		{name: "LimitedLoss", phase: "limited", old: "loss 0.2%", replacement: "loss 0%", wantErr: "expected loss 0.2%"},
		{name: "RestoredRate", phase: "restored", old: "rate 1Gbit", replacement: "rate 32Kbit", wantErr: "expected rate 1Gbit"},
		{name: "RestoredQueueLimit", phase: "restored", old: "limit 10000", replacement: "limit 8", wantErr: "expected limit 10000"},
		{name: "ResidualDelay", phase: "restored", old: "limit 10000", replacement: "limit 10000 delay 100ms", wantErr: "retains delay or loss"},
		{name: "ResidualLoss", phase: "restored", old: "limit 10000", replacement: "limit 10000 loss 0.2%", wantErr: "retains delay or loss"},
		{name: "ParentBytesReset", phase: "restored", old: "Sent 10000 bytes", replacement: "Sent 0 bytes", wantErr: "resets capacity queue 0"},
		{name: "ChildBytesReset", phase: "restored", old: "Sent 9000 bytes", replacement: "Sent 0 bytes", wantErr: "resets capacity queue 1"},
		{name: "ParentDropsReset", phase: "restored", old: "dropped 3,", replacement: "dropped 0,", wantErr: "resets capacity queue 0"},
		{name: "ChildDropsReset", phase: "restored", old: "Sent 9000 bytes 100 pkt (dropped 3,",
			replacement: "Sent 9000 bytes 100 pkt (dropped 0,", wantErr: "resets capacity queue 1"},
		{name: "NoLimitedTraffic", phase: "limited", old: "Sent 10000 bytes", replacement: "Sent 0 bytes", wantErr: "no additional shaped traffic"},
		{name: "NoRestoredTraffic", phase: "final", old: "Sent 20000 bytes", replacement: "Sent 10000 bytes", wantErr: "no additional shaped traffic"},
		{name: "InitialPacketDrops", phase: "initial", old: "dropped 0,", replacement: "dropped 1,", wantErr: "packet drops outside"},
		{name: "ParentPostRestorationDrops", phase: "final", old: "dropped 3,", replacement: "dropped 4,", wantErr: "fault in queue 0"},
		{name: "ChildPostRestorationDrops", phase: "final", old: "Sent 19000 bytes 100 pkt (dropped 3,",
			replacement: "Sent 19000 bytes 100 pkt (dropped 4,", wantErr: "fault in queue 1"},
		{name: "ByteCounterOverflow", phase: "final", old: "Sent 20000 bytes", replacement: "Sent 18446744073709551616 bytes", wantErr: "value out of range"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeCapacityChangeEvidence(t, directory)
			if tt.phase != "" {
				path := filepath.Join(directory, "client-qdisc-"+tt.phase+".txt")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(data), tt.old) {
					t.Fatalf("missing mutation target %q", tt.old)
				}
				if err := os.WriteFile(path, []byte(strings.Replace(string(data), tt.old, tt.replacement, 1)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := verifyCapacityChangeQdiscs(directory, "client")
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("verifyCapacityChangeQdiscs() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func writeCapacityChangeEvidence(t *testing.T, directory string) {
	t.Helper()
	for _, role := range []string{"client", "server"} {
		for name, contents := range map[string]string{
			"rate-dropped.txt":  "1700000005\n",
			"rate-restored.txt": "1700000012\n",
		} {
			if err := os.WriteFile(filepath.Join(directory, role+"-"+name), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		for _, phase := range []struct {
			name, tbf, netem        string
			parentBytes, childBytes int
			drops                   int
		}{
			{name: "initial", tbf: "rate 1Gbit burst 65500b lat 1s", netem: "limit 10000"},
			{name: "limited", tbf: "rate 32Kbit burst 4Kb lat 1s", netem: "limit 8 delay 100ms 20ms loss 0.2%",
				parentBytes: 10000, childBytes: 9000, drops: 2},
			{name: "restored", tbf: "rate 1Gbit burst 65500b lat 1s", netem: "limit 10000",
				parentBytes: 10000, childBytes: 9000, drops: 3},
			{name: "final", tbf: "rate 1Gbit burst 65500b lat 1s", netem: "limit 10000",
				parentBytes: 20000, childBytes: 19000, drops: 3},
		} {
			contents := fmt.Sprintf("qdisc tbf 1: root refcnt 2 %s\n"+
				" Sent %d bytes 100 pkt (dropped %d, overlimits 0 requeues 0)\n backlog 0b 0p requeues 0\n"+
				"qdisc netem 10: parent 1:1 %s\n"+
				" Sent %d bytes 100 pkt (dropped %d, overlimits 0 requeues 0)\n backlog 0b 0p requeues 0\n",
				phase.tbf, phase.parentBytes, phase.drops, phase.netem, phase.childBytes, phase.drops)
			if err := os.WriteFile(filepath.Join(directory, role+"-qdisc-"+phase.name+".txt"), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestVerifyCapacityChange(t *testing.T) {
	for _, tt := range []struct {
		name, scenario string
		mutate         func(*flowResult)
		wantErr        string
	}{
		{name: "NativeForward", scenario: "native-capacity-change"},
		{name: "NativeReverse", scenario: "native-capacity-change-reverse"},
		{name: "NativeBidirectional", scenario: "native-capacity-change-bidir"},
		{name: "RelayForward", scenario: "tcp-multipath-capacity-change"},
		{name: "RelayReverse", scenario: "tcp-multipath-capacity-change-reverse"},
		{name: "RelayBidirectional", scenario: "tcp-multipath-capacity-change-bidir"},
		{name: "MissingReceiver", wantErr: "forward direction: missing completed receiver",
			mutate: func(flow *flowResult) { flow.ServerOutput = nil }},
		{name: "ReceiverError", wantErr: "missing completed receiver",
			mutate: func(flow *flowResult) { flow.ServerOutput.Error = "interrupted" }},
		{name: "MissingReceiverEpoch", wantErr: "missing completed receiver",
			mutate: func(flow *flowResult) { flow.ServerOutput.Start.Timestamp.Seconds = 0 }},
		{name: "SenderBufferedWithoutDelivery", wantErr: "require at least 25 percent",
			mutate: func(flow *flowResult) {
				for index := 35; index < 40; index++ {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 0
				}
			}},
		{name: "ForwardBelowThreshold", wantErr: "require at least 25 percent",
			mutate: func(flow *flowResult) { flow.ServerOutput.Intervals[35].Sum.Bytes-- }},
		{name: "PostDeadlineRecovery", wantErr: "require at least 25 percent",
			mutate: func(flow *flowResult) {
				for index := 35; index < 40; index++ {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 0
				}
				late := flow.ServerOutput.Intervals[39]
				late.Sum.Start, late.Sum.End, late.Sum.Bytes = 40, 45, 25_000_000
				flow.ServerOutput.Intervals = append(flow.ServerOutput.Intervals, late)
			}},
		{name: "ReverseBelowThreshold", scenario: "native-capacity-change-bidir", wantErr: "reverse direction:",
			mutate: func(flow *flowResult) { flow.Intervals[35].Reverse.Bytes-- }},
		{name: "BaselineFloor",
			mutate: func(flow *flowResult) {
				for index := range 4 {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 625_000
				}
			}},
		{name: "BaselineBelowFloor", wantErr: "require at least 5.000 Mbit/s",
			mutate: func(flow *flowResult) {
				for index := range 4 {
					flow.ServerOutput.Intervals[index].Sum.Bytes = 624_999
				}
			}},
		{name: "ReverseBaselineBelowFloor", scenario: "native-capacity-change-bidir", wantErr: "reverse direction:",
			mutate: func(flow *flowResult) {
				for index := range 4 {
					flow.Intervals[index].Reverse.Bytes = 624_999
				}
			}},
		{name: "UnknownIntervalDirection", wantErr: "invalid receiver interval",
			mutate: func(flow *flowResult) { flow.ServerOutput.Intervals[35].Sum.Sender = nil }},
		{name: "SenderIntervals", wantErr: "invalid receiver interval",
			mutate: func(flow *flowResult) { *flow.ServerOutput.Intervals[35].Sum.Sender = true }},
		{name: "DuplicateIntervals", wantErr: "invalid receiver interval",
			mutate: func(flow *flowResult) { flow.ServerOutput.Intervals[36] = flow.ServerOutput.Intervals[35] }},
		{name: "ZeroDuration", wantErr: "invalid receiver interval",
			mutate: func(flow *flowResult) { flow.ServerOutput.Intervals[35].Sum.End = 35 }},
		{name: "TruncatedReceiver", wantErr: "lacks complete",
			mutate: func(flow *flowResult) { flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:39] }},
		{name: "ReverseSenderEvidence", scenario: "native-capacity-change-reverse", wantErr: "reverse direction:",
			mutate: func(flow *flowResult) {
				flow.Intervals[35].Sum = flow.ServerOutput.Intervals[35].Reverse
			}},
		{name: "ReverseDoesNotNeedServerOutput", scenario: "native-capacity-change-reverse",
			mutate: func(flow *flowResult) { flow.ServerOutput = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flow := rttStepDirectionalFlow()
			flow.Intervals = flow.Intervals[:40]
			flow.ServerOutput.Intervals = flow.ServerOutput.Intervals[:40]
			for index := range 40 {
				bytes := uint64(0)
				if index < 5 {
					bytes = 2_500_000
				} else if index >= 35 {
					bytes = 625_000
				}
				flow.ServerOutput.Intervals[index].Sum.Bytes = bytes
				flow.Intervals[index].Reverse.Bytes = bytes
				if strings.HasSuffix(tt.scenario, "-reverse") {
					flow.Intervals[index].Sum = flow.Intervals[index].Reverse
				}
			}
			if tt.mutate != nil {
				tt.mutate(&flow)
			}
			err := verifyCapacityChange(tt.scenario, flow)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("verifyCapacityChange(%q) = %v, want error containing %q", tt.scenario, err, tt.wantErr)
			}
		})
	}
}

func TestVerifyThroughputRecovery(t *testing.T) {
	for _, tt := range []struct {
		name          string
		duration      int
		intervals     int
		recovered     uint64
		reverse       uint64
		bidirectional bool
		earlyRecovery bool
		lateRecovery  bool
		wantErr       bool
	}{
		{name: "Restored", duration: 20, intervals: 20, recovered: 100},
		{name: "Boundary", duration: 20, intervals: 20, recovered: 25},
		{name: "NominalFinalProgress", duration: 20, intervals: 20, recovered: 1, wantErr: true},
		{name: "BelowThreshold", duration: 20, intervals: 20, recovered: 24, wantErr: true},
		{name: "Truncated", duration: 20, intervals: 17, recovered: 100, wantErr: true},
		{name: "MissingFinalInterval", duration: 20, intervals: 19, recovered: 100, wantErr: true},
		{name: "PostDeadlineRecovery", duration: 20, intervals: 26, recovered: 100, lateRecovery: true, wantErr: true},
		{name: "BothDirections", duration: 20, intervals: 20, recovered: 100, reverse: 100, bidirectional: true},
		{name: "ReverseStalled", duration: 20, intervals: 20, recovered: 100, reverse: 1, bidirectional: true, wantErr: true},
		{name: "LongFlow", duration: 40, intervals: 40, recovered: 100},
		{name: "LongBoundary", duration: 40, intervals: 40, recovered: 25},
		{name: "LongTruncated", duration: 40, intervals: 39, recovered: 100, wantErr: true},
		{name: "LongEarlyProgress", duration: 40, intervals: 40, recovered: 100, earlyRecovery: true, wantErr: true},
		{name: "LongPostDeadlineRecovery", duration: 40, intervals: 46, recovered: 100, lateRecovery: true, wantErr: true},
		{name: "LongBothDirections", duration: 40, intervals: 40, recovered: 100, reverse: 100, bidirectional: true},
		{name: "LongReverseStalled", duration: 40, intervals: 40, recovered: 100, reverse: 1, bidirectional: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var flow flowResult
			recoveryStart := tt.duration - 5
			if tt.earlyRecovery {
				recoveryStart = 15
			}
			if tt.lateRecovery {
				recoveryStart = tt.duration + 1
			}
			for index := range tt.intervals {
				interval := flowIntervalResult{
					Sum:     flowInterval{Start: float64(index), End: float64(index + 1)},
					Reverse: flowInterval{Start: float64(index), End: float64(index + 1)},
				}
				if index < 4 {
					interval.Sum.Bytes = 100
					interval.Reverse.Bytes = 100
				}
				if index >= recoveryStart && index < recoveryStart+5 {
					interval.Sum.Bytes = tt.recovered
					interval.Reverse.Bytes = tt.reverse
				}
				flow.Intervals = append(flow.Intervals, interval)
			}
			if err := verifyThroughputRecovery(flow, tt.bidirectional, float64(tt.duration), 0); (err != nil) != tt.wantErr {
				t.Fatalf("recovery validation = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyCarrier(t *testing.T) {
	for _, tt := range []struct {
		name     string
		scenario string
		active   int
		passive  int
		sockets  string
		wantErr  bool
	}{
		{name: "NativeCapacityChange", scenario: "native-capacity-change", active: 2},
		{name: "NativeCapacityReverse", scenario: "native-capacity-change-reverse", active: 2},
		{name: "NativeCapacityBidirectional", scenario: "native-capacity-change-bidir", active: 3},
		{name: "NativeCapacityExtraConnection", scenario: "native-capacity-change-bidir", active: 4, wantErr: true},
		{name: "DiscardedServerChildSockets", scenario: "tcp-blackhole", active: 3, passive: 5},
		{name: "CarrierReconnect", scenario: "tcp-loss", active: 4, passive: 4, wantErr: true},
		{name: "RTTStepSameCarrier", scenario: "tcp-rtt-step", active: 3},
		{name: "RTTStepCarrierReconnect", scenario: "tcp-rtt-step", active: 4, wantErr: true},
		{name: "RTTStepReverseSameCarrier", scenario: "tcp-rtt-step-reverse", active: 3},
		{name: "RTTStepBidirectionalSameCarrier", scenario: "tcp-rtt-step-bidir", active: 4},
		{name: "RTTStepBidirectionalCarrierReconnect", scenario: "tcp-rtt-step-bidir", active: 5, wantErr: true},
		{name: "UDPControlConnection", scenario: "tcp-udp", active: 2, passive: 2},
		{name: "AsymmetricStallInitialLanes", scenario: "tcp-asymmetric-stall",
			sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n"},
		{name: "AsymmetricStallMissingLane", scenario: "tcp-asymmetric-stall",
			sockets: "0 0 local:1 remote:51822\n", wantErr: true},
		{name: "AsymmetricStallMissingSnapshot", scenario: "tcp-asymmetric-stall", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			if tt.sockets != "" {
				if err := os.WriteFile(filepath.Join(directory, "client-tcp-sockets.txt"), []byte(tt.sockets), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(directory, "client-after.txt"),
				fmt.Appendf(nil, "TcpActiveOpens %d 0.0\n", tt.active), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "server-nstat.txt"),
				fmt.Appendf(nil, "TcpPassiveOpens %d 0.0\n", tt.passive), 0600); err != nil {
				t.Fatal(err)
			}
			if err := verifyCarrier(directory, tt.scenario); (err != nil) != tt.wantErr {
				t.Fatalf("verifyCarrier() = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyParallelCarriers(t *testing.T) {
	for _, tt := range []struct {
		name, scenario, sockets, afterSockets string
		opened                                int
		wantErr                               bool
	}{
		{name: "Repeated", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "LowRate32", scenario: "tcp-multipath-slow32", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "LowRate64", scenario: "tcp-multipath-slow64", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "LowRate128", scenario: "tcp-multipath-slow128", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "RTTStepSameCarriers", scenario: "tcp-multipath-rtt-step", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "RTTStepReconnected", scenario: "tcp-multipath-rtt-step", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3, wantErr: true},
		{name: "RTTStepReverseSameCarriers", scenario: "tcp-multipath-rtt-step-reverse", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "RTTStepBidirectionalSameCarriers", scenario: "tcp-multipath-rtt-step-bidir", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3},
		{name: "RTTStepBidirectionalReconnected", scenario: "tcp-multipath-rtt-step-bidir", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 4, wantErr: true},
		{name: "DistinctLowRate32", scenario: "tcp-multipath-distinct-slow32", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 2},
		{name: "DistinctLowRate64", scenario: "tcp-multipath-distinct-slow64", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 2},
		{name: "DistinctLowRate128", scenario: "tcp-multipath-distinct-slow128", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 2},
		{name: "DistinctLowRateMissingLane", scenario: "tcp-multipath-distinct-slow32", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2, wantErr: true},
		{name: "DistinctLowRateReconnected", scenario: "tcp-multipath-distinct-slow64", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 3, wantErr: true},
		{name: "LowRateReconnected", scenario: "tcp-multipath-slow32", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3, wantErr: true},
		{name: "LowRateMissingLane", scenario: "tcp-multipath-slow64", sockets: "0 0 local:1 remote:51822\n", opened: 2, wantErr: true},
		{name: "CapacityChange", scenario: "tcp-multipath-capacity-change", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3},
		{name: "CapacityChangeMissingLane", scenario: "tcp-multipath-capacity-change", sockets: "0 0 local:1 remote:51822\n", opened: 3, wantErr: true},
		{name: "CapacityChangeReverse", scenario: "tcp-multipath-capacity-change-reverse", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3},
		{name: "CapacityChangeReverseMissingLane", scenario: "tcp-multipath-capacity-change-reverse", sockets: "0 0 local:1 remote:51822\n", opened: 3, wantErr: true},
		{name: "CapacityChangeBidirectional", scenario: "tcp-multipath-capacity-change-bidir", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 4},
		{name: "CapacityChangeBidirectionalMissingLane", scenario: "tcp-multipath-capacity-change-bidir", sockets: "0 0 local:1 remote:51822\n", opened: 4, wantErr: true},
		{name: "Asymmetric", scenario: "tcp-asymmetric", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 2},
		{name: "AsymmetricMissingLane", scenario: "tcp-asymmetric", sockets: "0 0 local:1 remote:51822\n", opened: 2, wantErr: true},
		{name: "Mixed", scenario: "tcp-mixed", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 2},
		{name: "MissingLane", scenario: "tcp-mixed", sockets: "0 0 local:1 remote:51822\n", opened: 2, wantErr: true},
		{name: "UnexpectedSecondaryLane", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n0 0 local:3 remote:51823\n", opened: 2, wantErr: true},
		{name: "Reconnected", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3, wantErr: true},
		{name: "ReplacedPreopenedCarrier", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", afterSockets: "0 0 local:3 remote:51822\n0 0 local:2 remote:51822\n", opened: 2, wantErr: true},
		{name: "ReorderedSnapshot", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", afterSockets: "0 20 local:2 remote:51822\n0 30 local:1 remote:51822\n", opened: 2},
		{name: "FaultReplacesCarrier", scenario: "tcp-multipath-capacity-change", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", afterSockets: "0 0 local:3 remote:51822\n0 0 local:2 remote:51822\n", opened: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			afterSockets := tt.afterSockets
			if afterSockets == "" {
				afterSockets = tt.sockets
			}
			for name, contents := range map[string]string{
				"client-tcp-sockets.txt":       tt.sockets,
				"client-tcp-sockets-after.txt": afterSockets,
				"client-before.txt":            "TcpActiveOpens 3 0.0\n",
				"client-after.txt":             fmt.Sprintf("TcpActiveOpens %d 0.0\n", 3+tt.opened),
			} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := verifyCarrier(directory, tt.scenario); (err != nil) != tt.wantErr {
				t.Fatalf("verifyCarrier() = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestVerifyRouteExclusion(t *testing.T) {
	for _, tt := range []struct {
		name, unmarked, marked string
		wantErr                bool
	}{
		{name: "CapturingRoute", unmarked: "wgtest", marked: "eth0"},
		{name: "NoCapture", unmarked: "eth0", marked: "eth0", wantErr: true},
		{name: "NoExclusion", unmarked: "wgtest", marked: "wgtest", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			for name, device := range map[string]string{"unmarked-route.txt": tt.unmarked, "marked-route.txt": tt.marked} {
				if err := os.WriteFile(filepath.Join(directory, name), fmt.Appendf(nil, "192.0.2.1 dev %s src 192.0.2.2\n", device), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := verifyRouteExclusion(directory); (err != nil) != tt.wantErr {
				t.Fatalf("verifyRouteExclusion() = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}
