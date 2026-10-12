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

func TestVerifyThroughputRecovery(t *testing.T) {
	for _, tt := range []struct {
		name          string
		duration      int
		intervals     int
		recovered     uint64
		reverse       uint64
		bidirectional bool
		earlyRecovery bool
		wantErr       bool
	}{
		{name: "Restored", duration: 20, intervals: 20, recovered: 100},
		{name: "Boundary", duration: 20, intervals: 20, recovered: 25},
		{name: "NominalFinalProgress", duration: 20, intervals: 20, recovered: 1, wantErr: true},
		{name: "BelowThreshold", duration: 20, intervals: 20, recovered: 24, wantErr: true},
		{name: "Truncated", duration: 20, intervals: 17, recovered: 100, wantErr: true},
		{name: "MissingFinalInterval", duration: 20, intervals: 19, recovered: 100, wantErr: true},
		{name: "BothDirections", duration: 20, intervals: 20, recovered: 100, reverse: 100, bidirectional: true},
		{name: "ReverseStalled", duration: 20, intervals: 20, recovered: 100, reverse: 1, bidirectional: true, wantErr: true},
		{name: "LongFlow", duration: 40, intervals: 40, recovered: 100},
		{name: "LongBoundary", duration: 40, intervals: 40, recovered: 25},
		{name: "LongTruncated", duration: 40, intervals: 39, recovered: 100, wantErr: true},
		{name: "LongEarlyProgress", duration: 40, intervals: 40, recovered: 100, earlyRecovery: true, wantErr: true},
		{name: "LongBothDirections", duration: 40, intervals: 40, recovered: 100, reverse: 100, bidirectional: true},
		{name: "LongReverseStalled", duration: 40, intervals: 40, recovered: 100, reverse: 1, bidirectional: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var flow flowResult
			recoveryStart := tt.duration - 5
			if tt.earlyRecovery {
				recoveryStart = 15
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
			if err := verifyThroughputRecovery(flow, tt.bidirectional, float64(tt.duration)); (err != nil) != tt.wantErr {
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
