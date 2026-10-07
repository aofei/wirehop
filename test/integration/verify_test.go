package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyThroughputRecovery(t *testing.T) {
	for _, tt := range []struct {
		name          string
		intervals     int
		recovered     uint64
		reverse       uint64
		bidirectional bool
		wantErr       bool
	}{
		{name: "Restored", intervals: 20, recovered: 100},
		{name: "Boundary", intervals: 20, recovered: 25},
		{name: "NominalFinalProgress", intervals: 20, recovered: 1, wantErr: true},
		{name: "BelowThreshold", intervals: 20, recovered: 24, wantErr: true},
		{name: "Truncated", intervals: 17, recovered: 100, wantErr: true},
		{name: "BothDirections", intervals: 20, recovered: 100, reverse: 100, bidirectional: true},
		{name: "ReverseStalled", intervals: 20, recovered: 100, reverse: 1, bidirectional: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var flow flowResult
			for index := range tt.intervals {
				interval := flowIntervalResult{
					Sum:     flowInterval{Start: float64(index), End: float64(index + 1), Bytes: 100},
					Reverse: flowInterval{Start: float64(index), End: float64(index + 1), Bytes: 100},
				}
				if index >= 15 {
					interval.Sum.Bytes = tt.recovered
					interval.Reverse.Bytes = tt.reverse
				}
				flow.Intervals = append(flow.Intervals, interval)
			}
			if err := verifyThroughputRecovery(flow, tt.bidirectional); (err != nil) != tt.wantErr {
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
		{name: "DiscardedServerChildSockets", scenario: "tcp-blackhole", active: 3, passive: 5},
		{name: "CarrierReconnect", scenario: "tcp-loss", active: 4, passive: 4, wantErr: true},
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
