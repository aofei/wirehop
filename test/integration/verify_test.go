package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyCarrier(t *testing.T) {
	for _, tt := range []struct {
		name     string
		scenario string
		active   int
		passive  int
		wantErr  bool
	}{
		{name: "DiscardedServerChildSockets", scenario: "tcp-blackhole", active: 3, passive: 5},
		{name: "CarrierReconnect", scenario: "tcp-loss", active: 4, passive: 4, wantErr: true},
		{name: "UDPControlConnection", scenario: "tcp-udp", active: 2, passive: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
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
		name, scenario, sockets string
		opened                  int
		wantErr                 bool
	}{
		{name: "Repeated", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 2},
		{name: "Mixed", scenario: "tcp-mixed", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51823\n", opened: 2},
		{name: "MissingLane", scenario: "tcp-mixed", sockets: "0 0 local:1 remote:51822\n", opened: 2, wantErr: true},
		{name: "Reconnected", scenario: "tcp-multipath", sockets: "0 0 local:1 remote:51822\n0 0 local:2 remote:51822\n", opened: 3, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			for name, contents := range map[string]string{
				"client-tcp-sockets.txt":       tt.sockets,
				"client-tcp-sockets-after.txt": tt.sockets,
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
