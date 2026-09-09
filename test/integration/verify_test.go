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
