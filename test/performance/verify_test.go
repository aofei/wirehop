package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, file, content, wantError string
	}{
		{name: "Healthy"},
		{name: "Reconnected", file: "client-after.txt", content: "TcpActiveOpens 6 0\n", wantError: "unexpected TCP active opens"},
		{name: "MissingCounter", file: "client-after.txt", content: "TcpPassiveOpens 2 0\n", wantError: "missing TCP active-open"},
		{name: "MissingCarrier", file: "client-tcp-sockets-after.txt", content: "", wantError: "carrier counts"},
		{name: "ReplacedPreopenedCarrier", file: "client-tcp-sockets-after.txt", content: "0 0 192.0.2.1:40002 192.0.2.2:51822\n0 0 192.0.2.1:40001 192.0.2.2:51823\n", wantError: "socket identities changed"},
		{name: "ReorderedSnapshot", file: "client-tcp-sockets-after.txt", content: "0 20 192.0.2.1:40001 192.0.2.2:51823\n0 30 192.0.2.1:40000 192.0.2.2:51822\n"},
		{name: "Dropped", file: "router-qdisc.txt", content: "qdisc prio 1:\n dropped 1\n dropped 0\n dropped 0\n", wantError: "router dropped"},
		{name: "BypassedRouter", file: "router-qdisc.txt", content: "qdisc prio 1:\n dropped 0\n dropped 0\n dropped 0\n", wantError: "missing traffic"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeEvidence(t, directory, "Both")
			if tt.file != "" {
				writeFixture(t, directory, tt.file, tt.content)
			}
			err := verifyEvidence(directory, "Both", 100, false)
			if tt.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("verification = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestVerifyComparison(t *testing.T) {
	for _, tt := range []struct {
		name          string
		combined      float64
		seconds       float64
		badRepetition bool
		wantError     string
	}{
		{name: "RetainsBestPath", combined: 95, seconds: 20},
		{name: "ReintroducesSpillover", combined: 20, seconds: 20, wantError: "less than 85 percent"},
		{name: "IncompleteFlow", combined: 95, seconds: 10, wantError: "incomplete flow"},
		{name: "MedianConcealsCollapse", combined: 95, seconds: 20, badRepetition: true, wantError: "a combined flow retains less"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			for _, profile := range []string{"HighCapacity", "LowCapacity"} {
				for _, path := range []string{"Low", "High", "Both"} {
					for repetition := 1; repetition <= 3; repetition++ {
						directory := filepath.Join(root, fmt.Sprintf("%s-%d-%s", profile, repetition, path))
						if err := os.Mkdir(directory, 0700); err != nil {
							t.Fatal(err)
						}
						writeEvidence(t, directory, path)
						rate := 100.0
						if path == "Both" {
							rate = tt.combined
							if tt.badRepetition && repetition == 2 {
								rate = 20
							}
						}
						writeFixture(t, directory, "flow.json", fmt.Sprintf(
							`{"end":{"sum_received":{"bytes":100,"bits_per_second":%f,"seconds":%f}}}`+"\n", rate, tt.seconds))
					}
				}
			}
			err := verify(root, 3)
			if tt.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("verification = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestCompareBaseline(t *testing.T) {
	for _, tt := range []struct {
		name                          string
		rate                          float64
		baselineDrops, candidateDrops bool
		wantError                     bool
	}{
		{name: "RetainsBaseline", rate: 95},
		{name: "AllPathsRegressTogether", rate: 10, wantError: true},
		{name: "BaselineDropsAreEvidence", rate: 95, baselineDrops: true},
		{name: "CandidateDropsFail", rate: 95, candidateDrops: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			candidate, baseline := t.TempDir(), t.TempDir()
			for _, root := range []string{candidate, baseline} {
				for _, profile := range []string{"HighCapacity", "LowCapacity"} {
					for _, path := range []string{"Low", "High", "Both"} {
						directory := filepath.Join(root, profile+"-1-"+path)
						if err := os.Mkdir(directory, 0700); err != nil {
							t.Fatal(err)
						}
						writeEvidence(t, directory, path)
						if root == baseline && tt.baselineDrops || root == candidate && tt.candidateDrops {
							encoded, err := os.ReadFile(filepath.Join(directory, "router-qdisc.txt"))
							if err != nil {
								t.Fatal(err)
							}
							writeFixture(t, directory, "router-qdisc.txt", strings.Replace(string(encoded), "dropped 0", "dropped 10", 1))
						}
						rate := 100.0
						if root == candidate {
							rate = tt.rate
						}
						writeFixture(t, directory, "flow.json", fmt.Sprintf(
							`{"end":{"sum_received":{"bytes":100,"bits_per_second":%f,"seconds":20}}}`+"\n", rate))
					}
				}
			}
			err := compareBaseline(candidate, baseline, 1)
			if (err != nil) != tt.wantError {
				t.Fatalf("baseline comparison = %v, want error %t", err, tt.wantError)
			}
		})
	}
}

// writeEvidence creates complete healthy carrier and router evidence for a comparison case.
func writeEvidence(t *testing.T, directory, path string) {
	t.Helper()
	var sockets string
	if path != "High" {
		sockets += "0 0 192.0.2.1:40000 192.0.2.2:51822\n"
	}
	if path != "Low" {
		sockets += "0 0 192.0.2.1:40001 192.0.2.2:51823\n"
	}
	writeFixture(t, directory, "client-tcp-sockets.txt", sockets)
	writeFixture(t, directory, "client-tcp-sockets-after.txt", sockets)
	writeFixture(t, directory, "client-before.txt", "TcpActiveOpens 3 0\n")
	writeFixture(t, directory, "client-after.txt", "TcpActiveOpens 5 0\n")
	writeFixture(t, directory, "router-qdisc.txt", "qdisc prio 1:\n Sent 200 bytes (dropped 0)\n"+
		"qdisc netem 10: parent 1:1\n Sent 100 bytes (dropped 0)\n"+
		"qdisc netem 20: parent 1:2\n Sent 100 bytes (dropped 0)\n")
}

// writeFixture writes one verifier input and fails the test on any filesystem error.
func writeFixture(t *testing.T, directory, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}
