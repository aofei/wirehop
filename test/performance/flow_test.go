package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadFlow(t *testing.T) {
	for _, tt := range []struct {
		name, old, replacement string
		seconds                float64
		wantError              bool
	}{
		{name: "Complete", seconds: 20},
		{name: "CompletionAllowance", seconds: 19},
		{name: "BelowCompletionAllowance", seconds: 18.999999, wantError: true},
		{name: "CompleteLong", seconds: 60, old: `"duration":20`, replacement: `"duration":60`},
		{name: "MaximumDuration", seconds: 86400, old: `"duration":20`, replacement: `"duration":86400`},
		{name: "ExcessiveDuration", seconds: 86401, old: `"duration":20`, replacement: `"duration":86401`, wantError: true},
		{name: "TooShortWorkload", seconds: 20, old: `"duration":20`, replacement: `"duration":19`, wantError: true},
		{name: "TruncatedLong", seconds: 20, old: `"duration":20`, replacement: `"duration":60`, wantError: true},
		{name: "IncompleteSender", seconds: 20, old: `"sum_sent":{"seconds":20}`, replacement: `"sum_sent":{"seconds":10}`, wantError: true},
		{name: "MissingSender", seconds: 20, old: `"sum_sent":{"seconds":20}`, replacement: `"sum_sent":{}`, wantError: true},
		{name: "IncompleteReceiver", seconds: 10, wantError: true},
		{name: "ZeroReceiverBytes", seconds: 20, old: `"bytes":250000000`, replacement: `"bytes":0`, wantError: true},
		{name: "ReceiverDrain", seconds: 20, old: `"seconds":20}}}`, replacement: `"seconds":21}}}`},
		{name: "MissingReportedRate", seconds: 20, old: `"bits_per_second":`, replacement: `"ignored_reported_rate":`},
		{name: "ZeroReportedRate", seconds: 20, old: `"bits_per_second":100000000.000000000`, replacement: `"bits_per_second":0`},
		{name: "MissingWorkload", seconds: 20, old: `"duration":20`, replacement: `"duration":0`, wantError: true},
		{name: "UDP", seconds: 20, old: `"protocol":"TCP"`, replacement: `"protocol":"UDP"`, wantError: true},
		{name: "Parallel", seconds: 20, old: `"num_streams":1`, replacement: `"num_streams":2`, wantError: true},
		{name: "MissingBlockSize", seconds: 20, old: `"blksize":131072`, replacement: `"blksize":0`, wantError: true},
		{name: "OmittedStartup", seconds: 20, old: `"omit":0`, replacement: `"omit":1`, wantError: true},
		{name: "Reverse", seconds: 20, old: `"reverse":0`, replacement: `"reverse":1`, wantError: true},
		{name: "Bidirectional", seconds: 20, old: `"bidir":0`, replacement: `"bidir":1`, wantError: true},
		{name: "ByteLimit", seconds: 20, old: `"bytes":0`, replacement: `"bytes":1`, wantError: true},
		{name: "BlockLimit", seconds: 20, old: `"blocks":0`, replacement: `"blocks":1`, wantError: true},
		{name: "RateLimit", seconds: 20, old: `"target_bitrate":0`, replacement: `"target_bitrate":10000000`, wantError: true},
		{name: "SocketPacing", seconds: 20, old: `"fqrate":0`, replacement: `"fqrate":10000000`, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			writeFlowFixture(t, directory, 100, tt.seconds)
			if tt.old != "" {
				replaceFlowFixture(t, directory, tt.old, tt.replacement)
			}
			flow, err := readFlow(directory)
			if (err != nil) != tt.wantError {
				t.Fatalf("readFlow = %v, want error %t", err, tt.wantError)
			}
			if err == nil {
				want := 100e6
				if tt.name == "ReceiverDrain" {
					want = 100e6 * 20 / 21
				}
				if flow.End.Received.BitsPerSecond != want {
					t.Fatalf("receiver goodput = %g, want %g", flow.End.Received.BitsPerSecond, want)
				}
			}
		})
	}
}

func TestReadFlowPreservesError(t *testing.T) {
	directory := t.TempDir()
	writeFixture(t, directory, "flow.json", `{"error":"receiver process failed"}`+"\n")
	if _, err := readFlow(directory); err == nil || !strings.Contains(err.Error(), "receiver process failed") {
		t.Fatalf("readFlow concealed the producer failure: %v", err)
	}
}

func TestVerifyReceiverGoodput(t *testing.T) {
	for _, tt := range []struct {
		name             string
		combinedRate     float64
		reportedCombined float64
		reportedSingle   float64
		wantError        bool
	}{
		{name: "ConsistentRates", combinedRate: 95, reportedCombined: 95, reportedSingle: 100},
		{name: "InflatedCombinedRate", combinedRate: 50, reportedCombined: 100, reportedSingle: 100, wantError: true},
		{name: "DeflatedStandaloneRate", combinedRate: 50, reportedCombined: 50, reportedSingle: 10, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			for _, profile := range []string{"HighCapacity", "LowCapacity"} {
				for _, path := range []string{"Low", "High", "Both"} {
					directory := filepath.Join(root, profile+"-1-"+path)
					if err := os.Mkdir(directory, 0700); err != nil {
						t.Fatal(err)
					}
					writeEvidence(t, directory, path)
					rate, reported := 100.0, tt.reportedSingle
					if path == "Both" {
						rate, reported = tt.combinedRate, tt.reportedCombined
					}
					writeFlowFixture(t, directory, rate, 20)
					replaceFlowFixture(t, directory, fmt.Sprintf(`"bits_per_second":%.9f`, rate*1e6),
						fmt.Sprintf(`"bits_per_second":%.9f`, reported*1e6))
				}
			}
			err := verify(root, 1)
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), "less than 85 percent") {
					t.Fatalf("fifty percent receiver goodput escaped the gate: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := compareBaseline(root, root, 1); err != nil {
				t.Fatalf("identical receiver evidence failed baseline comparison: %v", err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPerformanceVerifierChild$")
			command.Env = append(os.Environ(), "WIREHOP_TEST_PERFORMANCE_CANDIDATE="+root,
				"WIREHOP_TEST_PERFORMANCE_BASELINE=")
			output, err := command.CombinedOutput()
			if tt.wantError {
				if failure, ok := err.(*exec.ExitError); !ok || failure.ExitCode() != 1 {
					t.Fatalf("verifier accepted contradictory rates: exit %v, output %s", err, output)
				}
			} else if err != nil {
				t.Fatalf("verifier rejected consistent rates: exit %v, output %s", err, output)
			}
		})
	}
}

func TestCompareBaselineReceiverGoodput(t *testing.T) {
	candidate, baseline := t.TempDir(), t.TempDir()
	for _, root := range []string{candidate, baseline} {
		directory := filepath.Join(root, "HighCapacity-1-Both")
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		writeEvidence(t, directory, "Both")
		rate := 100.0
		if root == candidate {
			rate = 50
		}
		writeFlowFixture(t, directory, rate, 20)
		replaceFlowFixture(t, directory, fmt.Sprintf(`"bits_per_second":%.9f`, rate*1e6),
			`"bits_per_second":100000000.000000000`)
	}
	if err := comparePathBaseline(candidate, baseline, "HighCapacity", "Both", 1); err == nil ||
		!strings.Contains(err.Error(), "less than 85 percent") {
		t.Fatalf("fifty percent receiver goodput escaped the baseline gate: %v", err)
	}
}

func TestVerifyWorkload(t *testing.T) {
	for _, tt := range []struct {
		name, old, replacement string
	}{
		{name: "Duration", old: `"duration":20`, replacement: `"duration":21`},
		{name: "BlockSize", old: `"blksize":131072`, replacement: `"blksize":16384`},
	} {
		for _, comparison := range []string{"WithinProfile", "AcrossRepetitions", "AgainstBaseline"} {
			t.Run(tt.name+comparison, func(t *testing.T) {
				candidate, baseline := t.TempDir(), t.TempDir()
				for _, root := range []string{candidate, baseline} {
					for _, path := range []string{"Low", "High", "Both"} {
						for repetition := 1; repetition <= 2; repetition++ {
							directory := filepath.Join(root, fmt.Sprintf("HighCapacity-%d-%s", repetition, path))
							if err := os.Mkdir(directory, 0700); err != nil {
								t.Fatal(err)
							}
							writeEvidence(t, directory, path)
							writeFlowFixture(t, directory, 100, 21)
							if root == candidate && (comparison == "AgainstBaseline" ||
								comparison == "WithinProfile" && path == "Both" ||
								comparison == "AcrossRepetitions" && repetition == 2) {
								replaceFlowFixture(t, directory, tt.old, tt.replacement)
							}
						}
					}
				}
				var err error
				if comparison == "AgainstBaseline" {
					err = comparePathBaseline(candidate, baseline, "HighCapacity", "Both", 2)
				} else {
					err = verifyProfile(candidate, "HighCapacity", 2)
				}
				if err == nil || !strings.Contains(err.Error(), "different flow workload") {
					t.Fatalf("incomparable flow workloads passed: %v", err)
				}
			})
		}
	}
}

// replaceFlowFixture changes a specific field and rejects missing or ambiguous test inputs.
func replaceFlowFixture(t *testing.T, directory, old, replacement string) {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(directory, "flow.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(encoded), old) != 1 {
		t.Fatalf("flow fixture must contain exactly one %q", old)
	}
	writeFixture(t, directory, "flow.json", strings.Replace(string(encoded), old, replacement, 1))
}
