package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPerformanceRunnerRetainsFailureEvidence(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the performance shell harness requires Unix paths")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("POSIX shell is unavailable")
	}
	script, err := filepath.Abs("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	// Include the shell source in Go's test-result cache inputs.
	if _, err := os.ReadFile(script); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name     string
		exit     int
		duration string
		latency  bool
	}{
		{name: "ExistingResults", exit: 2},
		{name: "OverflowRepetitions", exit: 2},
		{name: "ExcessiveDuration", exit: 2, duration: "86401"},
		{name: "MaximumIntegerDuration", exit: 2, duration: "9223372036854775807"},
		{name: "OverflowDuration", exit: 2, duration: "9223372036854775808"},
		{name: "MaximumDuration", exit: 1, duration: "86400"},
		{name: "Startup", exit: 1},
		{name: "StartupWithLatency", exit: 1, latency: true},
		{name: "CounterCollection", exit: 17},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			results := filepath.Join(directory, "results")
			transcript := filepath.Join(directory, "docker-calls.txt")
			t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("WIREHOP_TEST_OBSERVE", "false")
			t.Setenv("WIREHOP_TEST_DURATION", "20")
			if tt.duration != "" {
				t.Setenv("WIREHOP_TEST_DURATION", tt.duration)
			}
			t.Setenv("WIREHOP_TEST_LATENCY_BINARY", "")
			t.Setenv("WIREHOP_MOCK_RESULTS", results)
			t.Setenv("WIREHOP_MOCK_TRANSCRIPT", transcript)
			t.Setenv("WIREHOP_MOCK_FAILURE_STAGE", tt.name)
			for name, content := range map[string]string{
				"wirehop": "#!/bin/sh\nexit 0\n",
				"go":      "#!/bin/sh\nprintf 'fixture Go build metadata\\n'\n",
				"docker": `#!/bin/sh
printf '%s\n' "$*" >> "$WIREHOP_MOCK_TRANSCRIPT"
case "$WIREHOP_MOCK_FAILURE_STAGE" in
  OverflowRepetitions|ExcessiveDuration|MaximumIntegerDuration|OverflowDuration) exit 37 ;;
esac
case "$1:$2" in
  network:create) printf 'fixture-network\n' ;;
  network:inspect) printf '192.0.2.0/24\n' ;;
  run:*)
    previous=
    for argument in "$@"; do
      if [ "$previous" = --cidfile ]; then printf 'fixture-container\n' > "$argument"; fi
      previous=$argument
    done
    case "$*" in
      *'/test/node.sh server tcp-asymmetric'*)
        if [ "$WIREHOP_MOCK_FAILURE_STAGE" = CounterCollection ]; then
          touch "$WIREHOP_MOCK_RESULTS/HighCapacity-1-Low/ready"
        fi
        ;;
    esac
    ;;
  inspect:-f)
    case "$3" in
      *Running*) printf 'false\n' ;;
      *) printf '192.0.2.254\n' ;;
    esac
    ;;
  wait:*) printf '0\n' ;;
  logs:*)
    if [ -f "$WIREHOP_MOCK_RESULTS/collection-failed" ]; then
      printf 'fixture late capture unavailable\n' >&2
      exit 1
    fi
    printf 'fixture original log\n'
    ;;
  exec:*)
    if [ -f "$WIREHOP_MOCK_RESULTS/collection-failed" ]; then exit 1; fi
    case "$3" in
      nstat) printf 'fixture original counters\n' ;;
      tc)
        touch "$WIREHOP_MOCK_RESULTS/collection-failed"
        exit 17
        ;;
    esac
    ;;
esac
exit 0
`,
			} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if tt.latency {
				t.Setenv("WIREHOP_TEST_LATENCY_BINARY", filepath.Join(directory, "wirehop"))
			}
			if tt.name == "ExistingResults" {
				if err := os.Mkdir(results, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(results, "kernel.txt"), []byte("original evidence\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repetitions := "1"
			if tt.name == "OverflowRepetitions" {
				repetitions = "9223372036854775808"
			}
			output, err := exec.Command(sh, script, filepath.Join(directory, "wirehop"), results, repetitions).CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != tt.exit {
				t.Fatalf("runner exit = %v, want %d: %s", err, tt.exit, output)
			}
			if tt.name == "ExistingResults" {
				if data, err := os.ReadFile(filepath.Join(results, "kernel.txt")); err != nil || string(data) != "original evidence\n" {
					t.Fatalf("overwrote previous evidence: %q, %v", data, err)
				}
				if _, err := os.Stat(transcript); !os.IsNotExist(err) {
					t.Fatal("contacted Docker before rejecting existing results")
				}
				return
			}
			if tt.exit == 2 {
				if _, err := os.Stat(transcript); !os.IsNotExist(err) {
					t.Fatal("contacted Docker before rejecting invalid arguments")
				}
				if _, err := os.Stat(results); !os.IsNotExist(err) {
					t.Fatal("created results before rejecting invalid arguments")
				}
				return
			}
			if data, err := os.ReadFile(filepath.Join(results, "runner.txt")); err != nil ||
				!strings.Contains(string(data), "fixture Go build metadata") {
				t.Fatalf("missing runner or executable identity: %q, %v", data, err)
			}
			caseResults := filepath.Join(results, "HighCapacity-1-Low")
			if tt.name == "CounterCollection" {
				for name, want := range map[string]string{
					"client-container.log": "fixture original log\n",
					"server-container.log": "fixture original log\n",
					"server-nstat.txt":     "fixture original counters\n",
				} {
					if data, err := os.ReadFile(filepath.Join(caseResults, name)); err != nil || string(data) != want {
						t.Fatalf("overwrote earlier %s: %q, %v", name, data, err)
					}
				}
			}
			for _, name := range []string{"client", "server", "router"} {
				for _, suffix := range []string{"failure-container.log", "failure-container.json", "failure-processes.txt"} {
					if _, err := os.Stat(filepath.Join(caseResults, name+"-"+suffix)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if data, err := os.ReadFile(filepath.Join(caseResults, "harness-error.txt")); err != nil || len(data) == 0 {
				t.Fatalf("missing harness error: %q, %v", data, err)
			}

			calls, err := os.ReadFile(transcript)
			if err != nil || !strings.Contains(string(calls), "rm -f ") || !strings.Contains(string(calls), "network rm ") {
				t.Fatalf("capture failure prevented cleanup: %s, %v", calls, err)
			}
		})
	}
}
