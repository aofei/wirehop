package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestIntegrationRunner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the integration shell harness requires Unix paths")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("POSIX shell is unavailable")
	}
	script, err := filepath.Abs("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	// Register shell changes as inputs to Go's test-result cache.
	if _, err := os.ReadFile(script); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ name, repetition, scenario string }{
		{name: "UnknownCase", scenario: "unknown"},
		{name: "DuplicateCases", scenario: "native"},
		{name: "InvalidRepetitions", repetition: "0", scenario: "native"},
		{name: "OverflowRepetitions", repetition: "9999999999999999999999999", scenario: "native"},
		{name: "ExistingResults", scenario: "native"},
		{name: "StartupFailure", scenario: "native"},
		{name: "StartupCaptureFailure", scenario: "native"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			binary := filepath.Join(directory, "wirehop")
			results := filepath.Join(directory, "results")
			transcript := filepath.Join(directory, "docker-calls.txt")
			t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("WIREHOP_TEST_REPETITIONS", tt.repetition)
			t.Setenv("WIREHOP_MOCK_DOCKER_LOG", transcript)
			t.Setenv("WIREHOP_MOCK_RESULTS", results)
			t.Setenv("WIREHOP_MOCK_CAPTURE_FAILURE", tt.name)
			for name, data := range map[string]string{
				"wirehop": "#!/bin/sh\nexit 0\n",
				"go":      "#!/bin/sh\nexit 0\n",
				"docker": `#!/bin/sh
printf '%s\n' "$*" >> "$WIREHOP_MOCK_DOCKER_LOG"
case "$1:$2" in
  network:create) printf 'fixture-network\n' ;;
  run:*)
    previous=
    for argument in "$@"; do
      if [ "$previous" = --cidfile ]; then printf 'fixture-container\n' > "$argument"; fi
      previous=$argument
    done
    if [ "$WIREHOP_MOCK_CAPTURE_FAILURE" = StartupCaptureFailure ]; then
      mkdir "$WIREHOP_MOCK_RESULTS/native/harness-error.txt"
    fi
    ;;
  inspect:-f) printf 'false\n' ;;
  logs:*) printf 'fixture server failed to start\n' ;;
  exec:*) exit 1 ;;
esac
exit 0
`,
			} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if tt.name == "ExistingResults" {
				if err := os.Mkdir(results, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(results, "runner.txt"), []byte("original"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			arguments := []string{script, binary, results, tt.scenario}
			if tt.name == "DuplicateCases" {
				arguments = append(arguments, tt.scenario)
			}
			output, err := exec.Command(sh, arguments...).CombinedOutput()
			if err == nil {
				t.Fatalf("runner accepted invalid setup: %s", output)
			}
			data, readError := os.ReadFile(transcript)
			if !strings.HasPrefix(tt.name, "Startup") {
				if !os.IsNotExist(readError) {
					t.Fatalf("contacted Docker before input validation: %q, %v", data, readError)
				}
				if tt.name == "ExistingResults" {
					data, err := os.ReadFile(filepath.Join(results, "runner.txt"))
					if err != nil || string(data) != "original" {
						t.Fatal("overwrote previous evidence")
					}
				}
				return
			}
			for _, want := range []string{"logs ", "top ", "rm -f ", "network rm "} {
				if readError != nil || !strings.Contains(string(data), want) {
					t.Fatalf("missing cleanup or capture %q: %s", want, data)
				}
			}
			for _, name := range []string{"server-container.log", "server-container.json", "server-processes.txt", "harness-error.txt"} {
				if tt.name == "StartupCaptureFailure" && name == "harness-error.txt" {
					continue
				}
				data, err := os.ReadFile(filepath.Join(results, "native", name))
				if err != nil {
					t.Fatal(err)
				}
				if name == "server-container.log" && !strings.Contains(string(data), "fixture server failed") {
					t.Fatal("lost startup diagnostics")
				}
			}
		})
	}
}

func TestIntegrationRunnerRetainsFailedRepetition(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the integration shell harness requires Unix paths")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("POSIX shell is unavailable")
	}
	script, err := filepath.Abs("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(script); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	results := filepath.Join(directory, "results")
	transcript := filepath.Join(directory, "verifications.txt")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("WIREHOP_TEST_REPETITIONS", "2")
	t.Setenv("WIREHOP_MOCK_VERIFICATIONS", transcript)
	for name, data := range map[string]string{
		"wirehop": "#!/bin/sh\nexit 0\n",
		"go": `#!/bin/sh
if [ "$1" = run ]; then
  printf '%s\n' "$3" >> "$WIREHOP_MOCK_VERIFICATIONS"
  case "$3" in */repetition-1) exit 1 ;; esac
fi
exit 0
`,
		"docker": `#!/bin/sh
case "$1:$2" in
  network:create) printf 'fixture-network\n' ;;
  run:*)
    previous=
    for argument in "$@"; do
      if [ "$previous" = --cidfile ]; then printf 'fixture-container\n' > "$argument"; fi
      previous=$argument
      case "$argument" in
        *:/results) touch "${argument%:/results}/ready" ;;
      esac
    done
    ;;
  inspect:-f) printf '192.0.2.1\n' ;;
  wait:*) printf '0\n' ;;
esac
exit 0
`,
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0700); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.Command(sh, script, filepath.Join(directory, "wirehop"), results, "native").CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		t.Fatalf("runner lost a failed repetition: %v, %s", err, output)
	}
	data, err := os.ReadFile(transcript)
	want := filepath.Join(results, "repetition-1") + "\n" + filepath.Join(results, "repetition-2") + "\n"
	if err != nil || string(data) != want {
		t.Fatalf("runner did not verify both repetitions: %q, %v", data, err)
	}
	for _, repetition := range []string{"repetition-1", "repetition-2"} {
		data, err := os.ReadFile(filepath.Join(results, repetition, "native", "client-exit.txt"))
		if err != nil || string(data) != "0\n" {
			t.Fatalf("lost evidence for %s: %q, %v", repetition, data, err)
		}
	}
}
