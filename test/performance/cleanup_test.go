package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunnersCleanUpOwnedResources(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the shell harnesses require Unix paths")
	}
	for _, harness := range []struct {
		name, script, argument string
	}{
		{name: "Integration", script: "../integration/run.sh", argument: "tcp"},
		{name: "Performance", script: "run.sh", argument: "1"},
	} {
		t.Run(harness.name, func(t *testing.T) {
			script, err := filepath.Abs(harness.script)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.ReadFile(script); err != nil {
				t.Fatal(err)
			}
			for _, stage := range []string{
				"NetworkCollision", "ServerCollision", "ClientCollision", "PartialStart", "CaseDirectoryFailure", "DiagnosticWriteFailure",
			} {
				t.Run(stage, func(t *testing.T) {
					directory := t.TempDir()
					results := filepath.Join(directory, "results")
					transcript := filepath.Join(directory, "calls.txt")
					t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
					t.Setenv("WIREHOP_TEST_OBSERVE", "false")
					t.Setenv("WIREHOP_TEST_LATENCY_BINARY", "")
					t.Setenv("WIREHOP_MOCK_FAILURE_STAGE", stage)
					t.Setenv("WIREHOP_MOCK_TRANSCRIPT", transcript)
					t.Setenv("WIREHOP_MOCK_RESULTS", results)
					ids := map[string]string{
						"router": strings.Repeat("a", 64), "server": strings.Repeat("b", 64),
						"client": strings.Repeat("c", 64), "network": strings.Repeat("d", 64),
					}
					for role, id := range ids {
						t.Setenv("WIREHOP_MOCK_"+strings.ToUpper(role), id)
					}
					for name, content := range map[string]string{
						"wirehop": "#!/bin/sh\nexit 0\n",
						"go":      "#!/bin/sh\nprintf 'fixture build metadata\\n'\n",
						"docker":  fixtureOwnedResourcesDocker,
					} {
						if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0700); err != nil {
							t.Fatal(err)
						}
					}
					output, err := exec.Command("sh", script, filepath.Join(directory, "wirehop"), results, harness.argument).CombinedOutput()
					exit, ok := err.(*exec.ExitError)
					wantExit := 37
					if stage == "CaseDirectoryFailure" {
						wantExit = 1
					}
					if !ok {
						t.Fatalf("runner exit = %v, want %d: %s", err, wantExit, output)
					}
					if exit.ExitCode() != wantExit {
						t.Errorf("runner exit = %d, want %d: %s", exit.ExitCode(), wantExit, output)
					}
					calls, err := os.ReadFile(transcript)
					if err != nil {
						t.Fatal(err)
					}
					removed := make(map[string]bool)
					for _, line := range strings.Split(string(calls), "\n") {
						fields := strings.Fields(line)
						if len(fields) < 3 || fields[0] != "rm" || fields[1] != "-f" {
							continue
						}
						for _, id := range fields[2:] {
							if id != ids["client"] && id != ids["server"] && id != ids["router"] {
								t.Fatalf("attempted to remove an unowned container %q: %s", id, calls)
							}
							removed[id] = true
						}
					}
					want := make(map[string]bool)
					if stage != "NetworkCollision" {
						if harness.name == "Performance" {
							want[ids["router"]] = true
						}
						if stage == "ClientCollision" || stage == "PartialStart" || stage == "DiagnosticWriteFailure" {
							want[ids["server"]] = true
						}
						if stage == "PartialStart" {
							want[ids["client"]] = true
						}
					}
					if len(removed) != len(want) {
						t.Fatalf("removed containers = %v, want %v: %s", removed, want, calls)
					}
					for id := range want {
						if !removed[id] {
							t.Fatalf("did not clean up created container %q: %s", id, calls)
						}
					}
					networkRemoved := strings.Contains(string(calls), "network rm "+ids["network"])
					if networkRemoved != (stage != "NetworkCollision") {
						t.Fatalf("network cleanup does not match ownership: %s", calls)
					}
					for _, line := range strings.Split(string(calls), "\n") {
						if strings.HasPrefix(line, "network rm ") && line != "network rm "+ids["network"] {
							t.Fatalf("attempted to remove an unowned network: %s", calls)
						}
					}
				})
			}
		})
	}
}

const fixtureOwnedResourcesDocker = `#!/bin/sh
printf '%s\n' "$*" >> "$WIREHOP_MOCK_TRANSCRIPT"
case "$1:$2" in
  network:create)
    if [ "$WIREHOP_MOCK_FAILURE_STAGE" = NetworkCollision ]; then exit 37; fi
    if [ "$WIREHOP_MOCK_FAILURE_STAGE" = CaseDirectoryFailure ]; then
      case "$*" in *wirehop-performance-*) blocked=HighCapacity-1-Low ;; *) blocked=tcp ;; esac
      touch "$WIREHOP_MOCK_RESULTS/$blocked"
    fi
    printf '%s\n' "$WIREHOP_MOCK_NETWORK"
    ;;
  network:inspect) printf '192.0.2.0/24\n' ;;
  inspect:-f) printf '192.0.2.2\n' ;;
  run:*)
    previous= name= cidfile= results=
    for argument in "$@"; do
      case "$previous" in --name) name=$argument ;; --cidfile) cidfile=$argument ;; esac
      case "$argument" in *:/results) results=${argument%:/results} ;; esac
      previous=$argument
    done
    case "$name" in
      *-router) id=$WIREHOP_MOCK_ROUTER ;;
      *-server)
        if [ "$WIREHOP_MOCK_FAILURE_STAGE" = ServerCollision ]; then exit 37; fi
        id=$WIREHOP_MOCK_SERVER
        touch "$results/ready"
        ;;
      *-client)
        if [ "$WIREHOP_MOCK_FAILURE_STAGE" = DiagnosticWriteFailure ]; then
          mkdir "$results/client-container.json" "$results/client-failure-container.json"
          exit 37
        fi
        if [ "$WIREHOP_MOCK_FAILURE_STAGE" = ClientCollision ]; then exit 37; fi
        id=$WIREHOP_MOCK_CLIENT
        ;;
    esac
    if [ -n "$cidfile" ]; then printf '%s\n' "$id" > "$cidfile"; fi
    case "$name" in *-client) exit 37 ;; esac
    printf '%s\n' "$id"
    ;;
esac
exit 0
`
