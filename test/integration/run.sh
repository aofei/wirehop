#!/bin/sh
set -eu

if [ "$#" -lt 2 ]; then
  echo "usage: sh test/integration/run.sh LINUX_BINARY RESULTS_DIRECTORY [CASE ...]" >&2
  exit 2
fi
binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
if [ ! -f "$binary" ] || [ ! -x "$binary" ]; then
  echo "binary must be an executable regular file: $binary" >&2
  exit 2
fi
result_argument=$2
repetitions=${WIREHOP_TEST_REPETITIONS:-1}
case "$repetitions" in
  ''|0*|*[!0-9]*) echo "repetitions must be positive without leading zeros" >&2; exit 2 ;;
esac
if ! [ "$repetitions" -gt 0 ] 2>/dev/null; then
  echo "repetitions must fit a positive shell integer" >&2
  exit 2
fi
scripts=$(CDPATH='' cd "$(dirname "$0")" && pwd)
shift 2
if [ "$#" -eq 0 ]; then
  set -- native forward tcp tls ws wss tcp-multipath tcp-asymmetric tcp-asymmetric-stall tcp-asymmetric-stall-reverse tcp-asymmetric-stall-bidir tcp-latency tcp-loss tcp-stall tcp-bidir tcp-idle \
    tcp-rekey forward-rekey forward-prohibit forward-blackhole tcp-prohibit tcp-blackhole \
    native-udp forward-udp tcp-udp wss-udp tcp-ipv6 wss-ipv6 forward-ipv6 tcp-inner-ipv6 tcp-fwmark forward-fwmark tcp-mixed \
    native-slow32 tcp-slow32 tcp-slow64 tcp-slow128 tcp-multipath-slow32 tcp-multipath-slow64 tcp-multipath-slow128 \
    tcp-multipath-distinct-slow32 tcp-multipath-distinct-slow64 tcp-multipath-distinct-slow128 \
    native-capacity-change native-capacity-change-reverse native-capacity-change-bidir \
    native-rtt-step native-rtt-step-reverse native-rtt-step-bidir \
    tcp-multipath-capacity-change tcp-multipath-capacity-change-reverse tcp-multipath-capacity-change-bidir tcp-outage tcp-roam \
    tcp-rtt-step tcp-rtt-step-reverse tcp-rtt-step-bidir \
    tcp-multipath-rtt-step tcp-multipath-rtt-step-reverse tcp-multipath-rtt-step-bidir \
    tls-slow32 tls-slow64 tls-slow128 tls-outage tls-roam \
    ws-slow32 ws-slow64 ws-slow128 ws-outage ws-roam \
    wss-slow32 wss-slow64 wss-slow128 wss-outage wss-roam tls-stall ws-stall wss-stall
fi
ipv6=false
selected_cases=
for scenario in "$@"; do
  case "$scenario" in
    native|native-slow32|native-capacity-change|native-capacity-change-reverse|native-capacity-change-bidir|\
    native-rtt-step|native-rtt-step-reverse|native-rtt-step-bidir|\
    forward|tcp|tls|ws|wss|tcp-multipath|tcp-asymmetric|tcp-asymmetric-stall|tcp-asymmetric-stall-reverse|tcp-asymmetric-stall-bidir|tcp-latency|tcp-loss|tcp-stall|tcp-bidir|tcp-idle|tcp-rekey|forward-rekey|\
    forward-prohibit|forward-blackhole|tcp-prohibit|tcp-blackhole|native-udp|forward-udp|tcp-udp|wss-udp|\
    tcp-ipv6|wss-ipv6|forward-ipv6|tcp-inner-ipv6|tcp-fwmark|forward-fwmark|tcp-mixed|\
    tcp-slow32|tcp-slow64|tcp-slow128|tcp-multipath-slow32|tcp-multipath-slow64|tcp-multipath-slow128|\
    tcp-multipath-distinct-slow32|tcp-multipath-distinct-slow64|tcp-multipath-distinct-slow128|\
    tcp-multipath-capacity-change|tcp-multipath-capacity-change-reverse|tcp-multipath-capacity-change-bidir|tcp-outage|tcp-roam|\
    tcp-rtt-step|tcp-rtt-step-reverse|tcp-rtt-step-bidir|\
    tcp-multipath-rtt-step|tcp-multipath-rtt-step-reverse|tcp-multipath-rtt-step-bidir|\
    tls-slow32|tls-slow64|tls-slow128|tls-outage|tls-roam|\
    ws-slow32|ws-slow64|ws-slow128|ws-outage|ws-roam|\
    wss-slow32|wss-slow64|wss-slow128|wss-outage|wss-roam|tls-stall|ws-stall|wss-stall) ;;
    *) echo "unknown case: $scenario" >&2; exit 2 ;;
  esac
  case " $selected_cases " in
    *" $scenario "*) echo "duplicate case: $scenario" >&2; exit 2 ;;
  esac
  selected_cases="$selected_cases $scenario"
  case "$scenario" in
    tcp-ipv6|wss-ipv6|forward-ipv6) ipv6=true ;;
  esac
done
if [ -e "$result_argument" ]; then
  echo "results directory must not already exist: $result_argument" >&2
  exit 2
fi
mkdir -p "$result_argument"
results=$(cd "$result_argument" && pwd)
result_root=$results
image=${WIREHOP_TEST_IMAGE:-wirehop-integration}
{
  uname -a
  go version
  go version -m "$binary"
  printf 'GITHUB_SHA=%s\nGITHUB_RUN_ID=%s\nGITHUB_RUN_ATTEMPT=%s\n' \
    "${GITHUB_SHA:-unknown}" "${GITHUB_RUN_ID:-unknown}" "${GITHUB_RUN_ATTEMPT:-unknown}"
  printf 'ImageOS=%s\nImageVersion=%s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"
  docker version
  docker info --format 'cpus={{.NCPU}} memory={{.MemTotal}} kernel={{.KernelVersion}} architecture={{.Architecture}}'
  docker image inspect "$image" --format 'image={{.Id}} digests={{json .RepoDigests}}'
  if [ -r /proc/cpuinfo ]; then cat /proc/cpuinfo; fi
} > "$result_root/runner.txt"
network=wirehop-tcp-$$
server=$network-server
client=$network-client

case_results=

fail_case() {
  printf '%s\n' "$1" >> "$case_results/harness-error.txt"
  printf '%s\n' "$1" >&2
  status=1
}

capture_case() {
  for role in client server; do
    container=$network-$role
    docker logs "$container" > "$case_results/$role-container.log" 2>&1 || true
    docker inspect "$container" > "$case_results/$role-container.json" 2>> "$case_results/capture-errors.log" || true
    docker top "$container" > "$case_results/$role-processes.txt" 2>> "$case_results/capture-errors.log" || true
  done
  docker exec "$server" ss -taoinm > "$case_results/server-tcp-sockets-final.txt" \
    2>> "$case_results/capture-errors.log" || true
  docker exec "$server" nstat -az > "$case_results/server-nstat.txt" \
    2>> "$case_results/capture-errors.log" || fail_case "server counter collection failed"
  docker exec "$server" wg show wgtest latest-handshakes > "$case_results/server-handshake.txt" \
    2>> "$case_results/capture-errors.log" || fail_case "server handshake collection failed"
  docker exec "$server" tc -s qdisc show dev eth0 > "$case_results/server-qdisc-final.txt" \
    2>> "$case_results/capture-errors.log" || fail_case "server queue collection failed"
}

cleanup() {
  runner_exit_code=$?
  trap - EXIT
  if [ -n "$case_results" ]; then
    printf 'runner exited with status %s before completing the case\n' "$runner_exit_code" \
      >> "$case_results/harness-error.txt" || true
    capture_case || true
  fi
  docker rm -f "$client" "$server" > /dev/null 2>&1 || true
  docker network rm "$network" > /dev/null 2>&1 || true
  exit "$runner_exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [ "$ipv6" = true ]; then
  docker network create --internal --ipv6 "$network" > /dev/null
else
  docker network create --internal "$network" > /dev/null
fi
status=0
repetition=1
while :; do
  results=$result_root
  if [ "$repetitions" -gt 1 ]; then
    results=$result_root/repetition-$repetition
    mkdir "$results"
  fi
  for scenario in "$@"; do
    echo "$scenario repetition=$repetition"
    mkdir "$results/$scenario"
    case_results=$results/$scenario
    docker run -d --name "$server" --network "$network" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
      -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$results/$scenario:/results" \
      --entrypoint sh "$image" /test/node.sh server "$scenario" > /dev/null
    ready=false
    for _attempt in $(seq 1 30); do
      if [ -f "$results/$scenario/ready" ]; then
        ready=true
        break
      fi
      if [ "$(docker inspect -f '{{.State.Running}}' "$server")" != true ]; then
        break
      fi
      sleep 1
    done
    if [ "$ready" != true ]; then
      docker logs "$server" >&2
      exit 1
    fi
    server_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$server")
    case "$scenario" in
      tcp-ipv6|wss-ipv6|forward-ipv6)
        server_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.GlobalIPv6Address}}{{end}}' "$server")
        if [ -z "$server_ip" ]; then
          echo "case requires an IPv6 container address: $scenario" >&2
          exit 1
        fi
        server_ip="[$server_ip]"
        ;;
    esac
    docker run -d --name "$client" --network "$network" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
      -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$results/$scenario:/results" \
      -e "WIREHOP_TEST_SERVER=$server_ip" --entrypoint sh "$image" /test/node.sh client "$scenario" > /dev/null
    container_status=$(docker wait "$client")
    printf '%s\n' "$container_status" > "$results/$scenario/client-exit.txt"
    case "$scenario" in
      *-rtt-step*|*-capacity-change*|tcp-asymmetric-stall*|*-stall|*-outage|*-roam|*-prohibit|*-blackhole)
        if [ ! -f "$results/$scenario/server-flow-done.txt" ]; then
          fail_case "server result collection did not complete: $scenario"
        elif [ "$(cat "$results/$scenario/server-flow-exit.txt")" != 0 ]; then
          fail_case "server iperf3 failed: $scenario"
        fi
        ;;
    esac
    if [ -f "$results/$scenario/server-observations-started" ]; then
      completed=false
      for attempt in $(seq 1 50); do
        if [ -f "$results/$scenario/server-observations-done" ]; then completed=true; break; fi
        sleep 0.1
      done
      if [ "$completed" != true ]; then
        fail_case "server observations did not complete: $scenario"
      fi
    fi
    capture_case
    case_results=
    docker rm -f "$client" "$server" > /dev/null
    if [ "$container_status" != 0 ]; then
      echo "case failed: $scenario (container exit $container_status), inspect $results/$scenario" >&2
      status=1
    fi
  done
  go run "$scripts/verify.go" "$results" "$@" || status=1
  if [ "$repetition" -eq "$repetitions" ]; then break; fi
  repetition=$((repetition + 1))
done
exit "$status"
