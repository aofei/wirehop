#!/bin/sh
set -eu

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: sh test/performance/run.sh LINUX_BINARY RESULTS_DIRECTORY [REPETITIONS]" >&2
  exit 2
fi
binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
if [ ! -f "$binary" ] || [ ! -x "$binary" ]; then
  echo "binary must be an executable regular file: $binary" >&2
  exit 2
fi
result_argument=$2
repetitions=${3:-3}
case "$repetitions" in
  ''|0*|*[!0-9]*) echo "repetitions must be positive without leading zeros" >&2; exit 2 ;;
esac
if ! [ "$repetitions" -gt 0 ] 2>/dev/null; then
  echo "repetitions must fit a positive shell integer" >&2
  exit 2
fi
duration=${WIREHOP_TEST_DURATION:-20}
case "$duration" in
  ''|0*|*[!0-9]*) echo "duration must be an integer from 20 through 86400 seconds without leading zeros" >&2; exit 2 ;;
esac
if ! [ "$duration" -ge 20 ] 2>/dev/null || ! [ "$duration" -le 86400 ] 2>/dev/null; then
  echo "duration must be from 20 through 86400 seconds" >&2
  exit 2
fi
observe=${WIREHOP_TEST_OBSERVE:-false}
latency=false
set --
if [ -n "${WIREHOP_TEST_LATENCY_BINARY:-}" ]; then
  latency_binary=$(cd "$(dirname "$WIREHOP_TEST_LATENCY_BINARY")" && pwd)/$(basename "$WIREHOP_TEST_LATENCY_BINARY")
  if [ ! -f "$latency_binary" ] || [ ! -x "$latency_binary" ]; then
    echo "latency binary must be an executable regular file" >&2
    exit 2
  fi
  latency=true
  set -- -v "$latency_binary:/latency:ro"
fi
if [ -e "$result_argument" ]; then
  echo "results directory must not already exist: $result_argument" >&2
  exit 2
fi
mkdir -p "$result_argument"
results=$(cd "$result_argument" && pwd)
scripts=$(CDPATH='' cd "$(dirname "$0")/../integration" && pwd)
image=${WIREHOP_TEST_IMAGE:-wirehop-integration}
network=wirehop-performance-$$
router=$network-router
server=$network-server
client=$network-client
case_results=
network_id=
client_cid=
server_cid=
router_cid=$results/router.cid

capture_failure() {
  for role in client server router; do
    cid_file=$case_results/$role.cid
    if [ "$role" = router ]; then cid_file=$router_cid; fi
    if [ ! -s "$cid_file" ]; then
      printf 'container was not created\n' > "$case_results/$role-failure-container.log"
      : > "$case_results/$role-failure-container.json"
      : > "$case_results/$role-failure-processes.txt"
      continue
    fi
    container=$(cat "$cid_file")
    docker logs "$container" > "$case_results/$role-failure-container.log" 2>&1 || true
    docker inspect "$container" > "$case_results/$role-failure-container.json" \
      2>> "$case_results/capture-errors.log" || true
    docker top "$container" > "$case_results/$role-failure-processes.txt" \
      2>> "$case_results/capture-errors.log" || true
  done
  if [ -s "$server_cid" ]; then
    container=$(cat "$server_cid")
    docker exec "$container" ss -taoinm > "$case_results/server-failure-tcp-sockets.txt" \
      2>> "$case_results/capture-errors.log" || true
    docker exec "$container" nstat -az > "$case_results/server-failure-nstat.txt" \
      2>> "$case_results/capture-errors.log" || true
  fi
  if [ -s "$router_cid" ]; then
    docker exec "$(cat "$router_cid")" tc -s qdisc show dev eth0 > "$case_results/router-failure-qdisc.txt" \
      2>> "$case_results/capture-errors.log" || true
  fi
}

cleanup() {
  runner_status=$?
  trap - EXIT
  if [ -n "$case_results" ]; then
    printf 'runner exited with status %s before completing the case\n' "$runner_status" \
      >> "$case_results/harness-error.txt" || true
    # Isolate fatal shell redirection errors so diagnostics cannot bypass resource cleanup.
    (capture_failure) || true
    if [ "$runner_status" -eq 0 ]; then runner_status=1; fi
  fi
  for cid_file in "$client_cid" "$server_cid" "$router_cid"; do
    if [ -s "$cid_file" ]; then docker rm -f "$(cat "$cid_file")" > /dev/null 2>&1 || true; fi
  done
  if [ -n "$network_id" ]; then docker network rm "$network_id" > /dev/null 2>&1 || true; fi
  exit "$runner_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

{
  uname -a
  go version
  go version -m "$binary"
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "$binary"
  else
    shasum -a 256 "$binary"
  fi
  printf 'GITHUB_SHA=%s\nGITHUB_RUN_ID=%s\nGITHUB_RUN_ATTEMPT=%s\n' \
    "${GITHUB_SHA:-unknown}" "${GITHUB_RUN_ID:-unknown}" "${GITHUB_RUN_ATTEMPT:-unknown}"
  printf 'ImageOS=%s\nImageVersion=%s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"
  docker version
  docker info --format 'cpus={{.NCPU}} memory={{.MemTotal}} kernel={{.KernelVersion}} architecture={{.Architecture}}'
  docker image inspect "$image" --format 'image={{.Id}} digests={{json .RepoDigests}}'
  if [ -r /proc/cpuinfo ]; then cat /proc/cpuinfo; fi
} > "$results/runner.txt" 2>&1

network_id=$(docker network create --internal "$network")
printf '%s\n' "$network_id" > "$results/network.id"
subnet=$(docker network inspect -f '{{(index .IPAM.Config 0).Subnet}}' "$network_id")
docker run -d --cidfile "$router_cid" --name "$router" --network "$network_id" --read-only --tmpfs /tmp --cap-add NET_ADMIN \
  -v "$scripts:/test:ro" -v "$results:/results" \
  --sysctl net.ipv4.ip_forward=1 --sysctl net.ipv4.conf.all.send_redirects=0 \
  --sysctl net.ipv4.conf.default.send_redirects=0 --entrypoint sh "$image" -c 'while :; do sleep 3600; done' > /dev/null
router_id=$(cat "$router_cid")
router_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$router_id")
docker exec "$router_id" uname -a > "$results/kernel.txt"

for profile in HighCapacity LowCapacity; do
  case "$profile" in
    HighCapacity) low_rate=10mbit; high_rate=100mbit ;;
    LowCapacity) low_rate=100mbit; high_rate=10mbit ;;
  esac
  for repetition in $(seq 1 "$repetitions"); do
    case "$((repetition % 3))" in
      1) path_order='Low High Both' ;;
      2) path_order='Both High Low' ;;
      0) path_order='High Low Both' ;;
    esac
    for path in $path_order; do
      case_results="$results/$profile-$repetition-$path"
      mkdir "$case_results"
      client_cid=$case_results/client.cid
      server_cid=$case_results/server.cid
      echo "$profile repetition=$repetition path=$path"
      if [ "$observe" = true ]; then
        docker exec -d "$router_id" sh /test/observe.sh router "/results/$profile-$repetition-$path"
      fi
      docker exec -i "$router_id" sh -s -- "$low_rate" "$high_rate" <<'SH'
set -eu
tc qdisc del dev eth0 root 2>/dev/null || true
tc qdisc add dev eth0 root handle 1: prio bands 3
# Retain delayed flights and return traffic without introducing an unintended loss impairment.
tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 5ms rate "$1" limit 8192
tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 150ms rate "$2" limit 8192
for field in sport dport; do
  tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip "$field" 51822 0xffff flowid 1:1
  tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip "$field" 51823 0xffff flowid 1:2
done
SH
      docker run -d --cidfile "$server_cid" --name "$server" --network "$network_id" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
        "$@" \
        -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$case_results:/results" \
        -e WIREHOP_TEST_ROUTED=true -e "WIREHOP_TEST_ROUTER=$router_ip" -e "WIREHOP_TEST_SUBNET=$subnet" \
        -e "WIREHOP_TEST_OBSERVE=$observe" \
        -e "WIREHOP_TEST_LATENCY=$latency" \
        --entrypoint sh "$image" -c \
        'ip route replace "$WIREHOP_TEST_SUBNET" via "$WIREHOP_TEST_ROUTER" dev eth0
exec sh /test/node.sh server tcp-asymmetric' > /dev/null
      server_id=$(cat "$server_cid")
      ready=false
      for attempt in $(seq 1 30); do
        if [ -f "$case_results/ready" ]; then ready=true; break; fi
        if [ "$(docker inspect -f '{{.State.Running}}' "$server_id")" != true ]; then break; fi
        sleep 1
      done
      if [ "$ready" != true ]; then docker logs "$server_id" >&2; exit 1; fi
      server_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$server_id")
      docker run -d --cidfile "$client_cid" --name "$client" --network "$network_id" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
        "$@" \
        -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$case_results:/results" \
        -e WIREHOP_TEST_ROUTED=true -e "WIREHOP_TEST_SERVER=$server_ip" -e "WIREHOP_TEST_PATH=$path" \
        -e "WIREHOP_TEST_OBSERVE=$observe" -e "WIREHOP_TEST_DURATION=$duration" \
        -e "WIREHOP_TEST_LATENCY=$latency" \
        -e "WIREHOP_TEST_ROUTER=$router_ip" --entrypoint sh "$image" -c \
        'ip route add "$WIREHOP_TEST_SERVER/32" via "$WIREHOP_TEST_ROUTER"
exec sh /test/node.sh client tcp-asymmetric' > /dev/null
      client_id=$(cat "$client_cid")
      status=$(docker wait "$client_id")
      printf '%s\n' "$status" > "$case_results/client-exit.txt"
      if [ "$observe" = true ] && [ "$status" = 0 ]; then
        observed=false
        for attempt in $(seq 1 50); do
          if [ -f "$case_results/client-observations-done" ] && [ -f "$case_results/server-observations-done" ] &&
            [ -f "$case_results/router-observations-done" ]; then
            observed=true
            break
          fi
          sleep 0.1
        done
        if [ "$observed" != true ]; then
          echo "observation did not complete: $case_results" >&2
          exit 1
        fi
      fi
      docker logs "$client_id" > "$case_results/client-container.log" 2>&1
      docker logs "$server_id" > "$case_results/server-container.log" 2>&1
      docker exec "$server_id" nstat -az > "$case_results/server-nstat.txt"
      docker exec "$router_id" tc -s qdisc show dev eth0 > "$case_results/router-qdisc.txt"
      if [ "$status" != 0 ]; then echo "flow failed: $case_results, exit $status" >&2; exit 1; fi
      case_results=
      docker rm -f "$client_id" "$server_id" > /dev/null
      client_cid=
      server_cid=
    done
  done
done
go run "$(dirname "$scripts")/performance/verify.go" "$results" "$repetitions"
