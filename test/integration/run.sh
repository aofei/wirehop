#!/bin/sh
set -eu

if [ "$#" -lt 2 ]; then
  echo "usage: sh test/integration/run.sh LINUX_BINARY RESULTS_DIRECTORY [CASE ...]" >&2
  exit 2
fi
binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
mkdir -p "$2"
results=$(cd "$2" && pwd)
scripts=$(CDPATH= cd "$(dirname "$0")" && pwd)
shift 2
if [ "$#" -eq 0 ]; then
  set -- native forward tcp tls ws wss tcp-multipath tcp-latency tcp-loss tcp-stall tcp-bidir tcp-idle tcp-rekey forward-rekey
fi
image=${WIREHOP_TEST_IMAGE:-wirehop-integration}
network=wirehop-tcp-$$
server=$network-server
client=$network-client

cleanup() {
  docker rm -f "$client" "$server" > /dev/null 2>&1 || true
  docker network rm "$network" > /dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker network create --internal "$network" > /dev/null
for scenario in "$@"; do
  case "$scenario" in
    native|forward|tcp|tls|ws|wss|tcp-multipath|tcp-latency|tcp-loss|tcp-stall|tcp-bidir|tcp-idle|tcp-rekey|forward-rekey) ;;
    *) echo "unknown case: $scenario" >&2; exit 2 ;;
  esac
  echo "$scenario"
  mkdir "$results/$scenario"
  docker run -d --name "$server" --network "$network" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
    -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$results/$scenario:/results" \
    --entrypoint sh "$image" /test/node.sh server "$scenario" > /dev/null
  ready=false
  for attempt in $(seq 1 30); do
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
  docker run -d --name "$client" --network "$network" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
    -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$results/$scenario:/results" \
    -e "WIREHOP_TEST_SERVER=$server_ip" --entrypoint sh "$image" /test/node.sh client "$scenario" > /dev/null
  status=$(docker wait "$client")
  docker logs "$client" > "$results/$scenario/client-container.log" 2>&1
  docker logs "$server" > "$results/$scenario/server-container.log" 2>&1
  docker exec "$server" nstat -az > "$results/$scenario/server-nstat.txt"
  docker exec "$server" wg show wgtest latest-handshakes > "$results/$scenario/server-handshake.txt"
  docker rm -f "$client" "$server" > /dev/null
  if [ "$status" != 0 ]; then
    echo "case failed: $scenario (container exit $status), inspect $results/$scenario" >&2
    exit 1
  fi
done
go run "$scripts/verify.go" "$results" "$@"
