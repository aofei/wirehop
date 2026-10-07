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
mkdir -p "$2"
results=$(cd "$2" && pwd)
repetitions=${3:-3}
case "$repetitions" in
  ''|*[!0-9]*|0) echo "repetitions must be positive" >&2; exit 2 ;;
esac
scripts=$(CDPATH='' cd "$(dirname "$0")/../integration" && pwd)
image=${WIREHOP_TEST_IMAGE:-wirehop-integration}
network=wirehop-performance-$$
router=$network-router
server=$network-server
client=$network-client

cleanup() {
  docker rm -f "$client" "$server" "$router" > /dev/null 2>&1 || true
  docker network rm "$network" > /dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker network create --internal "$network" > /dev/null
subnet=$(docker network inspect -f '{{(index .IPAM.Config 0).Subnet}}' "$network")
docker run -d --name "$router" --network "$network" --read-only --tmpfs /tmp --cap-add NET_ADMIN \
  --sysctl net.ipv4.ip_forward=1 --sysctl net.ipv4.conf.all.send_redirects=0 \
  --sysctl net.ipv4.conf.default.send_redirects=0 --entrypoint sh "$image" -c 'sleep 7200' > /dev/null
router_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$router")
docker exec "$router" uname -a > "$results/kernel.txt"

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
      echo "$profile repetition=$repetition path=$path"
      docker exec -i "$router" sh -s -- "$low_rate" "$high_rate" <<'SH'
set -eu
tc qdisc del dev eth0 root 2>/dev/null || true
tc qdisc add dev eth0 root handle 1: prio bands 3
tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 5ms rate "$1" limit 1000
tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 150ms rate "$2" limit 1000
for field in sport dport; do
  tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip "$field" 51822 0xffff flowid 1:1
  tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip "$field" 51823 0xffff flowid 1:2
done
SH
      docker run -d --name "$server" --network "$network" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
        -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$case_results:/results" \
        -e WIREHOP_TEST_ROUTED=true -e "WIREHOP_TEST_ROUTER=$router_ip" -e "WIREHOP_TEST_SUBNET=$subnet" \
        --entrypoint sh "$image" -c \
        'ip route replace "$WIREHOP_TEST_SUBNET" via "$WIREHOP_TEST_ROUTER" dev eth0
exec sh /test/node.sh server tcp-asymmetric' > /dev/null
      ready=false
      for attempt in $(seq 1 30); do
        if [ -f "$case_results/ready" ]; then ready=true; break; fi
        if [ "$(docker inspect -f '{{.State.Running}}' "$server")" != true ]; then break; fi
        sleep 1
      done
      if [ "$ready" != true ]; then docker logs "$server" >&2; exit 1; fi
      server_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$server")
      docker run -d --name "$client" --network "$network" --read-only --cap-add NET_ADMIN --tmpfs /tmp \
        -v "$binary:/wirehop:ro" -v "$scripts:/test:ro" -v "$case_results:/results" \
        -e WIREHOP_TEST_ROUTED=true -e "WIREHOP_TEST_SERVER=$server_ip" -e "WIREHOP_TEST_PATH=$path" \
        -e "WIREHOP_TEST_ROUTER=$router_ip" --entrypoint sh "$image" -c \
        'ip route add "$WIREHOP_TEST_SERVER/32" via "$WIREHOP_TEST_ROUTER"
exec sh /test/node.sh client tcp-asymmetric' > /dev/null
      status=$(docker wait "$client")
      docker logs "$client" > "$case_results/client-container.log" 2>&1
      docker logs "$server" > "$case_results/server-container.log" 2>&1
      docker exec "$server" nstat -az > "$case_results/server-nstat.txt"
      docker exec "$router" tc -s qdisc show dev eth0 > "$case_results/router-qdisc.txt"
      docker rm -f "$client" "$server" > /dev/null
      if [ "$status" != 0 ]; then echo "flow failed: $case_results, exit $status" >&2; exit 1; fi
    done
  done
done
go run "$(dirname "$scripts")/performance/verify.go" "$results" "$repetitions"
