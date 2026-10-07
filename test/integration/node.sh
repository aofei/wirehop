#!/bin/sh
set -eu
# Result files must be readable by the host verifier and artifact uploader.
umask 022

role=$1
scenario=$2
scheme=${scenario%%-*}
carrier_port=51822
if [ "${WIREHOP_TEST_PATH:-Both}" = High ]; then carrier_port=51823; fi
export WIREHOP_TOKEN=isolated-kernel-tcp-test-fixture
export SSL_CERT_FILE=/results/ca.pem
ip link add wgtest type wireguard
if [ "$role" = server ]; then
  case "$scenario" in
    tcp-prohibit|tcp-blackhole)
      ip route add local 192.0.2.1/32 dev lo table 100
      ip rule add pref 100 to 192.0.2.1/32 lookup 100
      (
        while [ ! -f /results/flow-start.txt ]; do sleep 0.1; done
        sleep 5
        ip route replace "${scenario#tcp-}" 192.0.2.1/32 table 100
        sleep 1
        ip route replace local 192.0.2.1/32 dev lo table 100
        date +%s > /results/route-recovered.txt
      ) &
      target=192.0.2.1:51820
      ;;
    tcp-ipv6|wss-ipv6) target='[::1]:51820' ;;
    *) target=127.0.0.1:51820 ;;
  esac
  private_key='AD7V1ztVgGww3j+Ke9qzivE1OSIFMwVeY1aQuLh61kE='
  peer='+SjU9sG4bBLyViwQsHxVXFxX/QD1npDI2NiHZyccv3w='
  local_ip=10.253.91.2
  remote_ip=10.253.91.1
else
  private_key='CH7G4Uu+0hDnIVzcc0aN+iPwgKG/uGZbL9gJvZnSg3k='
  peer='xMjphMUyLIGExyJluSslD9tjaIcF9QS6ADyI8DOTzyg='
  local_ip=10.253.91.1
  remote_ip=10.253.91.2
fi
prefix=32
if [ "$scenario" = tcp-inner-ipv6 ]; then
  prefix=128
  if [ "$role" = server ]; then
    local_ip=fd34:253:91::2
    remote_ip=fd34:253:91::1
  else
    local_ip=fd34:253:91::1
    remote_ip=fd34:253:91::2
  fi
fi
(
  umask 077
  printf '%s\n' "$private_key" > /tmp/private.key
)
wg set wgtest private-key /tmp/private.key listen-port 51820 peer "$peer" allowed-ips "$remote_ip/$prefix"
ip addr add "$local_ip/$prefix" dev wgtest
ip link set wgtest mtu 1420 up
ip route add "$remote_ip/$prefix" dev wgtest
if [ "$role" = client ]; then
  case "$scenario" in
    tcp-fwmark|forward-fwmark)
      wg set wgtest peer "$peer" allowed-ips 0.0.0.0/0
      ip route add default dev wgtest table 51820
      ip rule add pref 100 not fwmark 51820 table 51820
      ip route get "$WIREHOP_TEST_SERVER" > /results/unmarked-route.txt
      ip route get "$WIREHOP_TEST_SERVER" mark 51820 > /results/marked-route.txt
      ;;
  esac
fi
case "$scenario" in
  tcp-latency) tc qdisc replace dev eth0 root netem delay 600ms ;;
  tcp-loss) tc qdisc replace dev eth0 root netem delay 40ms 10ms loss 0.5% ;;
  tcp-asymmetric|tcp-asymmetric-stall*)
    if [ "${WIREHOP_TEST_ROUTED:-false}" != true ]; then
      tc qdisc replace dev eth0 root handle 1: prio bands 3
      tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 5ms rate 100mbit limit 1000
      tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 150ms rate 20mbit limit 1000
      for field in sport dport; do
        tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip "$field" 51822 0xffff flowid 1:1
        tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip "$field" 51823 0xffff flowid 1:2
      done
    fi
    ;;
  *-slow32|*-slow64|*-slow128)
    rate=${scenario##*slow}
    tc qdisc replace dev eth0 root handle 1: tbf rate "${rate}kbit" burst 4096 latency 1s
    tc qdisc replace dev eth0 parent 1:1 handle 10: netem delay 100ms 20ms loss 0.2% limit 8
    ;;
esac

case "$scenario" in
  tcp-multipath-capacity-change*)
    (
      while [ ! -f /results/flow-start.txt ]; do sleep 0.1; done
      sleep 5
      tc qdisc replace dev eth0 root handle 1: tbf rate 32kbit burst 4096 latency 1s
      tc qdisc replace dev eth0 parent 1:1 handle 10: netem delay 100ms 20ms loss 0.2% limit 8
      date +%s > "/results/$role-rate-dropped.txt"
      sleep 7
      tc qdisc del dev eth0 root
      date +%s > "/results/$role-rate-restored.txt"
    ) &
    ;;
esac

if [ "$role" = server ]; then
  iperf3 --version > /results/iperf-version.txt
  uname -a > /results/kernel.txt
  iperf3 -s > /results/iperf-server.log 2>&1 &
  case "$scheme:$scenario" in
    native:*|forward:*)
      touch /results/ready
      wait
      exit
      ;;
    tls:*|wss:*|*:tcp-mixed)
      openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=wirehop.test \
        -addext subjectAltName=DNS:wirehop.test -keyout /tmp/tls.key -out /results/ca.pem \
        > /results/certificate.log 2>&1
      set -- --tls-cert /results/ca.pem --tls-key /tmp/tls.key
      ;;
    *) set -- --allow-insecure ;;
  esac
  if [ "$scenario" = tcp-mixed ]; then
    set -- "$@" --allow-insecure --listen wss://:51823
  fi
  case "$scenario" in
    tcp-asymmetric|tcp-asymmetric-stall*|tcp-multipath-distinct-slow*)
      set -- "$@" --listen tcp://:51823
      ;;
  esac
  touch /results/ready
  exec /wirehop server --listen "$scheme://:51822" --allow-target "$target" "$@"
fi

duration=20
case "$scenario" in
  *-rekey) duration=140 ;;
  *-outage|*-roam) duration=40 ;;
esac
local_endpoint=127.0.0.1:51821
case "$scenario" in
  tcp-ipv6|wss-ipv6|forward-ipv6) local_endpoint='[::1]:51821' ;;
esac
case "$scheme" in
  native)
    wg set wgtest peer "$peer" endpoint "$WIREHOP_TEST_SERVER:51820"
    ;;
  forward)
    set --
    if [ "$scenario" = forward-fwmark ]; then set -- --fwmark 51820; fi
    /wirehop forward --listen "$local_endpoint" --target "$WIREHOP_TEST_SERVER:51820" "$@" > /results/client.log 2>&1 &
    wg set wgtest peer "$peer" endpoint "$local_endpoint"
    ;;
  *)
    case "$scenario" in
      tcp-prohibit|tcp-blackhole) target=192.0.2.1:51820 ;;
      tcp-ipv6|wss-ipv6) target='[::1]:51820' ;;
      *) target=127.0.0.1:51820 ;;
    esac
    case "$scheme" in
      tls|wss) set -- --tls-server-name wirehop.test ;;
      *) set -- --allow-insecure ;;
    esac
    case "$scenario" in
      tcp-multipath|tcp-multipath-slow32|tcp-multipath-slow64|tcp-multipath-slow128|tcp-multipath-capacity-change*)
        set -- "$@" --lane "tcp://$WIREHOP_TEST_SERVER:51822"
        ;;
    esac
    case "$scenario" in
      tcp-asymmetric|tcp-asymmetric-stall*|tcp-multipath-distinct-slow*)
        if [ "${WIREHOP_TEST_PATH:-Both}" = Both ]; then
          set -- "$@" --lane "tcp://$WIREHOP_TEST_SERVER:51823"
        fi
        ;;
    esac
    if [ "$scenario" = tcp-fwmark ]; then set -- "$@" --fwmark 51820; fi
    if [ "$scenario" = tcp-mixed ]; then
      set -- "$@" --tls-server-name wirehop.test --lane "wss://$WIREHOP_TEST_SERVER:51823"
    fi
    /wirehop client --listen "$local_endpoint" --target "$target" \
      --lane "$scheme://$WIREHOP_TEST_SERVER:$carrier_port" "$@" > /results/client.log 2>&1 &
    wg set wgtest peer "$peer" endpoint "$local_endpoint"
    ;;
esac
sleep 2
if [ "$scenario" = tcp-idle ]; then
  timeout 30 iperf3 -c "$remote_ip" -t 2 -J > /results/warmup.json
  sleep 35
fi
case "$scenario" in
  tcp-asymmetric-stall*)
    (
      sleep 5
      tc qdisc replace dev eth0 parent 1:1 handle 10: netem loss 100%
      sleep 4
      tc qdisc replace dev eth0 parent 1:1 handle 10: netem delay 5ms rate 100mbit limit 1000
      date +%s > /results/path-recovered.txt
    ) &
    ;;
  *-stall|*-outage)
    outage=4
    if [ "${scenario##*-}" = outage ]; then outage=12; fi
    (
      sleep 5
      tc qdisc replace dev eth0 root netem loss 100%
      sleep "$outage"
      tc qdisc del dev eth0 root
      date +%s > /results/path-recovered.txt
    ) &
    ;;
  *-roam)
    (
      sleep 5
      original=$(ip -o -4 addr show dev eth0 | awk '{print $4}')
      prefix=${original##*/}
      address=${original%/*}
      replacement=${address%.*}.250
      ip addr del "$original" dev eth0
      ip addr add "$replacement/$prefix" dev eth0
      ip -o -4 addr show dev eth0 > /results/changed-address.txt
      date +%s > /results/path-recovered.txt
    ) &
    ;;
esac
case "$scenario" in
  forward-prohibit|forward-blackhole)
    (
      sleep 5
      ip route add "${scenario#forward-}" "$WIREHOP_TEST_SERVER/32" table 100
      ip rule add pref 100 to "$WIREHOP_TEST_SERVER/32" lookup 100
      sleep 1
      ip rule del pref 100 to "$WIREHOP_TEST_SERVER/32" lookup 100
      ip route flush table 100
      date +%s > /results/route-recovered.txt
    ) &
    ;;
esac
set --
case "$scenario" in
  *-bidir) set -- --bidir ;;
  *-reverse) set -- -R ;;
esac
case "$scenario" in
  *-udp) set -- -u -b 20M -l 1200 ;;
esac
date +%s > /results/flow-start.txt
nstat -az > /results/client-before.txt
ss -u -a -m > /results/client-sockets.txt
ss -tnH state established > /results/client-tcp-sockets.txt
cat /proc/sys/net/core/rmem_max > /results/receive-buffer-limit.txt
status=0
timeout "$((duration + 60))" iperf3 -c "$remote_ip" -t "$duration" -J "$@" > /results/flow.json || status=$?
nstat -az > /results/client-after.txt
ss -tnH state established > /results/client-tcp-sockets-after.txt
wg show wgtest latest-handshakes > /results/client-handshake.txt
exit "$status"
