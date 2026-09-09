#!/bin/sh
set -eu
# Result files must be readable by the host verifier and artifact uploader.
umask 022

role=$1
scenario=$2
scheme=${scenario%%-*}
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
  touch /results/ready
  exec /wirehop server --listen "$scheme://:51822" --allow-target "$target" "$@"
fi

duration=20
case "$scenario" in
  *-rekey) duration=140 ;;
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
    if [ "$scenario" = tcp-multipath ]; then
      set -- "$@" --lane "tcp://$WIREHOP_TEST_SERVER:51822"
    fi
    if [ "$scenario" = tcp-fwmark ]; then set -- "$@" --fwmark 51820; fi
    if [ "$scenario" = tcp-mixed ]; then
      set -- "$@" --tls-server-name wirehop.test --lane "wss://$WIREHOP_TEST_SERVER:51823"
    fi
    /wirehop client --listen "$local_endpoint" --target "$target" \
      --lane "$scheme://$WIREHOP_TEST_SERVER:51822" "$@" > /results/client.log 2>&1 &
    wg set wgtest peer "$peer" endpoint "$local_endpoint"
    ;;
esac
sleep 2
if [ "$scenario" = tcp-idle ]; then
  timeout 30 iperf3 -c "$remote_ip" -t 2 -J > /results/warmup.json
  sleep 35
fi
if [ "$scenario" = tcp-stall ]; then
  (
    sleep 5
    tc qdisc replace dev eth0 root netem loss 100%
    sleep 4
    tc qdisc del dev eth0 root
  ) &
fi
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
if [ "$scenario" = tcp-bidir ]; then
  set -- --bidir
fi
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
