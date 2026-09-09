#!/bin/sh
set -eu
umask 077

role=$1
scenario=$2
scheme=${scenario%%-*}
export WIREHOP_TOKEN=isolated-kernel-tcp-test-fixture
export SSL_CERT_FILE=/results/ca.pem
ip link add wgtest type wireguard
if [ "$role" = server ]; then
  printf '%s\n' 'AD7V1ztVgGww3j+Ke9qzivE1OSIFMwVeY1aQuLh61kE=' > /tmp/private.key
  peer='+SjU9sG4bBLyViwQsHxVXFxX/QD1npDI2NiHZyccv3w='
  local_ip=10.253.91.2
  remote_ip=10.253.91.1
else
  printf '%s\n' 'CH7G4Uu+0hDnIVzcc0aN+iPwgKG/uGZbL9gJvZnSg3k=' > /tmp/private.key
  peer='xMjphMUyLIGExyJluSslD9tjaIcF9QS6ADyI8DOTzyg='
  local_ip=10.253.91.1
  remote_ip=10.253.91.2
fi
wg set wgtest private-key /tmp/private.key listen-port 51820 peer "$peer" allowed-ips "$remote_ip/32"
ip addr add "$local_ip/32" dev wgtest
ip link set wgtest mtu 1420 up
ip route add "$remote_ip/32" dev wgtest
case "$scenario" in
  tcp-latency) tc qdisc replace dev eth0 root netem delay 600ms ;;
  tcp-loss) tc qdisc replace dev eth0 root netem delay 40ms 10ms loss 0.5% ;;
esac

if [ "$role" = server ]; then
  iperf3 --version > /results/iperf-version.txt
  uname -a > /results/kernel.txt
  iperf3 -s > /results/iperf-server.log 2>&1 &
  case "$scheme" in
    native|forward)
      touch /results/ready
      wait
      exit
      ;;
    tls|wss)
      openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=wirehop.test \
        -addext subjectAltName=DNS:wirehop.test -keyout /tmp/tls.key -out /results/ca.pem \
        > /results/certificate.log 2>&1
      set -- --tls-cert /results/ca.pem --tls-key /tmp/tls.key
      ;;
    *) set -- --allow-insecure ;;
  esac
  touch /results/ready
  exec /wirehop server --listen "$scheme://:51822" --allow-target 127.0.0.1:51820 "$@"
fi

duration=20
case "$scenario" in
  *-rekey) duration=140 ;;
esac
case "$scheme" in
  native)
    wg set wgtest peer "$peer" endpoint "$WIREHOP_TEST_SERVER:51820"
    ;;
  forward)
    /wirehop forward --listen 127.0.0.1:51821 --target "$WIREHOP_TEST_SERVER:51820" > /results/client.log 2>&1 &
    wg set wgtest peer "$peer" endpoint 127.0.0.1:51821
    ;;
  *)
    case "$scheme" in
      tls|wss) set -- --tls-server-name wirehop.test ;;
      *) set -- --allow-insecure ;;
    esac
    if [ "$scenario" = tcp-multipath ]; then
      set -- "$@" --lane "tcp://$WIREHOP_TEST_SERVER:51822"
    fi
    /wirehop client --listen 127.0.0.1:51821 --target 127.0.0.1:51820 \
      --lane "$scheme://$WIREHOP_TEST_SERVER:51822" "$@" > /results/client.log 2>&1 &
    wg set wgtest peer "$peer" endpoint 127.0.0.1:51821
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
set --
if [ "$scenario" = tcp-bidir ]; then
  set -- --bidir
fi
date +%s > /results/flow-start.txt
nstat -az > /results/client-before.txt
ss -u -a -m > /results/client-sockets.txt
cat /proc/sys/net/core/rmem_max > /results/receive-buffer-limit.txt
status=0
timeout "$((duration + 60))" iperf3 -c "$remote_ip" -t "$duration" -J "$@" > /results/flow.json || status=$?
nstat -az > /results/client-after.txt
wg show wgtest latest-handshakes > /results/client-handshake.txt
exit "$status"
