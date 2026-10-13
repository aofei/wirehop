#!/bin/sh
set -eu
umask 022

role=$1
results=$2

while [ ! -f "$results/flow-start.txt" ]; do sleep 0.1; done

{
  uname -a
  for setting in net/core/rmem_max net/core/wmem_max net/ipv4/tcp_rmem net/ipv4/tcp_wmem \
    net/ipv4/tcp_congestion_control net/ipv4/tcp_moderate_rcvbuf; do
    printf '%s=' "$setting"
    cat "/proc/sys/$setting"
  done
  ip -details link show
  tc -s qdisc show dev eth0
} > "$results/$role-environment.txt"

while [ ! -f "$results/flow-done.txt" ]; do
  {
    date +%s.%N
    ss -tinm state established
    cat /proc/net/dev
    if [ -r /sys/fs/cgroup/cpu.stat ]; then cat /sys/fs/cgroup/cpu.stat; fi
    if [ "$role" = router ]; then tc -s qdisc show dev eth0; fi
  } >> "$results/$role-observations.txt"
  sleep 0.25
done
touch "$results/$role-observations-done"
