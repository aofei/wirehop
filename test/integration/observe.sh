#!/bin/sh
set -eu
umask 022

role=$1
results=$2

{
  uname -a
  iperf3 --version
  tc -Version
  for setting in cpu.max cpuset.cpus.effective; do
    if [ -r "/sys/fs/cgroup/$setting" ]; then
      printf '%s=' "$setting"
      cat "/sys/fs/cgroup/$setting"
    fi
  done
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
    /usr/local/bin/wirehop-test-flow now
    ss -toinm state established
    ss -uanm
    cat /proc/net/snmp /proc/net/udp
    if [ -r /proc/net/udp6 ]; then cat /proc/net/udp6; fi
    cat /proc/net/dev
    cat /proc/stat
    if [ -r /proc/pressure/cpu ]; then cat /proc/pressure/cpu; fi
    if [ -r /sys/fs/cgroup/cpu.stat ]; then cat /sys/fs/cgroup/cpu.stat; fi
    tc -s qdisc show dev eth0
  } >> "$results/$role-observations.txt"
  sleep 0.25
done
touch "$results/$role-observations-done"
