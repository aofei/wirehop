# Kernel WireGuard integration tests

These tests exercise actual Linux WireGuard interfaces and iperf3 TCP and UDP traffic through the command-line binary.
They require Docker with a Linux kernel supporting WireGuard, plus Go for the result verifier. The containers receive
`NET_ADMIN` inside a disposable internal Docker network. Host interfaces, routes, WireGuard configuration, and sysctls
are not changed. The scripts remove their containers and network on exit and retain the result directory for review.

Result files are readable by the host verifier and artifact uploader even when containers run as root. Private keys stay
under the container's temporary directory with restricted permissions.

Build the tool image and a Linux binary matching the Docker engine's architecture:

```sh
docker build -t wirehop-integration test/integration
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/wirehop-linux ./cmd/wirehop
sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-kernel-results
```

Use `GOARCH=amd64` for an x86-64 engine. Each run requires a fresh results directory. To reuse an equivalent tool image,
set `WIREHOP_TEST_IMAGE`. It must provide `sh`, `ip`, `wg`, `tc`, `nstat`, `iperf3`, `openssl`, and `timeout`.

The default matrix contains 62 cases. It includes native WireGuard, forward, all four carrier schemes, two TCP lanes,
1.2-second RTT, 0.5% loss with delay and jitter, four-second and twelve-second one-way outages, client address
replacement, 32, 64, and 128 kbit/s carrier links, bidirectional TCP, an idle interval followed by a new burst, and
140-second TCP flows spanning an actual kernel WireGuard rekey. Each scenario uses fresh endpoints. Ordinary cases run
for 20 seconds. Twelve-second outage and address-replacement cases run for 40 seconds and require transfer progress
after recovery. Address replacement changes the container's carrier-facing address, forcing recovery from an obsolete
TCP connection. The rate-limited cases add 100 ms one-way delay, 20 ms jitter, and 0.2% loss with an eight-packet netem
queue. They test continued connectivity at low capacity, not comparative mobile throughput or battery consumption. The
`tcp-asymmetric` case uses distinct TCP path groups with 5 ms one-way delay at 100 Mbit/s and 150 ms one-way delay at 20
Mbit/s. `tcp-asymmetric-stall` additionally interrupts only the faster path for four seconds. Its `-reverse` and
`-bidir` variants exercise the opposite direction and simultaneous bidirectional traffic. Each exercised direction must
recover at least 25 percent of its measured pre-fault throughput during the final five seconds. These cases check
completed delivery and substantial recovery with unequal paths. They do not guarantee uninterrupted inner TCP progress
during a carrier stall.

The `tcp-multipath-slow32`, `tcp-multipath-slow64`, and `tcp-multipath-slow128` cases keep two TCP lanes behind the same
32, 64, or 128 kbit/s endpoint-egress bottleneck. Both carriers must remain connected before and after the transfer,
without additional carrier connection attempts during the flow. These cases check that provisional capacity estimates do
not cause healthy low-rate generations to be repeatedly abandoned.

The `tcp-multipath-capacity-change` case starts both same-group lanes at unrestricted capacity, limits both endpoint
links to 32 kbit/s five seconds into the flow, and restores capacity seven seconds later. Both endpoints must record
successful rate changes, both lanes must be connected before and after the flow, and final-window throughput must
recover at least 25 percent of its own pre-fault throughput. Reconnection is permitted during this injected fault
because previously measured capacity and accumulated TCP backlog can make the old generation unusable. Healthy low-rate
startup cases retain their stricter no-reconnection check. The capacity-change case's `-reverse` and `-bidir` variants
apply the same fault to opposite-direction and simultaneous bidirectional traffic. Each exercised direction must meet
the recovery threshold using its own pre-fault rate.

Select a subset by passing case names:

```sh
sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-kernel-smoke tcp-latency tcp-loss tcp-stall
```

The `tcp-ipv6`, `wss-ipv6`, and `forward-ipv6` cases use IPv6 carrier or forwarding destinations and IPv6 local UDP
listeners. Their Docker network enables IPv6 explicitly. `tcp-inner-ipv6` carries IPv6 TCP through WireGuard, while
`tcp-mixed` keeps TCP and WSS lanes in one session. `tcp-fwmark` and `forward-fwmark` install a policy route that sends
unmarked traffic into WireGuard. The marked relay traffic must bypass it and complete the transfer. Results retain both
route lookups so a non-capturing route cannot accidentally make the exclusion test pass.

The `forward-prohibit`, `forward-blackhole`, `tcp-prohibit`, and `tcp-blackhole` cases install a rejecting route for one
second during an active TCP transfer. Forward cases change the forwarder's target route. TCP cases change the server's
target route. The transfer must resume after the route is restored, and relay cases must retain their original carrier.
The `native-udp`, `forward-udp`, `tcp-udp`, and `wss-udp` cases send 1200-byte UDP datagrams at 20 Mbit/s inside
WireGuard. At least 95% of the offered bytes must arrive. Iperf3 still uses one TCP control connection in UDP mode.

The verifier rejects incomplete transfers and zero-byte results. Ordinary single-lane cases also check the client's TCP
active-open count to detect unexpected reconnect attempts. Server passive-open counts can include discarded child
sockets during concurrent handshake processing and are retained only as supporting evidence. Outage and address-change
cases permit reconnection. Healthy parallel-lane cases permit concurrent admission attempts but require both configured
carriers before and after the flow, with no additional carrier connection attempts during the flow. The asymmetric stall
case requires both carriers before fault injection and permits recovery through a new connection. Rekey cases require a
later kernel handshake while the same iperf3 TCP flow remains active. Results preserve iperf3 JSON, kernel counters,
socket receive limits, handshake timestamps, software versions, and WireHop diagnostics. Compare goodput,
retransmissions, and `UdpRcvbufErrors` across repeated runs on the same engine. Kernel counters are namespace totals, so
`TcpOutRsts` alone cannot identify an outer carrier reset. The tests use MTU 1420 and fixed public test keys exclusively
inside the isolated network.

These cases establish connectivity and recovery under the specified faults. Their endpoint-egress netem settings and
short flows do not establish comparative WAN throughput or multipath capacity aggregation. For performance comparisons,
apply impairments on an intermediate router or receiver ingress, verify that traffic traverses the configured queues,
and run each candidate separately before testing the combined lanes. Keep workloads sequential on the same engine and
record per-path traffic, retransmissions, and latency along with goodput. See the
[netem limitations](https://man7.org/linux/man-pages/man8/tc-netem.8.html#LIMITATIONS) for the effect of TCP Small
Queues on sender-side emulation.

An additional opt-in Go test exercises actual Linux `prohibit`, `blackhole`, and `unreachable` routes against both UDP
endpoint implementations. It verifies delivery through the same endpoint before and after every fault. Batch tests
inject `EINTR` around actual UDP syscalls, including interruption after a partial write, and check for lost or
duplicated datagrams. Run these tests only inside a disposable network namespace:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -o /tmp/wirehop-datagram.test ./internal/datagram
docker run --rm --network none --read-only --cap-add NET_ADMIN \
  -e WIREHOP_TEST_ROUTES=1 -v /tmp/wirehop-datagram.test:/datagram.test:ro \
  wirehop-integration /datagram.test \
  -test.run 'TestUDPRouteRecovery|TestLinuxUDPBatch.*Interrupted' -test.v
```

The route test is skipped during ordinary `go test` runs. CI runs the socket recovery tests in isolation and selects
twenty-three representative kernel flow cases, including directional multipath recovery, low-rate TCP/WSS and multipath,
shared-capacity changes with simultaneous bidirectional traffic, a twelve-second interruption, and address replacement.
The complete matrix remains available through `run.sh` without case arguments.
