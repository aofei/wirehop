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

The default matrix includes native WireGuard, forward, all four carrier schemes, two TCP lanes, 1.2-second RTT, 0.5%
loss with delay and jitter, four-second and twelve-second one-way outages, client address replacement, 32, 64, and 128
kbit/s carrier links, bidirectional TCP, an idle interval followed by a new burst, and 140-second TCP flows spanning an
actual kernel WireGuard rekey. Each scenario uses fresh endpoints.

Ordinary cases run for 20 seconds. Capacity-change, twelve-second outage, and address-replacement cases run for 40
seconds and require transfer progress after recovery. Address replacement changes the container's carrier-facing
address, forcing recovery from an obsolete TCP connection.

The rate-limited cases add 100 ms one-way delay, 20 ms jitter, and 0.2% loss with an eight-packet netem queue. They test
continued connectivity at low capacity, not comparative mobile throughput or battery consumption.

Configured random loss and queue overflow are separate. Overflow can substantially increase loss and create receive gaps
beyond the carrier's liveness budget. Inner TCP can also remain in retransmission backoff while the carriers stay
connected. Inspect the saved queue counters, socket identities, and endpoint logs when a run fails.

The `tcp-asymmetric` case uses distinct TCP path groups with 5 ms one-way delay at 100 Mbit/s and 150 ms one-way delay
at 20 Mbit/s. `tcp-asymmetric-stall` additionally interrupts only the faster path for four seconds. Its `-reverse` and
`-bidir` variants exercise the opposite direction and simultaneous bidirectional traffic. Each exercised direction must
recover at least 25 percent of its measured pre-fault throughput during the final five seconds. These cases check
completed delivery and substantial recovery with unequal paths. They do not guarantee uninterrupted inner TCP progress
during a carrier stall.

The `tcp-multipath-slow32`, `tcp-multipath-slow64`, and `tcp-multipath-slow128` cases keep two TCP lanes behind the same
32, 64, or 128 kbit/s endpoint-egress bottleneck. Both carriers must remain connected before and after the transfer,
without additional carrier connection attempts during the flow. These cases check that provisional capacity estimates do
not cause healthy low-rate generations to be repeatedly abandoned.

The `tcp-multipath-distinct-slow32`, `tcp-multipath-distinct-slow64`, and `tcp-multipath-distinct-slow128` cases use
distinct TCP destination ports behind those same shared low-rate bottlenecks. This enables capacity discovery and checks
that its padding neither prevents completed useful transfers nor causes extra carrier connections. Both distinct
carriers must remain connected before and after each flow.

The `tcp-multipath-capacity-change` case offers 20 Mbit/s with 16 KiB application writes. Both endpoint links have a 1
Gbit/s ceiling, drop to 32 kbit/s five seconds into the flow, and return to 1 Gbit/s seven seconds later. The TBF and
netem queues remain installed across both changes, so restoring capacity does not discard their pending packets. The
netem limit is 10000 packets outside the fault and eight packets during it. This allows queued retransmissions and
catch-up traffic to drain without adding queue overflow after restoration. Each exercised direction must deliver at
least 5 Mbit/s before the fault and recover at least 25 percent of its own pre-fault receiver throughput during the
final five seconds. The verifier checks both endpoints' rate-change timestamps, each phase's queue settings, cumulative
counter continuity, and the absence of new queue drops after restoration. Both same-group lanes must be connected before
and after the flow. Reconnection is permitted during this injected fault because previously measured capacity and
accumulated TCP backlog can make the old generation unusable. Healthy low-rate startup cases retain their stricter
no-reconnection check. The capacity-change case's `-reverse` and `-bidir` variants apply the same fault to
opposite-direction and simultaneous bidirectional traffic. Each exercised direction must meet the recovery threshold
using its own pre-fault rate.

The `native-capacity-change`, `native-capacity-change-reverse`, and `native-capacity-change-bidir` cases apply the same
offered load, fault, and recovery threshold directly to native WireGuard. An unrestricted pre-fault workload can fill a
large native TCP congestion window before the 32 kbit/s queue drops hundreds of packets. Retransmission timers can then
postpone useful recovery beyond a 40-second flow even after capacity returns. The controlled offered load and smaller
application writes bound the workload independently of runner speed. Preserving the queues also avoids an additional
packet purge when the fault ends. The original impairment settings and 40-second recovery deadline remain unchanged.
Both directions use receiver intervals, so buffered sender writes cannot satisfy the recovery check. These cases
automatically retain TCP timer, socket, and qdisc samples throughout the flow, plus queue counters immediately before
and after restoring capacity. The native controls help distinguish inner TCP recovery limits from relay-specific
regressions.

The `tcp-rtt-step` and `tcp-multipath-rtt-step` cases keep a 20 Mbit/s bottleneck throughout a 60-second transfer. Both
endpoint delays rise from 5 to 600 milliseconds after ten seconds and return to 5 milliseconds thirty seconds later. The
multipath case uses two lanes in one path group. Their `-reverse` and `-bidir` variants exercise the reverse direction
and simultaneous bidirectional traffic. Receiver interval data in each direction must show at least 5 Mbit/s before the
change and retain at least 25 percent of its own baseline during the settled high-delay and restored phases. Both
endpoints record the actual change times. Every phase must exercise the configured queue without drops, and the carrier
connections must remain unchanged. Sender-side buffered bytes do not satisfy the delivery checks.

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
carrier endpoint pairs to remain identical before and after the flow, with no additional carrier connection attempts
during the flow. The asymmetric stall case requires both carriers before fault injection and permits recovery through a
new connection. Rekey cases require a later kernel handshake while the same iperf3 TCP flow remains active. Results
preserve iperf3 JSON, kernel counters, endpoint qdisc settings and counters, socket receive limits, handshake
timestamps, software versions, and WireHop diagnostics. Compare goodput, retransmissions, and `UdpRcvbufErrors` across
repeated runs on the same engine. Kernel counters are namespace totals, so `TcpOutRsts` alone cannot identify an outer
carrier reset. The tests use MTU 1420 and fixed public test keys exclusively inside the isolated network.

These cases establish connectivity and recovery under the specified faults. Their endpoint-egress netem settings and
short flows do not establish comparative WAN throughput or multipath capacity aggregation. For performance comparisons,
use [the routed performance harness](../performance/README.md), or apply impairments on an intermediate router or
receiver ingress and verify traffic through the configured queues. Run each candidate separately before testing the
combined lanes. Keep workloads sequential on the same engine and record per-path traffic, retransmissions, and latency
along with goodput. See the [netem limitations](https://man7.org/linux/man-pages/man8/tc-netem.8.html#LIMITATIONS) for
the effect of TCP Small Queues on sender-side emulation.

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
representative kernel flow cases, including directional multipath recovery, low-rate TCP/WSS and multipath,
shared-capacity changes with simultaneous bidirectional traffic, changing RTT, a twelve-second interruption, and address
replacement. The selected cases are listed in [the test workflow](../../.github/workflows/test.yaml). The complete
matrix remains available through `run.sh` without case arguments.
