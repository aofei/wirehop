# Kernel WireGuard integration tests

These tests exercise actual Linux WireGuard interfaces and iperf3 TCP and UDP traffic through the command-line binary.
They require Docker with a Linux kernel supporting WireGuard, plus Go for the result verifier. The containers receive
`NET_ADMIN` inside a disposable internal Docker network. Host interfaces, routes, WireGuard configuration, and sysctls
are not changed. The scripts remove their containers and network on exit and retain the result directory for review.

Result files are readable by the host verifier and artifact uploader even when containers run as root. Private keys stay
under the container's temporary directory with restricted permissions.

Build the tool image and a Linux binary matching the Docker engine's architecture:

```sh
docker build -f test/integration/Dockerfile -t wirehop-integration .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/wirehop-linux ./cmd/wirehop
sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-kernel-results
```

Use `GOARCH=amd64` for an x86-64 engine. The results directory must not already exist. To reuse an equivalent tool
image, set `WIREHOP_TEST_IMAGE`. It must provide `sh`, `ip`, `wg`, `tc`, `nstat`, `openssl`, `timeout`, iperf3 with JSON
streaming that retains positive-duration zero-delivery intervals and uses each direction's own interval timing, a
60-second control-read budget, and a producer-recorded `started_boottime` measurement origin, plus the
`wirehop-test-flow` collector built from this repository. The provided image builds iperf3 3.18 from a checksum-pinned
source archive and identifies its adaptations as `3.18-wirehop-test`. It also pins the Go builder and Debian base image
digests. Tool versions and the resulting image identity are retained with each run.

The collector reads flushed iperf3 JSON events. Recovery checks use 200 ms reporting intervals, while ordinary cases
retain one-second intervals. The test build makes iperf3 use Linux `CLOCK_BOOTTIME` for its measurement clock and emits
the same timestamp used to initialize its interval origin in the `start` event. The collector uses this producer value
for scheduling and verification, independently of the control-connection wall-clock timestamp and event delivery time.
Missing, nonpositive, or future producer timestamps are rejected. Warmup omission is not used by these workloads and is
rejected because it resets the interval origin after the start event.

Successful saved results must retain the original producer origin in their `start` metadata and match it at the result
root. The verifier rejects missing or inconsistent provenance instead of treating an old collector receipt time as a
producer timestamp. Results from producers that failed before initialization retain their original failure message.

For fault scenarios, both endpoints retain their own measurement start, complete endpoint result, and event trace.
`flow.json` contains the client result and `server-flow.json` contains the server result for every fault scenario. Fault
tasks wait for the producer's start marker and use boot-clock deadlines. Their markers record completion of the actual
network changes. The traces separately retain event receipt times, so collector scheduling delays remain observable.
Delayed delivery does not move the measurement origin. A missed injection deadline fails timing validation.
`CLOCK_BOOTTIME` includes system suspend. The wall-clock `flow-start.txt` marker is derived from the producer origin and
the current wall clock, with whole-second precision, and is used only for kernel rekey validation.

The default matrix includes native WireGuard, forward, all four carrier schemes, two TCP lanes, 1.2-second RTT, 0.5%
loss with delay and jitter, four-second and twelve-second one-way outages, client address replacement, 32, 64, and 128
kbit/s carrier links, bidirectional TCP, an idle interval followed by a new burst, and 140-second TCP flows spanning an
actual kernel WireGuard rekey. Each scenario uses fresh endpoints.

Ordinary cases run for 20 seconds. Capacity-change, twelve-second outage, and address-replacement cases run for 40
seconds and require transfer progress after recovery. Address replacement changes the container's carrier-facing
address, forcing recovery from an obsolete TCP connection.

Ordinary interruption, address-change, and route-failure checks require positive receiver delivery during the complete
final second of the prescribed flow. They use the same overlap integration and coverage validation as recovery gates.
Sender buffering cannot satisfy this check. A final partial reporting interval with zero bytes does not conceal delivery
earlier in that same final second. These progress checks have no baseline fraction or percentage threshold.

The rate-limited cases add 100 ms one-way delay, 20 ms jitter, and 0.2% loss with an eight-packet netem queue. They test
continued connectivity at low capacity, not comparative mobile throughput or battery consumption.

These cases use 16 KiB application blocks. A default 128 KiB block exceeds the entire 80,000-byte payload budget of a
20-second flow at 32 kbit/s, even before protocol overhead and loss. Iperf3 3.18 updates its TCP byte counters only
after a block read or write returns. An unfinished block can therefore produce a zero-byte result despite kernel TCP
delivery. Smaller blocks make useful delivery observable within the prescribed flow without changing the bottleneck,
fault settings, or carrier stability assertions.

Configured random loss and queue overflow are separate. Overflow can substantially increase loss and create receive gaps
beyond the carrier's liveness budget. Inner TCP can also remain in retransmission backoff while the carriers stay
connected. Inspect the saved queue counters, socket identities, and endpoint logs when a run fails.

Iperf3's control protocol is another constraint at very low rates. Stock
[3.18 control reads](https://github.com/esnet/iperf/blob/3.18/src/net.c#L352) use a ten-second readiness timeout,
whereas [3.12 control reads](https://github.com/esnet/iperf/blob/3.12/src/net.c#L349) did not impose this timeout. A
result-exchange failure is still a failed experiment even if some payload arrived. The test build uses 60 seconds for
both control-read budgets, matching the harness's existing result-collection allowance. The outer flow timeout still
bounds the whole experiment. Data transfer duration, offered load, impairment settings, receiver windows, and carrier
assertions are unchanged. A delayed-cookie regression fails with the stock ten-second budget and passes with this build.
It observes consumption of the partial cookie and the server's wait state before withholding the final byte. A separate
test pauses either collector for three seconds while iperf3 continues, then checks that the producer origin and event
deadline remain correct. A final-interval regression checks that JSON streaming retains short zero-delivery intervals
through the local measurement end. The test build keeps these positive-duration intervals even when they are shorter
than ten percent of the reporting period. The test build also takes each direction's sum timing from a stream in that
direction. Stock 3.18 instead uses the first stream for both bidirectional sums, despite collecting each stream's
statistics at different times. A regression compares sums with their corresponding stream in forward, reverse, and
bidirectional transfers at both endpoints. CI runs these tool regressions before the kernel matrix. Comparing streamed
and plain output at the same version does not isolate the effect of upgrading iperf3. Retain the version and control
errors when assessing low-rate failures.

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

The `native-slow32` case applies the same low-rate workload and impairments directly to native WireGuard. It provides a
control for TCP recovery, byte-accounting granularity, and iperf3 result exchange under this bottleneck.

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

The `native-rtt-step`, `tcp-rtt-step`, and `tcp-multipath-rtt-step` cases keep a 20 Mbit/s bottleneck throughout a
60-second transfer. Both endpoint delays rise from 5 to 600 milliseconds after ten seconds and return to 5 milliseconds
thirty seconds later. The multipath case uses two lanes in one path group. Their `-reverse` and `-bidir` variants
exercise the reverse direction and simultaneous bidirectional traffic. Receiver interval data in each direction must
show at least 5 Mbit/s before the change and retain at least 25 percent of its own baseline during the settled
high-delay and restored phases. Both endpoints record the actual change times. Every phase must exercise the configured
queue without drops, and the carrier connections must remain unchanged. Sender-side buffered bytes do not satisfy the
delivery checks.

Recovery throughput gates select the receiving endpoint in each direction. All window boundaries use the client's
producer-recorded measurement origin, matching fault scheduling and the prescribed flow deadline. Receiver intervals are
translated by the difference between the endpoint origins before integration. Sharing a boot clock does not mean that
both endpoints initialize their measurements simultaneously. A control-message or scheduling delay must not move the
receiving endpoint's final window earlier or later in the experiment.

If the receiver initializes later, delivery before its producer origin is known to be zero because iperf3 initializes
its counters before starting the data workers. Only that pre-initialization prefix is supplied as zero evidence. Gaps
after initialization remain missing evidence and fail coverage validation. The initial baseline includes actual startup
delays within its prescribed client-time window.

Window bytes are estimated from each interval's overlap with the requested window, using a constant rate within that
interval. The denominator is the full window duration. This avoids discarding a reporting interval because it ends
slightly beyond a boundary. Missing window coverage, overlaps, unknown directions, and incomplete windows are rejected.
Coverage permits at most one serialized microsecond of timestamp rounding across a window. Recovery-fraction and
minimum-baseline comparisons allow a normalized ratio roundoff of `1e-12` to avoid rejecting exact boundary values
because of float64 summation. A complete zero-byte interval remains evidence of zero delivery. Recovery uses exactly the
final five seconds of the prescribed flow, with a four-second pre-fault baseline. RTT-step windows follow the observed
changes and keep their original 60-second deadline. The 25 percent threshold detects substantial recovery and is not a
statistical calibration of GitHub-hosted runner performance.

Each verified case writes `verification.json` and prints every measured direction and phase, including failures. The
report contains window boundaries, covered time, estimated received bytes, receiver bitrate, baseline fraction when
defined, and status. Independent throughput and carrier assertions are all reported. The runner retains container exit
codes and continues through the remaining cases and repetitions after a completed case fails verification.

Invalid baselines invalidate subsequent recovery comparisons. Fault timing checks also reject reversed events and
incorrect fault durations. Reverse-only checks still require a completed server result. Malformed client JSON does not
hide independent carrier assertions. The collector rejects missing event payloads and preserves subprocess failures even
when the producer emitted an otherwise complete result.

Before removing endpoints, the runner saves their logs, Docker state, process lists, and the server's TCP sockets. Early
startup failures use the same capture path. A node also records its addresses, routes, TCP and UDP sockets, WireGuard
state, and ephemeral-port settings before a failing shell exits, preserving evidence that disappears with its network
namespace. Diagnostic write failures do not prevent container and network cleanup. Collection failures are retained in
`harness-error.txt` and make verification fail.

Fault cases with separate server results keep the client's relay running for up to ten seconds after its producer exits.
The server can still need the relay for final control messages and TCP acknowledgments. A missing completion marker or
failed server producer fails the case. This collection allowance does not extend the traffic duration or the receiver
measurement windows.

Read process exit codes and verification errors together. An invalid or incomplete experiment is different from a
complete receiver window below the recovery threshold. When reviewing GitHub history, count executed kernel jobs
separately from workflows that never ran because their approval expired.

Select a subset by passing case names:

```sh
sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-kernel-smoke tcp-latency tcp-loss tcp-stall
```

Repeat a fixed executable with native controls and fresh endpoints:

```sh
WIREHOP_TEST_REPETITIONS=3 sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-recovery-results \
  native-rtt-step-bidir tcp-rtt-step-bidir tcp-multipath-rtt-step-bidir native-slow32 tcp-multipath-distinct-slow32
```

`WIREHOP_TEST_REPETITIONS` defaults to one and accepts positive decimal integers without leading zeros. Repeated runs
use `repetition-N/CASE` directories. Every repetition must pass. Results are not discarded when a later repetition
passes. Three runs provide regression evidence, not an estimate of the runner's false-positive probability.

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

Fault, changing-RTT, and low-rate cases automatically observe both endpoints from startup through flow completion. The
250 ms samples retain TCP timers, socket identities and memory, qdisc counters, container CPU use, VM CPU counters, and
available CPU pressure data. Environment files include CPU quotas, TCP settings, and tool versions. `runner.txt` records
the GitHub run and commit, executable build metadata, runner image version, Docker engine resources, kernel, Go version,
and test image identity. Observations consume CPU and must be enabled consistently when comparing binaries. A carrier
identity change is a stability assertion failure. Its cause still requires the socket, queue, and timing evidence. Short
transient reconnects can fall inside the product's initial quiet logging period.

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
replacement. The kernel job uses an explicit Ubuntu version. It repeats the bidirectional RTT cases, their native
control, native low-rate traffic, and distinct-port low-rate carriers three times on the same runner. The selected cases
are listed in [the test workflow](../../.github/workflows/test.yaml). The complete matrix remains available through
`run.sh` without case arguments.
