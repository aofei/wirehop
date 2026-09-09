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

The default matrix contains 29 cases. It includes native WireGuard, forward, all four carrier schemes, two TCP lanes,
1.2-second RTT, 0.5% loss with delay and jitter, a four-second one-way outage, bidirectional TCP, an idle interval
followed by a new burst, and 140-second TCP flows spanning an actual kernel WireGuard rekey. Each scenario uses fresh
endpoints. The ordinary cases run for 20 seconds. Select a subset by passing case names:

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
sockets during concurrent handshake processing and are retained only as supporting evidence. The outage case permits
reconnection. The parallel-lane cases permit concurrent admission attempts but require both configured carriers before
and after the flow, with no additional carrier connection attempts during the flow. Rekey cases require a later kernel
handshake while the same iperf3 TCP flow remains active. Results preserve iperf3 JSON, kernel counters, socket receive
limits, handshake timestamps, software versions, and WireHop diagnostics. Compare goodput, retransmissions, and
`UdpRcvbufErrors` across repeated runs on the same engine. Kernel counters are namespace totals, so `TcpOutRsts` alone
cannot identify an outer carrier reset. The tests use MTU 1420 and fixed public test keys exclusively inside the
isolated network.

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
thirteen representative kernel flow cases. The complete matrix remains available through `run.sh` without case
arguments.
