# Kernel TCP integration tests

These tests exercise actual Linux WireGuard interfaces and iperf3 TCP connections through the command-line binary. They
require Docker with a Linux kernel supporting WireGuard, plus Go for the result verifier. The containers receive
`NET_ADMIN` inside a disposable internal Docker network. Host interfaces, routes, WireGuard configuration, and sysctls
are not changed. The scripts remove their containers and network on exit and retain the result directory for review.

Build the tool image and a Linux binary matching the Docker engine's architecture:

```sh
docker build -t wirehop-integration test/integration
GOOS=linux GOARCH=arm64 go build -o /tmp/wirehop-linux ./cmd/wirehop
sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-tcp-results
```

Use `GOARCH=amd64` for an x86-64 engine. Each run requires a fresh results directory. To reuse an equivalent tool image,
set `WIREHOP_TEST_IMAGE`. It must provide `sh`, `ip`, `wg`, `tc`, `nstat`, `iperf3`, `openssl`, and `timeout`.

The default matrix includes native WireGuard, forward, all four carrier schemes, two TCP lanes, 1.2-second RTT, 0.5%
loss with delay and jitter, a four-second one-way outage, bidirectional TCP, an idle interval followed by a new burst,
and 140-second TCP flows spanning an actual kernel WireGuard rekey. Each scenario uses fresh endpoints. The ordinary
cases run for 20 seconds. Select a subset by passing case names:

```sh
sh test/integration/run.sh /tmp/wirehop-linux /tmp/wirehop-tcp-smoke tcp-latency tcp-loss tcp-stall
```

The verifier rejects incomplete transfers and zero-byte results. Ordinary single-lane cases also check the server's
accepted TCP connection count to detect unexpected reconnects. The outage case permits reconnection, and the multipath
case permits concurrent admission attempts. Rekey cases require a later kernel handshake while the same iperf3 TCP flow
remains active. Results preserve iperf3 JSON, kernel counters, socket receive limits, handshake timestamps, software
versions, and WireHop diagnostics. Compare goodput, retransmissions, and `UdpRcvbufErrors` across repeated runs on the
same engine. Kernel counters are namespace totals, so `TcpOutRsts` alone cannot identify an outer carrier reset. The
tests use MTU 1420 and fixed public test keys exclusively inside the isolated network.
