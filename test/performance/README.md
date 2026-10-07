# Routed multipath performance tests

This harness compares actual kernel WireGuard inner TCP goodput with each path alone and both paths together. It uses
three disposable containers on one internal Docker bridge. Explicit endpoint routes traverse a forwarding router with
redirects disabled. Impairments apply on the router egress in both directions rather than endpoint egress.

Use the existing [integration tool image](../integration/README.md) and a Linux binary for the engine's architecture:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/wirehop-linux ./cmd/wirehop
sh test/performance/run.sh /tmp/wirehop-linux /tmp/wirehop-performance-results 3
```

The final argument defaults to three repetitions. Use a fresh results directory. `WIREHOP_TEST_IMAGE` selects an already
available equivalent image. The harness never changes host routes or sysctls.

Both profiles use 5 ms one-way delay on port 51822 and 150 ms on port 51823:

| Profile | Low-delay rate | High-delay rate |
| --- | ---: | ---: |
| HighCapacity | 10 Mbit/s | 100 Mbit/s |
| LowCapacity | 100 Mbit/s | 10 Mbit/s |

Each 20-second flow uses fresh endpoints. Path order rotates between repetitions. Result directories preserve iperf3
JSON, carrier sockets before and after the flow, router qdisc counters, kernel version, and endpoint logs. The verifier
requires complete flows, identical configured carrier endpoint pairs in both socket snapshots, exactly two inner iperf3
TCP active opens, nonzero traffic through each selected router qdisc, and no router qdisc drops. Both the combined
median and every combined flow must retain at least 85 percent of the better standalone median in each profile. This
rejects a median that conceals a collapse in one repetition. Retransmissions are reported alongside goodput.

Also compare all six path medians against an untouched baseline to reject regressions shared by every candidate path:

```sh
go run test/performance/verify.go /tmp/wirehop-performance-results 3 /tmp/wirehop-baseline-results
```

Every candidate path must retain at least 85 percent of its baseline median. The baseline may contain router drops from
its previous scheduling behavior. Its flow completion, carrier stability, and router traversal evidence remain required.

The check detects a healthy-path combination that severely underperforms an available individual path. It does not prove
bandwidth aggregation, production WAN optimality, or packet latency. Short flows include startup, and the router's
priority classes, bridge offloads, kernel TCP algorithm, and shared processing affect results. Run candidate binaries
sequentially without competing network or CPU benchmarks.
