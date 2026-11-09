# Routed multipath performance tests

This harness compares actual kernel WireGuard inner TCP goodput with each path alone and both paths together. It uses
three disposable containers on one internal Docker bridge. Explicit endpoint routes traverse a forwarding router with
redirects disabled. Impairments apply on the router egress in both directions rather than endpoint egress.

The host needs Go, Docker, and either `sha256sum` or `shasum`. Use the existing
[integration tool image](../integration/README.md) and a Linux binary for the engine's architecture:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/wirehop-linux ./cmd/wirehop
sh test/performance/run.sh /tmp/wirehop-linux /tmp/wirehop-performance-results 3
```

The final argument defaults to three repetitions. Numeric arguments use decimal notation without leading zeros.
Repetitions must fit a positive shell integer. Use a fresh results directory. `WIREHOP_TEST_IMAGE` selects an already
available equivalent image. The harness never changes host routes or sysctls.

Both profiles use 5 ms one-way delay on port 51822 and 150 ms on port 51823:

| Profile | Low-delay rate | High-delay rate |
| --- | ---: | ---: |
| HighCapacity | 10 Mbit/s | 100 Mbit/s |
| LowCapacity | 100 Mbit/s | 10 Mbit/s |

Each netem queue permits 8192 packets, including delayed data and return traffic. At 100 Mbit/s, 150 ms of one-way
propagation already requires about 1.875 MB, or roughly 1300 full-sized packets, before accounting for bursts and
feedback. A smaller packet limit can introduce unintended loss into this healthy-path comparison. Segmentation offloads
also change the relationship between packet count and bytes. The verifier still rejects every router drop.

Both directions of one carrier port use the same router egress queue and share its configured aggregate rate. This
models a shared bottleneck. It differs from assigning an independent full-rate queue to each direction. Native controls
must state which model they use and preserve queue-drop and latency evidence under the same model.

Each 20-second flow uses fresh endpoints. Path order rotates between repetitions. Result directories preserve iperf3
JSON, carrier sockets before and after the flow, router qdisc counters, kernel version, and endpoint logs. The verifier
requires complete flows, identical configured carrier endpoint pairs in both socket snapshots, exactly two inner iperf3
TCP active opens, nonzero traffic through each selected router qdisc, and no router qdisc drops. Both the combined
median and every combined flow must retain at least 85 percent of the better standalone median in each profile. This
rejects a median that conceals a collapse in one repetition. Retransmissions are reported alongside goodput. The summary
also identifies the weakest combined repetition, its goodput, and its ratio to the best standalone median. Independent
profiles continue verification after a failure so one failed profile cannot conceal another profile's evidence. Every
collected repetition is checked, including repetitions after invalid evidence. Incomplete evidence prevents a median
comparison. When a baseline is supplied, all six baseline comparisons also run and retain their failures.

Goodput is calculated from receiver bytes and receiver duration rather than the redundant `bits_per_second` field. The
recorded `start.test_start` must identify one forward TCP stream without omitted startup, byte or block limits, or rate
caps. Both sender and receiver durations must reach the requested duration, with a one-second completion allowance.
Receiver drain time remains part of the denominator. Requested durations and block sizes must match across paths and
repetitions within each profile and across each candidate/baseline comparison. This prevents a truncated long flow or a
comparison with different startup amortization from producing a valid ratio. Environment equivalence still requires
reviewing runner and observation settings.

Early endpoint, observation, and counter-collection failures retain Docker logs, state, process lists, and available
socket and router counters before cleanup. A `harness-error.txt` marker prevents otherwise complete flow totals from
passing verification after a collection failure. Client process exit status is saved separately. Capture errors retain
the original failure status and do not prevent container and network removal. Additional snapshots use separate
`failure` filenames, preserving evidence already saved before a later collection error.

The root `runner.txt` records host and Go versions, executable build metadata and SHA-256, GitHub run and image
identifiers, Docker engine resources, the tool image identity, and the host CPU description when available. It is
collected before creating the test network, so early setup failures retain their environment evidence.

Each created container has a Docker-written `.cid` file. Diagnostics and removal use its recorded ID. The root
`network.id` identifies the owned bridge. A name collision preserves the existing resource, and a container created
before a failed start remains eligible for cleanup.

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

A failed short-flow ratio establishes the measured shortfall. It does not establish which component caused it. Inspect
both inner and outer TCP congestion windows, slow-start thresholds, retransmissions, queue residence, and CPU
observations. Kernel slow-start delay detection can react to scheduling or emulation jitter before a path reaches its
available rate. [HyStart++](https://www.rfc-editor.org/rfc/rfc9406) describes this premature-exit problem and a kernel
transport-level mitigation. WireHop cannot change the congestion controller of an encrypted inner connection.

For a native WireGuard control, match the rates, delays, CPU allocation, duration, and observation load, and preserve
the control separately from the candidate verdict. Check that netem has enough packet capacity for the delayed traffic
and its bursts. An identical packet-count limit does not imply an identical byte allowance when TCP segmentation
offloads differ from native UDP packetization. A control with queue drops cannot establish loss-free startup behavior.
Retain all predefined repetitions, including slow native controls. Do not discard outliers or rerun a failed candidate
until it passes. The 85 percent gates remain strict measurements, rather than a calibrated probability of regression.

For longer runs with outer TCP, queue, CPU, and inner UDP latency observations:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/wirehop-latency ./test/performance/latency
WIREHOP_TEST_DURATION=60 WIREHOP_TEST_OBSERVE=true \
  WIREHOP_TEST_LATENCY_BINARY=/tmp/wirehop-latency \
  sh test/performance/run.sh /tmp/wirehop-linux /tmp/wirehop-observed-results 3
```

`WIREHOP_TEST_DURATION` accepts an integer from 20 through 86400 seconds, matching iperf3's maximum duration. The router
stays alive until harness cleanup, including for matrices lasting more than two hours. `WIREHOP_TEST_OBSERVE=true`
records established socket details, socket memory, interface counters, container and VM CPU counters, and available CPU
pressure data every 250 milliseconds on both endpoints and the router. Sampling includes endpoint startup. Environment
files preserve kernel TCP buffer limits, congestion control, CPU quotas, interface details, and qdisc settings. These
observations consume CPU and must be enabled for both candidates in a comparison.

`WIREHOP_TEST_LATENCY_BINARY` enables a separate 64-byte UDP echo flow inside WireGuard. Its sender runs independently
of replies at a requested 10-millisecond cadence, so missing or delayed replies do not reduce the offered probe rate.
`latency.json` preserves sent, unique received, lost, duplicate, and unexpected counts, RTT p50/p95/p99/maximum,
requested and actual cadence and duration, datagram size, and the two-second reply drain timeout. The verifier requires
at least one reply and checks accounting and ordered quantiles. It reports loss and latency without imposing a latency
threshold. Quantiles describe replies received before the drain deadline. The `lost` count includes dropped packets and
replies that arrive too late. Evaluate quantiles with reply counts, since missing slow replies can reduce the reported
tail. Compare repeated results with the same offered load, and account for timer scheduling and the extra probe traffic.

The latency tool also supports standalone `-listen ADDRESS` and `-target ADDRESS` modes, including IPv6 addresses in
brackets. `-size`, `-interval`, `-duration`, and `-drain` select the probe parameters. It uses local monotonic send and
receive times and does not require synchronized host clocks.

## Ingress queue accounting benchmark

Compare queue admission, dequeue, and reservation release at fixed worker counts:

```sh
go test ./internal/packetqueue -run '^$' -bench '^BenchmarkQueuePushBatch' -benchmem -cpu=1,4 -count=5
```

The benchmarks cover one-, four-, and sixteen-packet batches with local or shared limits, exhausted aggregate budgets,
and partial acceptance under packet or byte pressure. Completed batches report per-packet cost. Rejected and partial
batches report cost per call, including release of any accepted prefix. Use the kernel WireGuard harness to measure
delivered goodput and latency.

## Transmission store CPU benchmark

Compare queued admission, batch commitment, and cumulative feedback with a persistent 1024-packet backlog:

```sh
go test ./internal/relay -run '^$' -bench '^BenchmarkTransmissionStoreBacklogCycle$' -benchmem -count=6 -cpu=1
```

The cases cover small transport packets, MTU-sized packets, jumbo packets, and handshake controls. Each operation commits
the reported number of packets, acknowledges that prefix, and replaces it. The store runs entirely in memory, and packet
buffers are shared without pool ownership. These measurements isolate metadata processing and exclude carrier writes,
UDP I/O, and payload copying. Use the kernel WireGuard harness to evaluate delivered goodput and latency.

## Receiver CPU benchmark

Run the receiver benchmark with the platform's protocol clock:

```sh
go test ./internal/relay -run '^$' -bench '^BenchmarkReceiverDeliverBatchProtocolClock$' -benchmem -count=5
```

The benchmark includes deadline generation, validation before delivery, deduplication, and deadline checks before UDP
submission. It covers single packets, batches, duplicates, reordered packets, and expired packets. The endpoint accepts
writes in memory to isolate receiver CPU cost. The replay bitmap and initial packet set are populated before timing. Use
the kernel WireGuard harness to measure network goodput and latency.
