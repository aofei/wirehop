# V1 performance review

This implementation improves stalled-path recovery, receive copying, and retained metadata while keeping protocol
version 1. Breaking changes were permitted. The wire representation was retained because these improvements do not
require more framing bytes, another admission round trip, or stateful compression.

## Implemented changes

Stalled-path recovery no longer waits for the source's old delivery-rate estimate to predict imminent expiry. The
scheduler first requires a path-aware lack of parsing progress. It then compares an eligible alternative against the
earliest future retained deadline and a recovery estimate based on the progress guard and observed stall duration.
Expired sent entries preserve their ownership and accounting while useful later entries can trigger recovery. An
alternative with independently stalled outstanding progress cannot trigger generation churn.

Recovery checks every healthy candidate with capacity rather than imposing the ordinary same-group ranking on the
recovery decision. A healthy third or fourth same-group lane remains usable when the faster members stall together.
Deterministic regressions reproduced the ranking bug before its removal and pass after the correction. They also remove
stalled generations in both orders, verify sent and queued packet IDs, payloads, and wire deadlines on the healthy lane,
and acknowledge the migrated batch. An isolated dropped-migration mutation failed all four original cases.

Generation removal and replacement use the same live recovery-candidate check at migration time. Four additional
regressions reproduced packet loss when removal preceded the next maintenance tick and stale scores directed migration
into another stalled lane. Both removal orders now preserve every eligible packet on the healthy lane. Boundary tests
also preserve slow serialization and recent partial feedback, reclaim expired destination capacity, and verify aggregate
ownership after accepted and rejected migration.

Replacement events have matching regressions in both orders. Omitting the shared progress check makes all eight new
ordering cases fail. The corrected removal, replacement, and progress-boundary cases passed one hundred race-enabled
repetitions.

Active recovery requires a measured delivery rate on at least the source or alternative. Three newly added low-rate
multipath scenarios reproduced reconnect churn when both paths still used provisional rates. Diagnostic traces showed
unmeasured capacity triggering abandonment before useful data expired. The corrected decision does not treat two initial
estimates as evidence that one healthy low-rate path can replace the other. Initial forwarding and migration after an
actual carrier removal remain available without capacity observation. Five deterministic cases cover neither, either, or
both rates being observed, including expired sent work. The uncorrected policy fails both unobserved cases. An isolated
mutation requiring both rates to be observed fails the two single-observation cases. These regressions and existing
recovery cases passed one hundred race-enabled repetitions. The default kernel matrix now includes all three low-rate
multipath cases, and CI selects the 32 kbit/s case.

The progress guard includes two feedback cycles, two report intervals, and first-frame serialization, with a 250 ms
floor. This protects healthy high-delay and low-rate paths while allowing a slower alternative to salvage traffic before
the source's default five-second packet lifetime closes that opportunity. Migration still happens at most once per
transport packet, preserves its relay ID, and validates its remaining deadline on the destination lane.

TCP and TLS readers borrow complete frames from their existing 32 KiB read buffers. WS and WSS retain one owned first
frame and copy buffered batch tails into one contiguous allocation. Reserving the already-buffered prefix once prevents
append growth from retaining additional backing arrays or exceeding the 32 KiB tail limit. Only the first frame can
perform a blocking read. Once a frame has been returned, additional batch frames never refill that buffer. Fragmented or
larger first frames use one owned frame buffer, replacing the previous sixteen per-frame buffers. Synchronous UDP
submission consumes every returned payload before the next carrier read. Invalid later tails preserve the valid prefix.
The implementation relies on the documented no-read guarantee of
[bufio.Reader.Discard](https://pkg.go.dev/bufio#Reader.Discard) when the requested count is already buffered.

Retained entries use one packet owner instead of repeating the payload slice, class, and priority. Wire metadata is
constructed from that owner when a writer takes a batch. Cumulative acknowledgement sums encoded bytes and releases
packet references in one traversal. Independent writer references still protect an active write during acknowledgement
or generation drain. On Darwin/arm64, each retained entry occupies 136 bytes, down from 168 bytes, a 19.0 percent
reduction. A full set of 262,144 retained transmission entries saves 8 MiB of live metadata before deque spare capacity.
Metadata remains separate from the charged payload and framing budget.

## Validation

The new differential buffered-frame fuzz target compares complete frames with the existing stream reader and checks that
later buffered decoding preserves earlier payloads. It covers canonical lengths, malformed tails, partial records, empty
content, ordinary packets, and maximum-size content. CI executes one million fuzz cases and includes forward, reverse,
and bidirectional asymmetric stall recovery.

The differential target requires every complete frame to be returned and counts underlying reads, including EOF reads.
Isolated mutation checks confirm that it rejects both suppressed complete frames and an attempted buffer refill.

An additional carrier differential target compares TCP/TLS and open WebSocket message batches with scalar stream
decoding under random fragment sizes and batch capacities. Its seeds cover one-byte reads, eighteen-frame sequences,
buffer boundaries, maximum-size content, partial records, and malformed tails. It passed one million executions and runs
in CI with the same budget. An isolated lost-first-frame mutation fails the original stream target. The extended target
also passed one million executions after the WebSocket ownership change.

Carrier regressions cover the 32 KiB buffer boundary, 40,000-byte content, and maximum-size content. The first two cases
require an owned first frame and borrowed later frames in the same batch. An isolated buffer-reuse mutation confirms
that the regression detects an overwritten earlier payload.

WebSocket regressions additionally read eighteen-frame messages followed by a distinct message, with large content at
the same boundaries, varying batch capacities, and a fresh canceled-after-return context for each read. Every returned
prefix must match before the next read. These cases passed one hundred race-enabled repetitions. The contiguous-tail
regression checks ordinary Data batches and content near the 32 KiB buffer limit, requires one tail allocation, and
verifies independence from read-buffer reuse. Removing the one-time capacity reservation fails both cases.

A combined lane regression blocks synchronous endpoint delivery, closes or aborts the carrier, and checks every returned
payload and its cumulative parsing count. It uses real loopback TCP, TLS, WS, and WSS connections, each with 1452-byte,
32 KiB, and maximum-size packets followed by a malformed tail. All twenty-four cases passed one hundred race-enabled
repetitions. TCP/TLS aborts unwrap the actual TCP socket, and WS/WSS retain their underlying socket for abortive close.
An isolated mutation that clears the owned tail during Close or Abort fails all eight ordinary and buffer-boundary
WS/WSS cases. The endpoint is controlled by the test. Actual UDP submission remains covered by separate socket and
kernel tests.

Fourteen partial-delivery regressions cover scalar and vector endpoint writes, drops at each position, missing local
peers, endpoint failure, cancellation after a successful prefix, and expiry between retries. Replaying the original
frames preserves their deadlines, retries only fresh undelivered packets, and never repeats a successful UDP write.
Cumulative parsing counts and bytes still acknowledge every fully validated carrier frame. Seven counter-boundary
regressions cover exhausted packet and byte counters, one-byte overflow, and successful prefixes of one or two packets.
They verify the retained suffix, writer references, and aggregate ownership through acknowledgement and generation
drain. All twenty-one cases passed one hundred race-enabled repetitions. Isolated compiling mutations that commit an
undelivered suffix to deduplication, omit retry expiry, or allow counter wrapping fail twelve, two, and seven cases,
respectively. These endpoints are controlled fixtures, and counter limits are injected into otherwise consistent store
state.

Eight combined feedback-completion regressions route reports through real control queues and the production control
writer, using controlled carriers. Concurrent writers cover two successes, either or both writes failing, either or both
queues being full, and an absent original generation. Queue admission cannot complete a report as sent. One successful
write completes duplicated feedback exactly once, while unsent progress remains available at its retry deadline. New
progress survives delayed completion, and an older completion cannot roll it back. All eight cases passed one hundred
race-enabled repetitions. Isolated compiling mutations that duplicate completion, treat queue admission as successful
delivery, or roll back newer progress fail one, seven, and eight cases, respectively.

The kernel harness now requires each exercised asymmetric-stall direction to recover at least 25 percent of its measured
pre-fault throughput during the final five seconds. The old baseline's final-window recovery was 0.9 percent, so it
fails this stronger check despite nominal transfer completion. The threshold establishes substantial recovery in this
controlled scenario. It does not assert uninterrupted inner TCP progress or general WAN optimality.

The implementation review includes repeated race checks for payload lifetime, accounting, migration, control fairness,
feedback, clocks, and authentication. Windows/amd64, FreeBSD/amd64, Linux/amd64, and Linux/386 checks compile every
package's tests. They do not execute those operating systems. Linux/arm64 kernel tests execute inside isolated Docker
networks.

## Measurement method

Buffered parsing compares the original copied reader and the borrowed reader using sixteen real Data records per batch.
Both cases include read-buffer filling and Data parsing. Packet IDs start at 1,000,000,000 and deadlines use
86,405,000,000 microseconds, so the metadata widths represent an established session. Cases use 32-byte and 1452-byte
payloads. The copied comparison uses the original implementation from commit `d2e334dc901d9c64d3f364a07441ba1e8bc60476`.

Carrier and transmission-store benchmarks compare the same existing benchmarks on the original source and the revised
implementation. Candidates ran sequentially on the same host without competing test workloads. Each result is the median
of ten samples and preserves allocation counts. The first comparison used both execution orders. A final comparison
remeasured the revised implementation after removing an extra buffered peek and a by-value metadata receiver. The
measurements use Go 1.27.1 on Darwin/arm64 with an Apple M4 Pro.

| Operation | Original ns/op | Revised ns/op | Time reduction |
| :-- | --: | --: | --: |
| Buffered Data batch, 16 x 32-byte payloads | 369.10 | 283.95 | 23.1% |
| Buffered Data batch, 16 x 1452-byte payloads | 736.60 | 571.90 | 22.4% |
| Transmission-store local cycle | 98.27 | 93.95 | 4.4% |
| Transmission-store aggregate cycle | 100.30 | 95.74 | 4.5% |
| Transmission-store backlog cycle | 1305.50 | 1224.00 | 6.2% |
| Retained deque feedback window | 2690.00 | 2282.00 | 15.2% |

These operations preserve zero allocations per iteration. Small nonzero bytes-per-operation values in the deque
benchmarks are amortized capacity growth and are rounded by the Go benchmark reporter.

| Initial loopback carrier read | Original ns/op | Initial revised ns/op | Time change |
| :-- | --: | --: | --: |
| TCP scalar frame | 1545.50 | 1538.00 | -0.5% |
| WS one frame, background context | 1773.00 | 1780.50 | +0.4% |
| WS one frame, cancelable context | 1666.00 | 1676.00 | +0.6% |
| WS sixteen frames, background context | 3602.50 | 3863.00 | +7.2% |
| WS sixteen frames, cancelable context | 3549.50 | 3814.00 | +7.5% |

The initial borrowed WebSocket batch reader regresses in this measurement, despite faster buffered parsing and lower
scratch retention. Background contexts preserve zero allocations, while cancelable contexts preserve three allocations
and 144 bytes per iteration. A separate copied-first WebSocket candidate did not consistently improve both context cases
and was rejected. CPU profiles were dominated by socket syscall samples. These results do not establish a WebSocket
throughput speedup and the parsing improvement should not be presented as a universal carrier improvement.

Repeated isolated batch measurements in both execution orders still showed a slowdown, with revised median times between
4.2 and 10.1 percent above the original depending on context. A GC-disabled diagnostic produced wide timing ranges and
did not isolate the cause. A separate candidate combined buffered prefix decoding into one peek and discard. Its initial
2.5 to 3.0 percent improvement against the revised implementation did not hold in a reverse-order check that preserved
the existing scalar fast path. The background batch was 0.5 percent slower and the cancelable batch was only 1.0 percent
faster. The candidate was not adopted because it did not establish a consistent carrier benefit.

The final WebSocket implementation restores an owned first frame and uses one contiguous owned tail buffer. Isolated
measurements in both execution orders improved all four batch workload/context combinations against the borrowed reader,
with median reductions of 2.9 to 7.9 percent across the comparisons. Background contexts retain zero allocations and
cancelable contexts retain three allocations and 144 bytes per iteration. The reader retains one first-frame buffer and
one tail buffer instead of sixteen per-frame buffers. Ordinary first-frame capacity and all tail capacity stay within 32
KiB each. A larger first-frame allocation is discarded at the next nonempty read that passes its initial cancellation
check.

The following owned-tail comparison precedes the duplicate-reset cleanup and uses five sequential two-second samples per
case with identical isolated benchmark sources. Data cases include valid 1452-byte Data records with established-session
metadata widths and parse every returned record. Opaque cases repeat the earlier carrier workload. Every measured batch
required one ReadFrames call, so extra batch calls do not explain the earlier regression. This counter does not measure
underlying socket read calls.

| Owned-tail WS read | Original ns/op | Revised ns/op | Time change |
| :-- | --: | --: | --: |
| Opaque one frame, background context | 1748 | 1802 | +3.1% |
| Opaque one frame, cancelable context | 1653 | 1683 | +1.8% |
| Opaque sixteen frames, background context | 3577 | 3706 | +3.6% |
| Opaque sixteen frames, cancelable context | 3472 | 3519 | +1.4% |
| Data one frame, background context | 1723 | 1769 | +2.7% |
| Data one frame, cancelable context | 1638 | 1667 | +1.8% |
| Data sixteen frames, background context | 3445 | 3550 | +3.0% |
| Data sixteen frames, cancelable context | 3305 | 3426 | +3.7% |

The remaining median differences do not establish parity or a speedup against the original WebSocket reader. The revised
ownership policy reduces the measured regression against the earlier borrowed implementation and bounds per-batch
storage, but does not establish globally optimal carrier performance.

The last cleanup removes a duplicate WebSocket FrameReader reset. First-frame decoding already performs the same reset,
including exceptional-capacity release. TCP/TLS retain their explicit reset because the borrowing path can bypass that
decoder. Sequential paired measurements against the preceding implementation showed changes of +2.94, -0.72, +0.52, and
+0.10 percent across the four batch workload/context combinations. A reverse-order recheck of the first case reduced the
difference to +0.40 percent. This cleanup simplifies the read path and does not establish a performance improvement.

Kernel throughput tests use endpoint-egress netem and are correctness and recovery evidence. Their short flows and TCP
Small Queues interactions limit general throughput conclusions. The earlier full 56-case matrix passed, including all
four framed carriers, all three asymmetric-stall directions, kernel rekeying, UDP, IPv6, fwmarks, routing faults,
sustained low rates, prolonged outages, and address replacement. After the final core cleanup, twelve affected scenarios
passed again with a rebuilt executable. A subsequent review corrected same-group recovery ranking and passed eight
further kernel scenarios with another rebuilt executable. After removing a comparison helper used only by tests, six
additional kernel scenarios passed on another rebuilt executable, including WS, WSS, same-group multipath, and all three
directional asymmetric stalls. After correcting direct migration progress validation, twelve further scenarios passed
with another rebuilt executable. They cover all four framed carriers, multipath, all three asymmetric-stall directions,
single-lane stalls, a twelve-second outage, address replacement, and low-rate TCP/WSS. After correcting provisional-rate
abandonment, fifteen further scenarios passed, including all three low-rate multipath cases. A final rebuilt executable
also repeated those three cases without extra carrier connection attempts. The directional recovery runs showed
bidirectional forward throughput between 17.6 and 70.5 Mbit/s. All runs recovered substantial throughput in each
direction relative to that run's own pre-fault rate. A single aggregate throughput value cannot establish general
bidirectional performance.

The latest fifteen-scenario run measured recovery against each direction's own pre-fault intervals:

| Direction | Pre-fault Mbit/s | Recovered Mbit/s | Recovered fraction |
| :-- | --: | --: | --: |
| Forward | 89.977 | 90.537 | 100.6% |
| Reverse | 88.455 | 88.922 | 100.5% |
| Bidirectional forward | 88.409 | 75.706 | 85.6% |
| Bidirectional reverse | 78.234 | 87.323 | 111.6% |

The bidirectional forward flow remains below its pre-fault rate. Recovery correctness does not establish optimal
post-fault capacity distribution across both lanes.

The final WebSocket executable passed seven further kernel scenarios covering WSS, WS/WSS stalls, mixed TCP/WSS,
low-rate multipath, bidirectional asymmetric recovery, and a shared-capacity change. Two further shared-capacity runs
also passed. Each capacity-change run starts unrestricted, applies a 32 kbit/s bottleneck at five seconds on both
endpoints, and restores capacity at twelve seconds. Both rate-change markers and both connected lanes are required, and
final-window throughput must reach at least 25 percent of the measured pre-fault rate. Recovery fractions were 32.6,
99.5, and 101.7 percent across the three runs. The spread limits performance conclusions. Bidirectional asymmetric
recovery in this run reached 87.8 percent forward and 101.1 percent reverse.

After the duplicate-reset cleanup, another rebuilt executable passed five kernel cases: WS/WSS at 32 kbit/s, a
twelve-second WS outage, WSS address replacement, and shared-capacity change. The fourth capacity-change run recovered
100.9 percent of its own pre-fault throughput, with both endpoints recording the five-second drop and twelve-second
restoration. This additional run does not eliminate the earlier recovery spread.

The unchanged executable subsequently passed forward, reverse, and bidirectional shared-capacity changes. Both endpoints
recorded the five-second drop and twelve-second restoration in every case. Each direction uses its own pre-fault rate:

| Shared-capacity change | Before fault Mbit/s | After recovery Mbit/s | Recovery fraction |
| :-- | --: | --: | --: |
| Forward | 1282.578 | 1313.234 | 102.4% |
| Reverse | 1292.305 | 1329.454 | 102.9% |
| Bidirectional forward | 736.381 | 1113.617 | 151.2% |
| Bidirectional reverse | 1273.583 | 1219.716 | 95.8% |

These are recovery checks, not evidence of comparative throughput or optimal capacity distribution. Initial TCP ramp-up
and the short baseline window limit rate comparisons. Eleven negative result replays confirm that both directional
variants reject any missing endpoint marker and a stalled final window even when completed-flow totals remain nonzero.

Initial capacity-drop experiments used the healthy no-reconnection oracle and failed its connection-count check, despite
completed flows and substantial late-window recovery. Diagnostic traces showed measured rates and large unreported
backlogs during the fault. The maintained capacity-change case permits reconnecting these stalled generations while
requiring both lanes after recovery. Healthy low-rate cases keep their no-reconnection check. The default matrix now
contains 62 unique cases and CI selects 23, including bidirectional shared-capacity changes.

## Decisions supported by the review

The compact varint framing remains in V1. An isolated fixed-header prototype improved parsing by tens of nanoseconds but
added bytes to both ordinary and small packets. Batch-local compression saved about 0.56 percent for the reviewed MTU
workload while requiring shared-byte acknowledgement and partial-batch rules. Neither experiment established a net
throughput benefit that justified the extra wire complexity.

Parsing feedback remains independent from local write success and TCP acknowledgement. It releases retained ownership
and provides application progress across alternate carriers. Its send-bounded delivery-rate sampling and handling of
application-limited samples remain intact.

Public WireGuard counters cannot establish authenticated receiver acceptance. The relay keeps its own packet IDs for
deduplication and does not discard data using an exact counter-age rule. Sender reordering constraints and bounded
real-data capacity discovery require separate measurements on independently provisioned paths before introducing new
scheduling state. The current stable-lane preference remains the direct policy supported by these tests.
