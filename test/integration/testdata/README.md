# Receiver measurement fixtures

`rtt-step-run77-forward.json` preserves the reporting intervals from historical Test #77. They do not contain
within-interval receive timing. They cannot substitute for counter evidence in the current verifier.

The `receiver-rtt-*` fixtures retain a local multipath reverse RTT-step flow that the former interval interpolation
accepted at 25.1225958459 percent during raised RTT. Its 592 application counter updates establish 24.9081175039 percent
for the same requested windows. The restored RTT window exceeds 99 percent. The full event sum, interval sum, and final
receive total agree at 77,594,624 bytes. This is a within-interval interpolation false pass, independent of the
separately reproduced interval counter-reset race.

The flow fixture selects only fields used by the verifier from the original client result. The JSONL counter records and
window boundaries are unchanged. The experiment used WireHop Go code from commit
`5930f4353c5915f440883610c52796ca7a21533f` and the optional receiver tracing prototype over iperf3 3.18, before the
atomic snapshot and worker-stop fixes. It ran on local LinuxKit ARM64 with four pinned CPUs, not a GitHub-hosted runner.
