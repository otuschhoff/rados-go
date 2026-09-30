# Concurrent Read Diagnostics

Run from the repository root with Docker available. This mode creates a
disposable cluster and does not write a certifying P07 report:

```sh
P07_DIAGNOSTIC_DIR=/tmp/rados-go-p07-diagnostic GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
GOTOOLCHAIN=go1.27.1 go run ./integration/p07/timing-summary /tmp/rados-go-p07-diagnostic/request-timing.json
GOTOOLCHAIN=go1.27.1 go tool pprof -top /tmp/rados-go-p07-diagnostic/benchmark /tmp/rados-go-p07-diagnostic/cpu.pprof
GOTOOLCHAIN=go1.27.1 go tool pprof -alloc_space -top /tmp/rados-go-p07-diagnostic/benchmark /tmp/rados-go-p07-diagnostic/allocs.pprof
```

Use a fresh absolute output directory for each experiment. The workload reads
the same 16 preseeded objects and payloads with both clients, using secure
framing. Each leg performs eight warmup reads per worker, followed by 256
measured reads per worker: 128 warmup and 4,096 measured reads in total.

Native-Go-Go-native legs are uninstrumented. Separate Go legs collect in-memory
request timestamps, a CPU profile and an allocation profile. Profiled and timed
legs perturb scheduling and are not baseline latency measurements. Additional
legs use `GOGC=off` and `GOMAXPROCS=2` as causal probes, not production settings
or certification results. Profiles and resource counters include warmup.
Per-leg OSD snapshots bracket connection,
warmup and measured reads; expect 4,224 OSD reads, not 4,096.

## Outputs

- `native-1.json`, `go-1.json`, `go-2.json`, `native-2.json`: baseline latency distributions.
- `*-osd-*-before.json`, `*-osd-*-after.json`: aggregate Ceph perf counters.
- `*-started-at`, `*-finished-at`: UTC timestamps bracketing baseline client processes.
- `go-timing.json`, `request-timing.json`: timed workload and per-read transaction stages.
- `go-cpu.json`, `cpu.pprof`, `benchmark`: separate CPU profile and matching binary.
- `go-memory.json`, `allocs.pprof`: separate allocation profile.
- `go-nogc.json`, `go-procs2.json`: diagnostic runtime controls.
- `seed.json`: confirmation of shared-object initialization.

Diagnostic Go reports include actual `environment.gomaxprocs`, `gc_cycles` and
`gc_pause_ns`. Normal reports omit these fields to preserve the strict report
schema. Disabling GC can substantially increase RSS; do not use it as a fix.

## Root Cause And Fix

The September 30, 2026 secure-read investigation found repeated payload copies
in secure decryption, messenger decoding, OSD reply decoding and objecter result
construction. Allocation-driven GC amplified latency on the laptop's Linux VM.
Disabling GC with the same workload removed most of the delay, independently of
the allocation changes. Reducing the runtime from ten Ps to two also lowered
GC pause time, demonstrating sensitivity to VM/runtime scheduling.

The fix decrypts private ciphertext storage in place, transfers capped payload
views only across explicitly owned transport/session boundaries, and avoids
unused compound-operation storage for ordinary reads. Default decoders and
custom sessions still copy. Compound results retain independent buffers.
Authentication, nonce validation and OSD admission ordering remain unchanged.

For 4,096 measured 64 KiB reads plus warmup, allocation volume fell from about
2.05 GB to 355 MB. Median default-runtime latency fell from roughly 9-10 ms to
2.0-2.3 ms. In `/tmp/rados-go-p07-scheduler-probe`, native medians were
0.60-0.63 ms, Go default medians 2.04-2.07 ms, the no-GC median 0.76 ms, and
the two-P GC-enabled median 0.93 ms. Default Go p99 was 10.7-12.5 ms versus
native 1.65-2.07 ms. These observations establish the dominant contributors,
not identical performance or a guarantee of low tails on this VM.

Two normal paired benchmark runs subsequently passed the unchanged numerical
budgets in all 72 pairs each. Worst p99 ratios were 2.88 and 5.38. The latter
run regenerated `docs/p07/integration-report.json` and passed `p07-verify`;
its lowest throughput ratio was 0.255 against the 0.10 minimum. The first run's
maximum Go RSS was 2.29 GB, with approximately 20.73 GB allocated per full run.
Requested CRC rows still have the negotiation caveat below. These results are
not P12 release certification.

Subtract cumulative OSD `osd.op_r_latency.sum` and `.avgcount` counters between
snapshots, then divide the summed latency delta by the count delta across OSDs.
The sum is in seconds. This produces a mean, not a server p99, and excludes time
before the OSD's measured operation interval.

## Stage Interpretation

The context-scoped collector is internal to the messenger; ordinary calls do
not collect events or perform request timestamp logging. The export contains
timestamps and transaction IDs, not object payloads or credentials.

| Interval | Included work |
| --- | --- |
| `read_enter` to `submit_enter` | Routing, request construction and OSD admission lock |
| `submit_enter` to `admitted` | Messenger registration |
| `admitted` to `write_begin` | Dispatch, message framing and outbound queue |
| `write_begin` to `write_end` | Transport codec encoding and socket write |
| `write_end` to `frame_received` | Peer/network wait, receive scheduling, full frame read and codec decoding |
| `frame_received` to `reply_delivered` | Owner scheduling, message decoding and pending-request bookkeeping |
| `reply_delivered` to `submit_return` | Result-channel delivery and caller scheduling |
| `submit_return` to `read_return` | Object reply decoding and result construction |

`frame_received` is captured after `ReadFrame` completes, not at first socket
byte arrival. A successful write means local kernel acceptance, not OSD receipt.
The reply can arrive while the writer is still returning; negative
`write_end` to `frame_received` intervals are reported rather than clamped.
Serialized timestamps use wall-clock time; significant clock adjustments can
invalidate interval analysis. The summarizer rejects missing or duplicate stages;
retries need attempt-specific analysis instead of collapsing multiple attempts.

The normal benchmark's CRC label is a requested policy, not proof of negotiated
CRC: Go allows CRC but prefers secure. Do not compare that row with native CRC
as identical wire modes without checking negotiation. This diagnostic explicitly
requires secure mode on both clients to avoid that ambiguity.

These artifacts are diagnostic, not source-bound certification evidence.
Source-bound qualification and integration reports must be regenerated after
final source changes; the performance budget is not altered by this mode.