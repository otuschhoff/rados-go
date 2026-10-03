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

## Sustained Collector Smoke

On the already approved Linux deployment only, the following entry point runs
a fixed Go/native pair on `readcache`, 4-KiB reads at concurrency one. It does
not provision a cluster, change pool/caps policy or exercise recovery. It
requires Go 1.27.1, Node, gcc, librados, `ceph`, `rados` and CPUs 0-9. Set the
existing private path variables `P07_PARITY_CONFIG`, `P07_PARITY_KEY`,
`P07_PARITY_KEYRING`, `P07_PARITY_MONITORS_FILE` and `P07_PARITY_FSID_FILE`.
The entity is `client.amakura`; do not use this driver on an unapproved target.

```sh
node integration/p07/parity.mjs /absolute/fresh/private-output p07-parity-fresh-namespace --qualification-smoke
node --test integration/p07/parity.test.mjs
```

Both collectors require warmup of at least 10 seconds AND 10,000 successful
operations, then measurement of at least 60 seconds AND 100,000 successful
operations. Each phase permits at most one million retained records across
workers. Reaching the cap before both minima fails, retaining the population.
Declared operation budgets are 30 seconds from recorded start; setup and final
verification RPC defaults are also 30 seconds. The attempt limit is 15 minutes.
Go cleanup gets an independent 15-second context. Native cleanup checks a
15-second boundary and uses one-second synchronous RPC timeouts; an in-flight
call cannot be canceled immediately. Late observations cannot count as success.

The driver retains source/binary/library pins, raw warmup/measured observations,
failed fixture-collision probes, boundary health/placement, exact payload and
removal/NotFound results, and independently checked actual MON/OSD modes.
Both clients also retain actual 100-ms interval RSS observations and an
immediately preceding connected/warmed baseline, using the same Linux
`/proc/self/status` method. Missing reads, storage limits, non-increasing clocks,
baseline/end coverage defects or sampling gaps reject the attempt. Samplers stop
before final verification, cleanup and Go raw-record assembly.
Capture and mode files are fresh/exclusive and private. Existing fixtures are
never overwritten or removed by a rejected attempt. Failed outer startup or
process executions retain exit metadata and private logs, but may have no
complete attempt JSON; they cannot become accepted legs.

This smoke never accepts qualification. Go counts request preparations beyond
the first plus messenger replay dispatches; incomplete coverage stays unknown.
Native counts remain null rather than invented zeroes. Native enqueue counters
do not cover replay writes. RSS includes unequal harness recorder retention,
and CPU includes worker startup, sampling, observation and recorder storage.
Native `debug_ms=1/1` also logs ordinary messages; its measured cost is explicitly
instrumented, not primary uninstrumented qualification timing. Library-only
costs are not established. The fixed-pair smoke is not an ABBA matrix.
Ordinary fixed-count diagnostic behavior is unchanged.
Go production builds remain CGO-free with standard-library crypto.

## Frozen Matrix And Analysis

The specification JSON declares `cells` (unique `id`, approved `pool`, `size`,
`concurrency`, `workload`), `rounds` (at least five), `seed`, `bootstrapSeed`,
and optionally `bootstrapReplicates` and `rssResolutionBytes`. The planner
rejects changed gates or unsupported methodology. Review the matrix's fixture
scope and disk capacity before executing it; this is not authorization for a
new cluster, lifecycle operation or recovery experiment.

```sh
node integration/p07/qualification.mjs freeze /absolute/private/specification.json /absolute/private/fresh-plan.json
node integration/p07/parity.mjs /absolute/fresh/private-matrix p07-parity-fresh-prefix --qualification-matrix /absolute/private/fresh-plan.json
node integration/p07/qualification.mjs analyze /absolute/private-matrix/plan.json /absolute/private-matrix/manifest.json /absolute/private-matrix/fresh-analysis.json
```

The driver freezes the plan before capture, builds once, runs serially in seeded
ABBA order, uses fresh processes and per-round fixture namespaces, checks every
worker's placement, and retains failures without replacing legs for a pass.
Unsafe capture failure aborts the run with incomplete evidence retained;
incomplete rounds cannot qualify. It writes exclusive private plan, manifest,
capture, mode, command, source/binary/library and analysis artifacts. Offline
analysis verifies input hashes and its executing tool pin and refuses an
existing output. Keep the source tree frozen throughout capture and re-analysis.

Estimation equally weights the two implementation legs within each round,
takes geometric means of paired-round ratios, and bootstraps whole round
vectors across the matrix. Predeclared Bonferroni two-sided intervals preserve
the original CPU 1.20, p99 1.25, throughput 0.90 and RSS 1.25 gates. Nonpositive
or sub-resolution native RSS is unknown, never passing. Raw validation covers
both phases, counters, operation budgets, modes, runtime, conditioning and
artifact/health/placement continuity. Labeled distributions and each rejected
attempt's findings are retained. Statistical tooling success is not parity
acceptance; unknown native retries currently reject these matrix captures.

The final Phase 8 tooling smoke at
`/root/proj/rados-go/phase8-smoke-20261003172259` passed correctness and guard
checks, validated Go observations and rejected native unknown retry evidence.
It also exercised the exclusive pinned manifest analyzer. A full qualifying
matrix and uninstrumented, library-attributed resource evidence remain later
qualification work, not results of this two-leg smoke.

## Outputs

Consumer PGO training and held-out diagnostics use a separate explicit driver:

```sh
node integration/p07/pgo.mjs /absolute/fresh/private-output p07-parity-pgo-fresh-prefix
node integration/p07/pgo.mjs analyze /absolute/private-output /absolute/private-output/fresh-assessment.json
```

Run only from the repository root on the already approved fixture deployment
with the private path variables above. The frozen six-cell training composition,
compiler-use proof, off/PGO correctness probes, five seeded held-out rounds,
unchanged native brackets and all three retained assessments are documented in the
[Phase 9 assessment](../../docs/performance-phase9/README.md). Explicit
`P07_PGO_DIAGNOSTIC=1` uses shorter 1-second/1,000 and 8-second/10,000 minima
and distinct unqualified statuses; it cannot change normal qualification
defaults. `P07_PGO_PROFILE_FILE` is allowed only for Go diagnostic training,
with exclusive paths and measured-window profiling. Training, probes and
held-out timing stay separate. Adoption is rejected; this is not a production
PGO recipe, hidden default profile or native parity claim.

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
Opt-in actual monitor/OSD negotiation capture and independent Phase0 claim
validation are documented in [MODE_EVIDENCE.md](MODE_EVIDENCE.md).
Source-bound qualification and integration reports must be regenerated after
final source changes; the performance budget is not altered by this mode.

## Full-Workload Resource Diagnostics

To investigate large concurrent writes and process-level CPU/RSS rather than
only the isolated read case:

```sh
P07_RESOURCE_DIAGNOSTIC_DIR=/tmp/rados-go-p07-resources GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
GOTOOLCHAIN=go1.27.1 go tool pprof -top /tmp/rados-go-p07-resources/benchmark /tmp/rados-go-p07-resources/cpu.pprof
GOTOOLCHAIN=go1.27.1 go tool pprof -alloc_space -top /tmp/rados-go-p07-resources/benchmark /tmp/rados-go-p07-resources/baseline-allocs.pprof
GOTOOLCHAIN=go1.27.1 go tool pprof -inuse_space -top /tmp/rados-go-p07-resources/benchmark /tmp/rados-go-p07-resources/baseline-resources.json.heap.pprof
```

Use a fresh absolute directory. This mode runs the normal 36-row matrix with
secure framing: native, Go with per-row resource snapshots, then Go with those
snapshots plus CPU profiling. It exits without replacing the normal P07 report.
The Go legs are diagnostic, not uninstrumented latency baselines. Native and Go
perform their own setup/seeding; these are not the shared-object ABBA legs above.

Outputs include `native.json`, `go-baseline.json`, `go-profile.json`, matching
`benchmark`, `cpu.pprof`, `baseline-allocs.pprof`, `profile-allocs.pprof`,
`baseline-resources.json`, `profile-resources.json` and their `.heap.pprof`
sidecars. Each resource row records cumulative CPU/allocation counters, lifetime
maximum RSS and current heap allocation, in-use, idle and released bytes.
Samples occur after row cleanup, so they can locate high-water increases but
cannot report the maximum live heap within a row. Maximum RSS is a historical
high-water mark and does not decrease when memory is freed.

CPU sampling stops at the end-of-workload resource snapshot, before exports and
forced GC. The final `post_gc` sidecar row and heap profile are captured after
one diagnostic-only GC, while the client is still open. Their cumulative counters
include post-workload diagnostic overhead; normal report counters do not. No
forced GC or process-wide runtime tuning is used in the production library.

### Allocation And Retention Findings

The first full-workload allocation profile attributed about 41% of sampled bytes
to secure encoding: copied normalization, plaintext staging, AEAD output and a
final wire-buffer copy. Encoding now allocates the final wire buffer once and
seals records in place in private storage. Caller input is unchanged, padding
remains zero and nonce preflight is unchanged. A 4 MiB local encode benchmark
fell from 16.80 MB / 13 allocations to 4.20 MB / 5 allocations, with time falling
from about 1.43 ms to 0.87-0.90 ms. These are macOS microbenchmark results, not
Ceph latency measurements.

Pending-request removal and replay acknowledgment also retained stale pointers
in unused slice backing-array slots. Both paths now clear discarded references
while preserving request order and acknowledgment semantics. After the first
removal fix, the live heap after GC was still about 129 MB; clearing replay
filter tails reduced it to about 0.69 MB with the client open. The sampled final
live-heap profile showed runtime allocations, not large retained request payloads.

Full secure diagnostic measurements on the laptop Linux VM:

| Metric | Initial | Verified Optimized |
| --- | ---: | ---: |
| Go allocated bytes | 20.73 GB | 14.25 GB |
| Go peak RSS | 2.44 GB | 2.16 GB |
| Go CPU time | 8.56 s | 7.83 s |
| Native peak RSS | 0.96 GB | 0.91 GB |
| Native CPU time | 6.43 s | 6.21 s |

Artifacts were captured in `/tmp/rados-go-p07-resource-before` and
`/tmp/rados-go-p07-resource-verified`. The final CPU profile spent about 55% of
samples in benchmark payload generation, 13% in memory copying, 9% in syscalls,
and 8% in AES-GCM encryption/decryption. The original profile showed about 1%
cumulative `runtime.selectgo` CPU, insufficient evidence to justify replacing
the session owner/channels with a different locking model. This does not rule
out contention in other workloads.

Peak RSS still exceeds native because concurrent writes retain multiple
ownership/replay-safe request copies, plus benchmark payloads and idle Go heap
pages. Remaining prominent allocation sites include mutation admission, request
encoding and messenger cloning/framing. Removing those copies requires a
separate explicit ownership design; they were not removed speculatively.
RSS is not live heap, and this experiment alone does not establish steady-state
memory usage, leak freedom or native CPU parity. Repeat runs varied; attribution
and allocation counts are stronger evidence than a single CPU/RSS ratio.

After these resource fixes, the regenerated source-bound P07 report passed
functional/schema/source verification and all resource limits. One numerical
latency pair narrowly missed the unchanged P12 budget: secure 64 KiB reads at
concurrency 16 had p99 ratio 8.0022 against 8.0. That result is retained in the
report committed in `d851ebe`; P07 functional status is not P12 performance approval.
An independent repeat in `/tmp/rados-go-p07-resource-repeat.json` had no
latency/throughput failures, worst p99 ratio 5.11 and minimum throughput ratio
0.258. Both outcomes must be retained when assessing variability; a passing
repeat does not erase the threshold miss or establish stable compliance.

## Receive Latency Follow-Up

After the retention fixes, `/tmp/rados-go-p07-latency-current` still showed
64 KiB/concurrency-16 Go read p99 of 10.46-10.62 ms against native
1.02-1.08 ms. Go performed 165-166 collections, with 218-227 ms cumulative
pauses. A separate no-GC probe reached 2.22 ms p99. Instrumented stages located
most delay between socket write completion and full receive-frame completion,
not request admission or reply delivery. That interval includes socket wait,
receive scheduling and decoding; it is not a network-only measurement.

A 256 KiB append-only receive-allocation experiment was rejected. It increased
total allocation from about 350 MB to 408 MB without reliably meeting the
latency budget, and would amplify retained caller-slice memory. No receive-block
allocator or caller-buffer reuse is part of the final fix.

Built-in CRC and secure transports now use bounded socket read-ahead, up to
512 KiB per connection, capped by the frame-byte limit (subject to the standard
buffered reader's 16-byte minimum). Construction occurs only after authentication
and codec negotiation. Custom codecs retain the original connection reader.
The buffer amortizes small preamble/body reads and burst traffic, and its bounded
live storage also affects GC pacing. Returned replies never reference this
buffer. It adds persistent memory per active built-in connection; deployments
with many OSD connections must include that cost in their memory budget.

The receive pump has one queued frame of overlap with owner processing, rather
than an unbuffered handoff. Queue growth remains bounded, though a queued frame
and a reader-held frame can add frame-sized memory. Read faults use the same
ordered stream so EOF cannot overtake previously decoded receive frames.
Independent write faults still use the existing fault path. Non-owned custom
transport frames are detached before the next read, protecting reusable buffers.
No OSD admission lock, write ordering, protocol authentication or global runtime
setting was changed.

Three independent matched-secure ABBA runs with the 512 KiB receive window
passed the unchanged p99 threshold in all six Go legs. Each leg measured 4,096
reads after 128 warmups. Comparing every Go leg conservatively against its run's
faster native p99:

| Artifact Directory | Go p99 | Worst Ratio |
| --- | --- | ---: |
| `/tmp/rados-go-p07-latency-window512` | 7.90-8.25 ms | 6.58 |
| `/tmp/rados-go-p07-latency-confirm-1` | 7.46-8.62 ms | 7.40 |
| `/tmp/rados-go-p07-latency-confirm-2` | 7.34-8.43 ms | 4.84 |

Collection counts were 87-97 rather than 165-166, while measured process RSS
remained roughly 21-28 MB in these runs. Tail latency remains GC/VM-sensitive;
the repeat results support measured compliance here, not a guarantee on other
hosts or workloads, native-equivalent latency or P12 release certification.

Two subsequent full normal benchmark runs also passed the unchanged latency,
throughput and resource budgets in all 72 pairs each. The refreshed source-bound
`docs/p07/integration-report.json` had worst p99 ratio 4.25 and minimum throughput
ratio 0.368. The independent `/tmp/rados-go-p07-latency-matrix-repeat.json` run
had worst p99 ratio 7.02 and minimum throughput ratio 0.221. P07 source/schema
verification and the full repository race suite passed. Normal-row sample counts
and requested-CRC negotiation caveats still apply; the longer matched-secure
diagnostics provide the stronger evidence for the targeted read-latency fix.