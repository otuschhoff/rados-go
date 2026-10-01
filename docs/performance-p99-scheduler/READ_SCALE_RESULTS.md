# Secure Read Profiling And Scale Results

Status: all four final captures passed; originals verified, including all 120
Go legs and 40 native context legs. This is a diagnostic follow-up to
[the inventory results](INVENTORY_RESULTS.md), not renewed qualification or
closure of the historical latency gate. Production remains four slots; eight
slots are diagnostic-only, with no automatic promotion or global runtime tuning.

## Profile Method

`P07_READ_PROFILE=1` uses fixed ten-P parallelism, secure 64-KiB `ReadInto`
operations at concurrency 16, and 1024 measured operations plus eight warmup
operations **per worker**: 16,384 measured and 128 warmup per leg. Each capture
has five uninstrumented baseline legs. CPU, allocation, and execution-trace
profiles are separate executions, not simultaneous instrumentation of the
baseline. These profiles use the untagged production build: four scratch slots
and disabled inventory counters.

The final captures are `/tmp/rados-go-read-profile-idle-final-v1` and
`/tmp/rados-go-read-profile-alloc-final-v1`. Final trace cleanup stops tracing
before exporting results at the workload-end endpoint. Traces include warmup,
background work and background-worker shutdown, but exclude subsequent client
shutdown and result export. Earlier traces included
export work and are excluded from performance attribution. Instrumented timing
is perturbed by profiling and tracing; it must not replace baseline timing.

An allocation profile covers the profile process lifetime, including startup.
It is not the same interval as resource deltas. Resource snapshots cover warmup
and measured operations, including harness background work where enabled.
CPU profiles have before/after resource snapshots. Sampled profile bytes and
resource allocation deltas must not be equated. Scheduler-delay sums (including
9.80 seconds in the loaded final profile) aggregate waiting across goroutines,
not elapsed wall time or CPU spent in channels; they do not justify a channel
rewrite.

## Profile Findings

The initial idle capture, `/tmp/rados-go-read-profile-idle-v1`, sampled
186.72 MB of allocations. Scratch allocation accounted for 65.93 MB (35.31%),
and `sanitizeTickets` for 12.01 MB (6.43%). These are sampled profile quantities,
not retained heap or RSS measurements.

The initial loaded capture, `/tmp/rados-go-read-profile-alloc-v1`, sampled
5730.85 MB. Harness background allocation accounted for 5493.47 MB (95.86%),
and scratch allocation for 114.16 MB (1.99%). Its CPU profile totaled 13.79 s,
with background work accounting for 11.91 s (86.37%). Those totals describe the
profiled loaded process, not library-only costs or uninstrumented throughput.

Optimizing public `InstanceID` alone did not remove the hot paths. The objecter
was obtaining identity through `AuthMetadata().GlobalID`, which still traversed
the metadata sanitization path. The actual hot path was subsequently replaced
with a scalar accessor, guarded by mode under the lock; the root `InstanceID`
path also uses that accessor. Correctness and zero-allocation accessor tests
pass. The final profiles have no sampled
`sanitizeTickets` allocation. Absence from a sampled profile is not proof of
zero allocations in every execution. The intermediate idle identity capture is
excluded from performance attribution.

The five idle baseline legs have these before/final medians:

- Allocated bytes: 200738992 before; 184206160 final (about 8.24% lower).
- Allocation count: 1700875 before; 1667928 final (about 1.94% lower).
- Run-level p99: 5.291260 ms before; 5.455154 ms final.
- IOPS: 19812.591 before; 20881.7628 final.

These observations support reduced allocation in this idle workload, not a
stable tail win: p99 worsened from about 5.291 to 5.455 ms. The captures are
unpaired and use separate clusters; they do not isolate
the change from run-to-run or cluster variation. Medians are across five
baseline legs, not pooled request percentiles.

Loaded baseline p99 was 24.138183 ms before and 25.102593 ms final. Allocated
bytes changed from 6181244368 to 5899390944. Background allocation duration
varied with execution duration, and the totals include harness work. These
loaded differences cannot be claimed as a library allocation reduction or
latency improvement.

## Inventory Boundaries

Eight-slot storage is enabled only for the diagnostic experiment. The production
default remains four slots with counters disabled. The production charged
scratch bound is 512 KiB; the experimental eight-slot maximum is 1 MiB. Scratch
eligibility remains 32 through 128 KiB, so the 4-KiB and 1-MiB payload cases do
not expand the eligible range.

Idle eviction is local to an inventory, not coordinated across connections.
These bounds do not establish a process-wide memory cap. Background workers
retain an eight-entry reachable ring; that describes reachable harness state,
not RSS or total lifetime allocation.

## Scale Method

`P07_READ_SCALE=1` runs a fixed ten-P sweep using the `p12diagnostics`-tagged
eight-slot-capable build. Each capture compares four and eight slots at
concurrency 16, 32, and 64. All 120 Go legs have eight warmup operations and 1024
measured operations **per worker**: 128/16,384, 256/32,768 and 512/65,536
warmup/measured operations per leg respectively. Five repetitions alternate
slot order and rotate
concurrency order, yielding 30 Go legs per capture.

The four final capture directories are:

- `/tmp/rados-go-read-scale-idle-final-v1`: 64-KiB idle.
- `/tmp/rados-go-read-scale-alloc-final-v1`: 64-KiB allocation load with eight
  background workers.
- `/tmp/rados-go-read-scale-small-final-v1`: 4-KiB idle.
- `/tmp/rados-go-read-scale-large-final-v1`: 1-MiB idle.

All four used identical captured sources and the same tagged benchmark binary
(SHA256 `259ba43c613c65d06f7ec127e43d67e8591f1359a82d41b762a9f793d3be91df`).
All 120 Go and 40 native legs have zero cgroup throttle-counter deltas; this
does not rule out host or hypervisor contention. Published copies omit binaries,
so the analyzer verifies their manifest identity rather than rehashing a binary.

Resource deltas include warmup and measured operations and include the harness
background workload. Native measurements provide shorter, unmatched context
at 64 KiB, concurrency 16, and 4096 operations. They are not matched controls for
the scale matrix: no native ratios or parity conclusions should be derived.

Request latency is measured in a closed-loop workload, not an open-loop arrival
model. Small and large payloads were measured only idle; 4-MiB live reads were
not measured in this follow-up. Captures use one host, three OSDs, and replication
two. This matrix does
not establish cross-host scalability, saturation behavior under independent
arrivals, or performance on other cluster layouts.

## Excluded Captures

Historical captures are preserved under `read-scale-evidence/excluded/{idle,alloc}`.
The original `/tmp/rados-go-read-scale-alloc-v1` attempt is incomplete, with a
`running` status, and is excluded. Its final row recorded 66048 eligible
requests and one bypass. A harness assertion requiring strictly zero bypasses
rejected that row. The harness check was corrected to require
`hits + misses >= expected`; the rejected capture remains preserved and is not
retroactively treated as successful.

The complete `/tmp/rados-go-read-scale-idle-v1` capture used the older identity
source. It is excluded from the final comparison table. Initial export-
contaminated traces and the intermediate idle identity profile are likewise
excluded from performance attribution. Original captures must never be
rewritten, repaired in place, or silently substituted for final captures.

## Scale Results

The [summary](read-scale-evidence/summary.json) validates the workload shape,
source identity and resource/counter consistency. Values below are medians of
five run-level measurements, not pooled request percentiles. Arrows compare
four to eight slots; p99 is in milliseconds and IOPS is rounded to whole units.

| 64-KiB load | Concurrency | p99 ms, 4 -> 8 | IOPS, 4 -> 8 |
| --- | ---: | ---: | ---: |
| Idle | 16 | 5.441237 -> 4.686067 | 21344 -> 22661 |
| Idle | 32 | 7.089913 -> 6.623453 | 24529 -> 27188 |
| Idle | 64 | 10.009553 -> 9.551009 | 25873 -> 28235 |
| Allocation | 16 | 22.965039 -> 22.108075 | 4482 -> 5241 |
| Allocation | 32 | 28.084524 -> 25.764012 | 5861 -> 6317 |
| Allocation | 64 | 34.350225 -> 33.015094 | 7333 -> 7879 |

Eight slots win idle p99 in 11/15 pairs and IOPS in 15/15; allocation-loaded
p99 and IOPS each win in 14/15. Combined, that is 25/30 p99 and 29/30 IOPS
wins, not a uniform tail improvement. Loaded median p99 improves by about
3.7%, 8.3% and 3.9% at concurrency 16/32/64, with IOPS about 17.0%, 7.8%
and 7.4% higher. Eligible 64-KiB hit fractions increase with eight slots, at
the cost of more charged retained scratch. Loaded allocation totals include
duration-dependent background work, not library-only allocation.

Both 4-KiB and 1-MiB cases have zero hits and misses, all reads bypass scratch,
and zero retained scratch bytes. Eight-slot-labelled runs win p99 in 10/15
small and 9/15 large pairs, with mixed IOPS (8/15 and 7/15 wins). These are
fallback-path variations, **not scratch-reuse benefits**. They do not expand
eligibility or justify promoting eight slots.

## Published Evidence

The evidence retains complete losslessly compressed request timing arrays,
original hashes and CPU/allocation pprofs. All five profile sets are published
under `read-scale-evidence/profiles/{idle,alloc,idle-identity,idle-final,alloc-final}`.
Raw traces and private binaries are omitted, with original hashes retained;
initial export-contaminated traces and the intermediate identity capture remain
excluded from performance attribution. Historical failed/incomplete captures
remain unchanged and excluded.

Captured source inventories predate the added scale analyzer. They bind capture
inputs, not the later analyzer or final documentation. The separate
`source-final-validated.sha256` records final validated source, while
`PUBLISHED.sha256` binds published artifacts. Neither renews live qualification
or replaces original capture identities.

## Reproduction

Analyze the published evidence without a new live capture:

```sh
node integration/p07/scale-summary.mjs docs/performance-p99-scheduler/read-scale-evidence
```

With Docker and the existing pinned P07 image available, run captures serially
with Go 1.27.1 and fresh absolute output directories. These commands are
reproduction examples, not commands executed while writing this document.
Do not reuse any original capture directory.

Normal 64-KiB idle scale capture:

```sh
P07_SCHEDULER_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 P07_READ_SCALE=1 \
  P07_DIAGNOSTIC_DIR=/tmp/rados-go-read-scale-idle-recapture-v1 \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

64-KiB allocation-loaded scale capture:

```sh
P07_SCHEDULER_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 P07_READ_SCALE=1 \
  P07_BACKGROUND_WORKERS=8 P07_BACKGROUND_ALLOCATIONS=1 \
  P07_DIAGNOSTIC_DIR=/tmp/rados-go-read-scale-alloc-recapture-v1 \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

Idle payload overrides:

```sh
P07_SCHEDULER_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 P07_READ_SCALE=1 \
  P07_READ_SIZE=4096 \
  P07_DIAGNOSTIC_DIR=/tmp/rados-go-read-scale-small-recapture-v1 \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh

P07_SCHEDULER_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 P07_READ_SCALE=1 \
  P07_READ_SIZE=1048576 \
  P07_DIAGNOSTIC_DIR=/tmp/rados-go-read-scale-large-recapture-v1 \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

For the separate production-default profile workflow, replace
`P07_READ_SCALE=1` with `P07_READ_PROFILE=1` and select a fresh profile output
directory. Leave read shape overrides unset for the 64-KiB, concurrency-16
profile. Add the same eight-worker allocation-load options for a loaded profile.
Profile and scale modes cannot be combined. Keep unrelated diagnostic mode
variables unset and hold source files unchanged throughout each capture.

## Qualification And Validation

The existing historical qualification gate remains open. These diagnostics do
not replace historical evidence, close the latency finding, or demonstrate
native parity. Production remains four slots with disabled counters; there is
no global parallelism tuning or change to runtime GC defaults. No commit is
created by this documentation work.

Full normal and diagnostic-tagged repository race suites passed on final
validated source, along with host vet, Linux ARM64 normal/tagged benchmark
builds and tagged Linux benchmark vet. All 13 focused editor tests and 59 Node
tests passed, including 26 scale-analyzer regressions. Shell syntax and
whitespace checks passed. Source formatting and analyzer additions after live
capture are bound separately, not represented as captured binaries.

Next: a small correctness-backed Phase 5 benchmark for owner-maintained
in-flight counts, and profiling of per-request `Encoder` allocations. Preserve
ordering, cancellation and replay invariants; measure before changing behavior.
The scheduler profiles do not justify rewriting channels.