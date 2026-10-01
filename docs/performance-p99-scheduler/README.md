# Secure Read P99: Parallelism And GC Investigation

For the repository/cache separation, see [Experiment Cache](EXPERIMENT_CACHE.md)
and the [compact verdict catalog](EXPERIMENT_VERDICTS.md). Full evidence bundles
are archived outside Git; deep evidence links require restoration. Summaries,
source identities, decisions and Linux measurement tooling remain in Git.

Status: initial diagnostic experiment completed without production changes,
no Phase 4 exit-gate closure, and no native parity or renewed qualification.
The experiment provides a repeatable deployment-level mitigation on this host.

For highly parallel applications where two global Ps are inappropriate, see
[the library-local batching and mixed-workload results](BATCHING_RESULTS.md).
The measured command-batching prototype was rejected and removed. The
[caller-buffer design](CALLER_BUFFER_DESIGN.md) specifies an allocation-reducing
path without global runtime tuning. Its bounded secure implementation is now
available; see [caller-buffer results and limitations](READ_INTO_RESULTS.md).
The [bounded inventory follow-up](INVENTORY_RESULTS.md) measured longer runs
with CPU and allocation-heavy host load, retained four slots and rejected
admission windows without changing global runtime parallelism.
The [read profiling and scale results](READ_SCALE_RESULTS.md) verify all four
final captures (120 Go and 40 unmatched native context legs, zero throttling).
Eight slots win 25/30 paired 64-KiB p99 comparisons and 29/30 IOPS comparisons,
but remain diagnostic-only; production stays at four with no automatic
promotion. Fallback sizes show no reuse benefit, and qualification remains open.

The [request-path results](REQUEST_PATH_RESULTS.md) publish both completed ABBA
offered-load matrices, including every failed row. Production retains encoding
allocation reductions and O(1) in-flight accounting for local gains only;
cumulative messenger ACK coalescing was rejected and removed. Neither matrix
establishes a stable causal p99 gain. Four slots, global runtime policy,
qualification status and existing guardrails are unchanged; these matrices did
not perform a matched native comparison.

The request-path cancellation audit retains operation, routed-attempt, OSD
submission and control ACK contexts for their distinct lifetimes. The rejected
cumulative messenger ACK experiment is separate from the completed Phase 1
backoff control-progress fix; see the [Phase 1 results](../performance-phase1/RESULTS.md).

## Current Diagnostic Follow-Up

The [external Linux retest of 2026-10-01](LIVE_RETEST_20261001.md) records
36 passing primary legs and twelve separate passing observed legs in each of
readcache and test-3x. Initial test-3x Ceph `EPERM` failures remain historical;
the authorized rerun passed. This is not native parity or closure of the
Phase 4 latency gate.

The later matched Ceph v20.2.4 closed-loop comparison in that report completed
forty valid read legs with Go/native median p99 ratios 0.899 (test-3x) and
0.957 (readcache). Native offered-load parity is still unmeasured; ancillary
manager-statistics denials leave the raw capture metadata incomplete.

The factorial follow-up separates CPU and allocation load into `none`, `cpu`,
`alloc` and `both` cases. Arrival-phase decomposition distinguishes scheduled
arrival/delivery delay, queue/read-entry delay, read/transport intervals and
the final outcome rather than interpreting end-to-end p99 as client CPU cost.
Opt-in observation enables execution tracing with per-read and separate
CPU/allocation tasks. Trace overlap is temporal correlation, not causal
attribution; primary and instrumented timing samples must remain separate.

See the [Linux live observation handoff](LINUX_LIVE_OBSERVATION_HANDOFF.md) for
the implemented opt-in lifecycle and operator procedure. Selected source and
binaries were frozen for the captures. See the
[factorial stall results and published queue/backoff evidence](FACTORIAL_STALL_RESULTS.md)
for completed captures, all failed legs, exact phase populations and publication
validation limits. No passing factorial result, stable live p99 gain, matched
native parity or Phase 4 gate closure is asserted.

Phase 5 queue/backoff burst benchmarks have also been performed, but the phase
is not complete: production has no PG index, targeted notification scheme or
queue-removal rewrite. The [factorial results](FACTORIAL_STALL_RESULTS.md)
publish the raw queue/backoff evidence and its source-binding limits. Known depth-4096 completion-burst
medians span 17.14-20.90 ms and cancellation-burst medians span 3.35-7.38 ms;
lookup with 4096 unrelated PGs has a 58.9 us median, and the 4096-waiter case
has a 241 ms median. These are synthetic costs, not live p99 attribution or
evidence that a production rewrite passes. Full latest-state validation has
not been established by this documentation update.

## Results

Three fresh isolated Ceph clusters on the same Docker Linux ARM64 VM (10 CPUs),
Go 1.27.1. Each cluster ran five serial repetitions of secure 64-KiB reads at
concurrency 16, 128 warmup operations and 4096 measured operations per leg.
Each repetition had native before/after brackets. P counts ran in ascending
order on odd repetitions, descending on even repetitions. `GOGC=100` and
`GOMEMLIMIT=off` were explicit. No concurrent validation workloads ran during
timing. Entries below are medians of five run-level p99 values, not pooled p99.

| GOMAXPROCS | Cluster A p99 ms | Cluster B p99 ms | Cluster C p99 ms |
| --- | ---: | ---: | ---: |
| 2 | 5.231 | 4.935 | 5.151 |
| 4 | 7.826 | 8.120 | 7.639 |
| 6 | 8.452 | 8.140 | 8.213 |
| 10 | 8.422 | 8.025 | 8.001 |

Two Ps reduced these median p99 values by approximately 38%, 39%, and 36%
relative to ten Ps. Median throughput at two versus ten Ps was 14967/8071,
15988/8112, and 15171/8845 IOPS. This is a substantial, repeated effect in this
specific short read workload, not evidence that two Ps is best for general
read/write, larger payloads, other concurrency levels, or other hardware.

All fifteen two-P sweep legs stayed below the conservative 8x comparison against
the faster native bracket in their repetition (ratios approximately 3.48-5.15).
Ten Ps had three misses across fifteen sweep legs: A repeat 4 (10.1168x), B
repeat 5 (8.4269x), C repeat 3 (8.0832x). Four and six Ps also had misses. All
misses remain recorded. These comparisons do not rerun the P12 comparator or
certify the Phase 4 default environment. No statistical significance or
cross-host equivalence is asserted.

## Quota And GC Evidence

Every measured sweep container reported `cpu.max = max 100000`, cpuset `0-9`,
and zero increments in `nr_throttled` and `throttled_usec`. **Cgroup CPU quota
throttling is not the cause supported by this capture.** Hypervisor/host
contention remains possible and is not measured by these counters.

Two versus ten Ps had median cumulative GC pauses of 57.5/148.2 ms in A,
44.7/130.9 ms in B, and 64.3/133.2 ms in C. This supports GC/runtime parallelism
as a contributor. It does not isolate a single GC or scheduling mechanism.

Separate two-P and ten-P traced runs followed each sweep; their timing is not
used in the table or the conservative guardrail comparison. Tracing materially
changed heap/GC behavior: for example, A traced two/ten-P executions reported
44/36 GC cycles versus about 94/83 in corresponding uninstrumented executions.
The trace includes warmup and post-measurement work, while resource deltas cover
warmup and measured reads; raw range totals therefore differ from those deltas.

The analyzer maps Go's trace `Sync Trace/Wall` clock to the request timing
timestamps using nanosecond integer arithmetic. All 41 requests in the slowest
1% of each of the six traced runs overlap a global GC stop-the-world interval.
Median overlap fractions were 62%/40% (two/ten Ps) in A, 12%/2% in B, and
38%/43% in C. The variation rules out claiming GC pauses explain every tail.
Temporal overlap is not causal attribution; no per-request assist/goroutine
attribution is made. Assist totals sum concurrent goroutine time and are not
elapsed wall time. C's two-P trace has one unfinished range, explicitly flagged
as incomplete coverage; only completed ranges are summarized, so overlap may be
underestimated. Other traces have complete range coverage.

Scheduler-delay profiles show channel/select wakeup sites and runtime/GC work.
These profiles aggregate time runnable goroutines waited after being unblocked;
they do not measure CPU spent inside channels or establish that replacing
channels with mutexes would fix the tail. No channel or receive-budget redesign
was justified from this evidence alone.

## Implemented Tooling

- Opt-in `P07_TRACE_FILE` captures Go execution traces only in read diagnostic
  mode. A checked writer retains errors and short writes; shutdown joins writer
  and file-close errors into the benchmark result. Trace-output unit tests pass.
- `P07_SCHEDULER_SWEEP=1` runs the serial repeated matrix with native brackets,
  cgroup settings/counters, timestamps, separate traces, binary hashes and source
  checks. Fresh absolute output is required; conflicting diagnostic modes reject
  before Docker setup. Required trace/timing outputs are checked.
- The summary validates implementation/mode/workload/size/concurrency/counts,
  numeric metrics and counters, positive native denominators, source stability,
  4096 unique valid request intervals, and reports trace boundary coverage.
  Six deterministic summary regression tests pass.

No application-owned receive buffer is pooled or reused. Reader size, receive
limits, operation semantics, GC settings in library code, and public APIs are
unchanged. All changes are diagnostic instrumentation and analysis.

## Reproduction

With Docker available and the existing pinned P07 image, run timing serially:

```sh
P07_SCHEDULER_SWEEP=1 P07_DIAGNOSTIC_DIR=/tmp/p99-fresh-sweep \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
GOTOOLCHAIN=go1.27.1 go tool trace -d=parsed \
  /tmp/p99-fresh-sweep/trace-procs-2.trace > /tmp/p99-fresh-sweep/trace-procs-2-events.txt
GOTOOLCHAIN=go1.27.1 go tool trace -d=parsed \
  /tmp/p99-fresh-sweep/trace-procs-10.trace > /tmp/p99-fresh-sweep/trace-procs-10-events.txt
node integration/p07/scheduler-summary.mjs /tmp/p99-fresh-sweep > /tmp/p99-fresh-sweep/summary.json
GOTOOLCHAIN=go1.27.1 go tool trace -pprof=sched \
  /tmp/p99-fresh-sweep/trace-procs-10.trace > /tmp/p99-sched.pprof
GOTOOLCHAIN=go1.27.1 go tool pprof -top /tmp/p99-sched.pprof
node --test integration/p07/scheduler-summary.test.mjs
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./integration/p07/benchmark
GOTOOLCHAIN=go1.27.1 go test -tags p12diagnostics ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=go1.27.1 \
  go vet ./integration/p07/benchmark
```

The cluster is removed after execution; capture directories retain results.
Malformed/preflight cases and Linux ARM64 cross-build were validated as well.

## Evidence And Next Step

[Manifest](evidence/manifest.json), [A summary](evidence/a/summary.json),
[B summary](evidence/b/summary.json), [C summary](evidence/c/summary.json), and
[SHA256 inventory](evidence/SHA256SUMS) preserve all outcomes. Full request timing
arrays are losslessly compressed. Private binaries, execution traces, decoded
trace events, cluster keys/configuration, and full Docker info are omitted;
binary/trace/event hashes are retained. This is not a complete reproduction
bundle for trace attribution. A/B source inventories began after compilation;
C binds selected inputs before build and verifies them unchanged after the
sweep. Later analyzer regression tests are separately hashed. These runs do not
replace historical Phase 4 evidence or its open latency finding.

Operational next step: test `GOMAXPROCS=2` at the application/deployment level for
this workload, then expand duration, concurrency and payload sizes before
adopting it. Do not set process-global runtime policy from a client library.
Library-side next step remains reducing allocation pressure with verified
ownership, or studying the GC/runtime behavior more deeply. The later
[request-path results](REQUEST_PATH_RESULTS.md) record completed local encoding
and accounting gains, but not a stable causal p99 improvement. The current
factorial arrival-phase and Linux observation follow-up above remains pending;
it does not supersede the historical sweep results or close qualification.