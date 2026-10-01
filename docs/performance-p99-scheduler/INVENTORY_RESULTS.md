# Bounded receive inventory and read admission experiments

Status: four-slot secure receive inventory retained as the default. ReadInto
retained. Global GOMAXPROCS, GOGC and GOMEMLIMIT are not changed by the library.
Admission windows were measured and rejected as a production change. Phase 4
qualification remains open; no P12/P13 renewal or native parity claim.

## Implementation

The former one-slot receive scratch cache now has fixed storage for at most
four free buffers per built-in secure connection. Eligible remaining-record
sizes remain 32-128 KiB, with 8-KiB capacity rounding. Maximum idle backing is
512 KiB per transport, charged to the existing shared aggregate receive budget.
At 256 connections this is a possible 128 MiB of that budget, not an additional
uncharged allowance. Active/queued frames, connection readers, metadata and
caller-owned outputs remain separate as previously documented. The configured
aggregate byte limit is unchanged. Actual idle occupancy can be lower.

Plaintext is wiped before return; ordinary Read still permanently transfers
its backing rather than recycling it. Borrowed ReadInto holds ownership through
validation and copying. Close disables return to retired generations. Exact
allocation fallback and local idle eviction remain available under budget
pressure. Eviction is local to the reading transport, not a cross-client cache
reclaimer: another connection can still hit aggregate saturation while idle
buffers remain elsewhere. Saturation remains terminal and observable; these
tests do not establish maximal cross-connection admission under pressure.

Inventory/counter configuration is available through p12diagnostics-only public
hooks, before connection admission, for measurement. Normal builds have no
new user configuration and keep hit/miss instrumentation disabled. Counters
are client-wide: hits/misses describe eligible backing allocations, bypasses
describe size/budget fallback, and shared_receive_retained_bytes is the whole
receive-budget snapshot, not idle scratch alone. Counters are sampled at the
workload endpoint, before cleanup/timing export.

## Methodology

Three fresh isolated Ceph v20.2.4 clusters on the same ten-CPU Linux ARM64 Docker
VM, Go 1.27.1. Each runs five repetitions of five variants, rotating first
position each time: slots/window 1/16, 2/16, 4/16, 1/8 and 1/4. All use the same
diagnostic-tagged binary and captured sources, ReadInto, secure 64-KiB payloads,
16 caller workers, 128 warmup and 16,384 measured reads per leg. Payloads are
validated after every read. GOMAXPROCS=10, GOGC=100, GOMEMLIMIT=off are explicit.
No concurrent validation commands ran during capture.

Loads: idle; eight same-process CPU hashing workers; and eight hashing workers
also allocating 64 KiB every 64 iterations. The allocation workload retains an
eight-buffer ring per worker (512 KiB of reachable payload each), plus transient
replacement storage. This is bounded reachable payload, not bounded Go heap or
RSS. Background startup precedes the resource baseline; workers are joined
after the final snapshot. Resource deltas include warmup, harness, measured
reads and host workload. RSS is process-lifetime high-water. Under allocation
load, a slower read variant also lets background workers run longer: total
allocated bytes are not a library-only comparison or a fixed amount of host
work. No constant offered-load or long-duration soak claim is made.

Admission gates are shared by all sixteen caller workers. Row timing begins
before acquiring the gate, so waiting is included in end-to-end p99. No
production admission control was added. Native before/after brackets remain
unloaded and use 4096 reads, making them shorter context, not matched-duration
or loaded parity. Preliminary ABBA and separate traced legs are excluded from
the variant results.

## Results

Medians of five run-level measurements, not pooled p99:

| Load | Slots | Window | p99 ms | IOPS | Hit Rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle | 1 | 16 | 7.631 | 11460 | 42.5% |
| Idle | 2 | 16 | 6.622 | 15734 | 72.2% |
| Idle | 4 | 16 | 5.361 | 20316 | 93.7% |
| Idle | 1 | 8 | 7.822 | 10764 | 50.8% |
| Idle | 1 | 4 | 7.789 | 9276 | 61.7% |
| CPU | 1 | 16 | 18.297 | 4132 | 39.7% |
| CPU | 2 | 16 | 16.460 | 5089 | 73.4% |
| CPU | 4 | 16 | 13.699 | 6350 | 96.8% |
| CPU | 1 | 8 | 20.858 | 2902 | 47.2% |
| CPU | 1 | 4 | 25.552 | 2131 | 62.4% |
| Allocation | 1 | 16 | 27.701 | 2810 | 41.5% |
| Allocation | 2 | 16 | 26.771 | 3163 | 69.7% |
| Allocation | 4 | 16 | 24.383 | 4397 | 90.2% |
| Allocation | 1 | 8 | 38.737 | 1658 | 48.6% |
| Allocation | 1 | 4 | 49.300 | 968 | 61.9% |

Four versus one slot reduces median p99 by 29.7% idle, 25.1% CPU-loaded and
12.0% allocation-loaded; throughput rises 77.3%, 53.7% and 56.4%. Both metrics
improve within every repetition under all three loads (15/15). This is stronger
evidence than the earlier small one-slot ReadInto p99 change, still limited to
one host, payload, concurrency and short closed-loop workload. Two slots show
smaller gains, with p99 improving in 13/15 repetitions. All samples, not only
improving repetitions, remain in summary.json.

Idle allocated volume falls from median 825.0 MB to 203.1 MB with four slots;
CPU-only from 859.4 MB to 164.8 MB. Under allocation churn, process totals fall
9197.4 MB to 5542.7 MB, but include different durations of background work and
must not be attributed solely to receive allocation. Admission windows reduce
reuse misses but slow reads enough to increase loaded total churn and tails.
Neither window improves IOPS in any repetition; window four improves p99 in
only two idle repetitions and none of the loaded ones. No admission default
or public admission API is retained.

A fourth fresh cluster confirmed the untagged production default, with no slot
override or counters, under eight allocation workers and 16,384 reads per leg:
five-run median p99 23.414 ms, IOPS 4461. This is consistent with the four-slot
diagnostic result, not a paired test against another default on that cluster.
All eighty measured Go legs have zero cgroup throttling increments.

## Validation and Evidence

Full normal and p12diagnostics race suites, repeated inventory/ownership/retry
race tests, host vet, Linux ARM64 normal and diagnostic builds, seven strict
summary regression tests and shell preflight/syntax checks passed. Independent
review found no blocking issue. Fixed-capacity accounting, wiping, old-generation
borrows and unchanged ordinary Read ownership remain covered. Broad fanout,
cross-connection idle eviction, other record sizes, sustained runs and
deployment hardware remain qualification gaps.

[summary.json](inventory-evidence/summary.json) retains all eighty samples,
medians, paired win counts and resource/cgroup deltas. Sources match before and
after each capture, and all three variant captures share the same source set.
Production confirmation is separately bound: it includes the four-slot default
and subsequent test/harness confirmation additions. Later formatting, summary
tests and documentation are not represented as captured binaries.

Original artifact lists retain binary and trace hashes; those large artifacts
are omitted from publication, while complete timing arrays are gzip-compressed.
PUBLISHED.sha256 binds actual published files. Final validated source has its
own inventory, not a renewed live qualification identity. failed-idle preserves
the first incomplete attempt: a valid first leg failed a harness assertion
because JSON omitted false allocation mode. Its original status is running,
not passed; it is excluded from results and was replaced by a fresh capture.

```sh
node integration/p07/inventory-summary.mjs docs/performance-p99-scheduler/inventory-evidence
node --test integration/p07/inventory-summary.test.mjs
P07_SCHEDULER_SWEEP=1 P07_INVENTORY_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 \
  P07_BACKGROUND_WORKERS=8 P07_BACKGROUND_ALLOCATIONS=1 \
  P07_DIAGNOSTIC_DIR=/tmp/fresh-inventory-capture GOTOOLCHAIN=go1.27.1 \
  ./integration/p07/reproduce.sh
```

Use workers zero and allocation mode zero for idle; workers eight and allocation
zero for CPU-only. Set P07_DEFAULT_SCRATCH_CONFIRM=1 instead of inventory mode
for untagged default confirmation. Do not combine competing diagnostic modes.