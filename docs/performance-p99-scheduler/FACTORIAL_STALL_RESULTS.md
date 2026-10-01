# Factorial Offered-Load And Stall Evidence

Status: diagnostic evidence, not a passing benchmark, production tail fix,
native parity result, Phase 4 exit-gate closure or renewed P12/P13 qualification.
No production implementation or global runtime policy is changed by this work.

## Experiment

Two independent fresh-cluster primary captures, `primary-v3` and `primary-v4`,
each contain five rotated repetitions of all four cases at 1,000, 2,000 and
4,000 scheduled arrivals/second. Each leg offers arrivals for eight seconds,
reads 64 KiB using `ReadInto`, uses 16 read workers, a queue capacity of 128,
four scratch slots and 128 warmup reads. Runtime controls are fixed at ten Ps,
`GOGC=100`, `GOMEMLIMIT=off`; Linux ARM64, Go 1.27.1, secure transport.

Cases are `none`, eight continuously hashing `cpu` workers, eight independently
paced `alloc` workers, and `both` with **16 background workers: eight CPU plus
eight allocation workers**. Each allocation worker schedules 100 allocations
of 64 KiB/second during issuance and retains up to eight payloads. Completed
allocation work, missed allocations, lateness, and warmup/window/drain hashing
are independently reconciled. Retained bytes in the summary are inferred from
the workload, not measured live heap. Resource deltas include warmup, issuance,
drain, background join and harness, not just library work.

The historical loaded workload coupled CPU and allocation work in eight
workers. It is not this factorial `both` case; no historical-baseline comparison
is made. `none` is arrival-rate capped, not a client capacity measurement. There
is no paired native offered-load measurement. Two clusters and rotated serial
repetitions provide descriptive replication, not statistical significance or
cross-host generality.

The separate `observed-v2` capture contains three repetitions of four cases at
2,000 arrivals/second, with execution tracing and request timing. Its 12 rows
are not pooled into the primary p99 summaries. Early `primary-v1`, `primary-v2`
and `observed-v1` stopped during setup: their logs report the missing
`docs/p00/evidence.json` image manifest. Their nonzero exits, unchanged source
manifests and actual source archives are retained; they contain no measured
rows and cannot be substituted with zero-latency successes.

## Statistics

All reported p99 values use nearest rank within one leg. Case/rate comparisons
use medians of ten run-level p99 values, not a pooled arrival percentile.
An eight-second leg offers 8,000/16,000/32,000 arrivals at the three rates;
case/rate totals therefore cover 80,000/160,000/320,000 arrivals respectively.

### Exact Primary Medians

Values below are nanoseconds, including exact half-nanosecond medians where
the two middle run-level values differ. Phase columns use attempted reads.
Each row contains ten runs and includes workload-invalid/capture-failed legs.

| Rate | Case | Total p99 | Delivery p99 | Queue p99 | Dispatch p99 | Service p99 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1000 | none | 17897174.5 | 11017378.5 | 2745898 | 54938 | 10530162.5 |
| 1000 | cpu | 12146400.5 | 6016361.5 | 1713416.5 | 14729.5 | 8993956.5 |
| 1000 | alloc | 16718561 | 11118917 | 2206853 | 309667.5 | 10894535 |
| 1000 | both | 16359802 | 6634386 | 1913422 | 5337495 | 10689361.5 |
| 2000 | none | 17566271.5 | 10500388 | 3916500 | 32542 | 9868506 |
| 2000 | cpu | 13763411 | 5747217 | 3296631.5 | 11417 | 10811633.5 |
| 2000 | alloc | 16605025.5 | 10326808 | 3470396 | 86896 | 9955349 |
| 2000 | both | 14841952.5 | 5782703 | 4254161.5 | 44042 | 11675754 |
| 4000 | none | 5233365.5 | 4010143.5 | 592646.5 | 8063 | 1893838.5 |
| 4000 | cpu | 21019630.5 | 6087059 | 14070396.5 | 7271 | 11642263.5 |
| 4000 | alloc | 5274622.5 | 4073089.5 | 586668.5 | 11604.5 | 1899754.5 |
| 4000 | both | 21053837.5 | 6583016.5 | 15108648.5 | 6979.5 | 12188696.5 |

Success-only p99 medians equal total medians except `cpu` at 4,000/s:
20,030,825.5 ns versus total 21,019,630.5 ns. Its failure-aware median is
21,286,746 ns and four individual runs have infinite failure-aware p99.
`both` at 4,000/s has two infinite runs; all other rows have zero. Every
case/rate failure-aware median remains finite, which is not evidence of
zero failures or a passing leg. Reached/attempted/success populations and
their separate exact medians remain in the machine-readable summary.

### Counts And Validity

| Rate | Case | Workload Valid / Invalid | Capture Failed | Success | Overload | Delivery Invalid | Background Invalid | Allocation Misses |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1000 | none | 10 / 0 | 10 | 80000 | 0 | 0 | 0 | 0 |
| 1000 | cpu | 10 / 0 | 10 | 80000 | 0 | 0 | 0 | 0 |
| 1000 | alloc | 10 / 0 | 10 | 80000 | 0 | 0 | 0 | 0 |
| 1000 | both | 10 / 0 | 0 | 80000 | 0 | 0 | 0 | 0 |
| 2000 | none | 10 / 0 | 10 | 160000 | 0 | 0 | 0 | 0 |
| 2000 | cpu | 9 / 1 | 10 | 159876 | 124 | 0 | 0 | 0 |
| 2000 | alloc | 10 / 0 | 10 | 160000 | 0 | 0 | 0 | 0 |
| 2000 | both | 9 / 1 | 1 | 159942 | 58 | 0 | 0 | 0 |
| 4000 | none | 8 / 2 | 10 | 319979 | 21 | 0 | 0 | 0 |
| 4000 | cpu | 5 / 5 | 10 | 317309 | 2691 | 3 | 0 | 0 |
| 4000 | alloc | 9 / 1 | 10 | 319768 | 232 | 0 | 0 | 0 |
| 4000 | both | 7 / 3 | 3 | 318875 | 1125 | 0 | 2 | 24 |

Across 120 primary rows: 2,240,000 expected arrivals, 2,235,749 admitted,
attempted and successful reads, and 4,251 nonattempted overload rejections.
Timeouts, read errors, cancellations and deadline misses are all zero.
There are 107 workload-valid and 13 invalid rows, three delivery-invalid rows,
two background-invalid rows and 24 missed allocations. Each phase's attempted
and success population totals equal that row's success count; delivery reached
counts equal expected arrivals and queue reached counts equal admissions.

| Capture | Rows | Workload Valid / Invalid | Capture Valid / Failed | Overload | Delivery Invalid | Background Invalid | Allocation Misses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| primary-v3 | 60 | 51 / 9 | 11 / 49 | 2884 | 1 | 2 | 24 |
| primary-v4 | 60 | 56 / 4 | 15 / 45 | 1367 | 2 | 0 | 0 |
| observed-v2 | 12 | 10 / 2 | 2 / 10 | 179 | 0 | 2 | 0 |

Observed rows separately offer 192,000 arrivals with 191,821 admitted,
attempted and successful reads, and 179 nonattempted overload rejections.
Timeouts, errors, cancellations and deadline misses are zero there too.
Background-invalid observed rows exceed the lateness limit without missing
allocations. All three completed capture headers remain `diagnostic-failed`.

The frozen shell harness directly compared `.diagnostic.background_cpu_workers`
to zero and `.diagnostic.background_allocations` to false, although Go's
`omitempty` encoding omits those zero-valued fields. Thus all `none`, `cpu`
and `alloc` legs fail harness qualification independently of their outcome
contract. The analyzer checks the actual producer encoding strictly and retains
the original capture failures: 94 primary failures versus 13 workload-invalid
rows, and ten observed failures versus two workload-invalid rows. No capture
status is rewritten, and no failed row is removed from descriptive medians.

All-outcome latency is scheduled arrival to return. Success-only p99 keeps its
own population. Failure-aware p99 assigns infinity to every non-success or
deadline miss before ranking the expected arrival population; infinity is
represented by a status and JSON null, never silently dropped. A successful
return at or after 500 ms still misses the deadline. Delivery validity requires
maximum delivery delay at most 50 ms; allocation misses or excessive background
lateness independently fail the leg. Successful reads in an invalid leg remain
in the descriptive summary, with validity reported alongside them.

Phases are delivery (scheduled arrival to earliest enqueue timestamp), queue
(that timestamp to worker start), dispatch (worker start to read entry), and
service (read entry to return). Attempted reads reconcile their individual
phase sum exactly to arrival latency. The enqueue timestamp is captured before
the nonblocking send, not a measurement of an atomic queue insertion. Service
includes scheduling, runtime/GC, transport and server waiting, not just client
CPU. Rejected arrivals do not reach queue/service; admitted nonattempts do not
reach read entry. Missing phases retain `-1`, not zero. Reached, attempted and
successful phase populations are reported separately. **Phase p99s must not be
added or interpreted as a decomposition of total p99.**

Each row retains its 50 worst attempted arrivals and the largest sample for
each reached phase, including every timestamp and outcome. Largest-phase ties
are assigned to the earliest arrival index. Dominant-stage counts among the
worst attempts count a tie in every tied phase; they are descriptive, not
causal attribution.

Largest reached primary phase samples are on different arrivals:

| Phase | Capture / Row | Arrival Index | Phase ns | Total ns |
| --- | --- | ---: | ---: | ---: |
| delivery | v3 / repeat 2 cpu 4000 | 20074 | 103512533 | 157934849 |
| queue | v3 / repeat 3 cpu 4000 | 23156 | 252415026 | 258883140 |
| dispatch | v4 / repeat 3 both 2000 | 6740 | 21782739 | 27283288 |
| service | v3 / repeat 4 both 4000 | 15055 | 137352381 | 163676371 |

These are maxima, not phase quantiles or one common worst request. The complete
timestamps and individual phase identities are preserved in `largest_phase_samples`.

## Quota And Trace Limits

Cgroup before/after counters and unlimited `cpu.max` are verified per row.
All 132 measured legs have zero increments in `nr_throttled` and
`throttled_usec`. This is evidence against quota throttling on these legs.
Zero quota-throttling increments cannot rule out hypervisor scheduling or VM
host contention. Original OSD before/after snapshots are retained, but are not
per-request network/server attribution. Instrumented runs can perturb the
behavior being observed.

All twelve original runtime traces and timing arrays are published losslessly.
The independent trace agent completed actual Go 1.27.1 high-level decoding and
trace-stall-summary API analysis of all twelve directories, on darwin/arm64 for
linux/arm64 captures. Its complete linked results, decoder logs, stage audits,
analyzer snapshot and before/after original hashes are published separately.
Decoding required a bounded 1-GiB output limit beyond the old 256-MiB default.
All twelve original parser replays failed with `reversed transport interval`;
the corrected independent analysis retains signed writer/receiver gaps instead
of discarding them or turning them into zero. Earlier empty CLI results were
not successful analysis. Original failed capture/analysis exits remain evidence.
Actual request arrays have no ReadFrame begin/end timing: `frame_received` is
a read-pump completion timestamp, not wire arrival. ReadFrame duration and
`write_end->read_frame_end` are unavailable, not inferred from that timestamp.
Temporal GC or runnable overlap is not proof that those mechanisms caused a
primary p99. Tagged caller scheduling is not transport-goroutine CPU cost.
Large decoded text can remain outside the bundle because the complete raw
trace is retained, rather than omitting the evidence needed to decode again.

The independent [trace summary](factorial-stall-evidence/trace-analysis/summary.json.gz)
links 191,821 attempted reads to trace tasks and preserves 179 overloads with
no task. Worst-service cohorts contain the largest `ceil(1% * attempted)`
service samples, with stable arrival-index ties. Across twelve legs they total
1,919 calls. Fractions below divide summed per-call temporal overlap by summed
per-call service duration in that leg's cohort; concurrent calls can include
the same wall instant. Midpoint percentages are rounded to three decimals;
conservative alignment bounds remain in the linked results.

| Repeat | Case | Attempted | Cohort | Service p99 ns | GC STW % | Caller Runnable % | Combined Union % |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | none | 16000 | 160 | 10674501 | 3.572 | 6.549 | 9.776 |
| 1 | cpu | 16000 | 160 | 11029252 | 0.176 | 6.657 | 6.811 |
| 1 | alloc | 16000 | 160 | 9693163 | 4.874 | 5.817 | 9.215 |
| 1 | both | 15821 | 159 | 11822422 | 14.609 | 1.792 | 16.388 |
| 2 | none | 16000 | 160 | 10483917 | 2.014 | 4.055 | 6.040 |
| 2 | cpu | 16000 | 160 | 7615572 | 1.388 | 5.161 | 5.936 |
| 2 | alloc | 16000 | 160 | 10537626 | 0.057 | 4.827 | 4.882 |
| 2 | both | 16000 | 160 | 11178252 | 1.093 | 4.003 | 4.809 |
| 3 | none | 16000 | 160 | 11718004 | 0.065 | 5.965 | 6.010 |
| 3 | cpu | 16000 | 160 | 10356874 | 1.798 | 5.928 | 7.474 |
| 3 | alloc | 16000 | 160 | 12637508 | 1.495 | 2.828 | 4.320 |
| 3 | both | 16000 | 160 | 10346874 | 0.217 | 6.055 | 6.268 |

Combined midpoint overlap ranges from 4.320% to 16.388%. GC and caller runnable
intervals can overlap, so their percentages are not additive. Neither-overlap
time remains unknown, not evidence of network delay or transport CPU. Offered
delivery/queue time precedes the tagged read call and has no caller attribution.
No causal primary p99 attribution is made.

## Queue And Backoff Microbenchmarks

All 69 cases, raw repetitions, the supplied median TSV/generator, queue-depth
128 follow-up, smoke output and race output are retained under `microbench`.
Depth-4096 completion burst medians are 17.135765, 20.904602 and 19.254181 ms
for head, reverse and random order. Corresponding cancellation medians are
3.345448, 7.381921 and 6.180236 ms. Depth-128 completion medians are
32.091/36.333/35.356 us and cancellation medians 45.503/41.207/39.683 us.
These are batch costs with benchmark reset/setup policy stated in each name,
not per-request live latency percentiles.

With 4,096 registered backoff ranges, the one-waiter rescan median is 60.017 us;
the 4,096-waiter batch median is 241.256916 ms (58.900614 us/request derived by
division, not a request percentile). The synthetic workloads demonstrate local
scaling costs. Their stdout lacks contemporaneous source/binary hashes, so
publication digests preserve original output identity without claiming these
microbenchmarks are cryptographically bound to the live-capture source.

Queue-removal indexing and PG indexing/targeted backoff notification remain
deferred production candidates. The largest microbench depths do not establish
their contribution to the measured read tails, and these live read captures do
not measure backoff storms. Validate production-scale mixed workloads, actual
queue depths and backoff exposure before promoting either optimization. No
channel rewrite or global runtime tuning is justified from delay overlap alone.

## Evidence And Verification

The [evidence bundle](factorial-stall-evidence/publication.json),
[summary](factorial-stall-evidence/summary.json),
[source binding](factorial-stall-evidence/source-binding.json) and
[complete inventory](factorial-stall-evidence/PUBLISHED.sha256) retain all
outcomes and capture failures. Original manifests are unchanged. Every original
artifact is verified before publication; every retained original is reverified
after lossless gzip decompression. Each actual source tar archive is retained,
with exact path-set and file-byte checks against its captured source manifest.
Source before/after checks remain authoritative, not the later workspace.
Workload image references are verified against the retained P00 qualification
manifest. Only duplicate benchmark executables are omitted after initial actual
byte verification; their digests and explicit non-reverification status remain.

`PUBLISHED.sha256` covers every file in the bundle except itself, including raw
artifacts, source archives, tool snapshots, publication metadata and supplemental
originals. Tool snapshots describe publication-time tooling, not historical
workload source. Coordinator-owned final-source and validation sidecars can be
appended with `addSupplements`, which verifies the existing bundle before
rebuilding and checking the complete inventory.

Six source archives retain every source artifact: 297 files per completed
capture and 296 per setup-failed archive. The completed three share exact
source-manifest SHA256
`4bc52cc3eb1ed51174bbd2f0c506e41b4d58ee7dade8ba5322de9254d8200a33`.
Their P00-bound arm64 workload image is
`quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa`.
There are 2,880 original artifact entries: 2,874 available original artifacts
are reverified from publication and six executables are initially verified,
explicitly omitted. All 132 original JSON outcome arrays remain lossless.

```sh
node --test integration/p07/factorial-summary.test.mjs \
  integration/p07/publish-factorial.test.mjs
node integration/p07/publish-factorial.mjs --verify \
  docs/performance-p99-scheduler/factorial-stall-evidence
```

The copied analyzer was tested immediately before broader analysis. Real
captures exposed incorrect fixture assumptions about Go's `omitempty` fields
and a top-level matrix selector accidentally matching nested observation JSON;
both were fixed with regressions, without weakening outcome reconciliation or
discarding failures. The focused analyzer/publisher suite currently has 28
passing tests. Final coordinator validation passed full normal and diagnostic
race suites, host vet in both modes, Linux ARM64 and amd64 builds, and the amd64
diagnostic build. Linux-only observation tests passed three race repetitions on
Linux. All 155 P07 Node tests, shell syntax, editor diagnostics and whitespace
checks pass. The validated Linux amd64 preparation capture is
`/tmp/rados-go-linux-amd64-factorial-ready-v2`; its build/source binding must
accompany the staged binary. These correctness checks do not close qualification.