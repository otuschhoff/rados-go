# Native Linux Librados Parity Phase

Status: in progress; no CPU, RAM, latency or throughput parity qualification is
claimed. The earlier source-bound Linux write qualification compares current
Go with historical Go, not librados, and remains a separate result.

## Measurement Contract

The diagnostic runner is [parity.mjs](../../integration/p07/parity.mjs).
It uses the approved test-3x/readcache pools on native Linux/amd64 with secure
Ceph 20.2.4, Go GOMAXPROCS=10/GOGC=100/GOMEMLIMIT=off and the same process-local
ten-CPU affinity for both clients. It does not change host defaults, caps,
queue/receive limits, cluster policies or OSD/MON lifecycle.

Both drivers use a fresh exclusive p07-parity namespace, identical deterministic
object names, payload seed 1, eight warmups per worker, and equivalent complete
calls. Reads use caller buffers: ReadInto versus rados_read. Allocating Read is
a separate API contract, not silently mixed into caller-buffer parity.

Six fixed alternating ABBA/BAAB blocks per pool/cell include two legs/client.
The twelve cells cover 1-MiB c1 write/mixed, 4-MiB c1/c16 write, 64-KiB c16 read,
and 4-KiB c1 read. This is 288 primary timing legs and 3686400 timed operations.
Four separate secure-mode probe legs are excluded. Diagnostic results cannot
be promoted to an acceptance sample because their ratios look favorable.

CPU counters bracket the all-worker timing barrier after setup/warmup and
before final verification/cleanup. They include timed calls, scheduler work
and per-read payload validation. RSS is observed after warmup, after all timed
workers finish and after cleanup. Peak process RSS remains a distinct lifetime
metric. Go post-GC live heap is explicitly labeled and is not native retained
heap. Native retained-heap and long-duration growth remain open checks.

Every successful timed leg must have the exact operation count and positive
ordered quantiles. After all timed workers finish, every final full payload
must match, exact object removal must succeed, and NotFound must be observed.
Placement snapshots must match before/after each cell. Health categories and
PG states are checked outside timing before every block. Any command,
verification, placement or health gate failure stops and retains the capture;
failed legs cannot be silently replaced. Observation is not continuous proof
of every map transition or hidden host contention.

Source, driver binary and actual librados hashes are pinned before/after.
Actual secure monitor/OSD modes are checked using the existing mode verifier
in separate probes. Raw data, native logs and credentials stay outside Git.
The first diagnostic capture is retained privately at
/root/proj/rados-go/native-parity-20261002-a.

## Open Phase Gates

1. Complete and analyze the fixed diagnostic sample, including failures and
   environment/resource validity. Identify stable gaps rather than attributing
   historical varying-placement ratios to the library.
2. Profile confirmed slow cells separately from primary timing. Implement only
   locally demonstrated defects/overhead reductions, preserving payload
   ownership, replay, cancellation, ordering and immutable-map contracts.
3. Implement matched bounded native/Go offered-load measurements with the same
   arrival schedule, queue/worker limits and outcome accounting. Compare IOPS
   and MiB/s at the same latency budget; closed-loop results are not capacity
   or saturation certification. Production-safe ceilings must remain explicit.
4. Measure memory growth/retention over repeated in-process workload windows,
   not just fresh-process peak RSS or Go allocation totals. Native allocator
   and Go GC observations must be labeled rather than equated.
5. Predeclare independent source-bound qualification samples and explicit
   CPU/op, resident-memory, p99 and throughput budgets. Require every scoped
   metric and correctness/environment gate to pass; preserve inconclusive or
   failing bounds without changing budgets to force acceptance.
6. Reevaluate all findings and publish a truthful phase verdict. Other
   architectures, environments, destructive recovery and arbitrary workloads
   cannot inherit these results; they need separate authorized execution.

The phase is not complete while these gates or confirmed performance findings
remain open. A commit of tooling alone must not be labeled parity completion.

## First Controlled Findings

Capture A completed 288 valid primary legs and 3686400 timed operations in
72 fixed blocks. All payload/cleanup, observed placement, secure-mode,
source/binary/library identity and visible cgroup throttling/OOM gates passed.
These are matched diagnostic results, not independent parity qualification.

| Pool | Shape | CPU/op Go/native | IOPS Go/native | P99 Go/native | Timed-End RSS Go/native |
| --- | --- | ---: | ---: | ---: | ---: |
| test-3x | 1-MiB c1 write | 5.967 | 0.696 | 1.613 | 0.671 |
| readcache | 1-MiB c1 write | 6.199 | 0.571 | 2.158 | 0.645 |
| test-3x | 1-MiB c1 mixed | 3.036 | 0.803 | 1.574 | 0.611 |
| readcache | 1-MiB c1 mixed | 3.029 | 0.753 | 1.692 | 0.548 |
| test-3x | 4-MiB c1 write | 3.326 | 0.753 | 1.511 | 1.000 |
| readcache | 4-MiB c1 write | 3.322 | 0.771 | 1.436 | 1.010 |
| test-3x | 4-MiB c16 write | 2.231 | 0.904 | 1.209 | 0.704 |
| readcache | 4-MiB c16 write | 2.190 | 0.968 | 1.000 | 0.918 |
| test-3x | 64-KiB c16 read | 1.148 | 1.041 | 1.086 | 0.807 |
| readcache | 64-KiB c16 read | 1.089 | 0.998 | 1.134 | 0.840 |
| test-3x | 4-KiB c1 read | 2.480 | 0.873 | 1.279 | 0.495 |
| readcache | 4-KiB c1 read | 2.425 | 0.906 | 1.192 | 0.494 |

Ratios are geometric means of paired mirrored-block ratios, not pooled
request samples. Lower CPU/RSS/p99 and higher IOPS are better. RSS observations
do not prove retained-heap parity or bounded long-duration growth.

A separate 1-MiB readcache write CPU/allocation profile passed the final
payload/cleanup gates. Copying, encryption, syscalls, clearing and GC work
dominate sampled CPU. Roughly one payload-sized allocation each occurs in
mutation admission, OSD encoding, messenger cloning and secure encoding.
The admission copy preserves caller ownership; it is not removed merely
because it is expensive.

The first candidate optimization explicitly transfers freshly encoded objecter
message buffers to messenger admission, removing its redundant clone while
ordinary submissions remain defensive. Transfer/default-copy, rejection,
cancellation/index cleanup and messenger/objecter race suites pass. A fresh
matched Capture B completed at /root/proj/rados-go/native-parity-20261002-b
using the same fixture namespace/placement: 72 blocks, 288 primary legs and
3686400 operations, with no capture failures. Its paired geometric means are:

| Pool | Shape | CPU/op Go/native | IOPS Go/native | P99 Go/native | Timed-End RSS Go/native |
| --- | --- | ---: | ---: | ---: | ---: |
| test-3x | 1-MiB c1 write | 4.229 | 0.809 | 1.260 | 0.664 |
| readcache | 1-MiB c1 write | 3.958 | 0.705 | 1.869 | 0.658 |
| test-3x | 1-MiB c1 mixed | 2.786 | 0.851 | 1.347 | 0.559 |
| readcache | 1-MiB c1 mixed | 2.718 | 0.814 | 1.585 | 0.542 |
| test-3x | 4-MiB c1 write | 3.274 | 0.785 | 1.361 | 0.829 |
| readcache | 4-MiB c1 write | 3.045 | 0.772 | 1.423 | 0.857 |
| test-3x | 4-MiB c16 write | 1.919 | 0.931 | 1.212 | 0.667 |
| readcache | 4-MiB c16 write | 1.909 | 0.984 | 0.823 | 0.831 |
| test-3x | 64-KiB c16 read | 1.129 | 1.004 | 1.110 | 0.803 |
| readcache | 64-KiB c16 read | 1.111 | 0.972 | 1.191 | 0.838 |
| test-3x | 4-KiB c1 read | 2.399 | 0.923 | 1.236 | 0.493 |
| readcache | 4-KiB c1 read | 2.288 | 0.914 | 1.209 | 0.493 |

Remaining gaps are not closed by these favorable changes. Sequential A/B
captures are diagnostics, not a randomized causal optimization qualification.
A separate B allocation profile confirms the messenger clone is absent,
reducing sampled allocation from about 4.07 GiB to 3.05 GiB for 1024 writes.
Admission, OSD encoding and secure encoding each still account for about one
payload-sized allocation. Instrumented CPU totals are not acceptance samples.

The next candidate uses explicitly immutable admission-owned payloads for a
single-operation OSD message, preserving defensive default encoding and the
public caller's admission copy. Independent retries share only that owned
immutable payload; ordinary routed requests do not opt in. Encoding equality,
limits, default ownership, caller mutation isolation, retry sharing and
messenger cancellation checks pass, as do full OSD/objecter/messenger/benchmark
race suites. Its live performance effect still requires a fresh capture.

The next runners also enforce the expected cluster FSID and require every
observed PG to be exactly active+clean. This closes a preparation finding where
a negative state-name pattern could overlook recovering or other non-clean
states. Missing PG evidence, wrong identity and HEALTH_ERR are rejected.
Retrospective validation caught a second guard defect: Ceph 20.2 reports
pgmap.pgs_by_state, while the old runner read nonexistent num_pg_by_state with
an empty fallback. The corrected shared gate requires the actual field and
reconciles positive state counts with num_pgs. All 148 retained A/B health
observations pass this corrected identity/clean-state/count validation. This
retrospective result does not turn observational snapshots into continuous
cluster monitoring.

## Offered-Load And Retention Preparation

The native async driver [native_offered.c](../../integration/p07/native_offered.c)
matches the existing Go diagnostic's absolute arrivals, sixteen workers,
128 queued jobs, eight-second issuance and 500-ms arrival-relative deadlines.
It cancels timed-out async reads and drains completion before reusing their
buffers; this native drain cost is explicit rather than hidden. Payload
validation is included in read-return timing. Rejected arrivals never claim
an enqueue phase, and every expected arrival has a reported outcome.

The paired runner [offered-parity.mjs](../../integration/p07/offered-parity.mjs)
prepares fresh binaries and source/library pins, separate actual-mode probes,
fixed placement, visible host counters and strict final fixture verification/
cleanup, including on failure. It predeclares a diagnostic 1k/2k/4k/8k/16k/32k/
64k reads-per-second sweep with six alternating blocks per pool/rate, zero
background load and a common 10-ms goodput budget. Overload/timeout outcomes
are retained as SLO failures, not discarded. Delivery lag above 50 ms invalidates
comparison; observed delivery at those rates is not assumed in advance.
The Go high-rate opt-in is implemented and tested: the original rate policy
remains unchanged without a validated parity namespace and explicit factorial
case none. The live paired sweep has not run yet.

Native fake-client tests pass for scheduled arrivals, successful completion,
deadline cancellation/drain, bounded-queue overload, API errors, warmup failures,
final payloads and exact removal/NotFound. The JS validator independently
recomputes counts, all-outcome p99 and within-budget goodput; missing outcomes,
wrong arrival timestamps, wrong p99 and altered counts are rejected. Strict
native compilation passes. Preparation checks run outside Capture B's ten-CPU
affinity and do not mutate its pinned sources.

The native [retention driver](../../integration/p07/native_retention.c) repeats
fixed workload windows on one connected client, reporting post-cleanup RSS and
explicitly partial glibc allocator bytes. It compiles on this Linux/glibc host.
The paired [Go helper](../../integration/p07/benchmark/retention.go) requires an
explicit sustained parity matrix and 6..64 windows. It preserves the same
connected pool and payload seed for every window, reports post-GC heap
separately, and stops on an operation or verification failure. Its configuration
and collection tests pass; entry-point integration is now complete and requires
secure transport. No live retention capture has run yet.

The [retention capture and analysis](../../integration/p07/retention-parity.mjs)
predeclares sixteen windows, four initial warmup windows, and a growth budget
of max(16 MiB, 10% baseline). It compares the median final four windows with the
median windows 5..8 separately for RSS and each implementation's allocation
metric. Three workload shapes run in each pool with native/Go/Go/native process
ordering. Every process uses one connection across all windows; primary
captures exclude separate actual-driver secure-mode probes. Source, binary,
library, health and observed placement continuity are gated. Analysis tests
reject missing, misnumbered, incorrect-count or unverified windows and detect
growth above the declared budget. These bounded checks do not establish
endurance or equal total live heap.

The live retention run and independent final metric qualification remain open.
Fresh-process RSS or glibc allocator counters must not be relabeled as total
retained-heap parity. Latest focused messenger/objecter/benchmark race suites
pass, including ownership and retention preparation, outside Capture B's
measurement affinity.

## Completed Capture C And Reevaluation

The fixed serial sequence completed all three drivers without a capture
failure. Closed-loop C contains 72 blocks, 288 primary legs and 3686400 timed
operations. Offered C contains 84 blocks, 336 primary legs and 48768000
scheduled arrivals. Every offered leg passes the delivery-lag validity gate;
all outcomes, including overloads, are retained. Retention C contains 24
processes and 384 verified same-client windows, with no declared growth-budget
finding. Source continuity and visible throttling/OOM checks pass for all three
captures. Private roots are /root/proj/rados-go/native-parity-20261002-c,
/root/proj/rados-go/native-offered-parity-20261002-c and
/root/proj/rados-go/native-retention-parity-20261002-c.

Closed-loop C's CPU/op ratios remain 3.860/3.632 for test-3x/readcache 1-MiB
c1 writes and 2.237/2.299 for 4-MiB c1 writes. Corresponding IOPS ratios are
0.821/0.738 and 0.863/0.855. The 4-MiB c16 write CPU ratios are 1.630/1.537,
with IOPS 0.956/1.004. The 4-KiB c1 read CPU ratios are 2.639/2.341 and p99
ratios 1.322/1.203. CPU and serial latency/throughput findings remain open;
lower RSS and favorable parallel goodput do not cancel failing cells.

Offered runs up to 8k/s have no overloads or timeouts in either client/pool.
At 16k/s some legs have overloads; these cannot be called zero-failure capacity
qualification. At 32k/64k both clients overload. Per-client median-of-twelve
within-10-ms goodput is approximately 23.6k..24.2k reads/s in these saturation
cells, but successful goodput is not successful delivery of all offered work.
No timeout, API/payload error or canceled arrival occurs. The complete arrival
records, not success-only tails, remain authoritative. CPU in offered reports
includes warmup and producer/accounting work and is not isolated library CPU.

A separate C 1-MiB write allocation profile reports about 2057 MiB, versus
about 3.05 GiB in B. OSD payload allocation is absent; admission and secure
encoding each still contribute about one payload-sized allocation. Sampled
write CPU remains dominated by AES-GCM, syscalls, copying and runtime work.
The separate 4-KiB read profile is dominated by syscalls and runtime futex/
scheduling work. Profiling is excluded from all primary comparisons.

The next candidate reuses one private secure-wire buffer per connection under
the existing write mutex, retaining at most 8 MiB per connection. Larger frames
fall back to temporary allocations without enlarging the retained cache.
Encoding failure, write failure and close drop the cache. Public SecureCodec
encoding still returns independently owned bytes; CRC and custom codecs keep
their existing path. Every authenticated padding region is explicitly cleared
before in-place sealing. Dirty-buffer byte equality across inline/block
boundaries, nonce/limit/vector tests, serialized and short writes, reuse,
oversized fallback and cache-release tests pass. Full messenger/objecter race
suites pass. Wire limits, admission limits and queue policies are unchanged.
This added per-connection retention cost requires a fresh resident-memory and
repeated-window evaluation; it is not assumed harmless from allocation savings.

The existing [provisional contract](../performance-phase0/QUALIFICATION.md)
remains the acceptance reference: CPU <=1.20, p99 <=1.25, throughput >=0.90
and incremental RSS <=1.25, using the specified confidence procedure. Its
connected idle baseline/peak RSS sampling, raw per-operation records and
time/count minima must be implemented for any final qualification. The current
short diagnostic captures do not satisfy that contract and cannot be promoted.
Maintainer signoff and broader topology/release qualification remain distinct
deferred obligations; native CPU findings are not waived or averaged away.

The earlier preparation statements above are chronological: live offered and
retention diagnostics have now run, but final qualification and phase completion
remain open. No completion commit is justified by Capture C.

## Completed Capture D And Remaining Blocker

The wire-reuse candidate completed the same fixed three-driver sequence with
zero capture failures: 288 closed-loop primary legs, 336 delivery-valid offered
legs, and 24 retention processes containing 384 verified windows. No retention
growth-budget finding, visible throttling/OOM increment or source-continuity
failure occurred. The [compact source-bound record](native-parity-20261002.json)
contains all twelve D ratios, source/binary/library identities, C/D offered
outcome populations and remaining gates.

Write CPU improved, but D does not meet the provisional CPU target. The 1-MiB
c1 write ratios are 2.442/2.502 for test-3x/readcache; 4-MiB c1 ratios are
2.150/2.049 and c16 ratios 1.463/1.384. The 4-KiB c1 read ratios remain
2.389/2.458. Some serial throughput and p99 cells also miss or have inconclusive
bounds against their unchanged targets. Timed-end RSS remains a diagnostic
absolute observation, not the contract's baseline-subtracted interval peak.

A separate D write profile reports about 1049 MiB allocation, almost entirely
the admission copy. Messenger cloning, OSD single-payload copying and repeated
secure-wire allocation are absent. That remaining admission copy is required
to preserve caller ownership and immutable replay; removing it is not an
acceptable way to pass a benchmark. Sampled write CPU includes AES-GCM (31%),
syscalls (22%), copying (19%) and runtime work. Small-read sampled CPU is
dominated by runtime scheduling and syscalls. These profiles include auxiliary
work and do not prove an irreducible CPU floor.

Separate strace count-only 4096-read probes passed payload/cleanup validation.
Go recorded 38800 futex, 11888 epoll_pwait, 8295 write and 8357 read calls;
native recorded 13179 futex, 8295 epoll_wait, 4158 sendmsg, 4148 write and
16668 read calls. Counts include setup/warmup/cleanup, file/event/socket I/O
and profiler perturbation. Summed syscall durations include blocked time and
must not be interpreted as CPU or causal latency attribution. The nearby
routed-attempt cleanup correctly removes tracking before cancellation; this
check did not identify a redundant cancellation wakeup defect to patch.

Further closure requires a deeper request-engine/transport scheduling
investigation, while preserving nonblocking owner/control progress, cancellation,
replay, defensive custom-codec behavior and standard cryptographic guarantees.
No safe small local fix has been demonstrated for the remaining gap. Crypto
backend changes, runtime-policy changes, ownership weakening, a smaller target
scope or relaxed acceptance thresholds must not be silently substituted.

The phase remains incomplete and uncommitted. The measured improvements and
bounded memory evidence are retained, but they do not authorize a phase-complete
commit or a native parity claim. Final contract-compliant sampling/analysis is
also still unimplemented; the failed diagnostic CPU findings must be resolved
before an independent qualification can establish the requested outcome.

## Capture E And Registration Follow-Up

The user authorized the deeper scheduling investigation. Separate raw-address
write tracing of the frozen D 4-KiB read driver found about one 96-byte secure
ACK plus one 384-byte request write per operation; native used about one
network send per operation. Only addresses, lengths and socket lifecycle were
traced, not buffer contents. These auxiliary probes are excluded from timing.
The session never populated outgoing AckSequence. The E candidate stamps the
latest inbound sequence at dispatch, including replay, and reserves one
coalesced acknowledgment slot with a non-restarting 200-microsecond timer.
An intervening message carries the acknowledgment; otherwise the timer makes
it eligible for sending. This is not a hard wall-clock delivery guarantee under
a stalled writer or scheduler. Other queued control traffic keeps priority,
and the slot counts against the existing control queue limit. Drained renewal
flushes the acknowledgment before reconnect. No admission limit was increased.

E completed the same fixed 72-block, 288-leg closed-loop matrix. Its exploratory
Go/native geometric means, not qualification bounds, are:

| Pool | Size | Concurrency | Workload | CPU/op | p99 | IOPS |
| --- | --- | --- | --- | --- | --- | --- |
| test-3x | 1 MiB | 1 | write | 2.644525 | 1.230860 | 0.880902 |
| test-3x | 1 MiB | 1 | mixed | 1.764735 | 1.181560 | 0.932757 |
| test-3x | 4 MiB | 1 | write | 2.128318 | 1.131176 | 0.869770 |
| test-3x | 4 MiB | 16 | write | 1.401498 | 1.240869 | 0.947359 |
| test-3x | 64 KiB | 16 | read | 0.987340 | 1.098587 | 1.033313 |
| test-3x | 4 KiB | 1 | read | 2.197215 | 1.250596 | 0.882747 |
| readcache | 1 MiB | 1 | write | 2.714460 | 1.701265 | 0.793923 |
| readcache | 1 MiB | 1 | mixed | 1.757996 | 1.230286 | 0.923511 |
| readcache | 4 MiB | 1 | write | 2.098385 | 1.207738 | 0.863995 |
| readcache | 4 MiB | 16 | write | 1.349028 | 0.970631 | 1.008618 |
| readcache | 64 KiB | 16 | read | 0.984982 | 1.143121 | 0.955991 |
| readcache | 4 KiB | 1 | read | 2.076220 | 1.200240 | 0.902576 |

The offered sweep retained 84 blocks, 336 delivery-valid legs and 48,768,000
arrivals: 30,324,812 successes, 18,443,188 overload rejections, and zero timeout,
error or canceled outcomes. Overload occurred at 32k and 64k arrivals/s, not at
the lower rates in this capture. All 24 retention processes and 384 windows
passed the declared bounded-growth budgets. Source manifests remained unchanged
within all three captures; closed-loop visible throttling/OOM increments were
zero. Raw evidence is private under the native-parity, native-offered-parity and
native-retention-parity 20261002-e directories. E does not establish overall
parity: small-read and serial-write CPU gaps remain, and some latency and
throughput cells miss their targets. Cross-capture D/E differences are diagnostic,
not a randomized D-versus-E treatment-effect claim. The earlier rejected ACK
experiment remains rejected; synthetic frame counts alone do not qualify E.

After E, terminal failure and new-identity reset explicitly clear deferred ACK
state, with stale-flush regression tests. The next candidate also removes an
admission scheduling handoff in the built-in objecter path. SubmitRegistered and
SubmitBorrowedRegistered execute a trusted, nonblocking registration notification
on the session owner after the admission decision and clearing command payload
references. Such notifications must not wait, reenter the session or panic.
The sole objecter notification releases its admission/backoff mutex through
sync.Once; no arbitrary application callback is moved to the owner. Existing
SubmitAdmitted callbacks remain on the caller goroutine, and custom transports
retain the old fallback. Completion waits for admission cleanup even on rejection
or cancellation. Borrowed-buffer release and replay ownership remain unchanged.
Fresh/replayed headers, idle flush, control priority, renewal, terminal/reset,
registration ownership and eight registered completion outcomes pass focused
tests; full messenger/objecter race suites pass. Live measurement of this
registration candidate is still pending. The phase remains incomplete and
uncommitted, with unchanged qualification gates.

## Capture F And Bounded Completion Candidate

The registered-admission candidate completed all three fixed captures with
unchanged source manifests and no capture or bounded-retention failure. Its
72 blocks contain 288 closed-loop legs and 3,686,400 timed operations. The
exploratory Go/native geometric means are:

| Pool | Size | Concurrency | Workload | CPU/op | p99 | IOPS |
| --- | --- | --- | --- | --- | --- | --- |
| test-3x | 1 MiB | 1 | write | 2.551642 | 1.232116 | 0.878892 |
| test-3x | 1 MiB | 1 | mixed | 1.778530 | 1.127923 | 0.935827 |
| test-3x | 4 MiB | 1 | write | 2.163531 | 1.190364 | 0.873722 |
| test-3x | 4 MiB | 16 | write | 1.379746 | 1.172143 | 0.966535 |
| test-3x | 64 KiB | 16 | read | 0.998582 | 1.096520 | 1.025576 |
| test-3x | 4 KiB | 1 | read | 1.862232 | 1.216999 | 0.932945 |
| readcache | 1 MiB | 1 | write | 2.576511 | 1.545086 | 0.803471 |
| readcache | 1 MiB | 1 | mixed | 1.775421 | 1.213800 | 0.918629 |
| readcache | 4 MiB | 1 | write | 2.152009 | 1.185090 | 0.867178 |
| readcache | 4 MiB | 16 | write | 1.365269 | 0.951176 | 0.996370 |
| readcache | 64 KiB | 16 | read | 0.963902 | 1.117979 | 0.975047 |
| readcache | 4 KiB | 1 | read | 1.931337 | 1.178402 | 0.901494 |

All 336 offered legs were delivery-valid. The 48,768,000 arrivals comprise
29,867,611 successes and 18,900,389 overload rejections; timeout, error and
canceled counts are zero. Overload appeared at 32k/64k arrivals/s. All 24
retention processes and 384 windows passed the existing growth budgets, and
closed-loop visible throttling/OOM increments were zero. Evidence remains
private in the three 20261002-f capture directories. F still fails the CPU
objective for small reads and writes; lower diagnostic ratios in some cells
do not establish a causal E/F improvement or qualification.

The next candidate changes only the writer-completion channel from an
unbuffered rendezvous to one buffered notification. It does not buffer write
tasks or increase admission, replay, control or in-flight limits. writeBusy
still permits only one outstanding write, and generation checks still reject
stale completion notifications. Pump tests cover completion without an owner
rendezvous and stop with the slot occupied by an older generation. Ten race
repetitions pass, as do full repository race tests, diagnostic tests, vet and
whitespace checks. Live performance measurement of this bounded-completion
candidate is pending. No parity claim or phase-completion commit is justified.

## Capture G And Explicit Payload Lifetime Candidate

The single-slot completion candidate completed the same fixed three-driver
sequence. Source continuity, payload/cleanup and observed health/placement
guards passed; retention findings are empty. Closed-loop diagnostic geometric
means, not qualification bounds, are:

| Pool | Size | Concurrency | Workload | CPU/op | p99 | IOPS |
| --- | --- | --- | --- | --- | --- | --- |
| test-3x | 1 MiB | 1 | write | 2.590020 | 1.213319 | 0.882391 |
| test-3x | 1 MiB | 1 | mixed | 1.813966 | 1.179469 | 0.928301 |
| test-3x | 4 MiB | 1 | write | 2.160350 | 1.138096 | 0.884255 |
| test-3x | 4 MiB | 16 | write | 1.402011 | 1.208710 | 0.960076 |
| test-3x | 64 KiB | 16 | read | 0.972329 | 1.084678 | 1.037362 |
| test-3x | 4 KiB | 1 | read | 2.061622 | 1.298141 | 0.875325 |
| readcache | 1 MiB | 1 | write | 2.669827 | 1.602565 | 0.782864 |
| readcache | 1 MiB | 1 | mixed | 1.801030 | 1.205759 | 0.910571 |
| readcache | 4 MiB | 1 | write | 2.095900 | 1.197855 | 0.865718 |
| readcache | 4 MiB | 16 | write | 1.379485 | 1.031655 | 0.965410 |
| readcache | 64 KiB | 16 | read | 0.972506 | 1.111739 | 0.995521 |
| readcache | 4 KiB | 1 | read | 1.916786 | 1.236032 | 0.915796 |

The closed-loop population remains 72 blocks, 288 legs and 3,686,400 timed
operations. The 336 delivery-valid offered legs retain 48,768,000 arrivals:
29,475,470 successes, 19,292,530 overload rejections, zero timeout/error/canceled
outcomes. The 24 retention processes contain 384 windows with no growth-budget
finding. G still does not meet native parity; the completion slot alone does
not demonstrate closure or a causal F/G improvement. All evidence is retained
privately under the three 20261002-g capture directories.

Separate frozen-G write and read profiles passed correctness, cleanup, health
and placement checks and are excluded from primary timing. Write CPU samples
total 2.46 seconds: AES-GCM 0.94 seconds (38.21%), syscalls 0.61 seconds
(24.80%), copying 0.39 seconds (15.85%). Allocation totals about 1041 MiB,
with admission copying accounting for 1025 MiB (98.42%). The small-read profile
has 930 ms samples, including syscalls 190 ms and futex 120 ms. These whole-probe
samples are attribution clues, not a measured irreducible library CPU floor.

A separate synthetic AES-128-GCM probe retains 32 legs: four mirrored ABBA/BAAB
blocks for each of 1-MiB and 4-MiB buffers, two seconds per leg, one matched CPU,
and executable/library/source pins. Warmed, allocation-free, in-place Go GCM
geometric throughput is 4.738/4.728 GB/s; system OpenSSL 3.5.8 is
10.700/10.690 GB/s. The system executable loads the same libcrypto.so.3 as
librados. An earlier PATH-resolved OpenSSL 1.1.1s comparator was identified as
mismatched and excluded. OpenSSL speed's AEAD setup/AAD behavior is not identical
to Go's no-AAD record loop, and neither probe reproduces cold buffers, messenger
framing, scheduling or network I/O. This diagnoses standalone backend cost,
not end-to-end parity, a crypto replacement approval or a CPU floor. The standard
Go cryptographic backend remains unchanged.

The next candidate preserves the caller-to-admission payload copy while reusing
its storage only after an explicit immutable lifetime ends. New MessageLease
references are held independently by the mutation producer, admitted/replay
entry and active writer. Cancellation removes the pending reference but cannot
recycle storage while WriteFrame still uses it; replay and independent retries
continue holding references. RetainLeasedMessage is a distinct internal contract:
holders must keep the payload immutable while leased and stop accessing it when
their lease ends. RetainImmutableMessage remains permanently immutable, including
after rejection/cancellation. Lease reclamation runs on whichever holder releases
last and must not block, reenter the session or panic.

Only production-created sessions enable this mutation cache. Custom factories,
compound operations, class calls and unsupported sizes retain the existing
permanent allocation path. Exact 4-KiB, 64-KiB, 1-MiB and 4-MiB single-payload
mutations use nonblocking size buckets with an aggregate 8-MiB idle cap per client.
Reclamation has at most eight reservation CAS attempts and drops storage on
contention/full buckets. All bytes are overwritten by the next admission copy.
Active mutation/replay limits are unchanged; up to 8 MiB additional idle retained
memory is a real new cost requiring live evaluation. Close marks the cache closed
and detaches it from the client; later completions cannot reactivate it. Unreleased
active leases still retain their payloads until their actual writer/producer
lifetime ends, rather than reclaiming early to satisfy a memory measurement.

Concurrent reference reclamation, canceled-writer lifetime, rejected admission,
cache cap/concurrency, defensive copying, custom-factory fallback and close tests
pass repeated race checks. Full repository race tests, diagnostic tests, vet and
whitespace checks pass; messenger/objecter tests also pass on Go 1.26.8. Live
measurement of this explicit-lifetime candidate is complete as diagnostic H below. Final raw-sample,
duration/count, incremental-peak-RSS and bootstrap qualification remain open;

## Frozen H results and allocation attribution

All three H captures completed with unchanged source pins, no capture failures,
and unchanged observed health-warning categories and placement. Closed-loop H
contains 72 blocks, 288 legs and 3,686,400 timed operations. Go/native geometric
means, not final qualification bounds, are:

| Pool | Size | Concurrency | Workload | CPU/op | p99 | IOPS |
| --- | --- | --- | --- | --- | --- | --- |
| test-3x | 1 MiB | 1 | write | 1.765030 | 1.069051 | 0.922895 |
| test-3x | 1 MiB | 1 | mixed | 1.503615 | 1.069252 | 0.946517 |
| test-3x | 4 MiB | 1 | write | 1.658749 | 1.096640 | 0.907097 |
| test-3x | 4 MiB | 16 | write | 1.278453 | 0.974742 | 0.995403 |
| test-3x | 64 KiB | 16 | read | 1.037381 | 1.071476 | 1.040987 |
| test-3x | 4 KiB | 1 | read | 2.155633 | 1.294244 | 0.857660 |
| readcache | 1 MiB | 1 | write | 1.757326 | 1.121629 | 0.870576 |
| readcache | 1 MiB | 1 | mixed | 1.549639 | 1.061986 | 0.936941 |
| readcache | 4 MiB | 1 | write | 1.600021 | 1.092436 | 0.908159 |
| readcache | 4 MiB | 16 | write | 1.237219 | 0.940402 | 1.028142 |
| readcache | 64 KiB | 16 | read | 1.005910 | 1.158884 | 0.974068 |
| readcache | 4 KiB | 1 | read | 1.912147 | 1.189570 | 0.907862 |

The 336 offered legs are delivery-valid and retain 48,768,000 arrivals:
30,583,949 successes, 18,184,051 overload rejections, and zero timeout, error or
canceled outcomes. Overload appears at 16k, 32k and 64k offered operations/second.
Retention contains 24 processes and 384 windows with no growth-budget finding.
All three H host-counter boundaries show zero CPU-throttling and memory-event
increments, including OOM counters. These are boundary observations, not
continuous transition monitoring. Private evidence remains in the three
`*-20261002-h` capture directories; timed-end RSS and bounded retention still
are not incremental-peak-RSS or endurance qualification.

Excluded frozen-H profiles passed payload, cleanup, health, placement and binary
checks. Write allocation falls from frozen G's approximately 1041 MiB to
14,792 KiB in H; initial mutation-cache allocation is approximately 1184 KiB.
This establishes storage reuse, not removal of the caller's admission copy.
H write CPU samples total 2.03 seconds: AES-GCM 0.83 seconds (40.89%), syscalls
0.63 seconds (31.03%), and copying 0.34 seconds (16.75%). H small-read samples
total 920 ms: syscalls 240 ms (26.09%), futex 60 ms (6.52%), and findRunnable
cumulative 160 ms (17.39%). Profiles include auxiliary work and do not prove an
irreducible CPU floor. Serial writes, mixed operations, concurrent writes and
small reads still exceed the 1.20 CPU target. Native parity remains unqualified.

## Rejected I writer-affinity probe

I changed only the writer pump to LockOSThread/UnlockOSThread. The fixed
Go-versus-Go comparison against frozen H retained 48 legs: two readcache cells,
six mirrored ABBA/BAAB blocks each, 1024 serial 1-MiB writes or 4096 serial 4-KiB
reads per leg. Runtime, CPU mask and fixture namespace match H. Source/binary
pins, payload, cleanup, observed health and placement checks passed. Threads
were sampled every 20 ms, not continuously.

Candidate/H CPU ratios are above one in all six blocks for both cells. Paired
log-ratio geometric means and exploratory two-sided 95% Student-t intervals are
1.011613 [1.001543, 1.021784] for writes and 1.183428 [1.144275, 1.223921] for
small reads. Individual small-read CPU increases span 13.46% to 22.93%.
Sampled write thread peaks span 10-11 for H versus 10-14 for I; read peaks span
10-13 versus 12-14. I is rejected and its source change was removed; all H
source hashes were verified restored and messenger/objecter race tests passed.
Evidence remains in private `writer-affinity-probe-20261002-i`. These intervals
are not matrix-wide bootstrap qualification and do not extrapolate to c16.

## Provisional J small receive scratch cache

H's excluded small-read allocation profile attributes 22.13 MiB of 47.91 MiB
to receiveScratch.get. J lowers existing cache eligibility from 32 KiB to
4 KiB, retaining the existing 8-KiB capacity rounding, upper size limit, slot
limits, charged receive budget, plaintext wiping, leased handoff and close
rules. Small records therefore consume real rounded idle budget; records that
cannot reserve the rounding overhead still fall back without retaining storage.
Ordinary Read ownership remains defensive. Existing reuse, handoff and secure
mixed-ownership tests now cover small records; the scratch race suite passed
ten repetitions and messenger/objecter race suites passed.

The fixed H/J comparison retains 48 legs in six mirrored blocks per readcache
cell: 4-KiB c1 reads and 64-KiB c16 reads, 4096 operations/worker. Exact runtime,
namespace, source/binary, payload, cleanup, observed health and placement checks
passed. Candidate/H paired geometric ratios and exploratory two-sided 95%
Student-t intervals are:

| Cell | CPU/op | CPU interval | p99 | p99 interval |
| --- | --- | --- | --- | --- |
| 4 KiB c1 | 0.965276 | 0.918671-1.014246 | 0.920495 | 0.805340-1.052116 |
| 64 KiB c16 | 1.013673 | 0.999209-1.028346 | 1.019359 | 0.954452-1.088679 |

Across timed legs, arithmetic mean allocated bytes/op falls from 9576.895 to
4775.903 for small reads; 64-KiB reads remain approximately 4818 bytes/op.
Allocation counts remain approximately 83/op. An excluded frozen-J profile
totals 24.05 MiB and no longer lists receiveScratch.get among its top eight
allocation owners. Its source/binary, payload, cleanup, observed health and
placement checks passed. This is a modest, provisional reduction, not evidence
that native CPU parity or incremental-peak-RSS is satisfied. All legs and the
excluded profile remain in private `small-scratch-probe-20261003-j`.

J's c16 placement observations cover worker 0 only; the later full K driver
checks placement for every worker. The fixed L c16 probe below has the same
worker-0 placement limitation. Neither narrow probe qualifies the full matrix.

## Frozen K results and residual gaps

J's profile attributes 2.50 MiB to map address cloning through routing and
sessionTarget. K removes only the second defensive copy during current-map
session validation by returning an immutable netip endpoint value. Routing still
returns owned address vectors. The accessor preserves first-valid-v2/nonzero-port
selection and distinguishes missing vectors from present-but-invalid vectors;
the latter still rejects the target. Address/state/generation revalidation and
creation supersession remain unchanged. Selection, presence, zero-allocation,
map immutability and objecter lifecycle race tests pass. Full repository race,
diagnostic and vet checks pass; map/messenger/objecter tests also pass with
Go 1.26.8. The fixed three-driver K capture is complete under the private
`*-20261003-k` roots. All source pins match within each driver, capture failures
are empty, health/placement guards pass, and all three host-counter boundaries
show zero throttling and memory-event increments. Closed-loop K retains
72 blocks, 288 legs and 3,686,400 operations. Go/native geometric means are:

| Pool | Size | Concurrency | Workload | CPU/op | p99 | IOPS |
| --- | --- | --- | --- | --- | --- | --- |
| test-3x | 1 MiB | 1 | write | 1.768686 | 1.069228 | 0.918737 |
| test-3x | 1 MiB | 1 | mixed | 1.502551 | 1.053166 | 0.951023 |
| test-3x | 4 MiB | 1 | write | 1.659276 | 1.091408 | 0.902252 |
| test-3x | 4 MiB | 16 | write | 1.276270 | 1.043644 | 0.988312 |
| test-3x | 64 KiB | 16 | read | 0.992041 | 1.063863 | 1.037310 |
| test-3x | 4 KiB | 1 | read | 1.877765 | 1.074967 | 0.928415 |
| readcache | 1 MiB | 1 | write | 1.781338 | 1.157688 | 0.865530 |
| readcache | 1 MiB | 1 | mixed | 1.532280 | 1.082347 | 0.922291 |
| readcache | 4 MiB | 1 | write | 1.648200 | 1.115526 | 0.896113 |
| readcache | 4 MiB | 16 | write | 1.262352 | 1.051214 | 0.972734 |
| readcache | 64 KiB | 16 | read | 1.014406 | 1.120970 | 0.968015 |
| readcache | 4 KiB | 1 | read | 1.845229 | 1.120242 | 0.914216 |

Offered K retains 336 delivery-valid legs and 48,768,000 arrivals: 28,435,323
successes, 20,332,676 overload rejections, one timeout, zero errors and zero
canceled outcomes. Overload occurs at 16k, 32k and 64k. The timeout is retained,
not replaced: `readcache-64000-r5-l3-go`, raw outcome index 51932. Delivery lag
is 9394 ns, queue delay 5593579 ns, read service 494435312 ns, and total latency
500041629 ns against the declared 500-ms deadline. The request was admitted
and attempted; neighboring records succeeded. These timestamps do not establish
a network, server, scheduler or cache cause. It remains an SLO failure, not a
successful zero-timeout verdict. Retention's 24 processes/384 windows have no
declared growth-budget finding. Serial/mixed/concurrent-write and small-read CPU
targets remain open; serial readcache write throughput is below 0.90. Timed-end
RSS, short legs and descriptive intervals still do not qualify the phase.

Four separate frozen-K native/Go perf probes cover readcache serial 1-MiB writes
and 4-KiB reads. CPU-clock sampling at 199 Hz with frame-pointer call graphs
does not dump user-stack memory. All probes pass binary, payload, cleanup,
observed health and placement checks and report zero lost samples. Native/Go
write profiles contain 243/427 samples, approximately 1.221/2.146 seconds of
CPU-clock events. Native write DSO attribution is 46.09% kernel, 30.04%
libcrypto.so.3.5.8 and 18.11% libc; Go write attribution includes AES-GCM 36.53%,
memmove 15.22% and kernel 34.19%. Native crypto's stripped internal symbols are
unresolved; DSO attribution is not exact AES-only attribution. Native/Go read
profiles contain 128/195 samples, approximately 0.643/0.980 seconds; kernel
shares are 32.03%/42.05%, with Go scheduling functions also visible. Whole
process setup/warmup/cleanup and profiler overhead are included. These excluded
single profiles support attribution, not a floor, causal change estimate or
qualification. Raw profiles and reports remain private in
`native-cost-profile-20261003-k`; no backend or runtime policy changed.

## Rejected L registered completion barrier

Registered submissions replace their unconditional admitted-channel allocation
with an embedded wait-group completion barrier. The owner still clears message
ownership, invokes the trusted registration callback, then releases the barrier;
every result return still waits for that completion. Legacy SubmitAdmitted keeps
its channel and caller-side callback unchanged. Direct owner tests with explicit
admission channels keep their existing behavior. Ten repetitions of complete
messenger/objecter race suites pass, including registered owned/borrowed reply,
cancel, stop and rejection outcomes.

The fixed K/L comparison retains 48 legs, six mirrored blocks each for readcache
4-KiB c1 and 64-KiB c16 reads, under the same source/runtime/fixture/correctness
guards as J. Arithmetic mean allocation falls by approximately 96 bytes/op and
one allocation/op. Paired CPU ratios are 1.011094 [0.933856, 1.094720] and
1.007477 [0.973093, 1.043077], respectively; p99 ratios are 1.222450
[0.828899, 1.802855] and 1.004862 [0.935819, 1.079000]. These exploratory
two-sided 95% Student-t intervals do not establish a CPU or latency benefit.
Two small-read blocks have unfavorable p99 ratios 2.427323 and 1.446904; no
claim of a demonstrated causal regression follows from this noisy population.
The small storage saving does not justify the extra mechanism without a
performance benefit, so L is rejected. Its source change was removed, every
frozen K source pin verified restored, and the same ten-repetition race suites
passed again. All evidence remains in private
`registered-barrier-probe-20261003-l`; no legs are dropped or replaced. Native
qualification and a phase-complete commit remain open.

## Failed M direct-plaintext activation

M reserved a 16-byte complete epilogue after the private aligned mutation
payload, charging actual capacity against the unchanged 8-MiB idle limit. Its
direct-plaintext branch accepted only one nonempty segment after segment 0.
Synthetic ciphertext, authentication, immutability and cancellation checks
passed, but the tests did not represent the actual encoded-message layout.
Real segment 0 is the message header; OSD metadata is in segment 1 and payload
in segment 3. Thus actual mutation frames always selected the existing copy
fallback. The fixed live result falsifies the claimed production copy removal;
it is not evidence that removing the copy cannot help.

The 48 retained K/M legs cover readcache 1-MiB c1 and 4-MiB c16 writes, six
mirrored blocks per cell. Runtime, namespace, source/binary, every worker's
observed placement, health, payload and cleanup checks passed. Candidate/K
paired geometric means and exploratory two-sided 95% Student-t intervals are:

| Cell | CPU/op | CPU interval | p99 | p99 interval |
| --- | --- | --- | --- | --- |
| 1 MiB c1 | 1.001295 | 0.987697-1.015080 | 0.976706 | 0.944348-1.010173 |
| 4 MiB c16 | 1.026836 | 0.979791-1.076140 | 0.984619 | 0.887442-1.092437 |

Arithmetic mean allocation/op is 5149.441/5179.726 bytes for K/M serial writes
and 34364.789/88832.624 bytes for concurrent writes. Timed-end c16 RSS ranges
263729152-340885504 bytes for K and 304447488-460394496 bytes for M. These are
not interval-peak incremental RSS values. Only one padded 4-MiB idle entry fits
the unchanged cap, rather than two plain entries; this is a real occupancy
tradeoff, not a proven sole cause of all observed memory differences. All
evidence remains in private `secure-plaintext-probe-20261003-m`. M is not
accepted as a working optimization; no legs are dropped or replaced.

## N complete secure plaintext and resource finding

N corrects the actual message layout by reserving 1024 bytes of prefix scratch
and a 16-byte complete epilogue beside the private aligned payload. The payload
remains immutable while leased; prefix scratch is explicitly mutable, prepared
and sealed under a per-lease mutex so simultaneous retry/session writers cannot
overwrite each other's metadata. Front and middle segments are copied into
that small prefix with exact secure padding; stdlib AEAD seals the resulting
contiguous plaintext into separate wire output without copying the large
payload first. Unrelated payloads, oversized prefixes, nonleased frames and
ordinary/custom ownership paths retain the existing fallback.

The caller-to-admission copy remains. Idle accounting charges full storage
capacity, including prefix and epilogue, under the unchanged 8-MiB cap; one
4-MiB entry still fits. Active prefix/epilogue storage adds 1040 bytes per cached
mutation payload and remains bounded by admitted mutation count. Limits on
payload bytes and wire descriptors are unchanged; physical allocator overhead
still requires actual memory measurement, not logical-capacity inference.
New tests use encodeOwnedMessage to assert real-layout fast-path selection,
byte-identical ciphertext over successive nonces, authenticated decoding,
unchanged payload, dirty storage and exact fallback. Sixteen simultaneous
writers with distinct unaligned front/middle metadata share one lease and
authenticate every result. Padded canceled-writer and full-capacity cache
ownership/close tests also pass repeated race checks. Full repository race,
diagnostic, vet and minimum-Go messenger/objecter gates pass.

The same fixed 48-leg K/N comparison completed with unchanged source pins,
matching runtime/namespace and every-worker placement, observed health, payload
and cleanup checks. Candidate/K paired geometric means and exploratory
two-sided 95% Student-t intervals are:

| Cell | CPU/op | CPU interval | p99 | p99 interval |
| --- | --- | --- | --- | --- |
| 1 MiB c1 | 0.928365 | 0.906327-0.950939 | 0.962281 | 0.916967-1.009835 |
| 4 MiB c16 | 0.932991 | 0.909756-0.956820 | 1.020429 | 0.884999-1.176583 |

CPU ratios are below one in all six blocks for both cells. This fixes M's
activation defect and provides a paired diagnostic improvement, not final
native parity. Serial allocated bytes/op are 5149.048/5179.743 for K/N;
concurrent bytes/op are 34794.717/88323.117. Timed-end c16 RSS ranges
280109056-355336192 bytes for K versus 310366208-480251904 bytes for N.
The concurrent memory finding remains unfavorable despite improved CPU and
must not be hidden by averaging with serial results. It is not incremental
peak RSS, and the exact causal share of cache occupancy versus other live/GC
effects is not established. N's 4-MiB variant is not accepted. All evidence
remains in private `secure-prefix-probe-20261003-n`.

## Completed O smaller payloads only

O restricts the contiguous prefix/epilogue optimization to the 4-KiB, 64-KiB
and 1-MiB mutation-cache buckets. Newly allocated 4-MiB payloads retain the
original plain storage, generic immutable lease and secure framing copy, so
two huge idle entries again fit within the unchanged 8-MiB cap. No budget is
raised. Seven fully charged padded 1-MiB entries fit, not eight; tests assert
both this capacity charge and the restored huge occupancy. Ownership, caller
copying, fallback and prefix serialization semantics remain unchanged. Mutation
and complete messenger/objecter race suites pass.

O completed the full fixed diagnostic population: 72 closed-loop blocks,
288 legs and 3,686,400 measured operations. Go/native paired geometric means
are below; the CPU upper column is the exploratory interval upper bound, not
a qualification bound. No failed legs are omitted or replaced.

| Pool | Cell | CPU/op | p99 | IOPS | CPU upper |
| --- | --- | --- | --- | --- | --- |
| test-3x | 1024 KiB c1 write | 1.613639 | 1.047514 | 0.945901 | 1.742543 |
| test-3x | 1024 KiB c1 mixed | 1.466909 | 1.047774 | 0.958399 | 1.587976 |
| test-3x | 4096 KiB c1 write | 1.626006 | 1.070083 | 0.910550 | 1.768536 |
| test-3x | 4096 KiB c16 write | 1.301960 | 1.059915 | 0.993814 | 1.362157 |
| test-3x | 64 KiB c16 read | 1.034552 | 1.156376 | 1.010717 | 1.141128 |
| test-3x | 4 KiB c1 read | 1.930571 | 1.143269 | 0.901712 | 2.050141 |
| readcache | 1024 KiB c1 write | 1.519593 | 1.105292 | 0.908266 | 1.832556 |
| readcache | 1024 KiB c1 mixed | 1.451823 | 1.161262 | 0.935835 | 1.520466 |
| readcache | 4096 KiB c1 write | 1.590677 | 1.103343 | 0.901359 | 1.865545 |
| readcache | 4096 KiB c16 write | 1.236967 | 0.944296 | 0.996943 | 1.292429 |
| readcache | 64 KiB c16 read | 0.939170 | 1.064181 | 1.033173 | 1.076099 |
| readcache | 4 KiB c1 read | 1.829512 | 1.230076 | 0.921064 | 2.172902 |

Ten of twelve CPU point estimates and exploratory upper bounds still exceed
1.20. In particular, serial writes and small reads remain unresolved; the
larger concurrent reads do not justify a matrix-wide parity verdict. Timed-end
4-MiB c16 RSS ratios are 1.180655/1.125243 for test-3x/readcache, with cleanup
ratios 1.077642/1.036556. These are not warmed-idle incremental interval peaks
and cannot satisfy the governing memory qualification gate.

The offered-load capture retains all 336 delivery-valid legs and 48,768,000
arrivals: 29,588,004 successes and 19,179,996 overload outcomes, with zero
timeouts, errors or cancellations. Overload occurs at 16k, 32k and 64k arrivals
per second. This does not erase K's retained timeout. Retention completed
24 processes and 384 windows with no budget findings; RSS growth ranges from
-8,028,160 to 16,955,392 bytes. The maximum exceeds 16 MiB but is within the
applicable 10% allowance. Partial native glibc allocation and post-GC Go heap
remain distinct metrics, not an equal-live-heap or endurance comparison.

All three source-before/source-after checks agree, failure arrays are empty,
and recorded host throttling and memory-event increments are zero. Health,
FSID, every-worker observed placement, payload verification and cleanup checks
passed at their recorded boundaries, not continuously. Raw evidence remains
private in native-parity-20261003-o, native-offered-parity-20261003-o and
native-retention-parity-20261003-o; the existing analysis is not overwritten.

A separate frozen-source O write profile is excluded from primary evidence.
Its 1.98 seconds of CPU samples attribute 45.96% to AES, 34.34% to syscalls and
8.59% to memmove, with 15.52 MiB total allocation and no payload-sized per-op
allocation. Source/binary and health/placement/payload/cleanup guards passed.
This supports reduced copy cost, not an irreducible CPU floor or qualification.
No crypto backend or runtime policy changed.

## Qualification Evidence Still Open

The new qualification-leg validator rejects missing evidence, short measured
or warmup populations, malformed or overlapping per-worker raw operations,
unknown retry counts, failures, censoring, quantile mismatches and endpoint-only
RSS. It requires a connected, equally warmed immediately preceding idle
baseline and an ordered interval trace with a predeclared cadence at most one
second and observed gaps no greater than twice that cadence.
Its successful return explicitly says leg_validated_not_matrix_qualification.
Synthetic positive and rejection tests pass; existing diagnostic collectors
do not produce this evidence and are not promoted by the validator.

Actual matched collectors, observed retry telemetry, independent seeded rounds,
predeclared matrix-wide paired-round bootstrap bounds and fresh qualifying
captures remain outstanding, along with the CPU findings above. No final
qualification or phase-complete commit follows from O or the validator.
the phase is incomplete and uncommitted.