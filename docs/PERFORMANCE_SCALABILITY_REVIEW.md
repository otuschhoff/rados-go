# Performance and Scalability Review and Remediation Plan

## Status and Scope

Review date: 2026-09-30.
Latest assessment and roadmap update: 2026-10-03.
Reviewed Go baseline: `dde29cde727dc963238acc4fa13a5a277a9f5c80`.
Current implementation checkpoint: `486305c` (committed, not qualified).
Native comparison target: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

**Conclusion: general performance and scalability parity with native librados
is not established.** Existing measurements pass local regression budgets but
leave material scaling risks and coverage gaps. This document preserves the
original read-only review and records subsequent implementation evidence,
decisions and remaining phases. Updating this plan does not implement its
pending work or confer qualification on the checkpoint.

The review covers messenger admission, transport, replay, OSD backoffs,
placement, session lifecycle, map updates, memory bounds, and benchmark scope.
Source-confirmed behavior is distinguished from inferred workload impact.
Large-cluster costs and saturation failure scenarios have not been reproduced
by new benchmarks or regression tests during this review.

Earlier copy reduction, secure encoding, replay-reference cleanup, and bounded
receive read-ahead improvements remain part of the baseline. Preserve their
ownership and lifecycle tests throughout remediation.

Follow-up: [caller-buffer reads](performance-p99-scheduler/READ_INTO_RESULTS.md)
now reduce secure moderate-record allocation volume without global runtime
tuning. Their measured throughput benefit does not establish stable p99
compliance, native parity or completion of the open Phase 4 latency gate.
The later [bounded inventory comparison](performance-p99-scheduler/INVENTORY_RESULTS.md)
retains four charged secure receive slots, with p99/throughput improvements in
15/15 rotated comparisons under idle, CPU and allocation-heavy host load.
Read admission windows were rejected; deployment qualification remains open.
The [read profiling and scale follow-up](performance-p99-scheduler/READ_SCALE_RESULTS.md)
verifies 120 Go and 40 unmatched native context legs without throttling. Eight
slots win 25/30 paired 64-KiB p99 comparisons and 29/30 IOPS comparisons, but
remain diagnostic-only: four is still the production default, with no automatic
promotion. Fallback sizes show no reuse benefit; 4-MiB live reads were not
measured. The [request-path results](performance-p99-scheduler/REQUEST_PATH_RESULTS.md)
now record completed in-flight-accounting benchmarks and per-request `Encoder`
allocation work. Encoding allocation reductions and O(1) in-flight accounting
are retained for local gains only; neither completed offered-load matrix
establishes a stable causal p99 improvement. Cumulative messenger ACK coalescing
was rejected and removed. Operation, routed-attempt, OSD submission and control
ACK contexts remain necessary for their distinct cancellation/retry lifetimes.

Current diagnostic follow-up separates CPU and allocation load in factorial
cases `none`, `cpu`, `alloc` and `both`, and decomposes arrival latency into
delivery, queue/read-entry, read/transport and outcome intervals rather than
treating end-to-end p99 as one client cost. Opt-in observation adds execution
tracing with per-read and separate CPU/allocation tasks; instrumented samples
remain separate from primary timing. See the
[Linux live observation handoff](performance-p99-scheduler/LINUX_LIVE_OBSERVATION_HANDOFF.md).
Selected source and binaries are frozen while captures run. The
[external Linux retest of 2026-10-01](performance-p99-scheduler/LIVE_RETEST_20261001.md)
completed 36 passing primary legs and twelve separate passing observed legs in
each of readcache and test-3x. Initial test-3x `EPERM` failures are retained;
the later authorized rerun passed with identical source and binary hashes.
No matched native parity, live p99 causal attribution or qualification claim
follows from that retest.

A later matched secure closed-loop comparison against Ceph v20.2.4 librados
completed forty read legs at 64 KiB/concurrency 16, with Go/native median p99
ratios 0.899 on test-3x and 0.957 on readcache. All workload/mode/provenance
checks passed; overall capture metadata remains incomplete because manager
statistics were denied. See the same live retest report for individual blocks,
client/server identities and limitations. This narrow idle read result does
not establish general native parity or close the Phase 4 latency gate.

## Current Assessment and Decisions

The [native parity investigation](performance-p99-scheduler/NATIVE_PARITY_20261002.md)
records captures A-H, K and O, narrower probes, rejected changes and retained
failures. Checkpoint `486305c` includes ownership-aware copy removal, leased
mutation-buffer reuse, bounded secure wire reuse, ACK piggybacking, registered
admission callbacks, a buffered writer completion, small receive scratch and
value-only endpoint lookup. These are tested local improvements, not proof of
general native parity. Earlier rejected cumulative-ACK batching is distinct
from the later retained bounded piggyback/deferred-ACK implementation.

The latest O diagnostic retains 72 closed-loop blocks, 288 legs and 3,686,400
operations. Ten of twelve CPU point estimates and exploratory upper bounds
exceed the 1.20 target. Serial-write CPU ratios are approximately 1.52-1.63;
4-KiB serial-read ratios are approximately 1.83-1.93. Offered load retains
336 delivery-valid legs and 48,768,000 arrivals: 29,588,004 successes and
19,179,996 overloads, with no other outcomes. The earlier K timeout remains
an unresolved finding, not erased by O. Retention has 24 processes and
384 windows with no declared bounded-growth findings, but is not endurance
qualification. Exact per-cell values and private evidence identities are in
the investigation; do not promote its short samples to acceptance evidence.

| Assessment | Finding or decision | Implementation owner phase |
| --- | --- | --- |
| A1 | Sustained collectors and interval RSS pass a matched tooling smoke; complete retry observation, harness-cost separation and matrix-wide analysis remain incomplete. | 8, 12 |
| A2 | Remaining CPU costs include crypto, syscalls and scheduling. No further safe small syscall fix is demonstrated; PGO is unmeasured. | 9, 10, 12 |
| A3 | Keep Go deployment builds pure Go/CGO-free and retain standard-library crypto. Defer evolving AES/SIMD backend work. | 14 monitoring only |
| A4 | Keep current RSS parity and memory limits. Bounded-window growth does not establish sustained memory behavior or equal native/Go live heap. | 8, 11, 12 |
| A5 | Active-backoff overflow, outbound/fanout memory and broad workload/recovery coverage remain separately scoped obligations. | 11, 13 |
| A6 | The checkpoint is committed, but older source-bound qualification and historical status text cannot certify it. | 12, 14 |

### Fixed Engineering Constraints

- Production and ordinary consumer builds remain `CGO_ENABLED=0` compatible,
  with no native crypto dependency, forked crypto toolchain or automatic
  `GOEXPERIMENT=simd` requirement. Native comparison executables are separate
  tools; Go race instrumentation may require CGO without changing this policy.
- Keep stdlib AES-GCM and its existing nonce, authentication and ownership
  contracts. The installed Go 1.27 GCM uses AES-NI/PCLMUL rather than wide
  VAES/VPCLMUL. The host and experimental archsimd API expose the latter, but
  enabling SIMD does not automatically accelerate stdlib crypto. A future
  backend needs separate approval, security review and independent evidence.
- PGO is a prospective explicit consumer-build variant, not a global runtime
  tuning change or a library-wide performance guarantee. Do not enable or
  distribute a training profile implicitly before Phase 9 evaluation.
- Preserve the [provisional qualification contract](performance-phase0/QUALIFICATION.md):
  CPU/op upper bound <=1.20, p99 upper bound <=1.25, successful-throughput
  lower bound >=0.90 and incremental-RSS upper bound <=1.25, with its stated
  confidence and failure rules. P12 guardrails remain separate and unchanged.
- Additional RAM may be economically useful, but no relaxed acceptance track
  is approved. A future deployment-efficiency budget must be explicitly
  approved and predeclared before new capture, reported separately from strict
  parity, and must never retrospectively convert failed evidence into a pass.
- Do not change queue/session/receive budgets, process parallelism, GC policy,
  TCP settings or cluster policy merely to improve a comparison. Live work
  remains restricted to approved fixtures and pools; recovery requires separate
  disposable-cluster authorization.

### Syscall and Crypto Assessment

The current built-in transport passes a contiguous encrypted frame to
`net.Conn.Write`, retrying short writes, and uses a bounded 512-KiB reader.
One Go Write is not necessarily one kernel syscall. The historical syscall
count probe predates the later scheduling and ACK changes; refresh it before
asserting current syscall excess. Futex and epoll counts may reflect runtime
handoffs or waiting, and accumulated blocked time is not process CPU cost.
Potential follow-ups are measured removal of unnecessary wakeups and combining
already-ready same-connection frames without waiting to form a batch. Neither
is an established win; concurrency-one workloads may offer no batching benefit.
Do not repeat rejected admission-window, affinity or completion-barrier changes
without a new falsifiable hypothesis and separately retained evidence.

The separate synthetic Go/OpenSSL AES-GCM comparison measured about 4.7 versus
10.7 GB/s, with setup/AAD differences; librados links the compared OpenSSL.
The latest excluded O profile attributes about 46% of sampled CPU to AES-GCM,
34% to syscalls and 9% to memmove. These are attribution clues, not a proof of
the selected native instruction path, an irreducible floor or an end-to-end
speedup prediction. OpenSSL and native Intel bindings do not fit the retained
CGO-free policy. MinIO sio uses stdlib AES-GCM and adds a different storage
format; sha256-simd accelerates SHA-256, not AES-GCM. CGO-free Go assembly or
archsimd could use wider instructions, but implementing reviewed AES-GCM is
substantial crypto work and is deferred rather than a prerequisite to this plan.

## Evidence and Native Comparison

The checked-in [P07 report](p07/integration-report.json) contains 72 Go/native
pairs: 36 rows per transport, four sizes (4 KiB, 64 KiB, 1 MiB, 4 MiB),
three concurrency levels (1, 16, 64), and read/write/mixed workloads.

| Metric | Secure | CRC-labelled |
| --- | ---: | ---: |
| Worst Go/native p99 ratio | 2.89 | 4.25 |
| Minimum Go/native throughput ratio | 0.375 | 0.368 |
| Total process CPU Go/native ratio | 1.26 | 1.36 |
| Peak RSS Go/native ratio | 2.18 | 2.36 |
| Go allocated bytes, whole process workload | 14.25 GB | 14.25 GB |

These are report-specific observations, not stable cross-host estimates.
Normal rows measure only two operations per worker. At concurrency one,
reported p99 is effectively the maximum of two samples, not a credible tail
distribution. Process CPU and RSS include payload generation, seeding, and
cleanup; they do not isolate library overhead. Native allocation counters are
not available in this report.

CRC labels do not prove matched negotiated wire modes: Go allows CRC while
CephX can prefer secure; native CRC selects CRC. The controlled secure read
diagnostic uses matched secure mode and longer ABBA runs, but covers only
64 KiB reads at concurrency 16. Consult
[diagnostic methodology](../integration/p07/DIAGNOSTICS.md).

The live matrix uses three OSDs, replication two, and one Docker host.
P12 endurance is growth/correctness evidence, not a paired sustained throughput
qualification. P13 recovery scenarios exercise functional behavior, not
large-scale recovery performance. Current P12 release, fuzz, and endurance
certification has not been regenerated for the latest optimization baseline.

[P12 policy](p12/performance.md) permits p99 up to 8 times native and throughput
down to 10% of native. It explicitly disclaims performance parity. Do not
relax those budgets or describe passing them as native equivalence.

Native source observations used in this review:

- [OSDMap.cc](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSDMap.cc):
  `_pg_to_raw_osds` uses retained `crush->do_rule`; `apply_incremental`
  decodes a replacement CRUSH map when the incremental contains one.
- [Objecter.cc](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.cc):
  `_calc_target` uses epoch-qualified PG mapping lookup/update;
  `_send_op` searches PG-specific ordered backoff ranges;
  `handle_osd_backoff` registers ranges and enqueues the ACK with
  `con->send_message`, outside application operation-budget acquisition.
- Native source also has global locks, request scans, session retention, and
  placement workspace allocations. Do not claim native paths are lock-free,
  allocation-free, or globally memory-bounded. Messenger ACK scheduling needs
  its own source analysis before asserting stronger native progress guarantees.

Existing provenance and correctness contracts are recorded in
[P13 provenance](p13/provenance.md) and [protocol sources](p00/protocol-sources.md).

## Findings

These findings describe the reviewed baseline, not a fresh assessment of the
current source. Preserve their historical text and anchors; subsequent phase
status and results record remediation. In particular, F1's scoped control
progress fix is complete; see the [Phase 1 results](performance-phase1/RESULTS.md).

### F1: High - Backoff ACK Progress Under Saturation

[OSD backoff handling](../internal/objecter/osd_session.go#L210) calls ordinary
`Send` for its acknowledgment. [Messenger admission](../internal/msgr/session.go#L663)
applies application pending-count and retained-byte limits. An ACK admission
error causes the wrapper to fail and stop the session, affecting unrelated
pending operations and potentially producing unknown mutation outcomes.
An admitted ACK also shares application dispatch/in-flight constraints; the
OSD dispatcher waits synchronously for its write before consuming more messages.

This is a source-confirmed path, not yet a saturation reproduction. Required
tests include full count/byte budgets, exhausted in-flight slots, stalled
writes, cancellation, reconnect, and shutdown. A control path must remain
bounded and preserve Ceph-required ordering; an unrestricted bypass is unsafe.

### F2: High - Whole-Topology Work on Each Placement

[Placement](../internal/maps/placement.go#L85) decodes the full CRUSH payload
for every route. Decode validates the graph, and
[Place](../internal/crush/place.go#L44) validates it again. This causes repeated
topology parsing, allocation, and traversal on an unchanged map. Map-driven
rerouting multiplies the work by tracked operations and watches.

Unlike this path, native Ceph retains decoded CRUSH state and caches placement
by PG and epoch. The structural difference is confirmed; actual CPU and
allocation growth needs an immutable-map topology benchmark.

### F3: Medium - Client-Wide Lock Across Session Replacement

[getSession](../internal/objecter/client.go#L831) holds the objecter mutex
through old-session `Stop()` and the factory call. Messenger `Stop()` waits
for its owner to exit. Slow teardown can block cached access to unrelated OSDs,
attempt bookkeeping, and closure. The built-in factory starts asynchronous
connection work; this finding does not establish a synchronous handshake under
the lock. Custom factories can nevertheless block there.

### F4: Medium - No Aggregate Client Memory Bound

[Built-in transports](../internal/msgr/transport.go#L41) allocate readers up
to 512 KiB per connection. [Session caching](../internal/objecter/client.go#L822)
has no count cap or idle eviction. One thousand established connections imply
approximately 500 MiB of reader buffers alone, excluding frames and goroutines.

[Incoming messages](../internal/msgr/session.go#L436) are count-bounded, not
byte-bounded. Defaults allow 128 queued messages and frames up to 64 MiB,
with a 32 MiB per-segment limit. The resulting theoretical multi-GiB envelope
is not an observed RSS result and is separate from outbound retained-byte
accounting. Receive read-ahead also holds queued and reader-active frames.

### F5: Medium - Global Backoff Scans and Broadcast Wakeups

[SubmitTarget and Wait](../internal/objecter/osd_session.go#L88) scan all stored
backoff ranges under one mutex, including unrelated PGs. Shared change
notifications wake unrelated waiters. With B ranges and W waiters, updates can
induce O(WB) serialized checking. Distinct IDs have no collection bound.
Unblock handling also scans active submissions. This requires an authenticated
peer/workload; it is not an unauthenticated attack claim.

### F6: Medium - Quadratic Queue Bookkeeping

[Dispatch](../internal/msgr/session.go#L704) scans pending requests and
recomputes in-flight counts. [Removal](../internal/msgr/session.go#L1455)
searches and shifts pending/replay slices; ACK trimming scans replay entries.
Completion, cancellation, and fault-drain batches can accumulate O(N^2) work
on the session owner. Defaults of 128 pending and 64 in flight constrain
current exposure. Raising these limits without measurement is not a fix.

### F7: Medium - Remaining Outbound Payload Copies

Admission cloning, [message framing](../internal/msgr/message.go#L129), and
secure private wire encoding each copy payload storage. Messenger copying for
a 4 MiB write is roughly 12 MiB before OSD/mutation/caller overhead. Replay
repeats framing and wire encoding. Copies currently protect caller and replay
ownership; removing them requires end-to-end ownership tests, not just a codec
microbenchmark.

### F8: Medium - Full Map Copy for Small Incrementals

[Incremental application](../internal/maps/incremental.go#L242) deep-clones
unchanged map collections and CRUSH bytes. Monitor transition collection scans
OSD state per epoch. Small epoch bursts therefore scale with full map size and
can delay publication through allocation pressure. Native also performs scans;
the incremental-copy cost must be isolated before claiming relative speed.

### F9: Medium - Duplicate Manager Session Creation

[Manager cache misses](../internal/mgr/client.go#L275) unlock before invoking
the factory and deduplicate afterward. Concurrent cold-start/failover calls
can create and stop multiple sessions for one target, potentially initiating
redundant authentication work. This is temporary churn, not an established leak.

## Implementation Protocol

For every phase, the implementing LLM must:

1. Verify current source and worktree state. Treat the baseline lines above as
   navigation aids; do not overwrite user changes or assume they remain current.
2. Read only the controlling code and a neighboring test first. State a local
   hypothesis and the cheapest test that could disprove it.
3. Add the smallest deterministic test or benchmark for that hypothesis. Run
   it immediately before broadening the patch. Distinguish expected regression
   failure from setup failure and preserve the baseline result.
4. Implement one behavior slice, run its focused check immediately, then run
   the relevant race tests. Avoid speculative rewrites of channels or locks.
5. Keep timing instrumentation disabled by default. Preserve authentication,
   nonce/counter behavior, replay identity, durable-completion semantics,
   cancellation, generation filtering, and independent returned-buffer storage.
6. Record commands, toolchain, source revision/diff, inputs, limits, outputs,
   and before/after measurements. Use fresh artifact directories. Do not run
   long terminal benchmarks concurrently with agents using a shared terminal.
7. Hand off changed files, test names, results, unresolved cases, and the next
   phase. Mark a phase complete only when its exit gate is satisfied. Do not
   commit or change public configuration semantics without user authorization.

Use `GOTOOLCHAIN=go1.27.1`. Normal focused tests use `go test` on touched
packages; race validation uses `CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race`
with those packages. Run benchmarks with `-run '^$' -bench <pattern>
-benchmem -count=5` on the same host before and after. Use benchstat when
available; otherwise report repeated samples without claiming significance.
Separate benchmark runs from race instrumentation.

## Phased Remediation

### Follow-Up Execution Order

Phases 0-7 retain their historical contracts and evidence; Phases 8-14 own
the remaining checkpoint work rather than restarting completed remediations.
Phase 8's tooling exit gate is complete; its native retry rejection and
instrumentation limitations remain explicit, not parity acceptance.
Phase 9's consumer PGO assessment is complete with adoption rejected.
Phase 10's fresh syscall/scheduler attribution is complete with a no-production-
change verdict; see the [evidence and reevaluation](performance-phase10/README.md).
Next complete Phase 11 policy/endurance work. Shared-host training,
profiling, timing and pressure captures run serially; freeze implementation
and build identities for each capture and retain rejected variants.

Phase 12 evaluates the selected frozen source against unchanged gates; an
unresolved CPU or measurement finding remains a blocker, even if no further
safe optimization is available. Phase 13 executes only separately approved
workload/topology/recovery scopes. Phase 14 documentation and validation are
maintained throughout and renewed after the final changes, with broader scope
deferrals explicit. Crypto watching is deferred monitoring, not permission to
add a backend or a prerequisite that must be implemented to advance this plan.

### Phase 0: Freeze Evidence and Define Qualification

Dependencies: none. Findings: all; measurement coverage.

Implementation status: technical baseline and evidence work completed under
delegated agent-selected provisional qualification criteria. See the
[Phase 0 results and audit](performance-phase0/README.md). Human approval of
the qualification contract and actual native parity remain separate, deferred
obligations; the original review findings and P12 guardrails are unchanged.

- Preserve the reviewed P07 report and diagnostic methodology as historical
  evidence, without rewriting old source-bound reports to match new code.
- Confirm native source symbols against the pinned commit. Establish actual
  negotiated mode for both clients; reject mismatched pairs from mode claims.
- Agree with the maintainer on target topology, concurrency, payload mix,
  acceptable relative latency/throughput/CPU/RSS, and statistical confidence.
  Do not invent or silently weaken a definition of "on par".
- Establish baseline microbenchmarks for placement, queue bookkeeping, full
  submission/replay allocations, map incrementals, and idle connection memory.
  Each phase below can add its own benchmark when it starts.
- Record raw samples, effective limits, queue depth, failures, and resource
  scope. Avoid library CPU claims based on payload-generation-heavy totals.

Exit gate: reproducible baseline and explicit qualification scope. The existing
P12 budgets remain mandatory guardrails, not the new parity definition.

### Phase 1: Protect Backoff Control Progress

Dependencies: Phase 0. Finding: F1. Owners: messenger and OSD session.

Implementation status: complete within the Phase 1 scope after five independent
audit cycles and deterministic regression repairs. See the
[Phase 1 contract, audit and results](performance-phase1/README.md). Ordinary
application saturation no longer rejects required ACKs or stops unrelated work;
true control exhaustion and stalled-writer deadlines remain bounded fail-stop
conditions. Native performance parity and renewed P12 certification are not
claimed.

- First discriminating test: fill a real messenger's count or byte admission
  budget with controlled pending requests, then inject a valid backoff block.
  Assert ACK progress and survival of unrelated requests. Use a fake transport
  with explicit gates; do not rely on sleep-based scheduling.
- Inspect native message ordering and Go sequence allocation before selecting
  a design. Specify whether ACK order is relative to admitted or written
  operations and how reconnect/replay preserves that order.
- Introduce bounded control admission/dispatch only after the regression
  demonstrates the failure. Reserve both count and byte capacity and avoid
  requiring an application reply slot for a no-reply ACK. Do not blindly put
  OSD messages into the existing transport-control queue.
- Decouple dispatcher consumption from write completion only with an ordered,
  bounded handoff and defined failure/cancellation behavior.
- Cover overlapping blocks/unblocks, a permanently stalled writer, reconnect,
  ACK timeout, full reserve, Stop, and mutation outcome classification.

Exit gate: ordinary application saturation alone neither loses required ACK
progress nor tears down unrelated work; control memory remains bounded; existing
admission/backoff ordering and lifecycle race tests pass.

### Phase 2: Reuse Immutable Placement State

Dependencies: Phase 0; execute after Phase 1 for priority. Finding: F2.

Implementation status: complete within the Phase 2 scope after three independent
audits, repairs, repeated source-bound measurements and full validation. See the
[Phase 2 contract, audit and results](performance-phase2/README.md). Warm routes
reuse decoded/certified state without unrelated-topology growth; first-use cost
and retained graph memory remain disclosed. This is not native runtime parity
or renewed release qualification. Phase 3 is complete below; Phase 4 is next.

- First benchmark: repeat `PlaceRawHash` on one immutable OSDMap while adding
  valid unrelated buckets outside the selected rule's subtree. Measure time
  and allocations independently from network IO.
- Retain decoded, validated CRUSH state per immutable map snapshot. Share it
  across incrementals only when CRUSH bytes are unchanged. Cache errors
  consistently if lazy initialization is chosen.
- Keep exported mutable CRUSH-map APIs defensive. A validated internal path
  must not allow graph mutation to invalidate prior validation silently.
- Preserve exact placement results for replicated and EC rules, weights,
  affinity, upmap, PG temporary mappings, and primary shard selection.
- Consider bounded epoch/PG placement caching only if decoded-state reuse is
  insufficient. Do not key it only by object hash or retain old epochs forever.
- Test concurrent routing, CRUSH replacement, unchanged-map increments,
  malformed maps, and returned placement-slice isolation.

Exit gate: repeated routing no longer decodes or globally revalidates unchanged
CRUSH state; placement golden/differential tests and race tests pass; measured
allocation and topology-growth results improve without stale routes.

### Phase 3: Isolate Session Creation and Replacement

Dependencies: Phases 0-1. Findings: F3, F9.

Implementation status: complete within the Phase 3 scope after iterative repairs,
three independent implementation audit rounds and source-bound validation. See
the [Phase 3 contract, audit and results](performance-phase3/README.md). Gated
session work permits unrelated cached progress; same-target creation is
coordinated with independent waiter cancellation and joined cleanup. Custom
noncooperative callbacks can still block shutdown. This is not live latency,
native parity or renewed qualification evidence; Phase 4 remains the next task.

- First OSD test: gate old session A's Stop during generation replacement and
  access cached B. B must complete before A's gate is released. Repeat with a
  gated factory. First manager test: count factory entries during N simultaneous
  misses for the same target.
- Move synchronous teardown and factory work out of the global objecter lock.
  Coordinate creation per target; recheck closed state, address, and generation
  before installation. Stop stale or losing sessions outside the lock.
- Apply per-target manager creation coordination with explicit waiter
  cancellation. One canceled caller must not cancel work required by all other
  callers accidentally.
- Preserve worker accounting, notification ownership, watch interruption,
  ObserveSession event meaning/order, and Close joining all created resources.
- Test replacement-versus-Close, factory failure, target change mid-creation,
  rapid generation changes, canceled waiters, and unrelated-OSD progress.

Exit gate: gated replacement does not block unrelated cached sessions; concurrent
same-target misses do not cause a creation stampede; no leaked workers or
post-Close session installation; package race tests pass.

### Phase 4: Bound Receive and Fanout Memory

Dependencies: Phases 1, 3. Finding: F4.

Implementation status: bounded receive/session policy implemented after explicit
maintainer approval; repeated memory/ownership/race checks pass. Phase remains
blocked on secure-read latency: the historical final live confirmation missed
the unchanged 8x native-p99 diagnostic guardrail. The later
[2026-10-01 execution record](performance-p99-scheduler/RECOMMENDATIONS_20261001.md)
contains 80 valid final-source read legs on the supplied external host, but no
agreed margin or rigorous no-regression closure. See the
[Phase 4 contract, audit and results](performance-phase4/README.md).
No completion commit or renewed qualification is claimed. Phase 5 benchmarks
and local in-flight accounting work have since been performed, as recorded
below; they do not close the Phase 4 latency gate.

Follow-up: the [serial parallelism/GC investigation](performance-p99-scheduler/README.md)
reproduces lower p99 with application-level `GOMAXPROCS=2` across three fresh
clusters. No cgroup throttling was observed. This is diagnostic deployment
evidence, not a library-default fix or closure of the Phase 4 latency gate.

- First measurements: establish increasing fake/loopback session fanout and
  record live heap, RSS, goroutines, reader storage, and post-close retention.
  Separately feed large unsolicited frames to a stalled consumer.
- Define receive-byte accounting that includes queue, active decode, and
  read-ahead ownership. Where allocation precedes admission, constrain decode
  as well as enqueue; a queue-only counter is not a hard receive-memory bound.
- Define bounded overload behavior that cannot silently drop required backoffs,
  notifications, or replies. Account for buffers retained by delivered views.
- Evaluate reader sizing/lazy allocation with the secure-read diagnostic before
  changing 512 KiB read-ahead. Preserve its measured tail-latency benefit.
- Propose aggregate budgets and idle-session policy to the maintainer before
  changing defaults. Never evict active requests, replay obligations, watches,
  or a session being installed. Document accounting versus actual RSS.
- Test limits at boundaries, reconnect, slow consumers, ownership transfer,
  buffer release, concurrent eviction/admission, and shutdown.

Exit gate: documented bounded internal receive/fanout envelope, measured idle
resource growth, no active-session eviction, and no regression in secure read
latency or protocol ordering. Application-retained outputs are reported separately.

### Phase 5: Scale Backoff and Request Bookkeeping

Dependencies: Phase 1; Phase 3 where shared state is involved. Findings: F5, F6.

Implementation status: PG-indexed backoffs/admitted targets, targeted waiter
notification, indexed FIFO request removal and ordered replay-prefix ACK
trimming are implemented and locally measured. See the
[2026-10-01 execution record](performance-p99-scheduler/RECOMMENDATIONS_20261001.md).
The bounded active-backoff overflow policy and renewed certification remain
open; no production limits or broader phase completion are claimed.

Earlier pre-index queue/backoff burst measurements are local evidence in
`/tmp/rados-go-queue-backoff-bursts-v1.txt`, not a checked-in results bundle.
At depth 4096, known completion-burst medians span 17.14-20.90 ms and
cancellation-burst medians span 3.35-7.38 ms across measured cases. Lookup with
4096 unrelated PGs has a 58.9 us median; the 4096-waiter case has a 241 ms
median. These synthetic costs motivate further investigation, not a live p99
causal attribution or native comparison. They are not measurements of the
current indexed implementation; its before/after results and validation are
recorded in the execution record above.

- Benchmark backoffs at 1/64/1024/4096 ranges across many PGs, with multiple
  waiters and unrelated-PG updates. Test duplicate IDs and overlapping ranges.
- Index by PG first; add ordered range lookup only if measured costs justify
  it and overlap semantics are explicit. Target notifications by affected PG
  or range. Define a bounded-state overflow policy without dropping blocks.
- Benchmark completion, cancellation, ACK trimming, replay, and fault drains
  at pending depths 64/128/512/2048 using explicit test-only configurations.
- Replace repeated in-flight scans with owner-maintained accounting if justified.
  Use indexed ordered queues or stable entries to avoid repeated slice shifts.
  Do not trade quadratic work for stale pointers or unbounded tombstones.
- Verify counts across admission failures, cancellation, writes failing before
  completion, ACK-before-reply, reply-before-ACK, reconnect, renewal, and Stop.

Exit gate: benchmark growth demonstrates the intended improvement; backoff
selectivity, FIFO/replay ordering, unknown-outcome semantics, reference cleanup,
and focused race tests pass. Keep production limits unchanged unless separately
approved and justified by load measurements.

### Phase 6: Reduce Write and Map Allocation Costs

Dependencies: Phases 2, 5. Findings: F7, F8. Implement as two separate slices.

Implementation status: secure-only admission-owned framing and component-scoped
immutable incremental sharing are implemented with ownership/snapshot/race
coverage, repeated benchmarks and separate post-GC profiles. CRC copying was
retained after its timing improvement was not established. See the
[execution record](performance-p99-scheduler/RECOMMENDATIONS_20261001.md) for
measurement scope and open certification/endurance gates.
Complete public 4-MiB writes reduce whole-process allocations and improve c16
elapsed time, but the historical comparison observed a serial elapsed-time
regression. The [source-bound Linux write qualification](performance-p99-scheduler/LINUX_WRITE_QUALIFICATION_20261002.md)
did not reproduce it and passed its predeclared scope for `576ce5e` only.
Checkpoint `486305c` adds leased admission/secure storage reuse and endpoint
copy removal, with ownership, authentication and race coverage; its O native
CPU gaps and independent qualification remain open. Do not transfer the older
write qualification to this source or describe the historical regression as
a demonstrated current defect. Report write and map outcomes separately.

- Write slice: benchmark complete Submit/Send and replay for 64 KiB and 4 MiB,
  including OSD encoding, admission, framing, and wire encryption. Preserve
  baseline allocated bytes and allocations per operation.
- Add an internal owned framing path that avoids redundant copies only after
  ownership transfer is explicit. Public and custom session/codec paths keep
  safe copying defaults. Preserve replay payload lifetime and caller-mutation
  isolation, cancellation cleanup, and secure counter preflight.
- Map slice: benchmark a one-pool rename with growing unchanged OSD arrays,
  PG overrides, and CRUSH payload. Isolate transition scanning from cloning.
- Share immutable unchanged collections or copy only modified components;
  never mutate a published prior snapshot. Preserve CRC/epoch/FSID validation,
  atomic publication, and per-epoch lifecycle observation semantics.
- Test old-snapshot isolation, failed incrementals, replacement full maps,
  CRUSH changes, concurrent readers, and reset/replay mutation isolation.

Exit gate: end-to-end bytes/op and time improve in repeated measurements;
ownership, incremental/golden-map, lifecycle, and package race tests pass;
post-GC retained heap does not regress. Report write and map outcomes separately.

### Phase 7: Qualify Sustained and Large-Scale Behavior

Dependencies: Phases 1-6, or explicit recorded deferrals.

Execution status: the earlier diagnostic matched secure matrix, sample-count
follow-up, concurrency sweep through 256, supplementary before/current public
writes and metadata semantics are completed on their recorded source states. The
[execution record](performance-p99-scheduler/RECOMMENDATIONS_20261001.md) and
[compact evidence](performance-p99-scheduler/recommendations-20261001.json)
retain 488 timed legs, observed regressions and all current qualification
blockers. The later A-O investigation adds matched native offered-load
diagnostics and bounded same-process retention; their implementation and
capture are no longer pending. Sustained saturation certification,
multi-host/fanout, snapshots, watch/notify load, disruptive recovery and formal
certification remain unqualified. No Phase 7 exit-gate completion or broad
parity is claimed. Phases 8-14 below decompose its remaining work and the
checkpoint follow-up into actionable ownership boundaries.

- Rerun the existing matched Go/native matrix and longer secure ABBA diagnostic
  on the final source state. Preserve both passing and failing artifacts.
- Add sustained read/write/mixed measurements with warmup, enough samples for
  meaningful p99, repeated alternating runs, and separate library/harness costs.
- Sweep concurrency beyond 64 and offered load through saturation. Record
  successful throughput, error rate, queueing, and recovery; do not exclude
  failed operations to make latency distributions appear faster.
- Exercise large synthetic routing/maps and real connection fanout separately.
  A loopback session count is not a large live Ceph deployment qualification.
- Measure paired metadata/OMAP, enumeration, compound operations, snapshots,
  watch/notify load, reconnect/replay, and map-churn recovery where supported.
- Repeat live tests on multi-host topology and representative deployment
  hardware. Record unavailable environments as unqualified, not passed.
- Run full unit/race validation and the relevant P07/P13 verifiers. Refresh
  source-bound integration reports through their harnesses, not manual edits.
  Re-run P12 qualification/fuzz/endurance/release gates where certification is
  required; do not manufacture independent reviewer signatures.

Full validation includes:

```sh
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=go1.27.1 \
  go build -o /tmp/rados-go-p07-benchmark-check ./integration/p07/benchmark
```

Use `GOARCH=amd64` for an amd64 Linux target. The Linux-only benchmark must be
cross-built: passing macOS tests does not validate code omitted by build tags.
Inspect current harness documentation and container configuration before live
runs. A failed Docker setup is a blocked experiment, not a client regression.

Exit gate: agreed Phase 0 criteria evaluated against repeated final-state
measurements, resource limits and correctness checks passed, remaining gaps
explicitly enumerated. Claim parity only within the measured scope and confidence.

Continuation validation on 2026-10-03: full Go 1.27.1 unit/race tests, vet,
the CGO-free Linux arm64 benchmark cross-build and focused minimum-Go 1.26.8
collector tests passed. P07 and P13 semantic verifiers both rejected stale
source artifacts against the current tree. Docker is unavailable on this host;
renewal requires the documented disposable harness environment. Existing live
fixture authorization does not authorize OSD/MON lifecycle changes or
multi-host recovery experiments. Reports were not rewritten to hide these
failures, and no Phase 7 completion commit is justified by these checks.

### Phase 8: Implement Qualification Capture and Analysis

Dependencies: Phase 0 contract and existing parity drivers. Assessments: A1,
A4. Owners: Go/native benchmark collectors and evidence analyzer.
Status: complete within the tooling exit gate, not native parity qualification.
A Go collection primitive now tests both stopping minima,
synchronized concurrent workers, retained failures/censoring, cancellation and
safety deadlines. Each phase has a one-million-record process limit, divided
among workers; reaching that limit before both minima returns an error and
retains collected records. This is an evidence-storage limit, not a library
memory-policy change or a passing incomplete leg. Unobserved retries remain
null. Explicit unequal worker populations are checked against raw records;
fixed-count diagnostic validation is unchanged. Both live collectors are now
wired, with 30-second operation budgets from recorded start, 30-second setup
and verification RPC defaults, a 15-minute attempt limit and independently
bounded cleanup. Native synchronous calls are not immediately cancelable;
late completion rejects the capture. Existing fixtures are rejected before
seeding, and cleanup owns only fixtures observed absent before the attempt.
Native recorder allocation is outside per-operation timing, as in Go; measured
CPU still includes worker startup and record storage in both implementations.

The earlier collector integration smoke is privately retained at
`/root/proj/rados-go/qualification-smoke-20261003T164603`. It used one fixed
Go/native pair on approved readcache, 4-KiB reads at concurrency one, not ABBA.
Go warmup/measured populations were 24,347/148,790; native populations were
23,423/136,339. Both measured intervals exceeded 60 seconds; payload, fixture
collision protection, removal/NotFound, source/binary/library continuity,
boundary health/placement and actual authenticated MON/OSD modes passed.
Both collectors retained 601 actual RSS samples at a predeclared 100-ms
cadence, with immediately preceding connected/warmed baselines and validated
clock, gap and end coverage. Go idle/peak/incremental bytes were
22,937,600/98,418,688/75,481,088; native bytes were
29,286,400/33,726,464/4,440,064. These are process observations with unequal
recorder representations/retention, not a library-only RSS comparison or a
passing memory gate. Go resource capture now stops before raw-record assembly;
native aggregation is constant work per worker. Sampling errors reject the
attempt without discarding raw operations.
Both captures deliberately fail qualification for unknown retries. Earlier
failed smoke assertions and separately rejected collision attempts remain
retained; they were not overwritten or promoted to passing evidence.

The parity payload investigation also found a 17-bit Go versus 19-bit native
worker-seed shift for concurrent parity fixtures. Go now uses the native shift
in explicit parity mode; ordinary benchmark payloads remain unchanged. The
worker-zero captures are unaffected, but older concurrent captures must not be
retroactively treated as byte-identical conditioning or qualification.

Native retry observation and library/harness cost separation remain qualification
blockers. Seeded ABBA capture and matrix-wide analysis are implemented; fresh
full-matrix qualification is not claimed. Boundary RSS fields remain separate
from the new timestamped interval samples. The one-million-record
limit can reject faster or imbalanced cells before the time minimum; such a
failure is retained, never accepted as a truncated leg. This tooling smoke is
not a Phase 7 exit-gate result. This phase can proceed before CPU gaps close.

Native retry-observation assessment: in the pinned Ceph 20.2.4 upstream
`src/osdc/Objecter.cc`, EAGAIN and redirect replies resubmit through
`_op_submit` without incrementing `op_resend`. A zero `op_resend` delta therefore
cannot establish zero retries. `_prepare_osd_op` increments `op_send` alongside
the request attempt counter, making complete, quiescent before/after samples a
candidate check for prepared objecter attempts. This is not implemented or
accepted retry telemetry: process/counter continuity, exact operation scope,
reset detection, messenger replay coverage and agreement with the Go retry
definition still require proof. Synchronous API success is insufficient.

The final Phase 8 tooling smoke is privately retained at
`/root/proj/rados-go/phase8-smoke-20261003172259`. Go warmup/measured counts
were 25,015/152,326; native counts were 24,993/148,461. Both measured windows
exceeded 60 seconds, and both retained 601 actual RSS samples. Go
idle/peak/incremental RSS was 24,014,848/103,202,816/79,187,968 bytes;
native was 29,450,240/34,234,368/4,784,128 bytes. These remain harness-inclusive
observations. Payload, collision preservation, cleanup/NotFound, artifact pins,
boundary health/placement and authenticated secure MON/OSD modes passed.
Go raw evidence validates with opt-in bounded preparation/replay-dispatch
counts; uninstrumented custom sessions or missing dispatch coverage stay unknown.
The real messenger reconnect test observes one replay, and observer isolation,
prepared retry accounting and collector failure paths pass race tests.
Native raw analysis deliberately rejects unknown retries. The earlier
`phase8-smoke-20261003171919` remains tied to its earlier analyzer source.

The new analyzer freezes matrix, five-or-more rounds, seeded ABBA ordering,
equal two-leg arithmetic weighting, geometric paired-round ratios, bootstrap
seed/count, nearest-rank quantiles and Bonferroni simultaneous 95% intervals.
It resamples whole paired-round vectors across all cells and preserves available
bounds when another estimator is unknown. Synthetic pass/fail/undefined,
single-cell failure, changed gates, missing/duplicate rounds, lifecycle reuse,
unequal weighting and variable-round cases pass. Raw validation binds warmup
and measured counters/records, conditioning, runtime, artifact continuity,
health, every worker's placement, modes and RSS. Exclusive pinned file analysis
retains all attempted leg findings and labeled raw tails/histograms, and checks
the executing analyzer digest. The smoke is a fixed pair, not a full ABBA matrix;
its manifest remains unqualified and reproducibly rejected.

Native `msgr_send_messages` does not count ProtocolV2 replay writes, so it cannot
prove zero retries. Also, `debug_ms=1/1` logs ordinary messages as well as ready
events. The plan explicitly binds this instrumentation and whole-process harness
scope: these native captures are not uninstrumented primary qualification timing.
Neither successful API calls nor a passing statistical-tool result can establish
library-only resource parity. Phases 10-12 retain the attribution and fresh
qualification obligations; P07/P13 historical report renewal remains Phase 14
work requiring a separately approved disposable environment.

- First discriminating checks: deterministic collector tests cross the time
  minimum before the count minimum and vice versa; neither may stop early.
  Test safety-deadline failure, warmup failure, incomplete output, unknown
  retries, timeout/censoring, missing mode records and gapped RSS traces.
- Implement matched Go/native warmup of >=10 seconds AND >=10,000 successful
  operations, followed by >=60 seconds AND >=100,000 measured operations per
  leg. Predeclare safety deadlines and retain incomplete/failed attempts.
- Retain per-operation round/leg/identity/type, start/end, success/error,
  timeout deadline, observed retry count and censoring. Unknown native retry
  telemetry blocks acceptance; do not fabricate zero or count only outer calls.
- Collect connected, equally warmed, immediately preceding idle RSS and all
  interval samples using the same OS method/cadence. Report absolute baseline,
  peak and incremental bytes. Nonpositive or sub-resolution native increments
  make a ratio unavailable, not passing. Keep native allocation and Go heap
  metrics explicitly distinct.
- Bind source/binaries, runtime, limits, topology, fixture conditioning and
  actual authenticated MON/OSD modes for every connection and reconnect.
  Preserve health/placement/correctness guards and observation limitations.
- Implement at least five independently conditioned process-lifecycle rounds,
  with seeded randomized ABBA assignment. Freeze matrix, exclusions, estimator,
  within-round weighting, bootstrap seed/count, quantile/interval convention
  and simultaneous-confidence method before capture. Resample whole paired
  rounds, not individual operations; retain all analysis inputs and tool pins.
- Test analysis on synthetic known pass/fail/undefined cases, including a
  single failing cell, missing or duplicate rounds, invalid ABBA pairing and
  denominator failures. Emit per-round tails/histograms and all four bounds;
  missing evidence must never yield an acceptance result.

Exit gate: both collectors and analysis pass focused tests and a separately
labeled integration smoke run, with reproducible raw records and fail-closed
validation. Tooling completion is not native parity; Phase 12 performs fresh
qualification on a frozen implementation.

### Phase 9: Evaluate Explicit Consumer PGO Builds

Dependencies: fixed diagnostic contract; Phase 8 for any qualifying claim.
Assessment: A2. Owner: benchmark/build tooling. Status: complete experiment;
reject adoption. See the [Phase 9 results and retained assessments](performance-phase9/README.md).
Six representative training profiles, explicit CGO-free off/PGO builds with
compiler-use proof, separate correctness probes and five seeded ABBA rounds
per held-out cell are implemented and exercised. The final private capture at
`/root/proj/rados-go/phase9-pgo-20261003195757` retains 90 held-out legs and
6,198,269 operations; dependency-pinned raw re-analysis is exact. Small-read
CPU upper 1.29144 and throughput lower 0.88027 do not establish the unchanged
gates. Mixed CPU improves modestly in two runs, but other CPU benefits are not
established; all three source-bound reject/defer/reject results remain retained.
Final PGO binary size grew 2.56%. This closes the reproducible assessment
with a reject decision, not proof of a causal regression,
native parity, a universal speedup, production PGO or runtime authorization.

- First check: build the same consumer executable with `-pgo=off` and an
  explicit pinned CPU profile, verify compiler use and distinct binary pins,
  then run byte-exact correctness tests. Keep `CGO_ENABLED=0` for both builds.
- Collect representative training profiles across reads/writes/mixed traffic,
  sizes, concurrency and real call sites; predeclare composition/weighting.
  Do not use the short excluded O write profile as the sole training set.
- Separate instrumented training, tuning/pilot runs and independent held-out
  evaluation. Compare randomized paired builds under identical runtime and
  limits; measure CPU/op, throughput, p99, interval RSS and binary size, with
  a stdlib/non-PGO baseline and unchanged native comparator.
- Explain consumer ownership of PGO: benefits depend on the final application's
  workload/profile. Avoid a hidden default.pgo or universal library speedup
  claim. PGO will not create a missing VAES AES-GCM implementation.
- Retain regressions and inconclusive cells. Recommend an opt-in build recipe
  only if independently repeated benefits justify its profile maintenance and
  do not violate the unchanged correctness/performance/memory gates.

Exit gate: reproducible held-out assessment and explicit keep/reject/defer
decision. A null or unfavorable result closes this experiment, not the CPU
parity finding. Any adopted variant needs its own Phase 12 source/build scope.

### Phase 10: Attribute and Reduce Confirmed Syscall Overhead

Dependencies: checkpoint baseline and Phase 1 control-progress contracts;
Phase 8 for qualification. Assessment: A2. Owners: messenger/transport.
Status: complete attribution experiment; no production change. See the
[Phase 10 contract, repeated evidence and verdict](performance-phase10/README.md).
The final source-bound 36-leg capture at
`/root/proj/rados-go/phase10-syscalls-20261003212821` has 2,294,809 measured
operations and exact raw reproduction. Separate phase-filtered baseline,
raw-address socket/futex/epoll traces and CPU/context-switch probes cover small
c1 reads and concurrent reads/writes. Small reads already issue about one
socket write/op; concurrent-write control candidates do not prove ready-frame
density. Scheduling costs vary by workload and observer overhead is explicit.
No causal safe small optimization or independent adoption benefit is established;
ACK ambiguity, unavailable scheduler tracepoints and native instrumentation are
not silently promoted to certainty. No transport, runtime, TCP or crypto policy
changed. This closes the permitted no-change experiment, not native CPU parity.

- First measurement: fresh matched, excluded profiles on small c1 reads and
  concurrent read/write cells, recording socket read/write, ACK, epoll and
  futex counts alongside CPU attribution and scheduler traces. Separate setup,
  warmup and cleanup; do not interpret blocked durations as CPU time or old D
  counts as current-source evidence.
- Establish a local hypothesis about avoidable wakeups or already-ready frame
  density. Add a deterministic handoff/control test and repeated benchmark
  before editing. Keep source/runtime/affinity/fixture limits identical.
- Investigate scheduling handoffs before adding transport complexity. Consider
  opportunistic same-connection coalescing only when measured queue density
  supports it, with no deliberate batch-fill wait and a strict memory bound.
  A complete-frame Write already exists; writev is not an automatic benefit.
- Preserve control priority/reserves, FIFO/replay and nonce order, generations,
  cancellation/deadlines, unknown mutation outcomes, short-write/error handling
  and producer/pending/writer lease lifetimes. Do not blindly buffer writeTasks
  or let reused wire storage overwrite an active batch.
- Compare c1 and concurrent workloads, idle and host-load cases, and full
  latency distributions. A lower syscall count alone is insufficient; require
  repeated CPU/throughput benefit without tail, memory or correctness regression.
  Leave TCP and global runtime knobs unchanged unless separately approved.

Exit gate: every retained change has causal local evidence, lifecycle/race
coverage and independent full-client benefit; otherwise publish a no-change
verdict. No-change does not waive the remaining native CPU target.

### Phase 11: Complete Resource Policy and Endurance Assessment

Dependencies: Phases 4-6 ownership/accounting, Phase 8 resource collection.
Assessments: A4, A5. Owners: backoff/session budgets and endurance tooling.
Status: pending policy and sustained evidence; bounded O windows already pass.

- First policy test: reach a proposed active-backoff bound with duplicate and
  overlapping IDs/ranges, then apply further blocks/unblocks under saturation.
  Define a bounded-state outcome that never silently drops required blocks or
  reports an unknown mutation as successful. Obtain approval before adding
  defaults, changing caps or choosing new overload semantics.
- Audit aggregate outbound and connection-scaled memory, not just receive
  charges. Include reader capacity, wire/mutation caches, active leases, queues,
  stacks, maps and allocator overhead; report application-retained outputs
  separately. Existing per-session limits are not a whole-client RSS cap.
- Specify any missing policy as a separate small implementation slice, with
  deterministic boundary/overflow/slow-consumer/reconnect/Close tests. Do not
  evict active operations, replay obligations, watches or installing sessions.
- Predeclare sustained duration, workload cycles, session fanout, sampling,
  memory-pressure envelope and growth criteria before execution. Exercise
  read/write/mixed traffic, cancellation, retries, overload and post-Close idle;
  retain RSS trajectories, GC behavior and native allocator observations.
- Compare controlled memory pressure against unconstrained runs, with no OOM,
  uncontrolled retention or unacceptable GC-tail behavior under the declared
  budget. Do not globally tune GOGC/GOMEMLIMIT or relax RSS parity to rescue a
  result. Finite logical backing does not prove an allocator/RSS bound.
- Preserve N's rejected padded-4-MiB memory tradeoff and K's timeout. A new
  approved deployment-efficiency experiment, if requested later, must keep its
  absolute memory budget and verdict separate from original parity failures.

Exit gate: approved required bounded-state policies are implemented and tested;
predeclared endurance/pressure criteria pass with correctly labeled memory
metrics. Unknown policy, growth or resource failures keep this phase blocked.

### Phase 12: Evaluate Final-Source Latency and Native Parity Gates

Dependencies: Phase 8; Phases 9-11 completed or explicitly assessed/deferred,
with unresolved safety/measurement blockers retained. Assessments: A1, A2,
A4, A6. Owners: qualification runner and Phase 4 regression assessment.
Status: pending; neither O diagnostics nor `486305c` qualify the current source.

- Freeze the approved final build, matrix, limits, exclusions and analysis plan;
  collect fresh independent samples without optional stopping or replacing
  unfavorable attempts. Qualify adopted PGO and non-PGO build scopes separately.
- Evaluate CPU, p99, successful throughput and incremental RSS confidence
  gates in every in-scope cell, alongside correctness/mode/environment checks.
  Do not average ten failing CPU cells away or turn AES deferral into a waiver.
- Reevaluate the specific Phase 4 secure-read no-regression/latency gate with
  its existing guardrail and explicitly agreed margin. Application P2 tuning
  or a short favorable c16 result is not a library-default closure.
- Investigate K's admitted timeout with retained queue/service/delivery evidence
  and narrowly targeted reproductions. Run sustained offered-load comparisons
  with all outcomes and goodput at the same latency budget; publish safe load
  ceilings per workload, not a success-only p99 or universal capacity claim.
- Assess the current write/map/ownership changes against their historical
  regressions and source pins. Keep `576ce5e`'s narrow write qualification
  distinct from current-source native parity and from independent local gains.
- Publish pass/fail/unknown per gate, unresolved causes and scoped deferrals.
  If CPU or another gate still fails under retained constraints, state that
  qualification is blocked rather than expanding the optimization scope or
  weakening acceptance automatically.

Exit gate: every scoped qualification and Phase 4 requirement passes with
source-bound independent evidence. A complete failure assessment is useful
work, but is not a passed phase or a phase-complete parity verdict.

### Phase 13: Extend Workload, Fanout and Recovery Coverage

Dependencies: Phase 8 evidence tooling and explicit topology/operation scope;
Phases 11-12 for any inherited resource or qualification claim. Assessment: A5.
Owners: workload-specific harnesses and authorized deployment operators.
Status: deferred beyond current native Linux two-pool fixture authorization.

- Predeclare independent read/write/mixed cells through higher concurrency and
  real connection fanout. Include allocating Read and caller-buffer ReadInto
  as distinct contracts. Synthetic routing/map/fanout evidence remains separate
  from live multi-node behavior.
- Implement paired metadata/xattr/OMAP, enumeration, compound operations,
  snapshots and watch/notify workloads with operation-specific completion,
  rates, value/cardinality limits and failure budgets. Byte throughput is not
  an appropriate substitute for every operation family.
- Build controlled reconnect/replay, map-churn and OSD/MON loss/rejoin cases
  only on an explicitly authorized disposable cluster. Distinguish injected
  failures from unexpected client errors and unknown mutation outcomes.
- Record representative hardware, fault domains, devices, network, placement,
  replication, occupancy and client connections. Separate local, 64-OSD and
  256-OSD results as required by the governing topology contract; none can
  inherit qualification from another without evidence.
- Cover supported deployment architectures and actual matched service modes;
  do not label CRC requested policy as observed CRC transport. Record missing
  hosts/containers/tooling as scoped deferrals, not successful execution.

Exit gate: each approved expanded scope passes its own predeclared evidence
and correctness/resource gates. Missing topology or destructive-test approval
keeps that scope deferred; it is not permission to modify production pools.

### Phase 14: Renew Validation, Documentation and Crypto Watch

Dependencies: frozen implementation/builds from preceding phases, with explicit
remaining findings and approved certification scope. Assessments: A3, A6.
Owners: release/verification tooling and documentation. Status: pending renewal;
AES work is monitoring only, not a scheduled backend implementation.

- Refresh the review/index/status catalog after each completed capture or
  checkpoint. Preserve dated historical failures and source-bound records;
  correct stale pending/uncommitted wording in current summaries without
  rewriting the older verdicts. Update the handoff with actual blocked gates.
- Run final source/build unit, diagnostic, vet and focused/full race gates,
  minimum-Go checks and CGO-disabled deployment builds. Cross-build the
  Linux-only benchmark and supported targets explicitly; a cross-build is not
  runtime coverage. Race-toolchain CGO does not authorize shipped native deps.
- Regenerate applicable P03/P04/P06-P11 integration evidence through harnesses,
  then the required P07/P12 qualification, fuzz, endurance and release gates.
  Run P13 recovery only within approved disposable scope. Never hand-edit
  hashes or manufacture independent/human signoff; unavailable required hosts
  or tools keep the relevant certification unqualified.
- Bind qualification to the actual consumer build, source/profile/analysis
  identities and negotiated-mode scope. A changed profile, dependency, runtime,
  resource policy or implementation needs the appropriate renewal, not an
  inherited certificate from an older checkpoint.
- Watch upstream Go AES-GCM/VAES/GHASH and archsimd maturity, including API
  stability across supported Go versions. Reconsider only on a concrete,
  maintained CGO-free implementation with independent benchmarks and security
  review. Keep native OpenSSL/Intel bindings and experimental SIMD out of the
  default build; do not implement bespoke crypto as routine phase cleanup.

Exit gate: current documentation and required source-bound validation/release
records agree with the real scope and unresolved findings; all required
certification gates and signoffs pass before any release-qualified claim.
Crypto monitoring has no backend-delivery exit gate and never blocks an
otherwise valid CGO-free build or silently relaxes the parity requirements.

## Handoff Record Template

For each phase, append or attach a concise record containing:

- Phase and findings addressed; status: pending, in progress, passed, deferred,
  or blocked, with reason and owner.
- Baseline/final revisions or patch identity; changed files and configuration.
- Hypothesis, first discriminating test, baseline result, and final result.
- Exact commands, environment, fixture/topology sizes, negotiated modes, and
  artifact locations/checksums.
- Repeated performance samples, allocation/resource results, and what they do
  not establish. Keep synthetic and live evidence separate.
- Correctness/race results, unresolved failures, public-behavior decisions,
  exit-gate assessment, and the next smallest implementation task.

## Review Validation

The read-only review ran 101 focused tests successfully across the selected
objecter backoff, messenger session, and map placement test files. No new live
cluster benchmark or deterministic saturation reproduction was run. These
passing tests establish existing regression coverage, not resolution of F1-F9.