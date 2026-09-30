# Performance and Scalability Review and Remediation Plan

## Status and Scope

Review date: 2026-09-30.
Reviewed Go baseline: `dde29cde727dc963238acc4fa13a5a277a9f5c80`.
Native comparison target: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

**Conclusion: general performance and scalability parity with native librados
is not established.** Existing measurements pass local regression budgets but
leave material scaling risks and coverage gaps. This document records a
read-only review; none of the proposed remediations is implemented by it.

The review covers messenger admission, transport, replay, OSD backoffs,
placement, session lifecycle, map updates, memory bounds, and benchmark scope.
Source-confirmed behavior is distinguished from inferred workload impact.
Large-cluster costs and saturation failure scenarios have not been reproduced
by new benchmarks or regression tests during this review.

Earlier copy reduction, secure encoding, replay-reference cleanup, and bounded
receive read-ahead improvements remain part of the baseline. Preserve their
ownership and lifecycle tests throughout remediation.

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
blocked on secure-read latency: final live confirmation misses the unchanged
8x native-p99 diagnostic guardrail, and no rigorous no-regression conclusion is
established. See the [Phase 4 contract, audit and results](performance-phase4/README.md).
No completion commit or renewed qualification is claimed; Phase 5 is not started.

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