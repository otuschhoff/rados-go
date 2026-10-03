# Phase 11: Resource Policy And Endurance Assessment

Status: **complete within the approved resource/endurance scope; not native
parity or release qualification**. Date: 2026-10-03. The user explicitly approved
configurable defaults of 4,096 active IDs / 8 MiB charged state per OSD session,
fail-stop on overflow, and isolated pressure diagnostics without host/GC policy
changes. Final source-frozen evidence satisfies all predeclared diagnostic gates.

## Completed Policy-Neutral Work

The active backoff owner stores unique IDs and a second index by PG. Replacing
an ID removes its old PG entry; overlap is not permission to merge/drop IDs.
The new deterministic test covers 4,096 overlapping IDs, duplicate blocks,
cross-PG replacements, unknown/repeated unblocks and complete index release.
Boundary tests additionally enforce both count and byte limits.

The audit found a separate terminal retention defect: a retained closed session
kept its backoff maps after the receive loop exited. The fix drops both maps
only after terminal receive/ACK-worker teardown, preserves the original terminal
error and wakes waiters. Stop and terminal-error regression cases prove release
and error preservation. No live block, request/replay obligation or watch is
evicted during normal operation.

Tests reside in [backoff_control_test.go](../../internal/objecter/backoff_control_test.go)
and [osd_session_test.go](../../internal/objecter/osd_session_test.go); the fix
is in [osd_session.go](../../internal/objecter/osd_session.go).

## Aggregate Memory Audit

These are logical backing/ownership observations, not additive RSS estimates.
Shared mutation/message leases may be charged by several limits while owning
one payload; runtime, allocator and application storage add different costs.

| Surface | Existing Policy Or Accounting | Aggregate Caveat |
| --- | --- | --- |
| Receive sessions | Default 256 | Connection fanout is bounded, not whole-client RSS |
| Receive backing | Default global 256 MiB | Not outbound, stacks, maps or application-owned results |
| Queued receive | Default 64 MiB per session | A queue-admission limit, not another independent global allocation |
| Outbound session | 128 messages, 320 MiB retained, 64 in flight | Hypothetical 256 fully charged sessions allow 80 GiB of logical retained charges; this is not measured or necessarily distinct backing |
| Built-in reader | Up to 512 KiB per transport | Hypothetical 256 readers account for 128 MiB before other surfaces |
| Secure wire cache | Up to 8 MiB per transport idle cache | Hypothetical 256 full caches account for 2 GiB; oversized active encodings are separate temporary backing |
| Mutation admission | 64 requests, default 32 MiB times 64 = 2 GiB of payload allowance | Shared leased payloads overlap outbound accounting; caller copies and compound encodings need separate attribution |
| Mutation idle cache | 8 MiB per object client | Actual capacities count; late releases after Close cannot refill it |
| Control reserves | 16 messages / 1 MiB in messenger and ACK dispatcher | Distinct queue/active lifetimes; not a limit on installed backoff IDs/ranges |
| Active backoffs | Approved default 4,096 IDs / 8 MiB charged per OSD session | Hypothetical 256 sessions allow 2 GiB of logical backoff charges, not additional proven RSS; decode/ACK bounds alone were insufficient |
| Other state | Request maps, PG indexes/waiters, watches, install/reconnect state, stacks, map generations, allocator overhead | Finite configured backing is not a demonstrated whole-client allocator/RSS envelope |
| Application storage | Caller payloads, returned data, retained borrowed results | Must be reported separately from library and harness storage |

Anchors: [client.go](../../client.go), [objecter/client.go](../../internal/objecter/client.go),
[mutation.go](../../internal/objecter/mutation.go), [transport.go](../../internal/msgr/transport.go),
[receive_budget.go](../../internal/msgr/receive_budget.go),
[session_control.go](../../internal/msgr/session_control.go).

## Approved Policy

`Config.MaxBackoffs` and `Config.MaxBackoffBytes` apply per OSD session. Zero
selects 4,096 and 8 MiB respectively; negative counts are invalid. The options
`max_backoffs` and `max_backoff_bytes`, corresponding command arguments and
explicit `ParseEnv` suffixes `MAX_BACKOFFS` / `MAX_BACKOFF_BYTES` accept strictly
positive decimal values. Count values must fit the platform `int`; byte values
may span `uint64`. Configuration parsing does not implicitly read environment.

The logical charge is 1,024 bytes for each unique ID's metadata/index allowance
plus the byte lengths of both range endpoints' key, object and namespace
strings. This is not an exact allocator charge or an RSS cap. Duplicate IDs do
not charge twice; replacement subtracts the old charge before checking the new
state and is transactional. A rejected replacement leaves the old block intact.
Unblock, generation reset and terminal teardown release the ledger.

Overflow fails only the affected session before ACK of the uninstalled block,
using the existing messenger saturation error path. No required active block is
silently evicted. A real-messenger regression proves a dispatched unresolved
request retains `ErrOutcomeUnknown` together with saturation, never success or
an unsafe replay. Slow ACK writer, reset/reconnect, cancellation and Close cases
use deterministic transport tests, without injecting faults into the live cluster.

Required invariants: retain every required block until unblock/reset/terminal;
never ACK an uninstalled block or silently evict state; wake blocked callers with
the correct failure; preserve uncertain-mutation outcomes; cover limit equality,
count/byte overflow, duplicate/replacement growth, slow ACK writers, generation
resets, reconnect and Close. Existing ACK overflow behavior is not approval of
a new active-state overflow policy; the explicit approval above authorizes it.
Bounded map entry counts alone would not
prove bounded string/map backing or allocator RSS.

## Frozen Endurance Plan

The [existing retention collector](../../integration/p07/benchmark/retention.go)
and [runner](../../integration/p07/retention-parity.mjs) report fixed-operation
windows and explicitly disclaim endurance guarantees. Prior O bounded windows
remain historical evidence, not fresh Phase 11 acceptance. They do not supply
the required whole-client duration, workload-cycle, pressure, retry/cancellation
or post-Close measurements. Repeating them would not close these gaps.

The new [driver](../../integration/p07/endurance.mjs) freezes `plan.json` before
execution: eight independent processes, two repetitions of Go/native under
plain/pressure conditions, with mirrored condition and implementation orders.
Each process retains one client across fifteen 1-MiB/concurrency-16 windows on
`test-3x`, cycling read/write/mixed five times. Each window requires 1 second
AND 1,000 warm operations, then 8 seconds AND 10,000 measured operations;
cumulative measurement is at least 120 seconds AND 150,000 operations per process.
The first complete cycle conditions all workloads; the remaining twelve windows
retain at least 96 measured seconds AND 120,000 operations under held pressure.
Native retries remain unknown; the short diagnostic labels cannot pass parity.

Raw operation records and 100-ms measured RSS samples are retained for every
window. Post-window forced GC is diagnostic-only: record Go live heap, cumulative
GC count/pause and process RSS separately from native glibc allocated bytes/RSS.
The fixed growth budget is `max(16 MiB, 10% of baseline)`, comparing medians of
windows 8-11 versus 12-15 for both RSS and the implementation-specific allocation
metric. After Close, retain 32 samples at 100 ms, covering at least 3 seconds;
late median RSS must not exceed the largest post-window RSS plus 16 MiB.
Every measured p99 must be at most 50 ms. Total GC pause between adjacent window
observations must be at most 500 ms, a conservative bound on any pause in that
interval rather than a pause-distribution estimate. Pressure/plain throughput
median ratios over windows 4-15 must be at least 0.90 independently per repetition
and implementation. These absolute diagnostic criteria do not replace parity's
confidence bounds, operation minima or incremental-RSS gate.

The [pressure helper](../../integration/p07/endurance_pressure.c) joins only a
fresh private child cgroup; the existing root memory controller is used without
changing host policy. Initial hard limit is 1 GiB, swap disabled. After window 3
closes, the fresh group's full-cycle `memory.peak` baseline must be at most
512 MiB; retain its instantaneous `memory.current` separately. This avoids
calibrating pressure to a read-only cache footprint or arbitrary GC timing.
Retain and touch 256 MiB of helper
memory, set high to baseline + 248 MiB and hard max to baseline + 512 MiB. Require
actual `memory.events.high > 0` with `max`, `oom` and `oom_kill` all zero; helper
storage is separate from target-process RSS. All children must exit before the
private group is removed. An early child exit wakes the helper, and a dead helper
terminates its child. No global `GOGC` / `GOMEMLIMIT`, swap or controller changes.

Source/analyzer imports, binaries, CGO-disabled Go 1.27.1 build and native library
are pinned. Runtime stays P10/GOGC100/GOMEMLIMIToff on CPUs 0-9. Actual secure
MON/OSD modes, all 48 workload/worker placements, warning/health continuity,
absent-before-seed fixture ownership, byte-exact payload and cleanup NotFound
are required. Root directories are 0700 and raw files 0600. Failed executions
remain retained; reproduction recomputes metrics/gates and refuses overwrites.

Compare unconstrained and process-isolated pressure runs without modifying host
policy or global GOGC/GOMEMLIMIT. Pressure must have a hard declared envelope,
host safety headroom and stop conditions; report pressure-helper memory separately.
Retain interval RSS, immediate equally warmed idle, post-GC heap/GC counters and
native allocator observations as different metrics. Predeclare plateau/growth,
tail/throughput and post-Close criteria; do not convert max backing into an RSS
pass. Fault injection that restarts/remaps OSDs/MONs requires a separately approved
disposable deployment; existing fixture access does not authorize lifecycle work.

## Final Evidence And Reevaluation

Private retained root: `/root/proj/rados-go/phase11-endurance-20261003230524`.
Eight fresh processes / 120 windows completed 1,430,585 measured operations over
1,040.737003 measured seconds. Each process met both time and population minima;
all raw correctness, secure mode, placement, source/binary/library and sampled
RSS checks passed. Observed authenticated OSD connection records were 15-17 per
process, not proof of 256-session or multi-host scalability. No unexpected
operation failures, timeout/censoring or unknown Go retry counts were accepted;
native retries remain unknown, not zero. Every pressure group had actual high
events, with hard-max/OOM/OOM-kill counters zero.

| Process | Operations | Measured Seconds | RSS Growth MiB | Allocation Growth MiB | Post-Close RSS MiB | Maximum Window p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| r1 plain Go | 175185 | 135.657264 | -10.554688 | 0.000778 | 25.035156 | 27.926922 |
| r1 plain native | 183844 | 125.789462 | 1.046875 | 0.002052 | 125.617188 | 22.251291 |
| r1 pressure Go | 178718 | 129.850962 | -7.775391 | 0.010143 | 26.400391 | 28.360156 |
| r1 pressure native | 178637 | 128.976850 | 0.003906 | 0.000366 | 125.734375 | 36.597212 |
| r2 pressure native | 186356 | 126.870095 | 3.238281 | 1.580399 | 125.996094 | 20.519724 |
| r2 pressure Go | 176247 | 129.497411 | -13.277344 | 0.009514 | 18.445312 | 24.275315 |
| r2 plain native | 172950 | 132.678732 | 0.023438 | 0.002815 | 123.726563 | 24.138238 |
| r2 plain Go | 178648 | 131.416226 | 2.677734 | 0.011650 | 16.779297 | 27.818915 |

Allocation is post-GC Go live heap versus partial glibc allocated backing; the
columns are not comparable heap definitions or library-only costs. Pressure
high-event counts were 123 / 359 / 403 / 44 in the table's pressure order.
Conditioning peak baselines were 197.660156 / 145.101563 / 145.781250 /
200.496094 MiB, with the separate 256-MiB helper excluded from target-process RSS.
Pressure/plain throughput ratios were Go r1 1.024229 / r2 1.011485, native r1
1.072028 / r2 1.130394; all exceed 0.90. These descriptive repeated diagnostics
do not prove pressure improves throughput or establish parity confidence bounds.
The largest inter-window total Go GC pause was 108.716856 ms, below 500 ms;
all 32-sample post-Close observations passed the fixed RSS criterion.

Exclusive raw reanalysis is byte-identical to `summary.json`:

```sh
node integration/p07/endurance.mjs analyze FRESH_CAPTURE_ROOT NEW_ANALYSIS_JSON
```

The retained `closure-audit.json` independently checks all 384 exact fixtures
with read-only `rados stat` returning NotFound, unchanged health/warnings and no
remaining private pressure groups. Credentials and private raw evidence are not
committed. No host/controller/GC, pool, credential, OSD/MON lifecycle or recovery
policy was changed.

Two incomplete assessments remain retained, not discarded or promoted:
`phase11-endurance-20261003222313` used a read-only baseline; its pressure write
window took 481.645144 seconds, p99 5,313.315273 ms and failed RSS sample cadence.
`phase11-endurance-20261003224500` used instantaneous memory after a full cycle;
five legs passed before the second Go pressure leg failed cadence. Passing and
failing Go baselines differed by about 17 MiB. The first failure also exposed
missing failed-run pressure-counter retention, repaired before the second run.
The final version uses the observed full-cycle peak, extends the assessment to
preserve twelve pressured windows, and retains failed-run counters/health. No
pressure increments or growth, latency, throughput, GC, cadence or parity gates
were relaxed; final evidence belongs only to its pinned version-3 plan/source.

Deterministic ten-repeat race tests cover active count/byte overflow before ACK,
duplicate/overlap/replacement accounting, dispatched unknown outcomes, stalled
ACK writers, cancel/reset/reconnect and terminal release. Live cluster faults
were not injected. Full Go 1.27.1 race tests, CGO-disabled build/vet, minimum Go
1.26.8 focused races, Linux/arm64 CGO-disabled benchmark build, module integrity,
six Node suites including strict native/helper builds, editor diagnostics and
documentation/whitespace checks pass. Reevaluation found no remaining Phase 11
exit findings within this declared scope.

N's rejected padded-4-MiB tradeoff, K's timeout, original incremental-RSS parity
gate, CGO-free/standard-library crypto constraint and unresolved native retry,
broader topology, P07/P13 renewal and P12 release/24-hour-soak gates remain intact.
This finite diagnostic assessment is not a whole-client RSS or long-term
endurance guarantee. Phase 12 evaluates final-source latency/native parity next.