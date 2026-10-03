# Phase 12: Final-Source Qualification In Progress

Status: **blocked, not complete and not qualification**. Work starts from
`b1a7792`; the Phase 11 resource/endurance verdict does not satisfy this phase's
CPU, p99, throughput, incremental-RSS or Phase 4 requirements. No completion
commit is justified while those requirements remain open.

## Implemented Evidence Repair

The qualification file reader previously pinned only its own module despite
executing imported validators. It now requires exact SHA256 bindings for both
[qualification.mjs](../../integration/p07/qualification.mjs) and
[parity.mjs](../../integration/p07/parity.mjs), records those bindings in its
output, and rejects missing, incomplete or changed dependency pins before
reading captures or creating analysis output. The owning runner records them
in the qualification manifest. Existing tests cover exclusive outputs and
missing/changed dependency pins, alongside the full raw-evidence contracts.

Old manifests without these bindings must not be silently renewed or filled
with current hashes. Historical captures remain source-bound diagnostics for
their original tools; new qualification captures need the new manifest contract.
This change does not alter production Go code, crypto, runtime or acceptance
thresholds.

## Controlling Open Gates

| Gate | Current Finding | Required Closure |
| --- | --- | --- |
| Native retry evidence | Native sustained records explicitly contain `retry_count: null`; successful synchronous completion is not a zero-retry proof | Observe objecter attempts and messenger replay for the exact request scope with complete lifecycle/reset coverage, then test the observation contract |
| Native instrumentation scope | `debug_ms=1/1` negotiation logging remains enabled during native timing | Bind actual secure modes while separating probes from uninstrumented timing, and disclose/resolve observer and recorder resource attribution |
| Final matrix | Existing matrix runner deliberately expects invalid evidence from unknown native retries | Implement a qualifying capture path after observability is resolved; freeze at least five independent seeded ABBA rounds and all four unchanged confidence gates before execution |
| Phase 4 comparison | No explicit non-regression margin is approved; inherited evidence is short, and ReadInto is not the original default owned-Read contract | Agree the margin and use a pinned Phase 3 baseline/current default-Read comparison plus the unchanged 8x guardrail; retain uncertainty and every unfavorable attempt |
| K timeout / load ceilings | An admitted request exceeded the 500-ms deadline; its service delay has no causal trace | Run source-bound targeted reproductions and sustained matched-load comparisons retaining timeout/overload/error/censoring and goodput at the same latency budget |
| Historical regressions | Prior write qualification, optimization probes and resource/endurance results have different source/method scopes | Evaluate current source independently; do not relabel `576ce5e`, K, O or Phase 11 evidence |

The Phase 4 approval prompt proposed a one-sided 95% upper current/baseline p99
ratio of 1.05, without changing the 8x guardrail or stricter parity thresholds.
The response was that the user is unavailable and work should proceed
autonomously. That is not approval of a numerical margin or an explicit
disposition of the inherited tail finding; **1.05 remains a proposal only**.
See the [original audit requirement](../performance-phase4/AUDIT.md#open-exit-finding).

The native collector and runner make the retry limitation executable, not merely
a documentation caveat: the runner asserts native retry fields remain unknown,
and qualification analysis rejects them. Repeating that matrix cannot produce
valid parity evidence. `op_resend` alone is insufficient: objecter redirect/
EAGAIN submission paths and messenger replay need their complete common
definition. Do not replace null with zero, infer absence from successful calls,
or weaken the validator to manufacture a pass.

## Retained Timeout Audit

The read-only audit of private K evidence
`native-offered-parity-20261003-k/readcache-64000-r5-l3-go.stdout`
recounts exactly one timeout at outcome index 51,932:

| Boundary | Nanoseconds |
| --- | ---: |
| Scheduled arrival | 811437500 |
| Queue admission | 811446894 |
| Worker start | 817040473 |
| Read start | 817043817 |
| Return | 1311479129 |
| Delivery delay | 9394 |
| Queue delay | 5593579 |
| Dispatch delay | 3344 |
| Service | 494435312 |
| Total arrival-to-return latency | 500041629 |

The request was admitted, attempted and marked as a deadline miss. Nearby
scheduled arrivals include successful service of 6,965,129 / 1,074,816 ns and
then overload rejections. Aggregate resources record 20 GC cycles and
5,024,520 ns of cumulative pause for that leg. These observations do not
identify the service stall as GC assistance, scheduling, networking, retry or
server latency; the recorded total stop-the-world pause alone is much smaller
than the service interval. No causal production fix or safe load ceiling is
claimed. Historical JSON, hashes and binaries were not rewritten.

## Verification And Next Decision

Qualification/native, PGO, syscall and endurance regression suites pass after
the dependency repair, including missing/drifted dependency rejection. No new
live matrix or production fault injection was performed. Existing approved
fixture access does not authorize OSD/MON restart, remap or recovery operations.

Phase 12 remains blocked until every scoped gate passes with fresh independent
evidence. An explicit Phase 4 margin/decision is required before freezing that
comparison; native observability and uninstrumented resource attribution remain
technical work, not waived requirements. Keep CGO-free deployment, standard
library crypto/AES WATCH, original RSS/parity thresholds and broader release/
topology/renewal obligations intact. Do not make a completion commit or mark
this phase complete on the basis of this partial repair or a completed failure
assessment. A partial-work checkpoint does not close these requirements.