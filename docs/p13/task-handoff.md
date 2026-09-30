# P13 Task Handoff

Status: **implementation complete; quick qualification passed, full source-bound qualification pending**.

This file is the durable handoff for P13. Update it after every completed task
with files changed, exact commands and exit status, evidence identities, and
remaining risks. Do not convert planned checks into passed claims without
observed output.

## Starting Point

- rados-go baseline: `2807112` (`Match librados replicated-pool outage recovery`).
- Prior EC placement baseline: `e934cec` (`Match librados erasure-coded shard placement`).
- Ceph source baseline: v20.2.0 commit
  `69f84cc2651aa259a15bc192ddaabd3baba07489`.
- Live server baseline: Ceph v20.2.4 commit
  `7f793731f1b39eb4f465e960113d2363c311b964`, using the image digests pinned by P12.
- Baseline validation recorded before P13 planning: 999 repository tests passed;
  race-enabled `internal/maps` and `internal/objecter` tests passed.
- The working tree was clean after commit `2807112`. These planning documents
  are the first P13 artifacts and do not themselves alter runtime behavior.

## Behavior Already Present

- Replicated placement compacts down OSDs; erasure placement preserves shard
  slots with `CRUSH_ITEM_NONE`.
- New object I/O, mutations, PG commands, and PG enumeration wait for a newer
  map when no acting primary exists, bounded by their context in public calls.
- Session reconnect replay preserves messenger sequence state; object mutation
  retries preserve Ceph transaction identity and conservatively retain
  `ErrOutcomeUnknown`.
- OSDMap full and incremental decoding covers state, weight, client addresses,
  `max_osd`, CRUSH, `pg_temp`, primary temp, and upmap families.
- OSD session backoff messages block matching objects until released and wake
  waiters on terminal session failure.
- Existing P06/P07/P09 live evidence covers selected size-2/min-1 primary
  failover and watch continuity through one acting-primary restart.

These points are regression constraints, not proof of P13 completion.

## Resolved Findings

| ID | Severity | Prior behavior | Resolution |
| --- | --- | --- | --- |
| P13-F01 | High | Silent in-flight requests ignored remapping maps | Ordered map publication now cancels and reroutes affected object and PG-command attempts. |
| P13-F02 | High | Watch recovery stopped after the internal attempt budget | Established watches recover for their lifetime with stable cookies and observable interruptions. |
| P13-F03 | Medium | Initial watch registration bypassed homeless-route waiting | Registration now uses recoverable routing. |
| P13-F04 | Medium | Explicit command routing lacked exact exists/up checks | Explicit commands now return native-matching `ENOENT`/`ENXIO` and never remap. |
| P13-F05 | Medium | Zero could not express unlimited OSD operation waiting | Zero now means unlimited, including retries; `DefaultConfig` keeps a finite convenience default. |
| P13-F06 | Medium | OSD lifecycle and address replacement lacked qualification | Deterministic tests and live add/remove/destroy/recreate evidence cover the lifecycle. |
| P13-F07 | Medium | Flaky behavior lacked controlled live evidence | Paired Go/native appends are held on paused primaries, remapped, and checked for exact marker multiplicity. |
| P13-F08 | Evidence | Maintenance/noout lacked dedicated qualification | The live matrix covers reads, writes, PG commands, and long-lived watches under global `noout`. |
| P13-F09 | High | Monitor sessions retried one failed endpoint indefinitely | Monitor reconnect is bounded per endpoint so the client can fail over through the accepted monitor set. |
| P13-F10 | Medium | A read-only monitor command could fail with the replaced session | Allowlisted read-only commands retry after definitive session replacement; mutating commands do not. |
| P13-F11 | API | Callers could not observe MON/OSD topology and local availability changes | Bounded subscriptions now separate authoritative map facts from observed session state. |

Severity describes the P13 parity risk, not a claim of data loss or a production
incident. Re-rank findings only with a reproducer and source evidence.

## Non-Negotiable Invariants

1. Never report mutation success without a valid durable OSD reply.
2. Preserve `ErrOutcomeUnknown` after any possibly accepted mutation, including
   later map, timeout, cancellation, or connection errors.
3. Preserve logical transaction identity across safe resend; allocate a new
   identity only where the protocol requires a new operation.
4. A messenger ACK is transport acknowledgement, not mutation durability.
5. Do not reroute an explicit OSD command to another daemon.
6. Do not infer `min_size`, peering, or maintenance availability locally; use
   acting-primary maps and OSD replies.
7. Map publication must remain nonblocking, immutable, ordered by epoch, and
   bounded in memory and goroutines.
8. Context cancellation bounds caller waiting but cannot undo accepted work.
9. Watch interruption remains observable because notifications are not durable.
10. No test may rewrite expected native results or evidence status to pass.

## Evidence Rules

- Record exact Ceph source paths, symbols, commit, and relevant configuration
  defaults in `docs/p13/provenance.md` before implementing each behavior.
- Generate map fixtures from the pinned Ceph binaries or a checked-in native
  encoder. Record command, binary digest, platform, and expected decoded state.
- The native driver must use `dlopen`/`dlsym` as test infrastructure only;
  production Go code and probes remain `CGO_ENABLED=0`.
- Live tests own a P13-only FSID, subnet, daemon names, volumes, pools, CRUSH
  rules, users, and temporary directory. Cleanup may touch only those resources.
- Every fault has an explicit injection timestamp and release condition. Avoid
  unbounded packet-loss commands or host-global firewall changes.
- Reports use strict JSON decoding, reject unknown fields and trailing data,
  bind source artifacts by SHA-256, and are published atomically only after all
  assertions pass.
- A failed or interrupted run writes failed evidence where practical and exits
  nonzero. Missing Docker, architecture, native library, or required privilege
  is a blocker, not a skip.

## Execution Order

1. Complete P13-T00 before production edits.
2. Complete P13-T01 and P13-T02 before operation-specific recovery work.
3. P13-T03 and P13-T08 may proceed in parallel after T00; both must finish
   before the live harness is frozen.
4. Complete P13-T04 before P13-T05 and P13-T06 so all operation families share
   one map-change mechanism.
5. Complete P13-T07 after map lifecycle accessors are frozen by P13-T08.
6. Complete P13-T09 before P13-T10; maintenance tests depend on the stable
   harness and lifecycle oracle.
7. P13-T11 closes verification, CI, documentation, and P12 evidence rebinding.

Do not parallelize edits to the shared map publication API, objecter request
state machine, or messenger replay state.

## Standard Validation

Run the narrow owning-package test after each behavioral edit. Before closing
any production task, run:

```sh
test -z "$(gofmt -l $(find . -name '*.go' -not -path './.git/*'))"
CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go test ./internal/maps ./internal/mon ./internal/msgr ./internal/objecter -count=1
GOTOOLCHAIN=go1.27.1 go test -race ./internal/maps ./internal/mon ./internal/msgr ./internal/objecter -count=1
CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go test ./...
CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go vet ./...
CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go build ./...
git diff --check
```

After the P13 tooling exists, run its focused targets from
[README.md](README.md). Do not substitute the P12 paused-workload churn report
for P13 in-flight failure evidence.

## Stop Conditions

Stop the affected task and record a blocker when:

- pinned Ceph source and observed native behavior disagree;
- a mutation resend cannot preserve or prove request identity;
- the fault injector cannot distinguish pre-send, accepted, and replied states;
- a map fixture lacks provenance or cannot be reproduced;
- a maintenance test would alter resources outside the disposable cluster;
- the implementation requires an unbounded goroutine, queue, retained payload,
  or lock held across network I/O;
- a required live platform, image digest, toolchain, or reviewer is unavailable.

## Per-Task Completion Record

Append one entry when a task closes:

```text
Task: P13-Txx
Status: complete / blocked
Baseline and resulting commit:
Ceph source symbols and revision:
Files changed:
Commands run with exit status:
Synthetic fixtures and provenance:
Live/native evidence and report hashes:
Behavior proved:
Known limitations or unresolved risks:
Next unblocked task:
```

## Current Next Step

Regenerate P12 qualification and the final full P13 report for the exact source
tree, run the cumulative gates, and commit the completed phase.

## Implementation Record

Tasks: P13-T00 through P13-T08
Status: complete
Baseline: `6f8abc7` planning commit
Ceph source: v20.2.0 `69f84cc2651aa259a15bc192ddaabd3baba07489`
Files changed: public configuration and coordination API; map, monitor,
messenger, objecter, and OSD protocol packages; focused tests and API docs.
Behavior proved: ordered map observation, map-driven request rescan, unlimited
timeout semantics, unbounded reconnect backoff, backoff replay, stable mutation
identity, recoverable watches, explicit command state errors, and OSD lifecycle
state/address handling.
Validation: full Go suite passed; race-enabled maps/monitor/messenger/objecter
suite passed; `go vet ./...` and `go build ./...` passed.

Tasks: P13-T09 and P13-T10
Status: complete
Files changed: `integration/p13` and `tools/p13-verify`.
Behavior proved: the current quick report completed 22 scenarios, including
five MON lifecycle cases, and recorded 60 ordered subscription events;
paired mutations remained pending across paused-primary failover; intermediate
destroyed state and same-ID/new-UUID/new-address recreation were observed;
forced failure exited 97, retained artifacts, published a failed report, and
removed all P13 containers, volumes, and networks. The strict verifier rejects
unknown/trailing JSON, invalid chronology and topology, nonzero errno, native
mismatch, unbound mutation markers, and incomplete lifecycle observations.
Known limitations: multi-host maintenance and probabilistic packet loss remain
explicitly unqualified.

Task: P13-T11
Status: in progress
Files changed: `Makefile`, `.github/workflows/p00.yml`, P13 documentation and
reports, and P12 source-binding logic and evidence.
Validation so far: the strict schema and semantic verifier accepted a fresh
quick report with 22 scenarios and 60 events. Full P12/P13 source-bound
qualification, cumulative gates, final review, and commit remain pending.
The default verifier and non-live CI intentionally reject quick evidence; the
checked-in report must be replaced by a passing full report before commit.
