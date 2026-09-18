# P13 Task Handoff

Status: **planning baseline; implementation has not started**.

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

## Open Findings

| ID | Severity | Current behavior | Required resolution |
| --- | --- | --- | --- |
| P13-F01 | High | An in-flight request blocked on a silent old primary is not directly awakened when the monitor publishes a remapping OSDMap | Add map-generation observation and rescan/cancel/resubmit semantics matching `Objecter::_scan_requests` |
| P13-F02 | High | Watch recovery stops after the internal `RefreshWait * MaxAttempts` window | Keep linger recovery alive according to watch/client lifetime and configured operation semantics; preserve observable possible loss |
| P13-F03 | Medium | Initial watch registration bypasses homeless-route waiting | Route registration through the shared recoverable path |
| P13-F04 | Medium | Explicit OSD command routing uses address presence without an exact exists/up check | Return absent versus down errors matching librados and never remap explicit targets |
| P13-F05 | Medium | Zero cannot express librados's unlimited OSD operation timeout and the native option name is not parsed | Freeze and implement compatible timeout/config semantics with migration documentation |
| P13-F06 | Medium | OSD add/remove/destroy/recreate and address replacement lack a native differential map corpus and live qualification | Add fixtures, session-retirement tests, and disposable lifecycle scenarios |
| P13-F07 | Medium | Slow/flaky behavior is covered mostly by scripted unit faults, not controlled live latency/loss | Add bounded fault injection with native comparison and exact outcome recording |
| P13-F08 | Evidence | Maintenance/noout behavior has no dedicated qualification | Add maintenance scenarios without inventing client-side policy |

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

Start P13-T00. Freeze the upstream behavior matrix and native oracle contract
before changing timeout defaults, map notifications, or request state.
