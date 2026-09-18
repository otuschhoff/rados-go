# P13 OSD Lifecycle and Failure Parity Tasks

Status: **planned**. Check a task only after its acceptance commands and
evidence requirements pass. Each task should produce one reviewable commit.

## P13-T00 Freeze Upstream Behavior and Oracle Contract

- **Goal:** Define exact librados outcomes for every P13 lifecycle scenario.
- **Prerequisites:** Clean baseline at `2807112`; pinned Ceph source and image available.
- **Scope:** `docs/p13/provenance.md`, native-oracle design, scenario matrix;
  no production Go changes.
- **Evidence:** Ceph v20.2.0 `Objecter` submit, map scan, reset, command, linger,
  and timeout paths; ProtocolV2 fault/replay; OSDMap incremental application;
  OSDMonitor down/out/noout paths. Record v20.2.4 deltas affecting these paths.
- **Contract:** For each event, record completion/error, timeout basis, request
  identity, map epoch, primary, and whether replay/remap is permitted.
- **Invariants:** Source observations and live observations remain distinct;
  no inferred errno or timing claim.
- **Discriminating test:** A native probe must distinguish no-primary waiting,
  down versus absent explicit commands, slow-without-remap, and remap wakeup.
- **Deliverables:** Provenance document, scenario IDs, native output schema, and
  an unresolved-differences table.
- **Acceptance:** Source links resolve to the pinned commit; native probe design
  covers every row in the P13 README matrix; an independent reviewer can map
  every expected value to source or an executable observation.
- **Stop conditions:** Missing source revision, unavailable native symbols, or
  conflicting behavior without a minimized reproducer.
- **Handoff:** Record all symbols, source line anchors, commands, and unresolved
  questions in `task-handoff.md`.

## P13-T01 Add Ordered OSDMap Change Observation

- **Goal:** Let objecter work react immediately to a newly published OSDMap.
- **Prerequisites:** P13-T00.
- **Scope:** `internal/mon`, its tests, and the narrow `objecter.MapSource`
  contract; no operation retry changes yet.
- **Evidence:** `Objecter::handle_osd_map` and `_scan_requests` ordering.
- **Contract:** Consumers can wait until epoch `> N`; publication never blocks;
  cancellation and monitor close wake waiters; no epoch is observed before the
  immutable map is stored.
- **Invariants:** Bounded memory, no goroutine per map or waiter, monotonic
  epochs, no lost wakeup, safe concurrent close.
- **Discriminating test:** Race a waiter registration with map publication and
  prove it returns the newer epoch under `go test -race`.
- **Deliverables:** Minimal map-generation wait/notification API and concurrency
  tests for publish-before-wait, wait-before-publish, cancellation, and close.
- **Acceptance:** `GOTOOLCHAIN=go1.27.1 go test -race ./internal/mon ./internal/objecter -run 'Map|Route' -count=20`.
- **Stop conditions:** API requires mutable map sharing, blocking monitor
  dispatch, unbounded subscribers, or polling goroutines.
- **Handoff:** Record API ownership and why lost wakeups are impossible.

## P13-T02 Align OSD Operation Timeout Configuration

- **Goal:** Represent librados timeout semantics, including zero/unlimited,
  without silently changing caller deadlines.
- **Prerequisites:** P13-T00.
- **Scope:** `Config`, parser/options, public operation context construction,
  tests, README/configuration documentation.
- **Evidence:** `rados_osd_op_timeout` schema default and Objecter timer setup.
- **Contract:** Explicit caller deadlines always win; zero has one documented
  meaning; native option naming is recognized or explicitly mapped; negative
  values fail. Any compatibility break from the old zero-means-30s behavior has
  a migration note and release decision.
- **Invariants:** Dial and handshake deadlines remain finite; shutdown always
  cancels unlimited operations; tests never wait without a release path.
- **Discriminating test:** Construct clients for omitted, zero, positive, and
  invalid timeout values and assert whether a deadline exists.
- **Deliverables:** Config implementation, parser aliases, tests, and migration
  documentation. Prefer `DefaultConfig` for policy defaults while preserving a
  way to request exact librados unlimited behavior.
- **Acceptance:** Focused config/client tests plus cancellation and shutdown
  tests for unlimited operations pass under the race detector.
- **Stop conditions:** Zero semantics remain ambiguous or an unlimited call can
  survive successful `Client.Shutdown`.
- **Handoff:** Record the chosen compatibility rule and public API impact.

## P13-T03 Match Flaky and Slow Messenger Behavior

- **Goal:** Match reconnect, replay, and timeout outcomes for reset, refusal,
  packet loss, delayed reply, and silent peer cases.
- **Prerequisites:** P13-T00; P13-T02 for timeout-sensitive assertions.
- **Scope:** `internal/msgr`, OSD session construction, deterministic transport
  tests; avoid object-operation policy in this task.
- **Evidence:** ProtocolV2 fault/reconnect paths and Objecter session reset.
- **Contract:** Replayed messages retain sequence and request identity; ACKed
  messages are not treated as committed OSD operations; reconnect exhaustion is
  observable; slow connections do not trigger unsafe reroute by themselves.
- **Invariants:** Queues and replay bytes stay bounded; no duplicate completion;
  cancellation releases retained requests; secure and CRC behavior agree.
- **Discriminating test:** Inject faults before frame write, after write before
  ACK, after ACK before reply, during reconnect, and after reconnect acceptance.
- **Deliverables:** Scripted fault tests, any proven reconnect/backoff fixes,
  diagnostics, and native timing comparison.
- **Acceptance:** `GOTOOLCHAIN=go1.27.1 go test -race ./internal/msgr ./internal/objecter -run 'Reconnect|Replay|Slow|Timeout|Outcome' -count=20`.
- **Stop conditions:** A mutation can complete twice, an ACK is mistaken for
  durability, or retry memory is unbounded.
- **Handoff:** Record each fault point and resulting completion classification.

## P13-T04 Rescan In-Flight Non-Mutating Operations on Map Change

- **Goal:** Wake and reroute reads, stats, checksums, enumeration, and PG reads
  when a newer map invalidates their active session or primary.
- **Prerequisites:** P13-T01, P13-T03.
- **Scope:** `internal/objecter` request execution and focused tests.
- **Evidence:** `Objecter::_scan_requests`, `_calc_target`, and `_kick_requests`.
- **Contract:** A map that leaves the route unchanged does not duplicate work;
  a changed acting primary cancels/detaches stale waiting and resubmits once;
  no-primary state parks until a later usable epoch or context completion.
- **Invariants:** One caller completion, monotonic retry metadata, bounded
  registrations, no session lock held while waiting for maps.
- **Discriminating test:** Hold a request on a silent fake primary, publish a
  remapping epoch without failing the transport, and require completion from the
  replacement before the original context deadline.
- **Deliverables:** Shared map-aware submission primitive and tests for unchanged
  maps, multiple rapid maps, down/all-down/up, removal, and cancellation races.
- **Acceptance:** Focused objecter tests pass repeatedly with `-race`; existing
  read redirect, backoff, and malformed-reply tests remain unchanged.
- **Stop conditions:** Rescan depends on polling, can submit concurrently to two
  primaries without controlled identity, or leaks a blocked transport request.
- **Handoff:** Document state transitions and ownership of detached attempts.

## P13-T05 Preserve Mutation Safety Across Failure and Recovery

- **Goal:** Match librados resend identity while never weakening conservative
  unknown-outcome reporting.
- **Prerequisites:** P13-T03 and P13-T04.
- **Scope:** Objecter mutation admission, retry/remap, flush, and tests.
- **Evidence:** Objecter request TID/incarnation handling, OSD duplicate-request
  semantics, and the P07 exactly-once append oracle.
- **Contract:** Pre-accept failures may retry; possibly accepted mutations keep
  one logical request identity; durable reply is required for success; timeout,
  cancellation, map failure, and malformed reply preserve `ErrOutcomeUnknown`.
- **Invariants:** No blind non-idempotent retry, no new TID after ambiguous
  delivery, retained mutation limits enforced, `Flush` waits for final outcome.
- **Discriminating test:** Lose the append reply while publishing down,
  replacement-primary, and recovery maps; verify one byte exists and the caller
  receives either proven success or `ErrOutcomeUnknown`, never false success.
- **Deliverables:** Mutation state-machine fixes, deterministic tests, and native
  append/create-exclusive comparisons.
- **Acceptance:** P07 regressions, focused race tests, and live exactly-once
  assertions pass for size-3/min-2 and size-1 recovery.
- **Stop conditions:** Request identity changes after possible acceptance or the
  harness cannot prove final object contents independently.
- **Handoff:** Record operation-by-operation retry classification.

## P13-T06 Align Watch/Linger Recovery

- **Goal:** Make watch registration and established watches survive recoverable
  no-primary, restart, remap, and maintenance intervals like librados linger ops.
- **Prerequisites:** P13-T02 and P13-T04.
- **Scope:** `internal/objecter/watch.go`, public coordination wrappers, tests.
- **Evidence:** Objecter linger registration, ping, reconnect generation, remap,
  and `ENOTCONN` behavior.
- **Contract:** Initial registration waits for a primary; established recovery
  is bounded by client/watch lifetime rather than internal attempt count;
  interruption remains observable; cookie is stable and generation increases;
  close cancels all recovery promptly.
- **Invariants:** Notifications complete at most once, queue overflow remains an
  error, no missed-event claim is made, and no worker survives close.
- **Discriminating test:** Keep a watched PG unavailable longer than the old
  eight-second budget, restore it, deliver and acknowledge one notification,
  and verify the same cookie with no duplicate event.
- **Deliverables:** Watch route/recovery changes, deterministic long-outage tests,
  shutdown tests, and live native interoperability.
- **Acceptance:** Race tests plus P09 regressions and P13 live watch scenarios
  pass on secure and CRC transports.
- **Stop conditions:** Recovery hides interruption, changes cookie, duplicates
  delivery, or prevents bounded shutdown.
- **Handoff:** Record interruption and generation sequence for every scenario.

## P13-T07 Match Explicit OSD and PG Command Semantics

- **Goal:** Preserve librados distinctions among explicit absent/down OSDs and
  recoverable PG-targeted commands.
- **Prerequisites:** P13-T04 and P13-T08.
- **Scope:** Map accessors, command routers, command retry tests, public errors.
- **Evidence:** `Objecter::_calc_command_target`, `_send_command`, map checks,
  and command `EAGAIN` handling.
- **Contract:** Nonexistent/removed explicit target reports `ENOENT`; existing
  but down target reports `ENXIO`; explicit commands never remap; PG commands
  wait/remap by acting primary; ambiguous command execution is not blindly
  retried.
- **Invariants:** Preserve server result, status, and output on errors; maintain
  command TID validation; deadline and shutdown always terminate waiting.
- **Discriminating test:** Apply identical maps with absent, down, up, destroyed,
  and recreated OSD states and compare Go/native errno and submission count.
- **Deliverables:** Exported immutable map-state query as needed, router fixes,
  command tests, and native differential results.
- **Acceptance:** Focused command tests and live direct/PG command matrix pass.
- **Stop conditions:** Error mapping is guessed or an explicit command reaches a
  replacement OSD.
- **Handoff:** Record exact errno mapping and source evidence.

## P13-T08 Qualify OSD Add, Out, Remove, Destroy, and Recreate Maps

- **Goal:** Prove full and incremental map behavior for the complete OSD
  lifecycle and session address replacement.
- **Prerequisites:** P13-T00; may run alongside P13-T03.
- **Scope:** `internal/maps`, fixture generator, map tests, session cache tests.
- **Evidence:** Pinned OSDMap encode/apply logic and native decoded state.
- **Contract:** `max_osd` resize initializes fields correctly; state XOR rules,
  weight/in status, addresses, destroyed flag, CRUSH changes, and ID reuse match
  Ceph; same-ID address change retires the stale session before new submission.
- **Invariants:** Strict bounds and CRC checks remain; nonexistent upmap targets
  fail closed; immutable published maps are never mutated in place.
- **Discriminating test:** Replay a provenance-bound incremental sequence:
  add -> up/in -> address change -> down -> out -> destroy -> purge -> recreate.
- **Deliverables:** Fixture corpus/manifests, differential tests, any decoder or
  accessor fixes, and placement/session regression tests.
- **Acceptance:** Fixture parity, map fuzz smoke, objecter session replacement,
  and `go test -race ./internal/maps ./internal/objecter` pass.
- **Stop conditions:** Fixture bytes are hand-authored without an oracle or a
  state-bit interpretation is inferred from names.
- **Handoff:** Record every epoch and expected existence/up/in/address state.

## P13-T09 Build the Disposable Lifecycle Harness

- **Goal:** Exercise all P13 scenarios against Go and native librados on the
  same pinned cluster and fault schedule.
- **Prerequisites:** P13-T00 and P13-T08.
- **Scope:** `integration/p13`, native driver, Go probe, schema, cleanup.
- **Evidence:** P12 image/toolchain pins and P13 oracle contract.
- **Contract:** Provision three BlueStore OSDs and pools for size-3/min-2 and
  size-1/min-1. Record exact commands, epochs, PG state, acting sets, operation
  start/completion, elapsed time, errno, identities, and final object contents.
- **Invariants:** P13-only resources, bounded commands, deterministic cleanup,
  no host-global network changes, atomic report publication.
- **Discriminating tests:**
  - one replica down remains available; loss below `min_size` parks work;
  - sole size-1 OSD loss parks work and restart completes it;
  - delayed/silent primary plus map remap wakes in-flight reads;
  - bounded reset/loss cycles preserve mutation identity and contents;
  - added OSD rebalances selected PGs without disturbing unrelated requests;
  - out/remove/destroy/recreate produce native-matching results;
  - maintenance/noout prevents automatic out while down/up recovery still works;
  - watch survives an outage longer than the former internal retry window.
- **Deliverables:** Reproducer, probes, native oracle, strict schema, quick mode,
  and failed placeholder report.
- **Acceptance:** Quick mode passes twice from fresh resources on the host
  architecture; forced mid-run failure proves cleanup and failed-report behavior.
- **Stop conditions:** Fault timing cannot be observed, cluster health cannot be
  restored, or a scenario relies on sleeps without bounded state polling.
- **Handoff:** Record image digest, platform, daemon versions, and report hash.

## P13-T10 Implement Strict Verification and Evidence Binding

- **Goal:** Make P13 claims machine-checkable and stale-tree resistant.
- **Prerequisites:** P13-T09 and completed production behavior tasks.
- **Scope:** `tools/p13-verify`, report schema/tests, source artifact contract.
- **Evidence:** P09-P12 strict verifier patterns.
- **Contract:** Reject unknown fields, trailing JSON, duplicate/missing scenarios,
  wrong topology, unpinned image/version, stale hashes, impossible timestamps,
  missing native pairs, mutation duplicates, and nonzero unexpected errno.
- **Invariants:** Verifier never changes evidence and never turns unavailable
  prerequisites into skips.
- **Discriminating test:** Mutate each critical report field independently and
  require rejection with a targeted diagnostic.
- **Deliverables:** Verifier, table-driven negative tests, schema validation,
  and source hashing that excludes only generated P13 reports.
- **Acceptance:** `CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go test ./tools/p13-verify -count=1` and verifier runs against both failed placeholder and passed live report modes as designed.
- **Stop conditions:** A stale or partially populated report can pass.
- **Handoff:** Record all verifier-owned files and exclusions.

## P13-T11 Wire CI, Final Qualification, and Release Evidence

- **Goal:** Make lifecycle parity a mandatory maintained gate and bind the final
  implementation to refreshed release evidence.
- **Prerequisites:** P13-T01 through P13-T10 complete.
- **Scope:** `Makefile`, `.github/workflows/p00.yml`, P13 docs/reports, P12
  qualification/release evidence affected by source hashes.
- **Evidence:** Existing P11/P12 target and workflow conventions.
- **Contract:** Add `unit-p13`, `differential-p13`, `integration-p13`,
  `quality-p13`, `cross-p13`, `verify-p13`, and `verify-p13-all`. PR CI runs
  deterministic non-live checks; pinned live qualification runs on an explicit
  capable runner and cannot silently skip. Nightly/extended jobs exercise the
  flaky, slow, maintenance, and repeated lifecycle matrix.
- **Invariants:** `CGO_ENABLED=0` for production builds/probes; native C only in
  test infrastructure; exact Go/image/tool pins; failures remain failures.
- **Discriminating test:** Break one report source hash and one scenario result
  and prove both local verification and CI commands fail.
- **Deliverables:** Build targets, workflow jobs, final README/provenance and
  compatibility update, passed P13 report, refreshed P12 qualification and
  release bindings for the exact final tree.
- **Acceptance:**

  ```sh
  make verify-p13-all
  make qualify-p12
  make verify-p12-evidence
  git diff --check
  ```

  All required checks pass on the documented platforms; repository status
  contains only intentional evidence changes before the final commit.
- **Stop conditions:** Live evidence is unavailable, P12 evidence is stale, a
  required workflow is optional/allowed-to-fail, or human review requirements
  are represented as automated approval.
- **Handoff:** Record final commit, report hashes, CI run URLs, remaining
  topology limitations, and release-review status.

## Completion Checklist

- [ ] P13-T00 upstream behavior and oracle contract
- [ ] P13-T01 ordered OSDMap change observation
- [ ] P13-T02 timeout/configuration parity
- [ ] P13-T03 flaky and slow messenger behavior
- [ ] P13-T04 in-flight non-mutating map rescan
- [ ] P13-T05 mutation safety through recovery
- [ ] P13-T06 watch/linger recovery
- [ ] P13-T07 explicit OSD and PG commands
- [ ] P13-T08 OSD lifecycle map corpus
- [ ] P13-T09 disposable lifecycle harness
- [ ] P13-T10 strict verifier and evidence binding
- [ ] P13-T11 CI, qualification, and release rebinding

## Commit Checkpoints

Use one commit per completed task unless a failed hypothesis requires a small
documented probe commit. Do not squash evidence provenance into later runtime
changes. Suggested subjects and minimum pre-commit gates are:

| Checkpoint | Suggested commit subject | Minimum gate before commit |
| --- | --- | --- |
| P13-T00 | `Document librados OSD lifecycle oracle` | Source anchors and scenario coverage reviewed |
| P13-T01 | `Publish ordered OSD map changes` | Monitor/objecter race tests, 20 repetitions |
| P13-T02 | `Align librados OSD operation timeouts` | Config, cancellation, unlimited shutdown tests |
| P13-T03 | `Match librados OSD reconnect behavior` | Messenger/objecter fault matrix under race detector |
| P13-T04 | `Rescan in-flight requests on OSD maps` | Silent-primary remap and no-lost-wakeup tests |
| P13-T05 | `Preserve mutation identity through OSD recovery` | Lost-reply and exactly-once mutation tests |
| P13-T06 | `Match librados watch recovery lifetime` | Long-outage watch and bounded-close tests |
| P13-T07 | `Match librados OSD command targeting` | Native errno matrix and PG-command recovery tests |
| P13-T08 | `Qualify OSD lifecycle map transitions` | Fixture parity, map fuzz smoke, session replacement |
| P13-T09 | `Add P13 OSD lifecycle integration harness` | Two clean quick runs and forced-failure cleanup |
| P13-T10 | `Verify P13 lifecycle evidence` | Verifier positive and mutation-based negative tests |
| P13-T11 | `Gate P13 lifecycle parity in CI` | Full P13 gate and refreshed P12 evidence validation |

Before every commit, run `git diff --check`, inspect `git status --short`, and
record the exact validation output in `task-handoff.md`. Commit only files owned
by that task. Never amend an earlier checkpoint after downstream evidence has
bound its tree; add a corrective commit and regenerate affected evidence.
