# P13 OSD and Monitor Lifecycle Parity

Status: **implemented; qualification is accepted only when the strict verifier validates a full live report**.

P13 closes lifecycle gaps between rados-go and pinned librados when OSDs or
monitors fail, become unreliable, recover, change, are added, or are removed.
It also adds bounded public observation of authoritative MON/OSD map changes
and local session availability. The implementation baseline is rados-go commit
`2807112`, which added map-driven no-primary recovery for ordinary object I/O,
mutations, PG commands, and enumeration. The source semantics baseline remains
Ceph v20.2.0 commit
`69f84cc2651aa259a15bc192ddaabd3baba07489`; live differential evidence must
use the pinned Ceph v20.2.4 image and commit already recorded by P12.

This phase does not treat unit tests, a synthetic router, or a successful OSD
restart as proof of complete lifecycle parity. Every client-visible claim must
be tied to an upstream source path, a deterministic fault test, and a live
native-librados comparison where the behavior depends on cluster state.

## Required Behavior

| Scenario | Required rados-go behavior | Required evidence |
| --- | --- | --- |
| OSD down before submission | Resolve the current acting primary; park PG-targeted work when none exists; resume after a newer usable map; honor caller/configured timeout | Unit map transition plus size-3/min-2 and size-1/min-1 live comparison |
| OSD fails during an in-flight request | Observe transport failure or a newer OSDMap, detach the stale request, and resubmit according to operation safety | Deterministic pre-send, post-send, lost-reply, and map-race tests plus native comparison |
| Flaky OSD or network | Reconnect and replay without changing request identity; avoid duplicate mutations; continue until the operation budget expires | Scripted messenger faults, packet-loss/connection-reset live test, exact transaction-ID assertions |
| Slow or silent OSD | Do not reroute merely because it is slow; react promptly if a newer map changes the target; return timeout/cancellation with conservative mutation outcome | Silent-server test, map-update wakeup test, and native timeout comparison |
| OSD recovers or is marked up | Re-resolve against the new epoch, reuse or replace the session as appropriate, and complete pending work without duplicate completion | Same-ID/same-address and same-ID/new-address tests plus live restart |
| OSD added | Decode `max_osd`, state, weight, address, and CRUSH changes; affect only requests whose placement changes | Full/incremental map fixtures and live add/rebalance comparison |
| OSD marked out or removed | Stop targeting absent/down OSDs, retire stale sessions, remap PG work, and preserve direct-command errno distinctions | Incremental fixtures, explicit-command tests, and live out/purge scenarios |
| OSD destroyed then recreated | Treat destroyed/down entries as unusable while preserving legal ID reuse and new address/session identity | Native map fixtures and a disposable recreate scenario |
| Maintenance / `noout` | Follow observed `up`, `acting`, and `pg_temp` state; do not infer availability from `in` weight or maintenance flags; recover when daemons return | Host/subtree `noout` or equivalent single-host scenario with reads, writes, watches, and commands |
| Monitor offline or unreliable | Bound reconnect to one endpoint, fail over through the seed/MonMap set, and preserve command availability while quorum remains | Three-monitor outages plus paired Go/native read-only monitor commands |
| Monitor changed, removed, or added | Accept ordered MonMaps, update endpoint identity and location, and never treat local connection loss as authoritative removal | Epoch snapshots for mon.c location change, removal, and re-addition |
| Public cluster changes | Separate authoritative map facts from locally observed availability; deliver owned ordered events through bounded cancellable queues | Long-lived Go subscription covering the complete live matrix and strict sequence/state verification |

## Resolved Findings

1. Monitor OSDMap generations now wake and rescan in-flight routed requests.
2. Initial watches use recoverable routing, and established watches reconnect
  for their lifetime with stable cookies and increasing generations.
3. Messenger reconnect uses unbounded exponential backoff and exposes reset
  events to watch recovery; OSD backoff unblock replays admitted operations.
4. Explicit OSD commands distinguish absent and down targets without remapping.
5. `OperationTimeout: 0` and `rados_osd_op_timeout = 0` mean unlimited waiting;
  `DefaultConfig` retains a finite convenience default.
6. OSD map state, weight, address replacement, removal, destruction, and ID
  reuse are covered by deterministic tests and the disposable live harness.
7. Monitor sessions use bounded same-peer reconnect so alternate monitors can
  be selected; only allowlisted read-only commands retry after session change.
8. Public subscriptions distinguish accepted map facts from local connection
  observations and fail explicitly on queue overflow.
9. The harness records 22 size, outage, maintenance, OSD, and MON lifecycle
  scenarios plus one long-lived stream of ordered cluster-change events.

## Upstream Anchors

Evidence tasks must pin and quote the relevant implementation at the baseline
commit, including:

- `src/osdc/Objecter.cc`: `_op_submit`, `_calc_target`, `_get_session`,
  `_scan_requests`, `_kick_requests`, `ms_handle_reset`, `handle_osd_map`,
  `_maybe_request_map`, command targeting, linger reconnect, and `tick`.
- `src/osdc/Objecter.h`: operation, command, linger, session, timeout, and
  request identity state.
- `src/msg/async/ProtocolV2.cc`: fault, reconnect, replay, ACK, and reset paths.
- `src/osd/OSDMap.cc` and `src/osd/OSDMap.h`: incremental application,
  `exists`, `is_up`, `is_in`, address replacement, and CRUSH remapping.
- `src/mon/OSDMonitor.cc`: automatic down/out decisions and `noout` behavior.
- `src/common/options/global.yaml.in`: `rados_osd_op_timeout`, reconnect, and
  Objecter timeout defaults.

The source citation is necessary but not sufficient. Where source behavior is
timing- or cluster-dependent, the native driver must record the observed errno,
completion state, elapsed interval, map epochs, acting primary, and operation
identity needed for a differential assertion.

## Implementation Shape

P13 adds a bounded OSDMap publication signal owned by the monitor client. The
objecter uses it to wake route waits and in-flight submissions when a new epoch
can change their target. Public observation is a separate bounded broker whose
events cannot block map publication or share mutable state across subscribers.

The objecter must retain one logical operation identity across reconnect/remap
where librados resends the same request. Deterministic encoded-request tests
verify the Go transaction ID and incarnation directly; the live C API cannot
expose librados's internal TID, so live evidence records unique payload markers
and independently verifies their final multiplicity. Reads and idempotent work
may be resubmitted after transport loss. Mutations require the existing
conservative `ErrOutcomeUnknown` contract whenever acceptance or completion
cannot be proved; cancellation never retracts accepted work.

Direct OSD commands and PG-targeted operations remain distinct. A PG command
may become homeless and later recover. A command to a caller-selected OSD must
report the same absent/down distinction as librados and must not silently move
to another OSD.

Maintenance is tested as a composition of monitor/OSDMap behavior. P13 does not
add a client-side maintenance mode or duplicate Ceph orchestrator policy.

## Artifacts

- `internal/mon`: nonblocking map-generation notification API and race tests.
- `changes.go`: bounded public MON/OSD authoritative and observed subscriptions.
- `internal/maps`: exact OSD existence/up/in/address lifecycle accessors and
  differential full/incremental fixtures.
- `internal/msgr`: deterministic slow, reset, reconnect, replay, and exhaustion
  tests; implementation changes only where comparison proves a mismatch.
- `internal/objecter`: map-driven request rescan, timeout semantics, command and
  watch corrections, and operation identity tests.
- `integration/p13`: disposable-cluster runner, Go probe, native librados
  oracle, report schema, and immutable report.
- `tools/p13-verify`: strict report, source-hash, topology, scenario, and native
  differential verifier.
- `docs/p13`: provenance, compatibility limits, observed results, and final
  handoff added as tasks complete.
- `Makefile` and `.github/workflows/p00.yml`: P13 unit, differential, live,
  race, cross-build, schema, and verification gates.

## Exit Gate

The following targets are implemented:

```sh
make unit-p13
make differential-p13
make integration-p13
make quality-p13
make cross-p13
make verify-p13
make verify-p13-all
```

The final gate must also run the complete repository tests, race tests for the
map/session/objecter paths, strict JSON-schema validation, the pinned native
differential suite, and P12 qualification regeneration for the exact final
tree. A quick live run may aid development but cannot satisfy certification.

## Completion Definition

P13 is complete only when all scenarios in the table have passed on both secure
and CRC transports where transport behavior is relevant, the native and Go
results agree, no mutation duplicates are observed, all waits terminate under
explicit deadlines, unlimited mode is tested without leaking workers, and the
final evidence is bound to one exact source tree. Multi-host failure domains
remain unqualified unless the P13 runner actually provisions them.

The checked-in report records 22 scenarios and ordered cluster-change events.
Its `mode` and source-artifact binding are authoritative: quick mode is
development evidence only, while certification requires a current full report.
