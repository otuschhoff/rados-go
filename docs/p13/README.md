# P13 OSD Lifecycle and Failure Parity

Status: **planned; no P13 implementation or qualification is claimed**.

P13 closes the remaining behavioral gap between rados-go and the pinned
librados Objecter when OSDs fail, become slow or unreliable, recover, are
added or removed, or enter and leave maintenance. The implementation baseline
is rados-go commit `2807112`, which added map-driven no-primary recovery for
ordinary object I/O, mutations, PG commands, and enumeration. The source
semantics baseline remains Ceph v20.2.0 commit
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

## Known Gaps at Phase Start

1. Monitor OSDMap publication is atomic but has no objecter subscription that
   rescans an already blocked request. A silent old primary can therefore hold
   a request until its context expires even after a newer map selects another
   primary. librados runs `Objecter::_scan_requests` for each new map.
2. `Watch` resolves its initial route directly instead of entering the shared
   homeless-route wait. A watch created while a PG has no primary fails early.
3. Watch keepalive recovery uses `RefreshWait * MaxAttempts`, currently about
   eight seconds, rather than the watch/client lifetime behavior of librados
   linger operations. Extended maintenance can terminate an otherwise
   recoverable watch.
4. Explicit `OSDCommand` routing checks address presence but not the map's
   `exists` and `up` state, so down and removed OSDs may not produce the same
   `ENXIO`/`ENOENT` distinction as `Objecter::_calc_command_target`.
5. rados-go's zero-value `OperationTimeout` becomes 30 seconds and the parser
   accepts only positive `operation_timeout`. librados exposes
   `rados_osd_op_timeout`, where zero means no Objecter timeout. Exact behavior
   and a safe migration path must be made explicit.
6. Incremental OSD add/remove/state/address application has unit coverage but
   no P13 differential corpus covering destroyed entries, ID reuse, address
   replacement, and session retirement.
7. Existing live evidence covers selected size-2/min-1 failover, watch restart,
   and paused-workload churn. It does not certify size-3/min-2 loss thresholds,
   size-1 outage/recovery, slow or flaky OSDs, add/remove, or maintenance.

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

P13 should add a bounded OSDMap publication signal owned by the monitor client.
The objecter should use it to wake route waits and in-flight submissions when a
new epoch can change their target. Do not poll by spawning one goroutine per
operation, and do not let a slow consumer block map publication.

The objecter must retain one logical operation identity across reconnect/remap
where librados resends the same request. Reads and idempotent work may be
resubmitted after transport loss. Mutations require the existing conservative
`ErrOutcomeUnknown` contract whenever acceptance or completion cannot be
proved; cancellation never retracts accepted work.

Direct OSD commands and PG-targeted operations remain distinct. A PG command
may become homeless and later recover. A command to a caller-selected OSD must
report the same absent/down distinction as librados and must not silently move
to another OSD.

Maintenance is tested as a composition of monitor/OSDMap behavior. P13 does not
add a client-side maintenance mode or duplicate Ceph orchestrator policy.

## Planned Artifacts

- `internal/mon`: nonblocking map-generation notification API and race tests.
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

The following targets are planned deliverables and are not available merely
because this document exists:

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
