# P13 OSD and Monitor Lifecycle Provenance

Status: source contract frozen and implemented; live evidence is validated by `tools/p13-verify`.

## Revisions

- rados-go planning baseline: `6f8abc72472cc3eb039349b6083928de7a1937b2`.
- rados-go runtime baseline: `2807112`.
- Ceph source semantics: v20.2.0 commit
  `69f84cc2651aa259a15bc192ddaabd3baba07489`.
- Live differential target: Ceph v20.2.4 commit
  `7f793731f1b39eb4f465e960113d2363c311b964` using the image digests pinned by P12.

Source references below are from the exact v20.2.0 checkout. Line numbers are
orientation aids; symbols and commit identify the normative source.

## Objecter Contract

| Behavior | Pinned source | Contract |
| --- | --- | --- |
| No primary at submission | `src/osdc/Objecter.cc:2457-2535`, `_op_submit`; `1879-1909`, `_get_session` | A negative target enters `homeless_session`, requests another map, and is sent only after a usable session appears. |
| OSDMap processing | `src/osdc/Objecter.cc:1195-1402`, `handle_osd_map`; `1061-1180`, `_scan_requests` | Every applied epoch scans homeless and OSD sessions. Operations, commands, and lingers are recalculated; only changed or forced targets are detached and queued for resend. |
| Target change | `src/osdc/Objecter.cc:2906-3156`, `_calc_target` | Acting/up-primary, interval, split/merge, pause, and force-resend changes determine whether a request moves. Slow completion alone does not select a new primary. |
| Session reset | `src/osdc/Objecter.cc:4678-4719`, `ms_handle_reset`; `2106-2162`, `_kick_requests` | If the map still marks the OSD up, reopen the session, clear backoffs, replay eligible operations and commands, and re-register lingers. |
| Slow operation | `src/osdc/Objecter.cc:2184-2278`, `tick` | Mark old requests laggy, request newer maps, send messenger pings, and ping watches. Ordinary operations are not failed or rerouted solely for age. |
| Explicit OSD command | `src/osdc/Objecter.cc:5056-5129`, `_calc_command_target` | Nonexistent target returns `-ENOENT`; existing but down target returns `-ENXIO`; an explicit target is never replaced by another OSD. |
| PG command | `src/osdc/Objecter.h:3003-3025`, `pg_command_`; `src/osdc/Objecter.cc:5116-5129` | Resolve through PG placement. Missing pool returns `-ENOENT`; no usable target returns `-ENXIO`. `-EAGAIN` requests a map and resends. |
| Watch/linger | `src/osdc/Objecter.cc:557-619`, `688-778`, `1077-1116`, `1681-1791`, `2135-2143`, `2220-2234` | Map scans and resets re-register lingers with stable cookie and increasing generation. Reconnect errors are observable but do not by themselves remove the linger. |

## Messenger Contract

`src/msg/async/ProtocolV2.cc:374-410` reconnects after transport faults. The
defaults in `src/common/options/global.yaml.in:1119-1134` start reconnect delay
at 0.2 seconds, double it, and cap it at 15 seconds. This path has no fixed
attempt-count limit. Replay retains messenger and Objecter request identity; a
messenger ACK is not an OSD durable-completion reply.

OSD sessions retain this unbounded reconnect behavior. Monitor sessions instead
bound same-peer reconnect so the monitor client can hunt another endpoint from
its seed and accepted MonMap set. Transparent command resubmission is restricted
to the existing read-only command allowlist; mutating monitor commands cannot be
replayed because their first outcome may be unknown.

## MonMap and Observation Contract

Accepted MonMaps are authoritative for monitor name, rank, addresses, priority,
weight, and CRUSH location. A local monitor connection transition is only an
observation about this client and cannot imply membership or quorum. The same
separation applies to authoritative OSDMap state versus local OSD sessions.

The live oracle uses paired Go/native monitor commands and MonMap snapshots for
offline, unreliable, changed, removed, and added scenarios. Librados exposes no
matching public C subscription callback, so ordered subscription delivery is a
Go API contract verified by unit/race tests and one long-lived live probe.

## Timeout Contract

`rados_osd_op_timeout` is a runtime `secs` option with exact default `0`, meaning
unlimited, in `src/common/options/global.yaml.in:6463-6470`. Objecter loads it
during construction and configuration changes (`src/osdc/Objecter.cc:235-246`,
`5240-5247`) and installs operation and command timers only when it is greater
than zero (`2353-2362`, `5071-5077`).

## OSDMap Lifecycle Contract

| Transition | Pinned source | Contract |
| --- | --- | --- |
| Add | `src/mon/OSDMonitor.cc:9664-9774`, `do_osd_create`; `src/osd/OSDMap.h:833-840`, `set_weight` | Allocate or reuse an ID, set weight to `CEPH_OSD_IN`, toggle `NEW`; nonzero weight establishes `EXISTS`. |
| Boot/up | `src/mon/OSDMonitor.cc:3599-3747`, `prepare_boot`; `src/osd/OSDMap.cc:2437-2448` | Publish client addresses, set `EXISTS|UP`, clear `STOP`, clear pending-out state, and apply auto-in policy. |
| Out | `src/mon/OSDMonitor.cc:12145-12288` | Set weight to zero but retain existence. CRUSH placement can move while the daemon identity remains. |
| Remove | `src/mon/OSDMonitor.cc:9648-9661`; `src/osd/OSDMap.cc:2413-2435` | Requires down state. Toggling `EXISTS` on an existing entry clears all state and addresses. |
| Destroy | `src/mon/OSDMonitor.cc:10199-10267`, `13055-13142` | Requires an existing down OSD, retains the ID, toggles `DESTROYED`, and clears UUID. |
| Recreate | `src/mon/OSDMonitor.cc:9919-10004`, `10092-10114` | Reuse only a destroyed ID, clear `DESTROYED`, ensure `NEW`, clear anomalous `UP`, and install the new UUID. |

State increment values are XOR masks except the special removal behavior above.
Address replacement is authoritative for subsequent sessions.

## Maintenance Contract

`OSDMap::is_noout` combines global, per-OSD, CRUSH-node, and device-class flags
(`src/osd/OSDMap.h:952-1004`). `OSDMonitor::can_mark_out` rejects automatic out
when an applicable `noout` flag is present (`src/mon/OSDMonitor.cc:3137-3165`,
automatic marking at `5156-5226`). `noout` does not prevent marking an OSD down,
client primary failover, or an explicit `osd out` command. Clients consume the
resulting `up`, `acting`, `pg_temp`, state, and weight; they do not implement a
separate maintenance policy.

## Closed Baseline Gaps

1. Public rados-go operations without caller deadlines use 30 seconds, and zero
   cannot select librados's unlimited OSD operation timeout.
2. A request already blocked in messenger submission is not detached solely by
   a newly published remapping OSDMap; it waits for transport/result failure or
   caller timeout instead of Objecter's per-epoch scan.
3. OSD sessions use two immediate reconnect attempts rather than Ceph's
   unbounded, exponentially backed-off reconnect behavior.
4. Explicit OSD command routing checks address presence instead of exact
   `EXISTS` and `UP` state, collapsing or misrouting `ENOENT`/`ENXIO` cases.
5. Watch registration initially bypasses homeless-route waiting, and established
   watch recovery stops after `RefreshWait * MaxAttempts` rather than retaining
   the linger until cancellation, shutdown, or terminal object/pool loss.
6. Incremental nonzero `newWeight` does not currently establish `EXISTS`, unlike
   Ceph `OSDMap::set_weight`.
7. Add/out/remove/destroy/recreate, controlled slow/flaky transport, long
   maintenance, size-3/min-2 thresholds, and size-1 outage/recovery do not yet
   have P13 native differential evidence.
8. Monitor failover inherited the OSD session's unbounded same-peer reconnect,
   preventing timely selection of another healthy monitor endpoint.
9. No public API distinguished authoritative map changes from locally observed
   monitor/OSD connection availability.

The P13 runtime and tests close these gaps. Certification still depends on a
passed full report whose source hashes match the current tree.

## Native Observation Schema

Each P13 native and Go scenario must record: scenario ID, transport, operation
class, start and finish monotonic timestamps, configured timeout, completion or
errno, mutation outcome classification, request identity where observable, OSD
map epochs, acting set and primary before/after the event, injected fault and
release point, final object bytes/version, watch cookie/generation/events, and
final cluster health. Unknown or unavailable fields must be explicit rather
than omitted.