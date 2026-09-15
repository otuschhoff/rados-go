# P09 Class Execution, Locks, and Watch/Notify

Status: implementation and automated qualification complete.

## Delivered

- Class execution support through `ObjectRef.Exec`, `ReadOp.Exec`, and `WriteOp.Exec` with preserved result bytes and ordered per-sub-operation result semantics.
- Lock interoperability for exclusive and shared locks, renewal, breaker ownership, expiration handling, and locker listing through `Lock`, `Unlock`, `ListLockers`, and `BreakLock`.
- Watch/notify interoperability with explicit acknowledgments, partial-timeout reporting, watcher listing, remap-triggered re-registration, continuity through restart and failover of the watched object's acting primary, bounded watch close, and whole-client shutdown with active coordination work through `Watch`, `Notify`, and `ListWatchers`.
- Native differential evidence from a `dlopen`/`dlsym` driver proving exact class output, the lock lifecycle in both directions, and same-cookie watch/notify interoperability across remap and restart.

## Certified Scope

P09 certifies class execution plus lock and watch/notify coordination semantics for replicated pools on Ceph 20.2.4 under the pinned three-OSD disposable topology.

## Caveats

- Watches are liveness callbacks, not a durable log or replayable queue.
- Locks are coordination primitives, not fencing tokens for external systems.

## Exit Gate

- `./integration/p09/reproduce.sh`
- `go run ./tools/p09-verify`
- `CGO_ENABLED=0 go test ./tools/p09-verify`
- `make verify-p09-all`

The checked-in integration report requires all ten scenario statuses, all fifteen Go probe booleans, and native seed/watch/notify/verify assertions to pass. The native watch remains active through remap and restart and rejects duplicate phase delivery. Deterministic failure injection proves ambiguous class execution and lost-watch observability before the live interoperability run. Quality, race, security, cross-build, and fuzz checks are executable release gates in `make verify-p09-all`; the integration report intentionally attests the disposable-cluster run rather than claiming those separate commands ran.
