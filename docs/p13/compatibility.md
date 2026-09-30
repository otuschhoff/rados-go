# P13 Compatibility

P13 targets the librados Objecter lifecycle behavior documented from Ceph
v20.2.0 commit `69f84cc2651aa259a15bc192ddaabd3baba07489` and exercises it against
Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`.

## Covered

- OSDMap-driven rerouting of pending reads, mutations, PG commands, and watches.
- Stable mutation identity across safe replay and conservative unknown outcomes.
- Go transaction identity is inspected in deterministic encoded-request tests;
  live Go/native comparisons use unique payload markers because librados does
  not expose its internal transaction ID through the C API.
- Unlimited or explicitly bounded operation timeouts.
- OSD backoff block/unblock, messenger reconnect, and watch re-registration.
- Existing, down, out, removed, destroyed, and recreated OSD distinctions.
- Size-3/min-2 and size-1/min-1 availability boundaries.
- Global `noout` with an acting-primary restart on a disposable single host.
- Three-monitor failover while quorum remains, including bounded instability,
  mon.c location change, removal, and re-addition.
- Ordered public observation of accepted MonMap/OSDMap changes and local
  monitor/OSD session availability.

## Explicit Limits

- The harness does not qualify multi-host or CRUSH-subtree maintenance.
- It does not inject probabilistic packet loss or alter host-global networking.
- The native helper registers a concurrent `rados_watch3` watcher through the
  same primary restart and acknowledges notifications with `rados_notify_ack`.
- Quick reports use smaller BlueStore devices and are non-certifying. The
  default verifier accepts only a full report; `-allow-quick` is explicit.
- Direct OSD commands preserve librados's absent/down distinction and are never
  silently redirected to another OSD.
- Only allowlisted read-only monitor commands retry after definitive monitor
  session replacement. Mutating monitor commands remain single-attempt.
- Librados has no equivalent public C callback for the Go cluster-change
  subscription. Native parity therefore covers map state and command continuity;
  ordering, ownership, cancellation, and overflow are qualified directly in Go.
