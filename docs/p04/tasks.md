# P04 Monitor Client and Map Lifecycle

Status: implementation and automated qualification complete; fixture redistribution review remains pending.

## Delivered

- Explicit, DNS, and SRV monitor seeds with bounded resolution and modern v2 addresses.
- Configuration precedence of defaults, explicit file, opt-in environment, then programmatic options.
- CephX-backed monitor selection, priority/weight ordering, FSID pinning, subscriptions, failover, and gap recovery.
- Modern MonMap, full OSDMap, pool, CRUSH payload, and incremental OSDMap decoding with bounded collections and CRC validation.
- Immutable snapshots, complete retained client-map state, full/incremental equivalence, pool lookup, and allowlisted read-only commands.
- Upstream-generated map fixtures, decoder fuzz targets, race tests, and a cgo-disabled real-cluster gate.

## Exit Gate

`make verify-p04-all` runs formatting, cgo-disabled tests and builds, race checks, vet, module verification, cross-builds, strict fixture/report validation, fixture reproduction, the real pinned-Ceph workflow, and map fuzzing.

The checked-in arm64 report demonstrates M0, correct pool reporting, a real incremental map change, full/incremental state equivalence, recovery after loss of the initially selected monitor, a successful post-failover command, and rejection of an authenticated monitor with a foreign FSID.

The generated fixture bytes are marked `pending` for redistribution review. No human approval is inferred from automated validation.
