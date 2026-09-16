# P11 Administrative and Manager Operations

Status: implemented; live qualification is produced by `make integration-p11`.

## Delivered

- Direct CephX-authenticated manager sessions driven by immutable `MgrMap` snapshots, with active-manager failover and bounded retries.
- Structured monitor, manager, OSD, and PG command APIs that preserve output and status on server errors.
- Native monitor `MStatfs`, `MGetPoolStats`, and `MPoolOp` support for cluster statistics, pool statistics, and pool create/delete.
- Pool application enable/list/get/set/remove metadata operations with OSD-map visibility waits.
- Session-address reporting, strict Ceph entity-address validation, and blocklisting followed by OSD-map refresh.
- Inconsistent-PG discovery through the manager and paged inconsistent-object retrieval from the PG primary.
- Explicit inventory classification for targeted command variants, crush-rule pool creation, asynchronous duplicates, deprecated aliases, and the upstream self-blocklist test hook.

## Certified Scope

After a successful live run, P11 certifies these APIs on Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`. The runner creates a disposable one-OSD BlueStore cluster, two manager daemons, dedicated P11 pools, a full administrative identity, and a separate least-privilege identity restricted to monitor reads and data-pool object I/O.

## Safety And Failure Coverage

- The runner owns a fixed P11-only FSID, subnet, daemon names, volume, and pool names. Cleanup and destructive operations address only those resources.
- The manager recovery probe keeps one Go client connected while the active manager is removed, verifies a command through the promoted standby, removes that manager, and verifies ordinary OSD I/O through the same client.
- A newly connected client with no manager capability then performs object write/read while no manager daemon is available.
- The native driver is test-only and dynamically loads `librados.so.2`; the shipped module and Go probe remain cgo-disabled.

## Exit Gate

- `./integration/p11/reproduce.sh`
- `CGO_ENABLED=0 go test ./tools/p11-verify`
- `CGO_ENABLED=0 go run ./tools/p11-verify`
- `make verify-p11-all`

The reproducer publishes `docs/p11/integration-report.json` atomically only after all assertions pass. The verifier requires exact cluster and capability identities, strict JSON framing, complete scenario results, and source hashes matching the current tree.
