# P10 Snapshots, Specialized I/O, and Erasure Coding

Status: implemented and qualified.

## Delivered

- Named snapshot qualification for create, list, lookup, immutable reads, rollback, and removal, including snapshot name and creation stamp parity with native librados.
- Self-managed snapshot qualification for allocation, ordered write context, immutable reads, rollback, and removal.
- Specialized object qualification for `WriteSame`, `Checksum`, `SetAllocationHint`, `SparseRead`, `CopyFrom`, and `CopyFrom2`.
- Pool capability qualification for `IsErasureCoded`, `RequiresAlignment`, and `RequiredAlignment` across replicated and erasure-coded pools.
- Selected erasure-coded success coverage plus explicit partial-overwrite, writesame, and OMAP rejection with reported alignment requirements.
- A native `dlopen`/`dlsym` oracle that is compiled with `-ldl` only and checks native-created histories plus Go-created snapshot, copy, copy-from2, and EC-copy results.

## Certified Scope

After a successful live run, P10 certifies the listed snapshot and specialized-I/O behavior on Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`. The topology is a disposable three-OSD BlueStore cluster with two size-two replicated pools and one size-three/min-size-two `k=2,m=1` erasure-coded pool using the named P10 profile and rule.

## Caveats

- Named and self-managed snapshots use separate pools because Ceph fixes a pool's snapshot mode.
- EC qualification keeps `allow_ec_overwrites` disabled and requires the server to reject partial overwrite and writesame while aligned full-object writes and read-oriented operations succeed.
- OMAP on the EC pool is required to fail with the public `ErrUnsupported` classification.
- `CloneRange` is not covered. The current checked-in API inventory has no clone-range entry to qualify, so P10 makes no compatibility or conformance claim for that method.
- The checked-in P10 integration report is accepted only when its source hashes match the current tree and its native and Go evidence passes the strict verifier.

## Exit Gate

- `./integration/p10/reproduce.sh`
- `CGO_ENABLED=0 go test ./tools/p10-verify`
- `CGO_ENABLED=0 go run ./tools/p10-verify`
- `make verify-p10-all`

The reproducer writes `docs/p10/integration-report.json` atomically only after every scenario passes. The verifier then requires exact topology, identities, strict JSON framing, all scenario statuses, all probe/native assertions, and a complete content-addressed artifact map.
