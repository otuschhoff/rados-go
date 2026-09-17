# go-librados

`go-librados` is a pure-Go Ceph RADOS client. The shipped package has no cgo,
native librados, dynamic-library, subprocess, gateway, or proxy dependency.
Phases P00 through P11 implement the planned v1 API families; P12 release
qualification is still in progress. This repository has no published release
or tag, and the project does not yet claim production readiness.

The current public API includes:

- explicit CephX configuration, secure-by-default messenger v2.1 sessions,
	monitor discovery/failover, FSID validation, maps, and exact CRUSH placement;
- replicated-pool object CRUD, immutable namespace/locator/snapshot views,
	metadata, OMAP, atomic single-object operations, bounded enumeration,
	flush/shutdown, and typed outcome-unknown errors;
- class execution, locks, watch/notify, named and self-managed snapshots;
- erasure-coded pool capability discovery and the qualified EC operation subset;
- cluster/pool statistics, monitor/manager/OSD/PG commands, pool and application
	administration, session addresses, blocklisting, inconsistent-object queries,
	sparse reads, checksums, writesame, allocation hints, and server-side copies.

Live phase reports through P11 use Ceph 20.2.4 at commit
`7f793731f1b39eb4f465e960113d2363c311b964`. That evidence does not certify
other Ceph releases or every cluster topology. See the
[P12 compatibility matrix](docs/p12/compatibility.md) and
[known limitations](docs/p12/README.md#known-limitations) before deployment.

## Requirements

- Go 1.26.8 or later in the supported 1.26/1.27 toolchain range.
- Linux or macOS on amd64 or arm64.
- Ceph 20.2.4 with messenger v2.1 and CephX for the currently qualified server
	matrix. A cluster FSID pin is strongly recommended.

No Ceph client package or shared library is needed to build or run an
application. Native Ceph tools under `integration/` are isolated qualification
infrastructure only.

## Examples

The examples accept explicit credentials because the current public package
does not expose Ceph config/keyring file loaders. Use a least-privilege CephX
identity and avoid placing keys in shell history.

```sh
export RADOS_MONITORS='mon1.example.net:3300 mon2.example.net:3300'
export RADOS_ENTITY='client.application'
export RADOS_FSID='00000000-0000-0000-0000-000000000000'
export RADOS_KEY='<base64 CephX key>'
go run ./examples/basic application-pool example-object
go run ./examples/atomic application-pool existing-object
```

The basic example removes the object it creates. The atomic example requires an
existing object and appends data while asserting the version observed by
`Stat`. Both use only the current public API and compile with `CGO_ENABLED=0`.

Operational diagnosis is in [docs/p12/troubleshooting.md](docs/p12/troubleshooting.md).
Applications moving from cgo wrappers should read
[docs/p12/cgo-migration.md](docs/p12/cgo-migration.md).

## Development Status

The implementation contract is [SPEC.md](SPEC.md). Phase evidence and tasks are
under `docs/p00` through `docs/p11`; P12 public-release documentation and open
gates are under [docs/p12](docs/p12/README.md). The checked-in live reports are
historical, content-addressed evidence for their reviewed trees, not proof that
the current tree has passed all release gates.

Baseline cgo-disabled checks are:

```sh
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go build ./...
go vet ./...
go mod verify
```

The P12 gate additionally requires the qualification automation, a reproducible
24-hour soak, and accountable human security and distributed-systems reviews.
Those gates are not claimed complete here.

## License and Security

This project is licensed under `LGPL-2.1-only`; see [LICENSE](LICENSE) and the
[dependency/license audit](docs/p12/dependencies.md). Report vulnerabilities
according to [SECURITY.md](SECURITY.md).