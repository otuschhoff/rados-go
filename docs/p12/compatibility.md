# P12 Compatibility Matrix

Status: exact pre-release matrix derived from checked-in P00-P11 evidence. It is
not a final P12 certification.

## Qualified Server Matrix

| Dimension | Qualified value | Evidence boundary |
| --- | --- | --- |
| Source semantics | Ceph v20.2.0, commit `69f84cc2651aa259a15bc192ddaabd3baba07489` | Protocol/API baseline; not a live server certification |
| Live server | Ceph v20.2.4 Tentacle, commit `7f793731f1b39eb4f465e960113d2363c311b964` | Checked-in phase reports through P11 |
| Server image | `quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa` | `linux/arm64` reports; not evidence for another image/platform |
| Authentication | CephX | Production no-auth is unavailable |
| Messenger | v2.1 secure by default; CRC only by explicit `SecurityModeCRC` | No automatic downgrade; v1/v2.0/compression unsupported |
| Addressing | IPv4 and IPv6 v2 address vectors | v1-only endpoints unsupported |
| Server OS/arch | Linux/arm64 disposable containers | Other daemon platforms unqualified |
| Object store | BlueStore | Other object stores unqualified |

## P10 Pool Matrix

P10 ran on 2026-09-16 from `07:28:22Z` to `07:29:15Z` on three
8 GiB sparse OSD devices.

| Pool | Exact profile | Qualified behavior |
| --- | --- | --- |
| `p10-named` | replicated, size 2, min_size 1, 16 PGs, `p10-replicated-rule` | Named snapshots and specialized I/O |
| `p10-self` | replicated, size 2, min_size 1, 16 PGs, `p10-replicated-rule` | Self-managed snapshots |
| `p10-ec` | size 3, min_size 2, 16 PGs, jerasure `k=2,m=1`, OSD failure domain, stripe unit 4096, stripe width 8192, overwrites disabled | Qualified EC success subset and explicit operation restrictions |

P10 report scenarios `named_snapshots`, `self_managed_snapshots`,
`specialized_io`, `erasure_coded_io`, and `native_interoperability` are marked
passed for the report's content-addressed tree.

## P11 Administration Matrix

P11 ran on 2026-09-16 from `07:29:16Z` to `07:30:42Z` with one 4 GiB
BlueStore OSD, two managers, and replicated pool `p11-data` (size 1,
min_size 1, 8 PGs). It used separate full-admin and least-privilege I/O
identities. Its six checked-in scenarios are marked passed, including manager
failover/loss, native conformance, least privilege, and destructive-resource
validation. This topology does not certify multi-host or production failure
domains.

## Client Build Matrix

| Dimension | Supported target | Qualification distinction |
| --- | --- | --- |
| Go | minimum module directive 1.26.8; policy range Go 1.26.x and 1.27.x | This documentation change was checked with Go 1.27.1 |
| OS | Linux, macOS | Live phase probes ran in Linux containers; macOS is a client build/unit-test target |
| Architecture | amd64, arm64 | Checked-in live reports are `linux/arm64`; other combinations are cross-build targets |
| cgo | `CGO_ENABLED=0` | Required for shipped package and examples |

Ceph releases, images, pool profiles, CRUSH features, client platforms, and Go
versions not listed above must be separately qualified before support is claimed.