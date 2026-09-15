# P08 Metadata and Enumeration Evidence

The qualification server is Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, anchored to architecture-specific content-addressed images and pinned `ceph-mon`, `ceph-osd`, and `librados.so.2` identities enforced by `tools/p08-verify`.

`integration/p08/reproduce.sh` provisions a fresh three-OSD BlueStore cluster with 8 GiB sparse block devices and a size-two replicated `p08-data` pool. It creates `client.p08` with monitor read and pool-scoped OSD read/write permissions. The native driver and both independent Go clients use that identity. Secrets and payloads remain in the ephemeral directory and are removed with the containers, volumes, and network.

`integration/p08/native_driver.c` loads `librados.so.2` through `dlopen` and `dlsym`; no native library is linked into shipped Go code. Native seed writes binary xattrs and binary OMAP keys and values in one compound operation. The Go probe reads and pages those values, exercises FAILOK and version assertions, verifies keyed/header OMAP reads and key/range removal, proves a mutation preceding a failed compare leaves the object unchanged, and races two independent clients using compare-extent guards so exactly one write succeeds. It then writes binary metadata for native verification.

Enumeration evidence creates enough objects to require continuation, verifies default and named namespace isolation, traverses to the exclusive end cursor, and scans eight strict split partitions to prove disjointness and exact union with the full listing. Native librados independently confirms Go-written metadata and objects in both namespaces.

The protocol grounding is the pinned Ceph tree: `src/messages/MOSDOp.h` for the v8 operation vector and `RETURNVEC`; `src/osd/osd_types.h` and `src/osd/osd_types.cc` for hobject encoding and comparison; `src/osdc/Objecter.cc` for compound submission, object-list cursor slicing, PGNLS continuation, retries, and remapping; and `src/osd/PrimaryLogPG.cc` for metadata operations and server-side transaction application.

The checked-in report is content-addressed over production source, tests, phase tooling, and phase documentation. It does not contain CephX keys, tickets, object payloads, or connection secrets.