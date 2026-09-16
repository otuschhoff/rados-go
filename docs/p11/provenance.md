# P11 Administrative and Manager Evidence

The qualification server is Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`. Architecture-specific content-addressed images are inherited from the P00 evidence manifest and checked by `tools/p11-verify`.

`integration/p11/reproduce.sh` provisions FSID `21111111-2222-4333-8444-111111111111` on `172.30.111.0/24`, with one 4 GiB sparse BlueStore OSD and two manager daemons. It creates only the dedicated pools `p11-data` and `p11-native-app`; temporary pools created by the API probes use the `p11-` prefix. The cleanup trap removes only run-specific containers, network, volume, temporary files, and an incomplete report.

The native oracle resolves P11 librados entry points at runtime with `dlopen("librados.so.2")` and `dlsym`. It exercises cluster and pool statistics, monitor/manager/OSD/PG commands, pool create/delete, application metadata, session addresses, blocklisting, and inconsistent-PG listing. Native code is compiled and run only inside the pinned qualification image and is not a module dependency.

The cgo-disabled Go administrative probe exercises the public P11 API, including command error output/status preservation and paged inconsistent-object retrieval. Its recovery mode remains connected across active-manager removal and standby promotion, then proves ordinary object I/O after all manager daemons are gone. A separate `client.p11-io` identity has exactly `mon 'allow r'` and `osd 'allow rw pool=p11-data'`, with no manager capability, and proves a fresh connection and I/O while managers are unavailable.

Protocol behavior was checked against the same pinned source revision, principally `src/librados/RadosClient.cc`, `src/mon/MonClient.cc`, `src/mgr/MgrClient.cc`, `src/messages/MMonCommand.h`, `src/messages/MMgrCommand.h`, `src/messages/MCommand.h`, `src/messages/MStatfs.h`, `src/messages/MGetPoolStats.h`, `src/messages/MPoolOp.h`, `src/osd/OSDOp.cc`, and `src/msg/msg_types.cc`. Implementations are based on observed wire layouts and independently written fixtures; no upstream implementation file is copied into the distributed module.

The report artifact map includes every root and internal Go source/test file, the inventory generator/tests, P11 integration source and schema, verifier source/tests, P11 documentation, module files, the Makefile, and the generated API inventory. The report itself is excluded to avoid a circular digest. Credentials, daemon data, and runtime payloads are temporary and are never retained.
