# P09 Coordination Path Evidence

The qualification server is Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, anchored to architecture-specific content-addressed images and pinned `ceph-mon`, `ceph-osd`, and `librados.so.2` identities enforced by `tools/p09-verify`.

`integration/p09/reproduce.sh` provisions a fresh three-OSD BlueStore cluster with 8 GiB sparse devices, a size-two replicated `p09-data` pool, and the FSID `11111111-2222-4333-8444-999999999999`. It creates `client.p09` with monitor read plus pool-scoped OSD read/write/execute permissions and runs independent Go and native workflows.

Native interoperability uses `integration/p09/native_driver.c`, which resolves librados symbols via `dlopen`/`dlsym` at runtime. No native library is linked into shipped Go code. The native seed path records exact generic-class output and exercises exclusive acquisition and renewal, release, shared locking, and expiry. Go verifies and completes those lock histories, including breaking a native lock and joining a native shared lock; native verification lists and breaks a Go lock and releases the remaining shared holder. A single native watch consumes and acknowledges Go notifications before remap, after remap, and after restart with the same cookie and rejects duplicate phase delivery. Native notify also targets a Go watch.

Go evidence in `integration/p09/probe/main.go` compares generic class output byte-for-byte with native librados, covers lock contention and renewal, cross-client release and break behavior, shared lock coexistence, cross-client expiry reacquisition, watch acknowledgment, partial timeout reporting, remap-triggered watcher re-registration, continuity through restart of an acting primary, bounded watch close, and bounded client shutdown with an active watch and pending notify. The remap case forces acting-primary change and rejoins the former primary. The runner then stops and marks down the watched object's current acting primary, requires failover, restarts that daemon, and the probe confirms exactly-once delivery to both native and Go watchers with stable cookies after both transitions.

Before provisioning the live cluster, the reproducer runs deterministic objecter failure injection proving that ambiguous class execution surfaces `ErrOutcomeUnknown` and watch queue overflow surfaces an explicit lost-watch error.

Caveats are explicit: watches provide live callback delivery but are not durable logs; lock ownership coordinates object access but is not a fencing authority for external side effects.

Protocol and behavior grounding follows pinned Ceph sources for object operations and watch handling: `src/messages/MOSDOp.h` for operation vectors and return framing, `src/osdc/Objecter.cc` for class execution/watch submission and remap retry behavior, and `src/osd/PrimaryLogPG.cc` for lock/watch operation application.

The checked-in integration report is content-addressed over production source, tests, phase tooling, and phase documentation. It excludes CephX keys, credentials, and runtime object payloads. Separate reproducible release gates in `make verify-p09-all` run race detection, pinned static analysis and vulnerability scanning, cgo dependency auditing, four-platform cross-builds, and all required 60-second fuzz targets; these are executable gates rather than fields in the live-cluster report.
