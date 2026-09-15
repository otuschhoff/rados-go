# P06 Read Path Evidence

The qualification server is Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`, run from the content-addressed architecture-specific images recorded by the phase verifier and report schema.

`integration/p06/reproduce.sh` creates a fresh three-OSD BlueStore cluster, a size-two replicated pool, and a read-only client identity. Native `rados` writes deterministic binary, empty, namespaced, and locator-keyed fixtures. A cgo-disabled Linux build of `integration/p06/probe` reads and stats those fixtures through the public Go API, checks missing-object classification and operation versions, then repeats the binary read after the harness stops and marks down the actual acting primary.

The wire implementation was grounded in `src/messages/MOSDOp.h`, `src/messages/MOSDOpReply.h`, `src/messages/MOSDBackoff.h`, `src/osdc/Objecter.cc`, `src/osd/osd_types.cc`, and `src/common/hobject.cc` from the pinned source revision. Byte-level codec tests independently lock the v8 SPG/raw-hash split, locator widths, request identity and trace envelopes, snapshot sentinels, reply operation lengths, redirect envelopes, and backoff hobject ranges.

The report contains no credentials, tickets, connection secrets, or object payloads. Every reproduction uses generated ephemeral keys and Docker-managed ephemeral OSD volumes, all removed by the harness cleanup trap.