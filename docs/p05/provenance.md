# P05 Placement Evidence

The qualification oracle is Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964` in the content-addressed qualification image from `docs/p00/evidence.json`.

`integration/p05/crushmap.txt` defines the certified unequal-weight straw2 topology. `integration/p05/reproduce.sh` compiles it with pinned `crushtool`, generates normal and OSD-1-out CRUSH mappings, builds 256- and 32-PG offline OSDMaps, and uses a fixed native balancer seed to generate deterministic `pg_upmap_items` and complete normal, OSD-out, and upmap object mappings with `osdmaptool`. The Go corpus test reaches the PG-count and OSD-out states by applying immutable incremental pool and OSD-weight updates, then applies the native upmap commands to the same map model. Reproduction compares every deterministic generated artifact byte to the checked-in corpus. Temp and primary-affinity ordering is source-grounded and covered by focused synthetic tests rather than claimed as native corpus evidence.

The implementation was grounded in `src/crush/CrushWrapper.cc`, `src/crush/mapper.c`, `src/crush/hash.c`, `src/crush/crush_ln_table.h`, `src/osd/OSDMap.cc`, and `src/osd/osd_types.cc`. Source hashes are enforced by `tools/p05-verify`.

The binary fixture is upstream-derived and remains marked `pending` for redistribution review. No human approval is inferred from automated validation.
