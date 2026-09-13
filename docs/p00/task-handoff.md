# P00 Handoff

## Completed

- Resolved immutable Ceph v20.2.0 source and v20.2.4 qualification commits.
- Resolved multi-architecture OCI index and platform manifest digests.
- Selected module path, Go policy, finite compatibility matrix and pool profiles.
- Extracted and classified 243 C APIs and 376 C++ public operations, including
	overloads, operators and inline lifecycle methods.
- Added guarded cluster/oracle scripts, report schema and local validation.
- Built the checksum-pinned native oracle successfully for Linux amd64 and
	arm64.
- Recorded licensing decision, threat model, protocol sources and review roles.

## Evidence Status

Local source/manifests and static checks are reproducible with `make verify-p00`.
The live smoke is **blocked, not passed**, in this macOS workspace because the
canonical cephadm runner requires an isolated Linux host with systemd and raw
loop devices. Run `make p00-smoke` there and commit the sanitized report under
`integration/reports/` before declaring the P00 exit gate fully passed.
The P00 requirement to record review owners is met by the phase-owned roles in
`protocol-sources.md`; named human sign-off remains a later release gate.

## Next Task

Task ID: `P00-T04-live-smoke`. Execute the runner on the disposable Linux VM,
verify native write/read/delete and `ceph osd map` PG/primary output, retain the
report, then perform a separate P00 gate review. P01 must not infer protocol
encodings from this smoke result.