# P00 Handoff

## Completed

- Resolved immutable Ceph v20.2.0 source and v20.2.4 qualification commits.
- Resolved multi-architecture OCI index and platform manifest digests.
- Selected module path, Go policy, finite compatibility matrix and pool profiles.
- Extracted and classified 243 C APIs and 380 C++ public operations, including
	overloads, operators and inline lifecycle methods.
- Added guarded cluster/oracle scripts, report schema and local validation.
- Built the checksum-pinned native oracle successfully for Linux amd64 and
	arm64.
- Passed the destructive smoke on a Linux arm64 VM with three raw loop-backed
	OSDs, replicated and EC pools, native CRUD and authoritative object mapping.
- Recorded licensing decision, threat model, protocol sources and review roles.

## Evidence Status

Local source/manifests and static checks are reproducible with `make verify-p00`.
The live smoke passed on an isolated Linux arm64 VM against the pinned Ceph
20.2.4 image. Final review findings strengthened C++ inventory extraction,
cleanup evidence and report commit provenance. The first corrected checkpoint
passed, but repeat review found that the documented Makefile entry point also
needed commit binding. The final report at
the Makefile-bound checkpoint passed, but final review found that nested test
evidence also needed strict unknown-field rejection. That report is superseded
and a final clean-checkpoint rerun is required. The P00 requirement to record
review owners is met by the phase-owned roles in `protocol-sources.md`; named
human sign-off remains a later release gate.

## Next Task

Task ID: `P00-T04-final-rerun`. Commit strict report-schema enforcement, rerun
the live smoke from that exact checkpoint, retain its validated report and
confirm the final review finding is closed. Do not start P01, and do not infer
protocol encodings from the smoke result.