# P00 Task Records

## P00-T01 Pins and Policy

**Goal:** Freeze source, qualification server, images, Go support and licensing
policy. **Evidence:** upstream Git tag resolution, Quay OCI manifests, Go
download metadata and pinned Ceph `COPYING`. **Acceptance:** full commits and
digests validate in `evidence.json`; selected images are pullable; licensing and
module decisions are documented. **Result:** complete. **Handoff:** changes to
pins require inventory regeneration and compatibility review.

## P00-T02 API and Protocol Scope

**Goal:** Classify every public C function and C++ operation. **Scope:** pinned
`librados.h` and `librados.hpp`; no implementation. **Invariant:** overloads,
operators, inline lifecycle methods and deprecated APIs remain visible.
**Discriminating test:** `make inventory && GO111MODULE=off go run
./tools/p00-verify`. **Acceptance:** exact counts match the evidence manifest and
no row is empty or review-required. **Result:** 243 C and 380 C++ rows complete.
**Handoff:** P01 freezes concrete Go signatures; owning phases replace family
descriptions with exact symbols and conformance IDs.

## P00-T03 Oracle and Runner

**Goal:** Reproduce native CRUD and authoritative object mapping on a disposable
20.2.4 cluster. **Prerequisite:** fresh Linux systemd VM with root, Docker, three
free loop devices and the explicit destruction guard. **Evidence:** digest-pinned
server image, source-hashed oracle, exact compiler/development/runtime packages.
**Invariant:** no checked-in keys; refuse non-Linux, non-root or nonempty Ceph
hosts; cleanup cluster and loop devices. **Discriminating test:**
`P00_DISPOSABLE_CLUSTER=I_UNDERSTAND_THIS_DESTROYS_DATA make p00-smoke`.
**Acceptance:** schema-valid passed report containing CRUD bytes/version and
`ceph osd map` PG/acting-primary evidence. **Result:** complete. The runner
passed on Linux arm64 against Ceph 20.2.4 and cleaned the cluster and loop
devices. Oracle builds are verified for amd64 and arm64. The final report is
pending a clean-checkpoint rerun after runner and evidence changes. **Handoff:**
retain the sanitized report and rerun when any pinned input or runner behavior
changes.

## P00-T04 Gate Review

**Goal:** Reconcile every P00 exit criterion with evidence. **Acceptance:** local
gate, source hash verification, oracle image build and independent review have
no findings; live smoke report exists. **Result:** local checks and first review
identified inventory, cleanup and provenance findings. Those findings are fixed
and local checks pass; the final live rerun and repeat independent review remain
pending. Review ownership is recorded by role in `protocol-sources.md`; named
human sign-off is required at later gates.