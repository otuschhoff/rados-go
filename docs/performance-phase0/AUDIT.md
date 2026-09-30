# Phase 0 Audit

Status: Phase 0 technically complete under the delegated provisional criteria,
including final baseline and live mode captures, compact evidence publication,
validation, and three independent audit cycles. The last audit cycle found no
concrete bugs. This is not native performance parity, release
certification, maintainer approval, or final overall qualification signoff.

## Decision Authority

The user delegated decisions unavailable during implementation to the agent.
The topology, workload/sample minimums, and ratio confidence gates in
[QUALIFICATION.md](QUALIFICATION.md) are agent-selected provisional choices.
They are not maintainer agreement. Human qualification signoff and overall
native parity qualification remain deferred. This delegation allowed scoped
technical work to proceed; it does not waive or modify any P12 threshold,
schema, approval requirement, or release gate.

## Gate Ledger

| Gate | Status and required evidence |
| --- | --- |
| Explicit qualification contract | Documented provisionally; human signoff deferred |
| Reviewed P07 historical freeze | Complete: original diagnostics/report extracted via `git show` from the reviewed revision |
| Reproducible microbaseline | Complete: 37 cases x 5 repetitions, 185 records, zero failures, source verified after commands |
| Native symbol provenance | Complete for source inspection: Ceph v20.2.4 at the exact pinned commit below; not native runtime benchmarking |
| Actual per-service mode evidence | Complete: all-services secure passes; requested CRC mismatch rejected as expected; workloads all exit 0 |
| Independent technical audit | Three independent cycles complete; last cycle found no concrete bugs |
| Full race suite | Passed: `go test -race ./...` |
| Full diagnostic suite | Passed: `go test -tags p12diagnostics ./...` |
| Static analysis | Passed: `go vet ./...` |
| Mode capture harness | Passed: `sh integration/p07/mode_capture_test.sh` |
| Benchmark cross-build | Passed: Linux arm64 with `p12diagnostics` |
| Diff integrity | Passed: `git diff --check` |
| Public evidence publication | Complete: 26 allowlisted files plus generated checksum inventory; baseline artifact hashes and strict documentation links verified |
| Sustained and scale parity | Not established; local preliminary and separate multi-node 64/256-OSD experiments required |
| Existing P12 obligations | Unchanged and mandatory; this audit does not assert current-source P12 completion |

The [results](RESULTS.md) report descriptive Go microbaseline medians and actual
mode observations. There are no native runtime performance comparisons,
statistical significance claims, or native performance improvement claims.
No benchmarks are rerun by this documentation update.

## Historical And Source Binding Checks

The runner extracted `integration/p07/DIAGNOSTICS.md` and
`docs/p07/integration-report.json` with `git show` from
`dde29cde727dc963238acc4fa13a5a277a9f5c80`. The working
[diagnostics document](../../integration/p07/DIAGNOSTICS.md) is not the frozen
original. Native source inspection is pinned to Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.

Final baseline capture:
`/tmp/rados-go-phase0-baseline-final-20260930T124950Z`.
Final live mode capture:
`/tmp/rados-go-phase0-live-final-20260930T124851Z`.
Keep historical provenance distinct from current-source hashes, executable
identities, build commands, and source drift checks. Capture hashes bind their
own exact measured dirty snapshot, distinct from any eventual commit; they do
not retroactively certify a clean commit or later source changes. These final
documentation edits do not alter measured code. Published evidence derives from
these existing final captures.

## Mode Audit

Actual secure mode is observed for both monitor and OSD connections on both
Go and native clients; secure validation passes. Under requested CRC, Go uses
secure for both services while native uses monitor secure and OSD CRC. The
all-services validator rejects that mismatched CRC pair as expected. All four
client/mode workloads exit 0; secure validator exit is 0 and CRC validator exit
is 1. Expected rejection is not workload failure or matched CRC qualification.

Preserve both passing and rejected observations, statuses, reconnect evidence,
and service distinctions. Do not weaken the validator or relabel the mismatch
as subset approval. Instrumented negotiation evidence does not measure
uninstrumented latency, throughput, CPU, or RSS.

## Publication Audit

Compact evidence is published under `docs/performance-phase0/evidence` from the
existing final captures: 26 allowlisted nonsecret files plus the generated
[checksum inventory](evidence/SHA256SUMS). The links in [RESULTS.md](RESULTS.md)
identify the baseline manifest, mode status, four normalized mode sidecars, and
four workload JSON files. All documentation links are validated strictly without
missing-target allowances. Baseline artifact hashes, 185 samples with zero
failures, secure acceptance, expected CRC rejection, and unchanged source status
were verified; the publication inventory was reviewed for secrets.

Retain every raw benchmark repetition and both accepted and rejected mode
outcomes. Publish nonsecret statuses, effective-limit metadata, source binding,
historical provenance, and hashes needed to reproduce decisions. Review the
export inventory for secrets and private addresses/identifiers. Compact
publication omits private archives, executable binaries, binary patches, stderr,
and logs; retain them privately for audit. Document omissions without changing
quantitative samples or implying that hashes authenticate external observations.

## Completion And Deferred Qualification

This documentation update changes only [README.md](README.md), this audit, and
[RESULTS.md](RESULTS.md), using `apply_patch`, then immediately runs strict
executable Node link validation without missing-target allowances.
The [qualification contract](QUALIFICATION.md) is unchanged.

The technical gates above and publication are complete. Maintainer approval,
human qualification signoff, native runtime parity, sustained multi-node scale
qualification, and current-source P12 recertification are separate deferred
obligations, not new audit findings. Existing P12 requirements remain unchanged
and mandatory.