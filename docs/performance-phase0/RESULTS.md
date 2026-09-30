# Phase 0 Results

Status: Phase 0 technically complete under the delegated provisional criteria;
final captures, source verification, compact publication, and validation are
complete. All numbers below are descriptive Go
microbaseline medians, not native runtime comparisons or statistical significance
claims. Native performance parity and final overall qualification are deferred.

## Final Captures

Baseline: `/tmp/rados-go-phase0-baseline-final-20260930T124950Z`.
Live modes: `/tmp/rados-go-phase0-live-final-20260930T124851Z`.
The baseline contains 37 cases x 5 repetitions = 185 records, zero failures,
and verified source binding after commands. Source hashes identify the measured
exact dirty snapshot, distinct from any eventual commit. These final documentation
changes do not alter measured code.

Historical P07 originals were extracted via `git show` from
`dde29cde727dc963238acc4fa13a5a277a9f5c80`, independently of the modified working
diagnostics. Native source inspection uses Ceph v20.2.4 at exact commit
`7f793731f1b39eb4f465e960113d2363c311b964`; source inspection is not native
runtime performance evidence.

## Selected Medians

Each row reports the median of five repetitions. `B/op` is allocated bytes per
benchmark operation, not retained memory or RSS. See [README.md](README.md) for
measurement boundaries and the published manifest below for all 185 raw records.

| Case | Median ns/op | Median B/op |
| --- | ---: | ---: |
| Placement, 0 unrelated buckets | 1721 | 1352 |
| Placement, 4096 unrelated buckets | 1018379 | 1789810 |
| Queue, depth 1, complete | 308.9 | 279 |
| Queue, depth 4096, complete | 26382896 | 1437804 |
| Pool rename, 4 OSDs | 1443 | 3552 |
| Pool rename, 4096 OSDs | 163185 | 412753 |
| Pool rename, 4096 overrides | 887713 | 1719341 |
| Secure submission, 4 MiB, initial | 2341461 | 16812525 |
| Secure submission, 4 MiB, initial plus replay | 4658830 | 29419541 |
| Idle secure, 128 connections, create and stop | 4229787 | 69882710 |

The separate post-GC retained-heap estimate for 128 idle secure connections is
69713352 bytes. It includes harness noise and is not stack accounting or RSS;
stacks and RSS are excluded. Bounded reader buffers account for 67108864 bytes.
Queue timing includes the complete batch lifecycle; submission includes fixture
setup and shutdown, not production OSD latency. Five repetitions do not establish
statistical significance or independent live qualification rounds.

## Actual Modes

| Requested mode | Go monitor / OSD | Native monitor / OSD | Validator | Workload exits |
| --- | --- | --- | --- | --- |
| Secure | Secure / secure | Secure / secure | Pass, exit 0 | Both 0 |
| CRC | Secure / secure | Secure / CRC | Expected rejection, exit 1 | Both 0 |

Both services on both clients actually negotiate secure in the secure capture.
The requested CRC pair is not matched CRC; preserve its rejection and service
distinctions. All four workloads exit 0. Mode capture is instrumented negotiation
evidence, not a native runtime performance comparison or parity result.

## Published Evidence

Publication is complete: 26 allowlisted nonsecret files plus the generated
[checksum inventory](evidence/SHA256SUMS), derived from the existing final
captures. All documentation links are validated strictly without missing-target
allowances. Baseline artifact hashes, all 185 samples with zero failures, secure
acceptance, expected CRC rejection, and unchanged source status were verified.

- [Baseline manifest and all raw repetitions](evidence/baseline/manifest.json)
- [Mode command and validation statuses](evidence/modes/status.json)
- [Go secure mode sidecar](evidence/modes/go-secure-modes.json)
- [Native secure mode sidecar](evidence/modes/native-secure-modes.json)
- [Go CRC mode sidecar](evidence/modes/go-crc-modes.json)
- [Native CRC mode sidecar](evidence/modes/native-crc-modes.json)
- [Go secure workload](evidence/modes/go-secure.json)
- [Native secure workload](evidence/modes/native-secure.json)
- [Go CRC workload](evidence/modes/go-crc.json)
- [Native CRC workload](evidence/modes/native-crc.json)

Compact publication omits private archives, binaries, stderr, and logs, retaining
them privately for audit. It preserves full quantitative samples and accepted and
rejected outcomes with documented omissions and provenance hashes.

## Audit And Deferred Qualification

Three independent audit cycles completed; the last found no concrete bugs. The
full race suite, full `p12diagnostics` suite, `go vet ./...`, mode capture harness
test, Linux arm64 `p12diagnostics` benchmark cross-build, and `git diff --check`
passed. Compact publication and strict link verification are complete. The [audit](AUDIT.md) records
that ledger. The [qualification contract](QUALIFICATION.md) remains agent-selected
and provisional under user delegation, not maintainer approval. Native runtime
comparisons, statistical qualification, multi-node scale parity, maintainer
approval, final overall human signoff, and current-source P12 recertification
remain separate deferred obligations, not new audit findings; all existing P12
requirements are unchanged and mandatory.