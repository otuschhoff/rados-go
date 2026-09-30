# Phase 1 Results

Status: complete for the scoped Phase 1 exit, with five independent audits and
no outstanding concrete findings. See the [implementation contract](README.md)
and [audit and coverage map](AUDIT.md). Results below are descriptive fixture
measurements, not statistical-significance claims, native runtime comparisons,
unchanged-performance claims, parity, or release certification.

## Evidence And Provenance

- [Baseline manifest](evidence/baseline/manifest.json): passing source-bound
  capture at `/tmp/rados-go-phase1-baseline-final-20260930T143653Z`, 37 inherited
  benchmark cells times five repetitions, exactly 185 after samples, zero
  failures, and source verified unchanged after commands.
- [Comparison metadata](evidence/comparison/comparison.json): capture at
  `/tmp/rados-go-phase1-comparison-bOA6X2`, before source from the archive of HEAD
  `3b8be0ceaa8a4ba6042df0b26b1ce6f15a640418`, identical inherited workloads on
  the same host with five repetitions, exactly 185 before samples and 30 new
  dispatcher samples; all comparison commands exit zero.
- [Before measurements](evidence/comparison/before-performance.stdout),
  [dispatcher measurements](evidence/comparison/dispatch-selection.stdout), and
  [focused control tests](evidence/comparison/focused-controls.stdout) retain
  raw selected output, including both inline-fault regression tests.
- [SHA256 inventory](evidence/SHA256SUMS): 13 published files plus this inventory;
  all file hashes and manifest/comparison artifact bindings verify.

The manifest binds the exact frozen dirty source snapshot, not a later final
commit. Documentation finalization changes no measured code and does not
retroactively certify any future commit. The compact exporter intentionally
omits private source archives, binary patches, and stderr files; selected stdout,
source manifest, command exits, environment, limits, and hashes remain published.
Private capture directories preserve the fuller capture. Historical files
included in the export are explicitly pinned provenance inputs, not new runs;
original Phase 0 reports remain unrewritten.

## Before And After

Each entry is the median of five samples. Time is ns/op; allocation volume is
B/op. Secure fixtures encode locally, without negotiated live secure mode.

| Inherited fixture | Before ns/op | After ns/op | Before B/op | After B/op |
| --- | ---: | ---: | ---: | ---: |
| Secure 64 KiB initial submission | 101742 | 111024 | 297350 | 297498 |
| Secure 4 MiB initial submission | 2395909 | 2408595 | 16812457 | 16812615 |
| Secure 4 MiB initial plus replay | 4578263 | 4660950 | 29419433 | 29419782 |
| Queue depth 64, complete | 24383 | 25935 | 20319 | 20319 |
| Idle lifecycle, 128 secure connections | 4366923 | 4711103 | 69888477 | 69887616 |

Positive evidence is deterministic control progress despite ordinary application
count/byte saturation, preserved unrelated and matched replies, generation-scoped
ACK invalidation, and allocation-free dispatcher selection. Negative evidence
also matters: all five displayed timing medians are higher after the change.
The secure 64 KiB initial fixture median is about 9% higher. Queue allocation
volume is unchanged in the displayed samples; secure submission/replay volume
increases slightly and idle lifecycle volume decreases slightly. These samples
do not support a general claim of unchanged or improved performance.

These are not p99 measurements or startup/business-operation latency. Submission
and idle lifecycle fixtures exclude socket/authentication/OSD work; idle heap
estimates are not RSS. No statistical significance is asserted from five samples.
No live Ceph or native runtime comparison was rerun for Phase 1.

## Dispatcher Selection

Five repetitions per cell, six cells, exactly 30 samples. Every sample reports
zero B/op and zero allocs/op.

| Queue depth | No controls median ns/op | No controls range ns/op | Controls present median ns/op |
| --- | ---: | ---: | ---: |
| 64 | 2.075 | 2.043-2.256 | 64.96 |
| 1024 | 2.218 | 2.151-2.241 | 875.6 |
| 4096 | 2.174 | 2.064-2.228 | 5014 |

The pooled no-control range is 2.043-2.256 ns/op. The no-control fast path avoids
the control-selection scan; controls-present cost grows with depth. This new
benchmark measures selection only, not authentication, transport writes, ACK
latency, or full-client throughput; it has no equivalent before-source cell.

## Bounds And Qualification

Each layer reserves 16 controls and 1 MiB logical retained bytes, including
headers and active work. Wrapper and messenger can hold two logical ACK copies,
up to 2 MiB together, plus one encoded frame, codec storage, metadata, and deadline
machinery. These are not heap/RSS or client-wide bounds. One extra ACK worker runs
per OSD session. ACK deadlines begin at enqueue. Permanent writer stalls and true
control-reserve exhaustion intentionally fail-stop; normal application saturation
does not. Custom ordinary-`Send` fallbacks do not inherit the built-in reserve
guarantee and must honor cancellation.

Incomplete generic `SendControl` retains legacy replay behavior; generation-scoped
wrapper ACKs do not replay across generations. Both use the same authenticated
transport and writer. Go permits control overtaking only for unsequenced
application work, not assigned replay. The pinned Ceph v20.2.4 source reference
and SHA-256 in the [README](README.md) establish registration-before-ACK, original
map epoch, and default application ACK priority, not native ordering equivalence
or measured native performance.

Final ordinary, race, `p12diagnostics`, vet, and Linux ARM64
`benchmarkdiagnostic` build checks all passed on the final implementation source.
Focused control race count 20, earlier package count 20 checks, and fifth-review
87 editor tests plus race count 2 passed. Mandatory capture prerequisites,
deterministic checks, exact sample counts, and unchanged-source verification
passed. This does not recertify P12/P13; historical live evidence remains scoped
to its original source and qualification boundaries.

## Reproduction

Use the full validation and after-capture commands in the [README](README.md).
Fresh output directories are required. To reproduce the before-source fixture
measurements without changing the current worktree, run from the repository root:

```sh
before_dir=$(mktemp -d /tmp/rados-go-phase1-before-XXXXXX)
git archive 3b8be0ceaa8a4ba6042df0b26b1ce6f15a640418 | tar -x -C "$before_dir"
(
  cd "$before_dir" || exit 1
  GOTOOLCHAIN=go1.27.1 go test ./internal/maps ./internal/msgr -run '^$' \
    -bench '^BenchmarkPerformance' -benchmem -benchtime=100ms -count=5
)
```

Preserve raw outputs and capture hashes; match host, toolchain, environment,
limits, workload cells, and repetitions before comparing. A new capture binds
its own source, not the frozen published snapshot. Phase 2 is the next scoped
task; no Phase 2 implementation is included here.