# Phase 3 Results

Status: complete within the F3/F9 session-lifecycle scope. Positive evidence is
deterministic gated progress, same-target coalescing, cancellation ownership and
shutdown joining. No synthetic fixture is presented as live operation latency,
p99, native runtime parity or renewed release certification.

## Gate Results

Cached OSD B completes while A is blocked in old-session Stop and while A's
factory is blocked. The regressions failed before implementation and pass after.
Manager concurrent same-target misses enter one shared factory rather than the
observed 12 entries for 16 initial callers. Both clients retain unrelated cached
progress during gated work. Waiter cancellation, failed factory fanout,
superseded/partial resource disposal, observed ABA, notification ownership and
concurrent Close joining are pinned by the [audit coverage](AUDIT.md).

The [captured focused race output](evidence/comparison/focused-lifecycle.stdout)
has 580 top-level passes across twenty repetitions, 1120 including subtests.
No race reports or failed tests. Full repository race and diagnostic tests, vet,
and Linux ARM64 diagnostic build pass on the same frozen source. Empty stdout for
vet and build is a successful zero-exit command, not missing execution evidence.

## Inherited Fixture Context

Same Apple M1 Max/macOS ARM64 host, Go 1.27.1, matching controlled environment,
100 ms duration and five repetitions per case. Timing runs were serial, before
race-validation runs. Each table entry is the median of five raw samples.

| Inherited fixture | Before ns/op | After ns/op | Before B/op | After B/op |
| --- | ---: | ---: | ---: | ---: |
| Placement, 4096 unrelated buckets | 849.8 | 846.5 | 51 | 51 |
| Queue depth 64, complete | 25385 | 25188 | 20319 | 20319 |
| Secure 64 KiB initial submission | 107888 | 98992 | 297505 | 297509 |
| Secure 4 MiB initial submission | 2483179 | 2336008 | 16812616 | 16812614 |
| Idle lifecycle, 128 secure connections | 4575260 | 4680528 | 69885409 | 69892642 |
| Incremental, 64 OSDs and zero overrides | 4185 | 4622 | 9840 | 9840 |

All 37 cells and all 185 raw samples per revision are retained. Some medians are
lower and some higher. The 64-OSD incremental median is about 10% higher, despite
that code not changing in this phase; these samples do not establish its cause.
The secure 128-connection lifecycle median is about 2% higher. This is not a
general unchanged-performance or improvement claim and no statistical
significance is asserted. The inherited benchmarks do not isolate the new
objecter/manager coordination paths. Their value here is reproducible context,
not direct evidence of reduced session-factory latency.

Submission fixtures encode locally and do not negotiate live secure mode; idle
lifecycle estimates are not RSS. No socket/authentication/OSD work, live cluster
or native runtime comparison was rerun. Previous allocation budgets, numerical
guardrails and historical qualification boundaries are unchanged. This phase
does not resolve aggregate session, receive-memory or map-clone scaling findings.

## Evidence And Provenance

- [Baseline manifest](evidence/baseline/manifest.json): passing private capture
  `/tmp/rados-go-phase3-baseline-final-20260930T172004Z`; 37 cells times five,
  exactly 185 after samples, zero failures and unchanged-source verification.
- [Source manifest](evidence/baseline/source-manifest.json): measured frozen
  dirty snapshot, not a later finalized commit or future qualification.
- [Comparison metadata](evidence/comparison/comparison.json): private capture
  `/tmp/rados-go-phase3-comparison-iVHLXE`; before archive of Phase 2 commit
  `190ce58132d9ac841be226f69574c1b8983c5b2f`, archive hash, source binding,
  environments, timestamps, zero exits and artifact hashes for all six commands.
- [Before raw measurements](evidence/comparison/before-performance.stdout):
  exactly 185 samples. After samples are retained in baseline command stdout.
- [Full race output](evidence/comparison/full-race.stdout),
  [diagnostic tests](evidence/comparison/diagnostics.stdout),
  [vet](evidence/comparison/vet.stdout) and
  [Linux build](evidence/comparison/linux-build.stdout): frozen-source validation.
- [SHA256 inventory](evidence/SHA256SUMS): 16 published artifacts plus inventory.

The compact exporter publishes allowlisted stdout, manifests and historical
provenance inputs, excluding private source archives, patches, binaries, stderr
and logs. Historical inputs are pinned references, not new live runs; original
Phase 0/1/2 and P07/P12 reports remain unchanged. Documentation finalization
changes no measured implementation and cannot retroactively certify a later
commit. See the [contract and reproduction commands](README.md) for ownership,
shutdown limits and explicit non-goals.