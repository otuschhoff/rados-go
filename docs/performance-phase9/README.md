# Phase 9: Explicit Consumer PGO Assessment

Date: 2026-10-03. Decision: **reject adoption**. The reproducible held-out
assessment closes this experiment; it does not close CPU parity or authorize
production PGO, runtime changes, custom crypto, or a hidden `default.pgo`.

## Scope And Reproduction

The consumer is the existing Go benchmark executable using real public
`ReadInto`/`WriteFull` call sites and authenticated secure MON/OSD traffic.
The [driver](../../integration/p07/pgo.mjs) freezes its plan before training.
Both Go builds use Go 1.27.1, `CGO_ENABLED=0`, identical compiler diagnostic
flags, P10/GOGC100/GOMEMLIMIToff and CPUs 0-9. Only explicit `-pgo=off` versus
the pinned merged profile differs. Production code and cryptography are not
optimized or reconfigured by this phase.

Run from the repository root, only on the already approved fixture deployment,
with the five existing private `P07_PARITY_*` path variables configured:

```sh
node integration/p07/pgo.mjs /absolute/fresh/private-output p07-parity-pgo-fresh-prefix
node integration/p07/pgo.mjs analyze /absolute/private-output /absolute/private-output/fresh-assessment.json
node --test integration/p07/pgo.test.mjs integration/p07/parity.test.mjs
```

These are experimental reproduction commands, not a recommended production
PGO recipe. The driver creates exclusive private artifacts, checks source,
binary and native library continuity, actual modes, health warning categories
and every worker placement, and verifies byte-exact reads/final payloads and
cleanup/NotFound. Collectors reject preexisting fixtures before seeding and own
only fixtures observed absent. Failure aborts safely with incomplete evidence
retained; no failed or unfavorable leg is replaced for a pass.

Offline analysis verifies executing analyzer dependencies, raw capture/mode
hashes, training composition/profiles, binary/compiler-log pins, identities,
budgets, counters, sampled RSS, runtime and guards. It recomputes metrics and
refuses an existing output. Private profiles, logs and captures are not in Git.

## Predeclared Design

Training consists of one non-PGO measured CPU profile for each of six cells:
4-KiB read/c1 and 64-KiB read/c16 on readcache; 1-MiB write/c1 and mixed/c16
on test-3x; 4-MiB write/c16 and read/c1 on readcache. The profiles are merged
additively with their original CPU samples, without tuning or normalization.
This deliberately states the weighting: one leg is not one equal CPU weight.
The short historical O write profile is not used.

Each diagnostic leg warms for >=1 second AND >=1,000 operations, then measures
for >=8 seconds AND >=10,000 operations. Normal qualification remains >=10
seconds/10,000 and >=60 seconds/100,000, respectively; opt-in PGO captures
have distinct unqualified status labels. CPU profiling covers only the training
measurement and sampler stop, not setup, warmup, final verification, cleanup
or raw-record assembly. Training is excluded from held-out timing.

After profile freeze and compiler-use proof, each Go build gets one excluded
small-read correctness probe, with no tuning. Held-out cells are 4-KiB read/c1,
64-KiB read/c16 and test-3x 1-MiB mixed/c1. Each has five fresh-process,
fresh-namespace rounds with seeded off/PGO ABBA order; unchanged native
processes bracket each round. All 90 held-out legs are retained.

PGO/off estimation uses equal two-leg arithmetic means within each round,
geometric paired-round ratios, whole-round-vector bootstrap, 10,000 replicates,
nearest-rank quantiles and Bonferroni simultaneous 95% intervals across the
three cells and four metrics. Gates stay CPU upper <=1.20, p99 upper <=1.25,
successful throughput lower >=0.90 and incremental RSS upper <=1.25.
Passing noninferiority checks alone does not demonstrate useful improvement.
Native brackets are descriptive only, not rearranged into randomized native
confidence evidence.

## Final Source-Bound Results

Final root: `/root/proj/rados-go/phase9-pgo-20261003195757`.
Six training captures and both probes passed. Fifteen held-out rounds contain
90 verified legs and 6,198,269 measured operations, without capture failures.
Offline `reassessment.json` exactly equals the original `assessment.json`.

The merged training profile contains 161.29 CPU seconds across 199.87 elapsed
seconds. Compiler logs contain 177 `-pgoprofile=` uses; build information
confirms CGO0 for both distinct binaries. Non-PGO is 8,535,122 bytes; PGO is
8,753,673 bytes, a 2.56% increase. Profile and binary pins are:

| Artifact | SHA-256 |
| --- | --- |
| Non-PGO | `7d0cba79e5f66c5132f7c742f72c3c912d820fab2bf3b90980c6b8adf7ac82cf` |
| PGO | `e0d407f7800ce22d8ab8e390822576162c49eb61250bc669ea7f4304954c3956` |
| Merged profile | `05284e8471869e2b00782b8a22c88b70d03d539ddfde9893048f064a84b960c3` |

All ratios below are PGO/non-PGO; intervals are simultaneous diagnostic bounds.

| Cell | CPU/op Point [Lower, Upper] | p99 Point [Lower, Upper] | Throughput Point [Lower, Upper] | Incremental RSS Point [Lower, Upper] |
| --- | --- | --- | --- | --- |
| 4-KiB read/c1 | 1.03266 [0.93242, 1.29144] | 1.01092 [0.96475, 1.09689] | 0.98687 [0.88027, 1.03700] | 1.06194 [0.99586, 1.18722] |
| 64-KiB read/c16 | 1.00020 [0.98207, 1.01791] | 0.91612 [0.85040, 0.97760] | 1.02847 [0.93636, 1.12054] | 1.01792 [0.92031, 1.11383] |
| 1-MiB mixed/c1 | 0.98678 [0.97000, 0.99913] | 1.00037 [0.98079, 1.01748] | 1.00542 [0.99632, 1.01006] | 0.93555 [0.70729, 1.19739] |

Small-read CPU upper 1.29144 and throughput lower 0.88027 do not establish
the unchanged gates; the other two cells pass the diagnostic gate checks.
Mixed CPU improves modestly, but small/concurrent CPU intervals include one.
Descriptive off/native CPU ratios are 1.35011, 0.88967 and 1.43794;
PGO/native are 1.39420, 0.88985 and 1.41892. These are not library-only or
native parity bounds. Failed noninferiority bounds are not proof of a causal
PGO regression.

## Retained Earlier Assessments

The earlier independently trained run at
`/root/proj/rados-go/phase9-pgo-20261003174342` retains all 90 held-out legs,
6,665,597 operations and an exactly reproduced assessment under its original
tool pin. Its decision was reject adoption: concurrent-read p99 upper 1.43621
and throughput lower 0.72973, plus mixed RSS upper 1.38725, did not establish
the unchanged gates. CPU points were 0.98396, 0.99372 and 1.00040; all CPU
intervals included one. Binary size grew 2.58%. Failed bounds do not prove
a causal PGO regression. This run was not overwritten or relabeled as passing.
The intermediate run at `/root/proj/rados-go/phase9-pgo-20261003184939`
contains 90 verified legs and 6,341,056 operations, with exact raw reproduction
under its original tool pin. All diagnostic gate checks passed; its decision
was defer, not adopt. CPU points were 0.98653, 0.99793 and 0.98317, with only
mixed CPU upper below one (0.99373). Binary size grew 1.91%. This favorable
diagnostic is retained, not hidden by the final rejection.

The intermediate run followed an analyzer dependency-pin repair; the final
run followed explicit training/probe profile, build and lifecycle binding
repairs. Neither changed weighting, workloads, gates or runtime, and neither
was tuning or optional stopping for better performance. Both earlier runs also
pass the new phase-isolation checks, without relabeling their source pins.
Each root retains its own source, profile, binaries, commands and complete
inputs; do not pool unlike profiles or reanalyze them using a newer tool pin.

## Decision And Limits

Reject production adoption: not all unchanged held-out gates are established.
Mixed CPU improves by about 1.3-1.7% in two runs, but the first run did not
establish that improvement, and small/concurrent CPU benefits are not
demonstrated. These results do not justify application-specific profile
maintenance or outweigh the incomplete noninferiority evidence. Final
consumer applications own their training composition and profile refresh;
there is no universal library PGO speedup or production opt-in recommendation.

These are short diagnostic windows on a shared host, not Phase 12 qualification.
CPU/RSS include worker, sampler, observer and retained-recorder costs. Native
retries remain unknown, and native debug_ms1/1 includes ordinary message logs;
native costs are instrumented. No library-only resource claim is made.
The training profile attributes 28.62% to syscall execution, 16.29% to memmove,
17.01% to AES-GCM encryption and 13.56% to decryption. This is an excluded
training composition, not a CPU floor or a fresh native syscall attribution.
PGO does not add VAES AES-GCM or authorize a crypto backend; CGO-free and
standard-library crypto remain required. Phase 10 is next; Phase 12 still owns
fresh parity qualification, and historical report renewal remains separate.

Validation passed: Go 1.27.1 full race suite, CGO0 build/vet, minimum Go 1.26.8
collector tests, Linux arm64 benchmark compilation, focused PGO tests and all
three adjacent parity suites including strict native builds/core tests. Audit
repairs cover required training profiles, cross-phase process/namespace/build
identity, local regular profile references, profile-scope isolation, unavailable
or failed metrics, native unknown retry preservation and analyzer dependency pins.