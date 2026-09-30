# Performance Phase 0

Status: Phase 0 technically complete under the delegated provisional criteria,
including final captures, compact evidence publication, and validation.
This is a baseline and qualification contract, not a performance parity claim,
release certification, or maintainer approval. The user delegated unavailable
decisions to the agent; the choices below are explicitly agent-selected and
provisional. Human qualification signoff is deferred, not a prerequisite for
continuing scoped technical work.

## Scope

Phase 0 freezes reviewed evidence, records reproducible internal baselines,
checks actual negotiated modes, and defines the next qualification experiment.
It does not implement native code improvements or the later remediations in the
[performance review](../PERFORMANCE_SCALABILITY_REVIEW.md).
The [qualification contract](QUALIFICATION.md) defines provisional parity;
the [audit](AUDIT.md) records completed technical gates and separately deferred
qualification. The [results](RESULTS.md) summarize the final captures and their limits.
Existing [P12 performance policy](../p12/performance.md) and all other P12
qualification, correctness, security, fuzz, endurance, review, and release gates
remain unchanged and mandatory. Passing P12 is not native parity.

## Frozen Historical Evidence

The reviewed Go revision is
`dde29cde727dc963238acc4fa13a5a277a9f5c80`. Preserve its P07 report and its
original diagnostics methodology, without replacing them with current source or
regenerating old source-bound reports. Read the original diagnostics with:

```sh
git show dde29cde727dc963238acc4fa13a5a277a9f5c80:integration/p07/DIAGNOSTICS.md
git show dde29cde727dc963238acc4fa13a5a277a9f5c80:docs/p07/integration-report.json
```

The working [P07 diagnostics](../../integration/p07/DIAGNOSTICS.md) has changed;
it is not the frozen original. The final baseline extracts the original
diagnostics and report using `git show`
at the reviewed revision above. Historical revision/path/hash provenance remains
separate from the measured dirty working-source manifest.

Native source references are pinned to Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Source-symbol inspection is not a
native runtime measurement and does not establish allocation-free or lock-free
behavior. Record native runtime/library and container identities separately.

## Reproduction Entry Points

From the repository root, use a fresh absolute output directory whose parent
exists. These commands are reproduction instructions, not evidence that they
were executed for this documentation task:

```sh
GOTOOLCHAIN=go1.27.1 go run ./tools/perf-baseline \
  -out /tmp/rados-go-phase0-baseline-final-20260930 \
  -benchtime 100ms -count 5

P07_MODE_DIAGNOSTIC_DIR=/tmp/rados-go-phase0-mode-final-20260930 \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

Never reuse an existing capture directory. The live mode harness needs Docker
and a disposable cluster; see [mode capture details](../../integration/p07/MODE_EVIDENCE.md).
Do not combine mode capture with the read or resource diagnostic selectors.
Record commands, exits, timestamps, toolchain, effective runtime settings,
source manifest/digest, dirty changes, input limits, artifact hashes, and errors.
The baseline runner fixes Go 1.27.1, disables ambient workspace/environment-file
overrides, and records its effective environment; do not silently tune GC or
processor counts to select favorable results. Use ordinary production settings
for qualification, with explicitly separate causal probes and profiles.

## Baseline Measurement Boundaries

| Baseline | Included | Excluded or limited |
| --- | --- | --- |
| Placement | Immutable synthetic topology decode, validation, and raw-hash placement with unrelated-bucket growth | Live routing, network, native runtime comparison |
| Incremental | Pool rename on independently cloned synthetic map state, varying OSD and override-table sizes | Live monitor update delivery or publication delay |
| Queue | Full batch at depths 1/64/1024/4096, timed refill, completion or fail-all, and result drain | Pumps, encoding, socket I/O, authentication |
| Submission/replay | CRC/secure fixture creation, admission, pumps, outbound wire encode, peer validation, ACK, shutdown; initial or initial plus replay | Sockets, authentication, inbound wire decoding, OSD processing; payload MB/s is not wire MB/s |
| Idle connections | CRC/secure batches 1/16/128, creation/readiness/snapshot/shutdown, bounded readers and queues | RSS, stack accounting, harness-subtracted retained memory |

Queue results describe whole batches, not isolated removal cost. Submission
results describe a full fixture lifecycle, not production OSD request latency.
Idle `B/op` is lifecycle allocation; post-GC retained heap is a separate noisy
live-heap estimate, not RSS or total connection memory. Exact fixture limits and
raw benchmark repetitions belong in the generated manifest. Five microbenchmark
repetitions are not five independent live paired qualification rounds.

## Modes And API Footprint

Requested secure/CRC configuration is not actual-mode evidence. Capture monitor
and OSD connections independently for both clients, including reconnects.
Native monitor secure with OSD CRC under a CRC request and Go secure for both
services is a service-specific mismatch, not a matched CRC pair. The current
validator requires every monitor and OSD connection to match the requested mode;
it rejects that CRC pair. Secure claims also need actual evidence supported by
qualification: any explicitly scoped service subset must be declared and paired
in advance, and unsupported services or claims remain unknown. Do not reinterpret
the existing all-services validator as subset approval.

The baseline tools and negotiation collector are internal, opt-in diagnostics.
The collector is context-scoped, disabled without an observer, and exports no
public library API. Diagnostic selectors and sidecars do not change public
configuration semantics, normal benchmark JSON schemas, or P12 policy.

## Published Evidence

Completed captures are
`/tmp/rados-go-phase0-baseline-final-20260930T124950Z` and
`/tmp/rados-go-phase0-live-final-20260930T124851Z`. The [results](RESULTS.md)
provide quantitative summaries and links to the published evidence. Publication
is complete: 26 allowlisted nonsecret files plus the generated
[checksum inventory](evidence/SHA256SUMS), including all raw benchmark samples,
mode sidecars, workload JSON, statuses, and provenance hashes. All documentation
links are validated strictly, without missing-target allowances. Baseline artifact
hashes, all 185 samples with zero failures, secure acceptance, expected CRC
rejection, and unchanged source status were verified. Compact publication
omits private archives, binaries, stderr, and logs. Preserve omitted material
privately for audit; do not rerun benchmarks merely to publish evidence. Source
hashes bind each capture's exact measured dirty snapshot, distinct from any
eventual commit.
These final documentation changes do not alter the measured code.