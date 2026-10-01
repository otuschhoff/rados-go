# Request-Path Encoding And Session Accounting

Status: publication of two completed ABBA diagnostic matrices, not a passing
performance gate. Retain the request encoding allocation reductions and O(1)
in-flight accounting for their local gains only. The cumulative messenger ACK
coalescing experiment was rejected and removed. Neither matrix establishes a
stable causal p99 improvement. No qualification is renewed, no Phase 4 finding
is closed, and the existing guardrails are unchanged. A matched native
comparison was not performed for these offered-load matrices.

Production remains at four secure receive scratch slots. No library-side
GOMAXPROCS, GC, admission-window, receive-limit, deadline or global tuning change
is recommended. See the [investigation index](README.md) for earlier experiments.
This publication makes no additional production edits.

## Measurement Contract

The original prefixes are `/tmp/rados-go-p99-next-offered-` (encoding/accounting
plus rejected ACK experiment) and `/tmp/rados-go-p99-next-noack-` (final narrower
encoding/accounting set), each followed by
`{before-a,after-a,after-b,before-b}-v1`, in that serial ABBA order. Each capture
contains five repetitions at 1,000, 2,000 and 4,000 offered arrivals/s: 15 rows
and 280,000 outcomes per capture, 120 rows and 2,240,000 outcomes overall. Live
measurement is complete; this publication did not rerun it.

Every row uses secure Go ReadInto, 64 KiB, 16 workers, queue capacity 128, 128
warmup operations, an eight-second issuance window, 500 ms arrival-relative
deadline, and a 50 ms delivery-lag limit. Eight CPU/background workers each
schedule 100 allocations/s of 64 KiB: 6,400 expected allocations and
419,430,400 expected background bytes per row. Runtime settings are
GOMAXPROCS=10, GOGC=100, GOMEMLIMIT=off on Linux ARM64, Go 1.27.1. These are
diagnostic settings, not library defaults. Builds are untagged, CGO disabled,
trimpath enabled; scratch counters are deliberately disabled.

All eight capture statuses are `diagnostic-failed`, exit 1. Every failed row,
outcome, benchmark/wrapper exit, resource measurement, OSD snapshot and cgroup
counter remains published. The original manifests have not been rewritten.
All 120 legs report `cpu.max=max 100000`, cpuset `0-9`, and zero increments in
`nr_throttled` and `throttled_usec`. BigInt decimal subtraction avoids loss of
precision; zero quota throttling does not rule out host/hypervisor contention.

## Complete Matrices

Entries below are medians of ten run-level **actual all-outcome** p99 values per
side/rate, including failed and invalid rows, not pooled request p99. Each side
combines its two captures. Failures below are request overload rejections; all
timeout, other error, canceled and deadline-miss counts are zero. SLO failures
equal overload counts here. A failed row can instead have invalid delivery or
background work even when all requests succeed.

| Matrix | Offered ops/s | Before p99 ms | After p99 ms | Before/after failed rows | Before/after overload + SLO failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| ACK experiment | 1,000 | 12.183 | 10.371 | 2/1 | 0/0 |
| ACK experiment | 2,000 | 12.927 | 12.574 | 0/1 | 0/9 |
| ACK experiment | 4,000 | 13.317 | 13.418 | 1/2 | 607/1,057 |
| Final no-ACK | 1,000 | 12.977 | 13.574 | 0/2 | 0/18 |
| Final no-ACK | 2,000 | 14.983 | 16.469 | 2/3 | 84/492 |
| Final no-ACK | 4,000 | 18.786 | 17.209 | 5/3 | 1,754/693 |

Each original capture is retained independently in [the machine-readable
summary](request-path-evidence/summary.json). Delivery/background counts can
overlap each other and overload rows; do not add them to obtain failed rows.
Each capture has 280,000 expected requests and 96,000 expected background
allocations. Each status below is failed, not passed.

| Capture | Successful requests | Overload/SLO | Failed rows | Delivery-invalid rows | Background-invalid rows | Background completed/missed | Background bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| offered-before-a | 279,393 | 607 | 2 | 1 | 2 | 95,998/2 | 6,291,324,928 |
| offered-after-a | 279,341 | 659 | 2 | 1 | 1 | 96,000/0 | 6,291,456,000 |
| offered-after-b | 279,593 | 407 | 2 | 0 | 0 | 96,000/0 | 6,291,456,000 |
| offered-before-b | 280,000 | 0 | 1 | 1 | 0 | 96,000/0 | 6,291,456,000 |
| noack-before-a | 278,849 | 1,151 | 6 | 1 | 3 | 96,000/0 | 6,291,456,000 |
| noack-after-a | 279,484 | 516 | 4 | 1 | 2 | 96,000/0 | 6,291,456,000 |
| noack-after-b | 279,313 | 687 | 4 | 1 | 2 | 95,997/3 | 6,291,259,392 |
| noack-before-b | 279,313 | 687 | 1 | 1 | 1 | 96,000/0 | 6,291,456,000 |

The final no-ACK matrix has 1,838 before and 1,203 after overload failures,
seven before and eight after failed rows. Its all-row medians regress at
1,000 and 2,000 arrivals/s. At 4,000 they improve only in the all-row view;
the valid-only comparison below reverses that direction. This does not support
a stable causal p99 improvement or capacity claim. The successful operations/s
metric is successful requests divided by the fixed eight-second issuance
window. Median IOPS is generally exactly the offered rate; final-before 4,000
is 3,997.6875. This is **rate-capped delivered throughput, not maximum capacity**.
Drain-inclusive successful byte rates are separately retained in every row.

## Failure-Aware Percentiles

The historical harness's `all_outcome_p99_ns` ranks actual latency, including
fast overload rejections. The analyzer independently reconstructs and verifies
both that number and `success_p99_ns` from all original outcomes, and checks
the row p99. They need not be equal: final-before 4,000 median success p99 is
18.817 ms versus actual all-outcome 18.786 ms. They agree in the other eleven
side/rate groups.

The additional failure-aware nearest-rank p99 assigns infinity to every
non-success or deadline-missed outcome, keeping all arrivals in the denominator.
An infinite result is serialized as JSON `null` with status `infinite_failures`,
never as a finite duration. It is not the historical field and does not alter
historical files. Small failure fractions can still yield a finite p99; any
SLO failure still makes a row failed. Failure-aware medians below include all
rows, and infinite-row counts remain explicit even when the median is finite.

| Matrix | Offered ops/s | Before/after failure-aware median p99 ms | Before/after infinite rows |
| --- | ---: | ---: | ---: |
| ACK experiment | 1,000 | 12.183/10.371 | 0/0 |
| ACK experiment | 2,000 | 12.927/12.574 | 0/0 |
| ACK experiment | 4,000 | 13.317/13.418 | 1/2 |
| Final no-ACK | 1,000 | 12.977/13.574 | 0/0 |
| Final no-ACK | 2,000 | 14.983/16.469 | 0/1 |
| Final no-ACK | 4,000 | 22.475/17.209 | 2/1 |

The summary includes every per-row kind/deadline distribution, SLO count and
failure fraction. In total, 22 rows fail and 4,714 requests are overloaded;
there are no request deadline misses. Delivery-lag invalidity and missed/late
background allocations are still failures of the measurement contract.

## Valid-Only View

This is a separate descriptive subset, **not matched pairs**: a row is included
only if request SLO, delivery and background validity checks all pass. Removing
different rows from the two sides introduces selection bias. These numbers
cannot replace the complete matrices or justify a causal claim. All request
p99 definitions agree within these valid rows because all outcomes succeed
within deadline.

| Matrix | Offered ops/s | Before/after valid rows | Before p99 ms | After p99 ms |
| --- | ---: | ---: | ---: | ---: |
| ACK experiment | 1,000 | 8/9 | 11.913 | 10.301 |
| ACK experiment | 2,000 | 10/9 | 12.927 | 12.534 |
| ACK experiment | 4,000 | 9/8 | 13.281 | 13.101 |
| Final no-ACK | 1,000 | 10/8 | 12.977 | 13.127 |
| Final no-ACK | 2,000 | 8/7 | 14.965 | 14.580 |
| Final no-ACK | 4,000 | 5/7 | 15.526 | 16.314 |

## Local Gains And Decisions

Raw Darwin ARM64/Apple M1 Max microbench stdout, five repetitions per benchmark,
is preserved under `request-path-evidence/microbench`. These isolated local
costs are not Linux live-tail measurements. Median full-front single-read
encoding changes from 669.5 to 450.1 ns/op, 1,464 to 616 B/op, 23 to 14
allocations/op. The final raw repetitions span 447.0-454.8 ns/op, consistent
with the approximate 447 ns observation but not a 447 ns median.
Nested Versioned encoding changes from 230.0 to 193.4 ns/op, 368 to 312 B/op,
13 to 11 allocations/op; final repetitions span 192.4-195.8 ns/op.

The initial encoding intermediate medians are 438.4 and 204.1 ns/op, respectively.
They are labelled separately from final encoding. O(1) saturated dispatch
accounting changes depth-64/1024/4096 medians from 34.44/2,410/10,165 ns/op to
3.813/3.812/3.822 ns/op in the final no-ACK run, with zero allocations in both
versions. `session-after` is the initial intermediate, `session-final` is the
pre-removal ACK confirmation, and only `session-noack` represents the final
narrower experiment. Historical filenames are not evidence of final chronology.

Retain the encoding ownership/allocation work and constant-time accounting for
these bounded local improvements. No live p99 reduction is attributed to them.
The ACK prototype reduced queued cumulative ACK frames by replacing superseded
ACK work, but its live matrix has overload failures and no stable p99 benefit.
It was rejected and removed. The matrix changes encoding/accounting alongside
ACK behavior and has environmental/order variability, so overload failures
cannot be causally attributed to ACK coalescing. The captures do not contain a
client ACK-frame count suitable for quantifying a live frame reduction.
Do not reintroduce it based on synthetic frame reduction alone.

## Cancellation Audit

The retained contexts control distinct lifetimes, not redundant plumbing:

- Public operation context: configured timeout and caller cancellation bound
  the whole operation. `Client.operationContextWithTimeout` also registers
  `context.AfterFunc` on the client lifetime so Close interrupts active work.
- Routed-attempt context: `objecter.Client.beginRoutedAttempt` tracks the route
  independently. Remap cancels an obsolete attempt and allows routing retry;
  completion unregisters and cancels it. A caller context alone cannot express
  route obsolescence without canceling the entire operation.
- OSD-target submission context: `osdSession.submitTarget` registers a separate
  cancellation function. An OSD backoff UNBLOCK marks matching submissions for
  resend and cancels the current attempt, while preserving a live caller for
  retry. Removing this context breaks the UNBLOCK/resend path.
- OSD backoff ACK lifetime/deadline: the session ACK context ends on Stop,
  with bounded per-ACK deadlines and generation reset cleanup. These control
  ACKs are distinct from the rejected messenger cumulative ACK coalescing.

Therefore these contexts cannot simply be dropped for allocation savings.
Caller/lifetime cancellation, route-remap retry, Close and OSD UNBLOCK must
remain independent. No context or cancellation behavior was changed here.

## Identities And Availability

[Reverified comparator provenance](request-path-evidence/comparator/provenance.json)
retains the [original reconstruction audit](request-path-evidence/comparator/original-reconstruction.json.gz),
its audit program, build/race logs, restored fixtures and excluded incompatible
tests. Three reconstructed old production hashes were independently verified
against actual reconstructed bytes and all four before captures:

| Old file | SHA256 |
| --- | --- |
| internal/encoding/codec.go | `6352b11ed23db97a34bd63f33b272caf162b69cad2da7b9accd51e05f3c6068d` |
| internal/osd/messages.go | `d70cfe58bf82ebd84004c4baadaf985d773a6b75773fab161d4a2decaf9e5c1c` |
| internal/msgr/session.go | `e5941de95989b5c7d57c4e28b6b25594c39617b43913c27dd773276830a08fd5` |

All eight source-before/source-after manifests are identical within a capture.
All four before source manifests agree; the two ACK candidate manifests agree,
and the two final no-ACK manifests agree. Candidate encoding/OSD source hashes
are identical across both matrices; the final messenger session hash is
`463153b8264e7f067f1cb29e3e5ae7ec7413b28cefbc91fee2f12a5d8c3152a4`, versus ACK
experiment `b74470cc9a125b7a682d7e9e5c70793f710d243eac84dc81ba931a6f2b3cdcc6`.
The summary records every selected source hash, full source-manifest hash,
buildinfo hash and binary identity, not just these selected changes.

All 34 captured P07 measurement inputs agree across all eight captures and
match the reconstructed comparator and final tree. The reconstruction's
38-file tree audit also verifies DIAGNOSTICS.md, MODE_EVIDENCE.md,
mode_capture_test.sh and report.schema.json. These four are outside the capture
selector (Go/C/module sources, reproduce.sh and summary MJS/tests), not missing
Go tests. The four excluded incompatible optimization tests are a separate
comparator issue, documented in provenance, not the reason for 34 versus 38.

Actual offered-load benchmark binary hashes, verified before omission:

| Role | SHA256 |
| --- | --- |
| All four before binaries | `de1e4080d038e274404f7e5fa4be68f884dd7be6654a2d4919b3d7e34b269b88` |
| Both ACK candidate binaries | `442262d9738c7669de5007ce5d7cf3a3530aee0b01626840dc75d094aa156bc4` |
| Both final no-ACK binaries | `fa12a282ce4f5c4cb691668285007ac26f44bc2d5875b40ee0d84f9d6fc289b0` |

The eight original artifact manifests each contain 326 entries: all 2,608
hashes verified against original bytes. Both earlier profile-before captures
add 55 verified manifest entries each (110), preserving raw CPU/allocation
profiles and context rows, not additional offered-load results or matched
native measurements. All 2,718 original manifest identities remain unchanged
in deterministic gzip copies; per-capture `availability.json` covers every
entry. Twenty Go/native executables are deliberately omitted after checking
their actual hashes. Container images are not bundled; image references and
environment metadata are retained. This is not a binary-complete reproduction
bundle, and omitted artifacts cannot be reverified from this publication alone.

[Source availability](request-path-evidence/source-availability.json) maps
2,782 source entries across eight matrices and two profile captures to
content-addressed verified gzip blobs. Ten entries (five distinct historical
versions) are unavailable: the ACK candidate session and its two queue tests
in both ACK after captures, plus the older profile harness main/reproduce files
in both profile-before captures. Their historical hashes remain visible; no
current file is substituted. All selected final no-ACK sources are available.
Microbench stdout lacks contemporaneous source/binary hashes; chronology labels
are explicitly not cryptographic binding. This deficiency is not repaired by
inventing a historical manifest.

## Reproduction And Verification

From the workspace root, analyze original completed captures without live runs:

```sh
node --test integration/p07/request-path-summary.test.mjs
node integration/p07/request-path-summary.mjs /tmp > /tmp/request-path-summary.json
node integration/p07/publish-request-path.mjs /tmp "$PWD" \
  /tmp/rados-go-p99-next-baseline-source /tmp/request-path-publication-fresh
node integration/p07/request-path-summary.mjs \
  docs/performance-p99-scheduler/request-path-evidence --published \
  > /tmp/request-path-published-summary.json
node integration/p07/publish-request-path.mjs --verify \
  docs/performance-p99-scheduler/request-path-evidence
(cd docs/performance-p99-scheduler/request-path-evidence && shasum -a 256 -c PUBLISHED.sha256)
```

The strict Node analyzer preserves all failures, checks outcome and background
counts/bytes, workload flags, all/success percentiles, source/binary/artifact
identities, exits, cgroups, and separates all-row from valid-only comparisons.
Regression tests cover dropped failures, wrong SLO/percentile/deadline counts,
invalid workload/background/resource metrics, BigInt deltas, unsafe/duplicate
manifests and compressed artifact tampering. Compressed originals and tooling
snapshots are lossless. [PUBLISHED.sha256](request-path-evidence/PUBLISHED.sha256)
covers every published evidence file except itself, including all original
manifests and availability maps. After ACK removal, the coordinator passed the
full normal and `p12diagnostics` race suites, host vet in both modes, Linux ARM64
builds in both modes, and Linux ARM64 diagnostic vet. All 84 P07 Node tests,
shell syntax, editor diagnostics and whitespace checks pass. Independent review
found no must-fix issue. These correctness checks do not convert the deliberately
retained diagnostic failures into a passing performance qualification.

Recommendation: keep encoding/accounting for local cost reduction only; keep
ACK coalescing removed; preserve cancellation and four-slot defaults. Any future
p99 claim needs a fresh, predeclared failure-aware experiment with stable
workload delivery/background validity and independent repetitions, followed by
the unchanged qualification/native guardrails. These matrices do not provide
that claim.