# Provisional Performance Qualification

Status: agent-selected provisional contract under user delegation. Maintainer
agreement and human qualification signoff have not been obtained. Technical
baseline work may proceed under this explicit scope; final overall qualification
signoff is deferred. No current native parity claim is made.

## Provisional Acceptance Definition

For each predeclared matched Go/native row, define ratios as Go divided by
native. Recommend these paired confidence-bound gates:

| Metric | Provisional gate |
| --- | --- |
| End-to-end p99 latency ratio | Upper 95% confidence bound <= 1.25 |
| Sustained successful throughput ratio | Lower 95% confidence bound >= 0.90 |
| Client CPU time per successful operation ratio | Upper 95% confidence bound <= 1.20 |
| Incremental client RSS ratio | Upper 95% confidence bound <= 1.25 |

All four gates must pass in every in-scope row, alongside correctness and
failure rules. Inconclusive intervals are not a pass. Do not average a failing
row away, select only favorable modes, or substitute point estimates for bounds.
These criteria are separate from, and never relax or replace, the unchanged
[P12 performance guardrails](../p12/performance.md) or other mandatory P12 gates.
Passing these relative gates alone is not release certification.

CPU/op uses process user plus system CPU deltas over the measured workload,
divided by successful operations, with errors/timeouts retained separately.
Keep payloads pre-generated for library-attribution runs; state explicitly when
payload generation, seeding, cleanup, connection setup, or instrumentation is
included. Whole-harness CPU is not library-only CPU.

Define incremental RSS as the measured interval's peak process RSS minus an
immediately preceding, connected and equally warmed idle-process RSS baseline,
sampled by the same OS method for both clients. Record sampling cadence, all
samples, baseline, peak, and subtraction method. Do not subtract lifetime maximum
RSS counters or substitute Go heap allocation/live heap for RSS. If a denominator
is nonpositive, below measurement resolution, or the baseline cannot be matched,
the ratio is unavailable and that row cannot pass; retain absolute bytes and
predeclare any future alternative rule before collecting replacement evidence.

## Topology And Workload Matrix

The existing single-host, three-OSD, replication-two topology is preliminary
local evidence only. General scalability qualification requires separate
multi-node 64-OSD and 256-OSD topologies. Record host count, fault domains,
replication, PGs, devices, network, Ceph build/configuration, pool settings,
capacity/occupancy, recovery state, and per-client connection counts. Preserve
each topology's results separately; local success cannot stand in for either
scale topology.

For each topology and supported matched service-mode scope, predeclare:

- Payload sizes: 4 KiB, 64 KiB, 1 MiB, and 4 MiB.
- Concurrency: 1, 16, 64, and 256.
- Sustained workloads: read, write, and 50/50 read/write, with identical object
  sets, payloads, operation semantics, replication, and durability for both clients.
- Metadata operations: xattr/omap mixes with fixed value sizes and cardinalities.
- Enumeration: fixed pool/object counts, page sizes, namespaces, and snapshot semantics.
- Watch/notify: watcher fanout, notification rates, reconnects, and completion semantics.
- Recovery/scaling: controlled OSD loss/rejoin, primary changes, map churn,
  connection growth, queue saturation, and post-recovery steady state.

Metadata, enumeration, watch, and recovery rows need their own declared rates,
sizes/cardinalities, failure budgets, completion definitions, and resource
intervals before execution. Do not apply byte throughput to a metadata row or
claim recovery parity from CRUD results. Recovery scenarios must distinguish
expected injected events from client failures and unknown mutation outcomes.
No full matrix or scalable recovery qualification is claimed to exist yet.

## Paired Execution And Sample Minimums

Each row requires at least five independent paired rounds, with randomized
client assignment to ABBA positions. A round uses one native-Go-Go-native or
Go-native-native-Go sequence chosen by the recorded random seed. Both middle
and outer legs are retained; no faster-native or faster-Go reference selection.
Record pairing and a fixed within-round aggregation rule before capture.

Each measured leg must last at least 60 seconds and contain at least 100,000
measured operations. Before each leg, warm up for at least 10 seconds and at
least 10,000 successful operations. Satisfy both time and count minima; a slow
row must continue until both are met, subject to a declared safety deadline.
Warmup is excluded from measured latency/throughput but retained in counters
and explicitly identified when resource scope includes it.

Independent rounds need distinct process lifecycles and declared cluster-state
reset/reconditioning. Record scheduling, cache/state policy, seeds, client
limits, queue depths, GC/runtime settings, and host load. Shared-host ABBA
controls drift but does not prove independence or multi-host generality. Keep
instrumented profiling and negotiation logging separate from uninstrumented
timing, while binding both to the same source, runtime, and declared mode scope.

## Failures And Statistical Analysis

Retain raw per-operation samples with round, leg, operation identity/type,
start/end or elapsed time, success/error, timeout deadline, retry count, and
censoring status. Retain timeout observations at their known lower bound;
never silently drop or convert them into successful samples. A failed operation,
unknown outcome, or right-censored latency rejects that qualification row under
this provisional zero-unexpected-failure policy. Publish censored counts and
failure reasons even when a finite success-only p99 can be computed. A capture
or safety-deadline failure is an invalid/incomplete row, not a passing one.

Use a cluster bootstrap whose resampling unit is the independent paired round,
preserving its ABBA legs and Go/native pairing. Operations within a round are
not IID independent replicates. Predeclare the estimator, within-round weighting,
bootstrap seed and replication count, quantile convention, interval method, and
95% bound calculation before measurement. Retain per-round metrics, paired
ratios, all bootstrap inputs, and analysis-tool identity. Five rounds is a
minimum, not assurance that tail confidence intervals will be narrow enough.

Publish raw tail samples and histograms with declared boundaries and overflow
counts, per-round and combined p50/p95/p99, throughput, CPU/op, incremental RSS,
confidence intervals, and threshold decisions. Histograms do not replace raw
samples. Missing inputs or undefined estimators yield unknown, never pass.
Declare whether a claim is row-specific or matrix-wide; matrix-wide confidence
needs a predeclared simultaneous/multiple-comparison method, not an unqualified
claim derived from many separate 95% intervals.

Freeze the matrix, thresholds, exclusions, and analysis version before capture.
Preserve all attempted rounds, failures, retries, and replacements with reasons.
No cherry-picking or optional stopping after a favorable interval: any increase
in sample size follows a predeclared rule or a separately labeled experiment.

## Actual Service Modes

Record monitor and OSD actual negotiated modes separately for every connection
attempt, including reconnects, using Go authenticated transport metadata and
native post-authentication ready records. Requested policy is not evidence.
Missing or unsupported observations are unknown. Reject mismatched Go/native
pairs from a matched-mode claim without deleting their raw results.

Observed native secure monitor plus CRC OSD under a CRC request, versus Go
secure monitor plus secure OSD, cannot qualify CRC parity. The
[current validator](../../internal/perfbaseline/modes.go) requires all monitor
and OSD connections to equal the requested mode and therefore rejects that
combination. It has no per-service subset qualification mechanism.

An all-services secure claim requires actual secure evidence for both services
on both clients and successful workloads. A narrower secure claim is permissible
only if qualification explicitly supports a predeclared paired service subset,
with excluded services marked unknown and not implicitly certified. This is a
provisional scoped option, not maintainer agreement or current validator support.
See the [live mode procedure](../../integration/p07/MODE_EVIDENCE.md); mode
diagnostic timings are not parity evidence.