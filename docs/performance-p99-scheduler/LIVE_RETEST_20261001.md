# External Linux Live Retest: 2026-10-01

Status: both readcache and the authorized test-3x rerun passed their primary
and separate observed matrices. A later matched Ceph v20.2.4 closed-loop
comparison completed all workload legs, with manager-statistics metadata
incomplete. Diagnostic evidence only: no general native parity,
maximum-capacity claim, Phase 4 gate closure
or renewed P12/P13 qualification.

## Current Work

Phase 1 backoff control progress, Phase 2 immutable placement reuse and Phase 3
session lifecycle work are complete within their documented scopes. Phase 4
receive/fanout bounds are implemented but its secure-read latency gate remains
open. Four secure scratch slots, caller-buffer reads, reduced encoding
allocations and O(1) in-flight accounting are retained. Eight scratch slots
remain diagnostic-only; admission windows, command batching and cumulative
messenger ACK coalescing were rejected. Phase 5 queue/backoff scaling and
sustained large-scale qualification remain incomplete. See the
[review](../PERFORMANCE_SCALABILITY_REVIEW.md) and
[factorial evidence](FACTORIAL_STALL_RESULTS.md).

## Setup And Provenance

Private capture root: `/root/proj/rados-go/live-performance-20261001-NU3yM4`.
The `prepared-v2` capture binds selected sources before/after compilation,
the Linux amd64 binary hash and build information. Every measurement verifies
that binding and checks unchanged sources/binaries at finalization.

Host: Linux amd64, 56 available CPUs, Go 1.27.1, Node 19.7.0. Runtime is fixed
at `GOMAXPROCS=10`, `GOGC=100`, `GOMEMLIMIT=off`. Visible cgroup v2 limits are
`cpu.max = max 100000`, effective cpuset `0-55`; primary before/after visible
`nr_throttled` and `throttled_usec` are zero. Process cgroup and mount mapping
are retained privately. Hidden ancestor limits and hypervisor contention are
not established by these visible counters.

The supplied external keyring selects `client.amakura`; no key content or key
digest is published. Supplied monitor hosts use explicit msgr2 port 3300, and
the supplied FSID binds the cluster. Operator-approved seeding writes sixteen
64-KiB deterministic objects `p07-shared-read-0` through `p07-shared-read-15`;
no objects or pools are deleted.

Two tooling repairs were required: configurable entity and keyring input while
preserving the `client.p07` default, and host validation accepting GNU uname's
kernel-before-architecture ordering. Native closed-loop still requires the
default entity. No library runtime policy or production implementation changed.

## Readcache Primary Results

Secure 64-KiB `ReadInto`, sixteen workers, 128 warmup reads, eight-second offered
issuance per leg, queue capacity 128, four production scratch slots. Each rate
runs three serial repetitions of `none,cpu,alloc,both` in fixed order.
CPU cases have eight hashing workers; allocation cases have eight separately
paced workers at 100 allocations/s each. `both` has sixteen background workers.
Rates/cases are not rotated, limiting drift-sensitive comparisons.

Values are medians of three run-level arrival-to-outcome p99 values, in ms,
not pooled percentiles or read-service-only latency.

| Offered Reads/s | None | CPU | Allocation | Both |
| ---: | ---: | ---: | ---: | ---: |
| 1000 | 2.172181 | 2.384578 | 2.078391 | 3.135395 |
| 2000 | 1.757619 | 1.867306 | 1.728114 | 1.992758 |
| 4000 | 1.313595 | 1.341814 | 1.326123 | 1.374939 |

All 36 primary legs passed: 672,000 expected, attempted and successful reads.
Payloads are validated by the driver. All source/binary stability checks passed.
Offered rates cap delivered load; 4,000/s is not maximum client capacity.
Higher-rate lower p99 does not establish a causal optimization or cross-host
improvement over historical Docker ARM64 measurements.

Median run-level phase p99 ranges across case/rate cells are 0.444628..1.050985
ms for delivery, 0.020192..0.118871 ms for queue, 0.009663..0.018328 ms for
dispatch and 0.981626..1.881666 ms for read service. These phase percentiles
describe different tails and must not be summed to reconstruct total p99.

## Readcache Observation

The `observed-readcache-2000` capture runs three repetitions of all four cases.
All twelve instrumented legs, trace decodes and stall analyses passed. All
192,000 reads succeeded; timing and analyzed call counts match attempted reads
in every leg. Trace/JSON alignment uncertainty spans 1,716..4,155 ns.
Observed run-level p99 medians are 1.518955 ms (`none`), 1.848329 ms (`cpu`),
1.514842 ms (`alloc`) and 2.243452 ms (`both`). These samples are excluded from
the primary table. Read intervals, missing transport stages and GC/caller
runnable overlap are retained; overlap is temporal correlation, not causation
or transport-goroutine attribution.

Among each leg's slowest 160 read-service intervals, 14..73 overlap a GC STW
range. Every leg's median GC overlap fraction within that population is zero.
This does not support GC STW as the sole explanation for the live read tail;
GC assist, transport scheduling, network and server waits are not ruled out.

## Authorized Test-3x Rerun

After the operator reported authorization was corrected, a fresh capture at
`/root/proj/rados-go/live-test-3x-20261001-RvDnz5` reused the exact `prepared-v2`
source-bound binary and supplied credential. Approved fixture seeding and the
smoke read succeeded. The same secure 64-KiB workload, runtime, worker count,
fixed case/rate ordering and three repetitions were used; no benchmark or
production code changed for this rerun.

Median run-level arrival-to-outcome p99, in ms:

| Offered Reads/s | None | CPU | Allocation | Both |
| ---: | ---: | ---: | ---: | ---: |
| 1000 | 1.961136 | 1.974401 | 1.995713 | 1.994913 |
| 2000 | 1.418425 | 1.752121 | 1.456353 | 1.808799 |
| 4000 | 1.266951 | 1.315544 | 1.297008 | 1.444573 |

All 36 primary legs passed: 672,000 expected, attempted and successful reads.
The verifier recomputed p99 from every outcome and checked source/binary
stability and report validity. Binary and preparation source-manifest hashes
match the earlier readcache run. Visible cgroup throttling counters remain zero
before and after primary and observed runs.

All twelve separate instrumented legs at 2,000 reads/s, trace decodes and stall
analyses also passed: 192,000 successful reads, with timing and analyzed call
counts matching attempted reads in every leg. Observed run-level p99 medians
are 1.442753 ms (`none`), 1.854919 ms (`cpu`), 1.470647 ms (`alloc`) and
2.101872 ms (`both`), excluded from the primary table. Alignment uncertainty
spans 1,441..3,341 ns. GC STW overlaps 18..92 of each leg's slowest 160
read-service calls; this is temporal correlation, not causal attribution.

These later serial captures are not simultaneous or randomized pool pairs;
differences from readcache do not establish a causal pool-performance ranking.
Across both pools, 72 primary legs returned 1,344,000 successful reads and
24 separate observed legs returned 384,000 successful reads, excluding smoke
and warmup reads. No native parity or qualification gate closure is asserted.

## Ceph V20 Closed-Loop Comparison

Private capture root: `/root/proj/rados-go/live-librados-20261001-o1lk1W`.
Official signed Ceph Tentacle EL9 packages installed librados and Ceph CLI
20.2.4, source commit `7f793731f1b39eb4f465e960113d2363c311b964`. The older
AlmaLinux v16 runtime was replaced before comparison. Loader evidence confirms
`/lib64/librados.so.2`; the resolved library path, RPM ownership, dependency
listing and SHA-256 before/after are retained. `rados_version()` reports API
version 3.0.0, not the Ceph release; the report labels that distinction.

Fresh `prepared` Go/native binaries bind the selected source manifest. Only
benchmark tooling changed: native optional entity, bounded diagnostic count
and API-version reporting, plus wrapper identity propagation. Production Go
implementation and runtime policy are unchanged. Both clients use
`client.amakura`, the same pools and preseeded objects, secure msgr2, 64-KiB
reads, sixteen workers and caller-owned read buffers (`ReadInto`/`rados_read`).
Each worker warms up eight reads and measures 4,096 reads: 128 warmup and
65,536 measured operations per leg. Latency covers the synchronous read-call
interval; payload comparison is outside the timer in both drivers. No new
seeding, writes or deletions occurred in this comparison.

Five serial blocks per pool use native/Go/Go/native on odd repetitions and
Go/native/native/Go on even repetitions. Each implementation has ten measured
legs per pool. All forty legs validated, totaling 2,621,440 reads. No CPU or
allocation background workers, tracing, timing collection or messenger debug
logs are enabled in primary legs. Go uses ten Ps, `GOGC=100`, `GOMEMLIMIT=off`,
with four production scratch slots. Separate instrumented mode probes verify
actual secure monitor/OSD negotiation for both clients and are excluded.

Entries are medians of ten run-level values, not pooled percentiles.

| Pool | Go p50 ms | Native p50 ms | Go p95 ms | Native p95 ms | Go p99 ms | Native p99 ms | Go/native p99 | Go IOPS | Native IOPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| test-3x | 0.591884 | 0.622676 | 0.865872 | 0.952961 | 1.022941 | 1.137657 | 0.899164 | 21659.65 | 20526.75 |
| readcache | 0.617053 | 0.613628 | 0.909953 | 0.949413 | 1.091903 | 1.141391 | 0.956643 | 21237.37 | 21077.99 |

The ratio of run-level p99 medians is about 10.1% lower for Go on test-3x and
4.3% lower on readcache. Median IOPS is about 5.5% and 0.8% higher respectively.
These are descriptive results, not significance or equivalence claims. Go has
lower mean-of-two-leg p99 in four/five test-3x blocks and three/five readcache
blocks; block ratios span 0.781132..1.126869 and 0.921172..1.035059 respectively.
The median block ratios differ from ratios of overall medians; exact individual
legs and both calculations remain in the
[closed-loop summary](compact/live-closed-loop-v20-20261001.json).

Source, binary and library hashes stayed unchanged. Visible cgroup throttling
counters remained zero. The cluster reports Ceph 20.2.4 at server commit
`4302593b7d5304637a99b628879b32886b798252`, distinct from the upstream client
build; 22 OSDs were up/in and the map epoch remained 1655. Health was
`HEALTH_WARN` before/after; health detail identifies `POOL_NO_REDUNDANCY`.
Pool metadata confirms readcache replication 1/min_size 1 and test-3x
replication 3/min_size 2, both 32 PGs. This does not make the later serial pool
runs a randomized comparison of replication policies.

The raw capture's overall status remains **failed** because OSD utilization and
pool-statistics commands returned `EACCES` without manager caps. All workload,
mode and provenance checks passed. Those five ancillary failures are retained,
not suppressed or relabeled. No permission escalation was attempted. This
comparison does not measure native offered-arrival latency, loaded cases,
write/mixed workloads, other payloads/concurrency, sustained large-scale
behavior or release qualification. Do not compare this closed-loop call p99
directly with the earlier offered-arrival p99 tables.

## Historical Failures And Limitations

Initial smoke captures failed before seeding due to the uname check and remain
retained. After repair, readcache seeding and its 8,000-read smoke leg passed
(2.175043 ms p99). test-3x seeding failed with Ceph errno -1 (`EPERM`); its
read-only leg also returned `EPERM` during warmup, with zero measured attempts.
Those initial failed attempts provide no test-3x latency result. The later
authorized rerun above supersedes the access blocker without rewriting its
failure evidence. The assistant changed no credentials, caps, pool settings or
cluster configuration.

At the time of the offered-load captures, neither librados nor Ceph CLI was
installed. Native comparison, daemon versions, cluster health/topology and pool
statistics were not collected for those captures. The later closed-loop
comparison above supplies its own contemporaneous evidence, not retroactive
environment validation for the earlier offered-load runs.
This retest does not cover write/mixed workloads, other payloads, larger
concurrency, OSD/session fanout, recovery, sustained endurance or a matched
baseline. Historical reports and guardrails remain unchanged.

## Validation

All 32 wrapper regression cases passed, including redaction, source/binary
binding and GNU uname ordering. Benchmark package tests and focused race tests
passed. Three existing config/keyring tests passed by exact name; an initial
`TestLoadKeyring` filter matched no tests and is not counted. Shell syntax and
`git diff --check` passed before measurement.

The [original readcache summary](compact/live-retest-20261001.json) retains
the initial test-3x access failure as historical context. The
[authorized test-3x summary](compact/live-retest-test-3x-20261001.json) records
the later passing rerun. Both are generated from retained captures;
validation checks p99 against every primary outcome, exact counts, report
validity, source/binary stability and instrumented timing/trace counts.

The closed-loop verifier checks all forty reports and retained artifact hashes,
actual-mode evidence, diagnostic shape/counts and source/binary/library stability.
Native strict C11 compilation with all warnings as errors, invalid-count guards,
custom-identity wrapper tests, benchmark/mode-parser tests and shell syntax pass.