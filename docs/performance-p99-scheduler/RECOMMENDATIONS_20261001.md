# Recommendation Execution: 2026-10-01

Status: implementation, available local checks and final-source read/native
matrix captures completed. Supplementary before/current public write and
metadata checks completed. Compact evidence is
[published](recommendations-20261001.json): 488 valid timed legs and 10182656
successful timed operations. This is not full phase closure, native parity,
or renewed P12/P13 certification.

## Implemented Scope

- Backoff predicates are indexed by PG. Notifications affect only subscribed
  waiters in the changed PG; reset/failure still wakes every waiter. Subscription
  and notification-channel capture are atomic under the session mutex, avoiding
  a wake-before-select race. Duplicate IDs remove the old PG membership.
- Admitted submissions have intrusive per-PG lists. Unblocks scan the affected
  PG rather than every admitted request. Completion removes links and empty PG
  entries without allocating a short-lived per-request map bucket.
- Request and replay storage use dense indices for constant-time arbitrary
  removal. Separate intrusive FIFO and sequence-ordered replay lists preserve
  dispatch/control/reconnect ordering. ACKs remove the acknowledged replay
  prefix, not a repeatedly rescanned and shifted full slice. Removed entries,
  fault drains and Stop clear backing references and links; no tombstones grow.
- Admission continues to copy caller payloads. Built-in secure framing reuses
  those immutable, admission-owned payloads; authentication forwards the
  ownership capability. Public encoding, CRC, and custom codecs/transports keep
  defensive defaults. CRC was not promoted because controlled replay timing
  improvement was not established. Replay retains its original payload and
  transaction/sequence identity.
- Incrementals share immutable unchanged map collections and copy changed
  components. Public mutable getters remain defensive. Later and failed
  incrementals cannot mutate prior snapshots; incoming changed data is copied.
  Full-map validation, FSID/epoch/CRC handling and placement reuse are unchanged.

No production queue, receive, session, runtime or backoff limit was changed.
A bounded active-backoff overflow policy still needs an explicit policy and
maintainer approval; this implementation does not claim a new hard bound.

## Local Measurements

Before: `25202fd`; after: the source-bound working-tree patch. Same Go 1.27.1,
five samples per cell, `-benchmem -benchtime=100ms -count=5`, serial execution.
These are synthetic costs, not causal attribution for live p99.

| Cell | Before median | After median |
| --- | ---: | ---: |
| 4096 unrelated PGs, one lookup | 100739 ns | 44.39 ns |
| 4096 ranges, registration batch 128 | 13.16 ms | 0.138 ms |
| 4096 admitted submissions, one unblock | 75.88 us | 13.56 us |
| Completion burst, depth 4096, random | 22.44 ms | 2.394 ms |
| Cancellation burst, depth 4096, random | 10.21 ms | 3.936 ms |
| ACK burst, depth 4096, width 1 | 17.73 ms | 0.445 ms |
| Pool rename, 4096 unchanged OSDs | 0.917 ms / 412785 B | 0.0038 ms / 1872 B |
| Pool rename, 4096 override PGs | 4.333 ms / 1719376 B | 0.0036 ms / 1872 B |

Unblock/burst fixtures include their documented timed resets. PG indexes add
metadata; they are not free receive-memory or process-RSS bounds. Registration
allocations stayed at 640 per 128-request fixture after replacing PG buckets
with intrusive links. Ordered range lookup within a single PG was not added.

Three alternating before/current blocks of 4-MiB secure replay measured time
ratios 0.771, 0.800 and 0.752. CRC ratios were 0.990, 1.086 and 1.043, so its
copying path was retained. These blocks contain six samples per implementation
and codec, with `GOMAXPROCS=56`, `GOGC=100`, `GOMEMLIMIT=off`.

Separate full-rate heap profiles after five instrumented operations retained
about 69 kB for secure replay in both implementations and about 50/45 kB for
the map fixture. They show no retained large payload/override allocation in
these completed fixtures, not a production endurance guarantee. Instrumented
timings are excluded from primary results.

The submission fixture includes actual messenger admission, pumps, framing,
wire encryption and replay, but excludes OSD request encoding and processing.
The native matrix is not a before/after Go write comparison. The supplementary
public-write comparison below adds that boundary, including OSD encoding and
completion, but does not establish the entire Phase 6 write exit gate: serial
timing regresses and whole-process allocation totals do not isolate library
bytes/op. These scopes must not be combined into a general improvement claim.

## Live Measurement Contract

Final-source capture uses Ceph 20.2.4 librados and the supplied existing pools
`test-3x` and `readcache`, secure transport, `GOMAXPROCS=10`, `GOGC=100`, and
`GOMEMLIMIT=off`. No cluster policy, pool redundancy or OSD state is changed.

The Phase 3 reference is `c3dc3d3`; only its private benchmark identity/keyring
handling and sample count were adapted. Compare its `Read` with final `Read`;
compare final `ReadInto` with native separately. This is a net comparison of
intervening changes, not isolated Phase 4 attribution. Five genuinely
alternating mirrored blocks per pool contain two legs per implementation,
65536 reads per leg, and 128 warmup reads. Mode probes are separate.

The final read capture completed all 80 timed legs successfully. Paired-block
geometric mean p99 ratios and approximate 95% intervals are:

| Pool | Final Read / Phase 3 Read | Final ReadInto / native |
| --- | ---: | ---: |
| test-3x | 1.030 [0.977, 1.086] | 0.935 [0.800, 1.092] |
| readcache | 1.010 [0.944, 1.080] | 1.000 [0.943, 1.060] |

Intervals use Student-t on five paired block log ratios (four degrees of
freedom), conditional on this host, schedule and cluster. Individual requests
are not independent repetitions. Both Read intervals span no change; without
an agreed margin these measurements do not close the non-regression gate.

The sustained matrix uses owned benchmark object prefixes and per-driver
seed/cleanup, eight warmups per worker, and three alternating Go/native ABBA
blocks per cell/pool. Payloads are 4 KiB, 64 KiB, 1 MiB and 4 MiB with
read/write/mixed workloads; 64-KiB reads also sweep concurrency 64/128/256.
Serial cells contain 4096 operations, c16 cells 16384, and larger concurrency
cells 256 operations per worker. Go uses `Read` in normal workload mode.
Warmup/setup/cleanup are outside latency samples but inside resource totals.

Preliminary nonalternating read captures and a matrix with reporting/counting
defects are retained separately. The failed mixed rows had zero quantiles
because the old two-operation branch was not yet looped; neither those rows
nor rejected metadata reports are performance evidence. Final capture validated
all 80 diagnostic and 360 matrix legs, with unchanged source/binary identities.
The diagnostic contains 5242880 timed operations and the sustained matrix
4816896, across 30 pool/workload cells. No cgroup CPU throttling was observed.

Go/native paired-block p99 ratios span 0.616-1.938 on test-3x and 0.642-2.554
on readcache across the heterogeneous matrix cells. These ranges are not a
pooled parity estimate: size, concurrency and operation type differ, and each
cell has only three blocks. The published JSON retains all cell medians and
individual block ratios, including slower Go results. Offered-load saturation
and deployment-wide confidence are not established by this closed-loop matrix.

## Supplementary Verification

An untouched `25202fd` library and final library used byte-identical final
benchmark harnesses for complete 4-MiB `WriteFull` calls. Three alternating
ABBA blocks per pool/concurrency contain 48 valid legs and 122880 timed writes:
1024 operations at c1, 256 per worker at c16, eight warmups per worker. Runtime
and secure transport settings match the final matrix. Results are net changes,
not isolated attribution to framing or queues.

| Pool / concurrency | Median elapsed current/before | Process allocated bytes current/before | Paired p99 current/before |
| --- | ---: | ---: | --- |
| test-3x / 1 | 1.059 | 0.800 | 0.954, 0.947, 1.194 |
| test-3x / 16 | 0.886 | 0.800 | 0.846, 0.885, 0.906 |
| readcache / 1 | 1.310 | 0.800 | 1.160, 1.109, 1.383 |
| readcache / 16 | 0.910 | 0.800 | 0.936, 0.900, 0.813 |

Process resources include payload generation, seeding, warmup and cleanup.
Dividing them by timed operations does not isolate library bytes/op. The
allocation reduction and c16 timing improvement do not erase the serial
regressions, especially readcache. Phase 6 write non-regression remains open.

Supplementary native-seed/Go/native-verify metadata, binary xattrs, OMAP
pagination, compound atomicity, cross-client contention, enumeration, namespace
and cursor checks passed in both supplied pools: ten Go flags and four native
verification flags per pool. Cleanup removed twelve objects per pool and
verified zero remaining in both owned namespace pairs. No pool-wide snapshot
mode or coordinated map change was attempted. This is semantics evidence,
not metadata latency qualification or formal P08 renewal.

The initial private native driver used unsupported `ms_osd_client_mode`; seed
failed before object creation and cleanup passed. Its capture is preserved.
The repaired driver uses the working benchmark's `ms_client_mode=secure` and
the final supplementary rerun passed. Production library sources did not change.

## Validation And Open Gates

Full repository race/vet, minimum Go 1.26.8 tests, latest Go 1.27.1 tests,
pinned staticcheck/govulncheck, cross-builds and deterministic release passed
through the qualification runner. The Linux-only benchmark also cross-builds
for linux/arm64. Focused backoff, queue, ownership and authentication race
checks were repeated; local session fuzzing completed 1324244 executions and
incremental decoder fuzzing 2926138. The minimized partial-reset fuzz script
is an in-file regression seed; it preserves original replay identity.

P12 qualification remains failed: P03/P04/P06/P07/P08/P09/P10/P11 reports are
stale for this source, and required Darwin and Docker runtime executions are
unavailable. Cross-builds are not runtime passes. Certifying fuzz, renewed
P13 integration, 24-hour endurance, independent signatures, multi-host load,
real large-scale fanout, and destructive reconnect/map-churn qualification
have not been substituted with synthetic or existing-cluster read/write runs.

Final P07 and P13 verifier attempts explicitly reject source artifacts that do
not match the current tree. The certifying fuzz runner explicitly rejects this
linux/amd64 host because it requires Darwin. These expected blocked outcomes
and exact retained logs are included in compact evidence; no report hashes or
independent reviewer signatures were manufactured.

Phase 4 has no maintainer-selected non-regression margin and no rigorous
closure claim. Offered-load native parity and saturation/recovery qualification
remain unmeasured. Existing guardrails and production defaults are unchanged.

Private raw evidence and scripts are retained under
`/root/proj/rados-go/recommendations-20261001-C1ZeJt`; keys, credential hashes,
raw captures and generated release archives are not added to Git.