# Native Linux Write Investigation And Qualification

Status: qualified for the declared native Linux workload on candidate
`576ce5e`. The independent fixed-size sample passed every predeclared bound
and correctness/environment gate. Docker, Darwin and other architectures are
outside this qualification, not reasons to reject this representative native
Linux workload. Existing P12/P13 multi-platform reports remain separate.

## Scope And Hypotheses

Candidate library: `576ce5e`; historical library: `25202fd`. Linux/amd64,
Go 1.27.1, Ceph 20.2.4, secure transport, the supplied production pools
`test-3x` and `readcache`, and complete public 4-MiB `WriteFull` calls at
concurrency 1 and 16. Runtime: GOMAXPROCS=10, GOGC=100, GOMEMLIMIT=off.
Qualification applies to this configuration and cluster, not arbitrary Linux
hosts, all workloads, endurance, failover, or native performance parity.

Two local hypotheses are tested rather than assuming the earlier serial
regression is caused by the library:

- Restoring only the secure framing copy removes the regression.
- Earlier per-leg timestamp-derived object names introduced PG/OSD placement
  variation, particularly for single-object serial writes.

Three private library variants use the same harness: historical, candidate,
and candidate with only `OwnsWriteFrames` returning false. The control is not
a proposed production change. Production source and limits remain untouched.

## Predeclared Investigation Contract

Six mirrored blocks per pool/concurrency rotate all six permutations of the
three implementations, two legs each per block. Each c1 leg has 1024 writes;
each c16 leg has 256 writes per worker. Eight warmup writes per worker precede
timing. All implementations use identical owned object names in namespace
`write-investigation-20261002`; placement is captured and must remain stable.
The namespace and exact names are exclusive experiment fixtures.

After all timed workers finish, the private harness reads each full payload,
checks it byte-for-byte, removes that exact object, and verifies NotFound.
These checks occur outside latency/elapsed measurement. Process allocation,
CPU and RSS include harness generation, warmup and verification/cleanup;
they are not isolated library bytes/op or retention bounds.

Performance acceptance requires the upper confidence bound for candidate /
historical elapsed time and p99 to be at most 1.10 in every cell. The 10%
margin is selected before new captures, not fitted to results. Each block
ratio uses geometric means of its two legs per implementation. Student-t
bounds on six block log ratios use five degrees of freedom and Bonferroni
alpha=0.05/8 for four cells times two metrics. These are conditional model
bounds, not proof of independence from arbitrary production load.

Correctness/environment acceptance also requires:

- Zero timed command failures or missing operations; failed legs remain
  retained and cannot be silently dropped or rerun into the same capture.
- Every final payload and exact-object cleanup check passes.
- Secure mode configuration and source/binary identities remain unchanged.
- No changed acting set/primary for measured objects, new health-warning
  categories, or degraded/misplaced PGs caused during the experiment.
- No observed cgroup CPU throttling. CPU/GC profiles are separate from primary
  timing and are not included in the acceptance calculation.

Runs are serial, on approved pools only, with no pool policy, OSD/MON lifecycle,
snapshots, queue/receive limits, host runtime defaults or cluster settings
changed. Cluster-health checks occur outside timed legs. Any new warning or
failed command stops the run; investigation must not repair production state.

Passing this contract yields a source-bound native Linux workload qualification
only. If the performance bound fails or is inconclusive, that result remains
explicitly unqualified. Additional architectures, environments and workload
extensions can be qualified later without pretending they passed here.

Private source stages, captures, binaries and profiles are retained under
`/root/proj/rados-go/write-investigation-20261002`. Credentials and their
contents/hashes are excluded from publication.

## Investigation Result And Independent Qualification Plan

The six-block investigation completed 144 uninstrumented legs and 368640 timed
writes. Payload, cleanup, negotiated secure mode, source/binary, placement,
health-category, throttling and cgroup OOM checks passed. Serial elapsed ratios
were 0.965 (test-3x) and 0.962 (readcache); current/copy-control serial ratios
were 0.969 in both pools. Restoring the copy did not remove a reproduced
regression. Earlier varying object placement is a confirmed confound, not proof
that it exclusively explains historical results.

The four-cell performance verdict was nevertheless not qualified: readcache
c1 p99, test-3x c16 p99, and readcache c16 elapsed/p99 confidence bounds exceeded
1.10. Their point ratios were near or below parity; these were inconclusive
bounds, not a confirmed client regression. The pilot is retained without
relaxing its criteria or representing it as a pass.

Six separate c1 CPU profiles passed correctness checks. Copying, encryption,
memory clearing and runtime/syscall work dominate sampled CPU; single
instrumented runs do not establish comparative CPU improvement. No production
fix or framing rollback is justified by this investigation.

A new independent qualification capture is predeclared before execution:
30 alternating historical/candidate ABBA blocks per pool/concurrency, 480 legs
and 1228800 timed writes. The same payload sizes, operation counts, runtime,
fixed fixtures, verification, placement and production-health boundaries apply.
The copy-control pilot and profiles are excluded from this acceptance sample.
All fresh blocks count; there is no optional stopping or extension until pass.

The 10% elapsed/p99 non-regression margin and family-wise one-sided 95%
Bonferroni alpha=0.05/8 remain unchanged. Student-t uses the 30 fresh block log
ratios (29 degrees of freedom). If these fixed-sample bounds still fail or are
inconclusive, overall qualification remains not passed. Other architectures and
environments remain explicitly deferred rather than blocking this scoped test.

## Independent Qualification Result

The independent sample completed all 480 legs and 1228800 timed writes across
120 alternating ABBA/BAAB blocks. All eight performance bounds passed the
predeclared 1.10 margin. Ratios below are candidate/historical; lower is better.
Upper bounds use the simultaneous one-sided model described above, not pooled
request samples. The critical t value is 2.6631956979134617.

| Pool | Concurrency | Elapsed Ratio | Elapsed Upper Bound | P99 Ratio | P99 Upper Bound |
| --- | ---: | ---: | ---: | ---: | ---: |
| test-3x | 1 | 0.9743 | 0.9799 | 0.9719 | 1.0023 |
| readcache | 1 | 0.9748 | 0.9841 | 0.9385 | 0.9932 |
| test-3x | 16 | 0.9855 | 1.0076 | 0.9792 | 1.0059 |
| readcache | 16 | 0.9601 | 1.0025 | 0.9571 | 1.0202 |

Every timed write succeeded. Every final full payload matched, exact fixture
removal and NotFound checks passed, actual monitor/OSD connections negotiated
secure mode, and source/binary identities remained unchanged. Observed
placement snapshots matched; there were no new health-warning categories,
observed cgroup CPU throttling or cgroup OOM events. The existing readcache
no-redundancy warning was not changed or treated as a newly introduced failure.
Placement observation is before/after each cell plus per-block health checks,
not continuous observation of every map transition.

Serial elapsed point estimates improved about 2.5-2.6%; c16 improved about
1.4-4.0%. Whole-process allocated-byte medians were about 20% lower in each
cell. These allocation figures include the harness and untimed work; they are
not isolated library bytes/op, retained-heap or arbitrary-workload guarantees.
No production source, limits, runtime defaults or cluster settings were changed
for this investigation. No secure framing rollback is warranted by these data.

The [source-bound evidence](write-qualification-20261002.json) includes all
fresh block ratios, the separate inconclusive pilot, exact source revisions,
binary/source/harness hashes, an applicable private harness patch, acceptance
parameters and a checksum pin for the private raw-capture manifest. The pilot's
144 legs and 368640 writes and the six instrumented profiles are excluded from
the acceptance sample, not opportunistically pooled with it.

This qualifies only Linux/amd64, Go 1.27.1, Ceph 20.2.4, secure 4-MiB complete
WriteFull operations, concurrency 1/16, GOMAXPROCS=10/GOGC=100/GOMEMLIMIT=off,
and the measured pools/cluster on the supplied representative environment.
Statistical bounds remain conditional on the block model and observed host/
cluster conditions. It does not establish native parity, offered-load or
saturation behavior, failure/recovery, endurance, other sizes or workloads,
all Linux hosts, or global P12/P13 release certification.

Additional architecture/environment certification is supported as a separate
source-bound record: predeclare its configuration, workload and acceptance
criteria; execute its required checks; retain failures and raw evidence; and
publish its own verdict. Cross-builds or this Linux result must not be used as
substitutes for execution on those environments.