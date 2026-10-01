# Experiment Cache And Verdicts

[experiments.json](experiments.json) is the compact catalog. It records seven
investigations, hypotheses, baseline/candidate changes, multiple decisions,
measured-source bindings and reproduction commands. The seven tables under
`compact/` contain 1,009 published Go/native reports, not request outcome arrays
or full analysis summaries. No production changes accompany this cache.

The cache has three layers: read-only main-git summaries and compact catalog,
the portable [archive index](archive-index.json), and external full-evidence
archives. The standard index is an exact byte copy of the verified external
index, with no absolute storage location. Its seven entries bind archive
filenames, SHA-256 digests, compressed byte sizes, file counts, unpacked byte
sizes and content-manifest digests. Catalog `cache_bundle` names match index
entries and restore as `ARCHIVE_PARENT/<bundle>`.

All seven archives were verified and restored locally. They contain 12,538
files, totaling 475,378,204 archive bytes and 501,187,559 unpacked bytes.
[Storage metadata](cache-storage.json) records the external local cache and
checked restore path separately from the portable index. Catalog archive state
is `verified_local_archive_pack`; durability is
`local_verified_no_remote_backup`. No remote backup, successful upload or
durable future availability is established. Original evidence remains in place;
the archives are external, not raw bundles added to main git.

## Decisions

Verdicts apply to the named decision, not every technique in an investigation:
`retained`, `rejected`, `diagnostic_only`, or `inconclusive`. A correctness test
passing does not turn a failed capture into a passing performance experiment.
Numbers below are documented medians of run-level measurements, not pooled
request p99 or evidence of statistical significance. Rounded display values
are distinct from the exact per-leg numbers retained in compact tables.

| Investigation | Retained Or Diagnostic Decision | Rejected Or Unresolved Decision |
| --- | --- | --- |
| scheduler-parallelism | Two Ps: diagnostic-only deployment probe | Global library tuning and channel rewrite not justified; qualification inconclusive |
| command-batching | Historical evidence retained for diagnosis | Eight-command drain rejected and removed |
| read-into | ReadInto and ownership-backed bounded reuse retained | Stable p99 improvement inconclusive; large-read fallback is not a reuse win |
| scratch-inventory | Four-slot charged inventory retained | Admission windows eight/four rejected |
| read-scale | Scalar identity accessor retained; eight slots diagnostic-only | No eight-slot promotion; fallback-size reuse benefit unsupported |
| request-path | Encoding allocation reductions and O(1) accounting retained locally | ACK coalescing rejected; causal live p99/capacity gain inconclusive |
| factorial-stalls | Phase and trace diagnostics diagnostic-only | Queue/PG indexing and targeted notification deferred; no passing qualification |

### Scheduler Parallelism

Source: [initial results](README.md). Ten versus two Ps keeps secure 64-KiB
Read, concurrency 16, GOGC=100 and GOMEMLIMIT=off fixed. Each of three clusters
has five rotated repetitions with native brackets. Two-P median p99 is
5.231/4.935/5.151 ms versus 8.422/8.025/8.001 ms at ten Ps; IOPS is
14967/15988/15171 versus 8071/8112/8845. All fifteen two-P legs satisfy the
conservative bracket comparison; ten Ps has three misses. This host-specific
effect is not a recommendation to set global runtime policy inside the library.
GC temporal overlap and runnable-delay profiles do not establish channel CPU
cost or causal tail attribution. Separate traced executions are excluded from
the uninstrumented sweep medians. A/B source selection began after compilation;
C selected inputs before build. Omitted raw traces/events make historical trace
reanalysis partial even though published summaries and timing arrays remain.

### Command Batching

Source: [batching results](BATCHING_RESULTS.md). Baseline and candidate differ
only in ready-command drain limit one versus eight, with the same diagnostic
instrumentation. Four fresh serial clusters cover before/after, idle/eight CPU
workers. Each side/load has five 4096-read legs at ten Ps. Median idle p99
8.035228 -> 8.513520 ms worsens; loaded 19.809042 -> 19.374333 ms improves
slightly with overlapping ranges. IOPS changes 8311 -> 8526 and 3034 -> 3110;
allocated volume is practically unchanged. This does not justify changing the
production event loop. The prototype was removed, not left as a runtime option.
Saved baseline session source and prototype tests permit the documented helper
change, but are not a complete historical source tree.

### ReadInto

Source: [caller-buffer results](READ_INTO_RESULTS.md). Five alternating
same-cluster Read/ReadInto pairs per idle/eight-worker load use the original
one-slot implementation and the same binary/seeded objects. Median allocated
MB falls 345.135 -> 213.759 idle and 345.100 -> 223.367 loaded; IOPS rises
8317 -> 11242 and 3210 -> 3839. Median p99 is 7.976073 -> 7.734015 ms and
18.777972 -> 18.280400 ms, but two of five pairs regress under each load.
Retain the API for byte-allocation and throughput gains, not a solved tail gate.
Allocation counts increase slightly. The 4-MiB local codec path remains copy
fallback and is slower in these short runs; do not generalize 64-KiB reuse.
Current production four-slot behavior is not the historical one-slot comparison.

### Scratch Inventory

Source: [inventory results](INVENTORY_RESULTS.md). The five rotated variants
are slots/window 1/16, 2/16, 4/16, 1/8 and 1/4. Three captures measure idle,
eight hashing workers, and eight coupled hashing/allocation workers; each leg
has 128 warmup and 16,384 measured reads. Four versus one slot gives median
p99 7.631 -> 5.361 ms, 18.297 -> 13.699 ms and 27.701 -> 24.383 ms, with
IOPS 11460 -> 20316, 4132 -> 6350 and 2810 -> 4397. Both metrics improve
in all fifteen pairs. Four slots retain at most 512 KiB charged idle backing
per secure transport within the existing receive budget, not an extra allowance
or process memory cap. Idle eviction remains local, not cross-connection.
Admission waits are included in latency; windows eight/four hurt throughput and
loaded tails and were rejected. Five separately bound untagged default legs
give 23.414 ms / 4461 IOPS under allocation load, not a paired default comparison.
The initial `failed-idle` attempt remains `running`, excluded and unrepaired.
Background allocation totals include duration-dependent harness work.

### Read Scale

Source: [profile and scale results](READ_SCALE_RESULTS.md). Scalar identity
access removes the sanitization hot path. Unpaired idle before/final profiles
show 200738992 -> 184206160 allocated bytes, but p99 5.291260 -> 5.455154 ms;
the retained accessor is not credited with a live tail win. Scale uses a tagged
four/eight-slot-capable binary at concurrency 16/32/64, five rotated pairs,
1024 measured plus eight warmup operations per worker. Four final captures
contain 120 Go and 40 shorter unmatched native context legs. Eight slots win
25/30 eligible 64-KiB p99 pairs and 29/30 IOPS pairs, not uniformly. Eight
slots remain diagnostic-only with a 1-MiB charged bound; four remains production
default. Small 4-KiB and large 1-MiB idle cases have zero hits/misses and use
fallback; their variation is not scratch-reuse evidence. No 4-MiB live case was
measured. Older idle/incomplete allocation captures, initial export-contaminated
traces and the intermediate identity profile stay excluded from attribution.

### Request Path

Source: [request-path results](REQUEST_PATH_RESULTS.md). Two ABBA matrices
combine encoding/accounting changes with ACK coalescing first, then exclude ACK
in the final narrower candidate. All eight captures remain `diagnostic-failed`,
exit 1: 120 rows, 2,240,000 arrivals, 22 failed rows and 4,714 overloads. No
deadline misses, timeouts, cancellations or other read errors were recorded;
delivery/background invalidity can still fail a row with successful requests.
Before/after valid-only subsets are not matched pairs and introduce selection
bias. Final no-ACK all-row p99 medians regress at 1000/2000 arrivals/s; at 4000
the all-row improvement reverses in the valid-only comparison. Throughput is
arrival-rate capped, not maximum capacity. Failure-aware p99 ranks failures as
infinity, represented by null plus `infinite_failures`, unlike historical actual
all-outcome p99; the full archived summary retains that additional analysis.

Local Darwin microbench medians support retaining read-front encoding
669.5 -> 450.1 ns/op, 1464 -> 616 B/op, 23 -> 14 allocations/op, and saturated
depth-4096 dispatch 10165 -> 3.822 ns/op with zero allocations. Raw intermediate
and final microbench labels are distinct; filenames alone do not prove chronology
or contemporaneous source binding. ACK coalescing was removed: neither its
synthetic frame savings nor these mixed live matrices justify reintroduction.
Overload cannot be causally attributed to ACK. Operation, routed-attempt, OSD
submission and control ACK cancellation lifetimes remain independent.

### Factorial Stalls

Source: [factorial results](FACTORIAL_STALL_RESULTS.md). `none`, eight CPU,
eight independently paced allocation workers and `both` with sixteen total
workers are different from the earlier coupled eight-worker workload. Two
primary captures yield 120 rows and 2,240,000 arrivals; 2,235,749 succeed and
4,251 overload. Workload validity is 107 valid / 13 invalid, while original
harness failures are 94: zero/false `omitempty` fields broke historical shell
assertions. Do not replace those exits with the corrected analyzer's validity.
Primary-v3 has 51/9 workload-valid/invalid and 11/49 capture-valid/failed rows;
v4 has 56/4 and 15/45. Twelve separate observed rows have 10/2 workload validity,
2/10 capture validity and 179 overloads. Three setup failures have no reports,
not zero-latency successful measurements.

At 4000 arrivals/s, exact total p99 medians in ns are none 5233365.5, CPU
21019630.5, allocation 5274622.5 and both 21053837.5. Phase p99s cannot be
added. Instrumented rows stay separate. All twelve raw traces remain available
for decoding; signed writer/receiver gaps are preserved, not clamped. There is
no ReadFrame begin/end timing. Worst-service-cohort combined GC/caller-runnable
temporal overlap spans 4.320%-16.388%, not causal primary p99 attribution or
proof the remaining time is network delay.

All 69 synthetic queue/backoff cases remain in archived raw output and summary
scope, outside compact live report tables. Depth-4096 completion bursts have
17.135765/20.904602/19.254181 ms medians; cancellation bursts have
3.345448/7.381921/6.180236 ms. The 4096-waiter backoff batch median is
241.256916 ms, not a request percentile. Output digests preserve bytes but
cannot supply missing contemporaneous source/binary identity. Queue removal,
PG indexing and targeted notification remain deferred production candidates.

## Compact Coverage

| Table | All Published Reports | Headline Go Population | Validity And Exclusion Rule |
| --- | ---: | ---: | --- |
| [scheduler](compact/scheduler-parallelism.json) | 108 | 60 uninstrumented sweep legs | Preliminary/native/trace reports retained; traces excluded from sweep |
| [batching](compact/command-batching.json) | 80 | 20 measured legs | Separate preliminary/native/traced context is not headline samples |
| [ReadInto](compact/read-into.json) | 50 | 20 paired API legs | Preliminary/native/traced context retained; native unloaded |
| [inventory](compact/scratch-inventory.json) | 146 | 75 variant + 5 default legs | Failed-idle and preliminary/traced context retained, not promoted |
| [scale](compact/read-scale.json) | 329 | 120 final scale legs | Profiles, excluded older captures and native context retained separately |
| [request path](compact/request-path.json) | 164 | 120 offered-load legs | Eight failed captures plus profile-before context; no matched native matrix |
| [factorial](compact/factorial-stalls.json) | 132 | 120 primary + 12 observed | Six capture identities include three setup failures without rows |

Tables retain every actual report in each bundle, not only the headline sample
population. Capture and filename distinguish source classes; neither total
report count nor a `passed` context row implies a passing investigation. Original
closed-loop reports have operation counts but do not always export explicit
success/failure totals; absent values remain unavailable. Offered-load reports
retain expected/admitted/attempted/success/overload/SLO counts and validity fields.
Full outcomes, phase-population reconstruction, failure-aware distributions,
trace attribution and microbench repetitions still require restored evidence.

`columns` defines table cells. `environment_id`, `diagnostic_id` and
`offered_configuration_id` index shared dictionaries. Nested `measurements`,
`resources`, `offered_metrics` and `background_metrics` use the corresponding
column definitions. Null is absence, not zero. Original p99 and IOPS precision
is unchanged. Resources include harness/warmup/background scope where documented;
RSS is a process high-water mark. Per-leg exits remain separate from capture
status and workload validity. Cgroup counter differences are decimal strings
computed with BigInt; zero throttling does not exclude host/hypervisor contention.

## Identity And Restoration

Run from the workspace root with the indexed archives in `/path/cache`:

```sh
go run ./tools/experiment-cache verify --index docs/performance-p99-scheduler/archive-index.json --cache-dir /path/cache
go run ./tools/experiment-cache restore --index docs/performance-p99-scheduler/archive-index.json --cache-dir /path/cache --dest /fresh/archive-parent
```

Replace the cache placeholder with the actual external location and choose a
fresh, nonexistent destination. Set `ARCHIVE_PARENT` to that destination for
catalog verification and reanalysis commands. Omit `--bundles` for all seven
bundles, or select indexed names, for example `--bundles evidence` or
`--bundles request-path-evidence,factorial-stall-evidence`. Each catalog archive
entry also supplies its single-bundle restore command. Restoration reads the
indexed archives without changing main-git summaries or original evidence.

Every catalog record uses `source_identity.kind = dirty-source-manifest` and
per-capture original before/after references. `file_digest` hashes stored bytes;
`decoded_digest` hashes uncompressed original bytes. For plain files they agree.
Compact digests bind this new projection; they do not mutate historical manifests.
`source_head_anchor` is not the measured dirty tree. Most anchors are
`1c168717a1767aaa242be955740cdf950e5e1a32`; batching anchors include a different
HEAD. Only batching's explicitly documented baseline derivation supplies
`base_commit = 1c16871`. Do not attach all measured code to that commit. Source
inventories can differ between captures and from final validated source.

Old bundles principally retain manifests, not complete historical snapshots.
Batching additionally saves baseline session/prototype tests. Request-path
content-addressed sources and comparator reconstruction are independently
verified; ten source entries/five historical versions remain unavailable,
including ACK session/tests and older profile harness inputs. Final no-ACK
selected sources are available. Factorial retains six actual source archives;
remove the publication gzip layer before extracting the original tar.gz and
verify the complete captured path set/file hashes. Completed factorial sources
share manifest digest `4bc52cc3eb1ed51174bbd2f0c506e41b4d58ee7dade8ba5322de9254d8200a33`.

Catalog live commands are fresh-capture recipes with exact documented harness
environment, not commands executed by this cache work. Read the variant
reconstruction instructions before running: current source includes later
changes and removed prototypes are not current runtime settings. Keep competing
diagnostic modes unset, use fresh absolute output directories, run captures
serially and retain GOMAXPROCS/GOGC/GOMEMLIMIT as diagnostic controls only.
Docker, pinned images, credentials and rebuilt historical executables require
external prerequisites; their availability is not guaranteed by local metadata.

After archive restoration, catalog verification commands take
`"$ARCHIVE_PARENT/<bundle>"`, never a missing archive-internal helper path. The
current workspace analyzers remain under `integration/p07/`. Scheduler checksum
verification works on retained files, but its original trace analyzer additionally
requires omitted raw/decoded trace artifacts; full trace reanalysis is partial.
Earlier bundles omit raw traces and private binaries. Request-path supports full
published outcome reanalysis but only partial historical source reconstruction.
Factorial supports raw trace and outcome reanalysis with available source
archives, but omits six previously byte-verified executables. Reanalysis is not
binary-complete live reproduction or renewed qualification.

## Validation And Gaps

Archive verification checks indexed bytes and content manifests, and local
restoration makes catalog evidence references available under their bundle
directories. The checked destination is recorded in storage metadata. These
checks do not recover artifacts omitted before publication, identify an unknown
measured commit, or make the dirty snapshot anchor a complete source identity.
Original per-capture provenance remains authoritative.

Cache validation checked seven unique IDs, structured decision values, result
document references and restored bundle command arguments. All 1,009 reports
were compared against original JSON/JSON.gz bytes, exact metric/resource/count
projections, compressed and decoded digests, and publication inventory entries.
These are current cache checks, not reruns of each historical Go/race suite or
live experiment. Historical correctness validations remain attributed to their
result documents; no individual old test is fabricated as passing here.

Qualification remains open: matched native offered-load comparisons, sustained
runs, other hosts/cluster layouts, broad fanout and cross-connection pressure,
larger live payloads, reconnect-with-borrow coverage and production-scale queue/
backoff exposure remain gaps. No default runtime tuning, eight-slot promotion,
admission API, channel rewrite, passing factorial benchmark, Phase 4 gate closure
or P12/P13 renewal follows from this catalog.