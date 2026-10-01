# Library-local batching and parallel host workload

Status: experiment completed; batching rejected and removed. No production
runtime settings changed. Phase 4 latency remains open; no P12/P13 renewal or
native parity claim. At this capture, caller-buffer reads were designed only; see
[the ownership and implementation gates](CALLER_BUFFER_DESIGN.md).
The later implementation is recorded in [READ_INTO_RESULTS.md](READ_INTO_RESULTS.md).

## Experiment

The session owner drained up to eight already-ready commands before returning
to its dispatch/select loop, without waiting. It preserved FIFO command order,
one-write-at-a-time and unchanged frame read-ahead. Focused race tests passed,
but the prototype did not remove underlying channel handoffs. Its admission
batch could also delay transport/control events by up to eight commands.

The baseline used identical diagnostic instrumentation and the same helper
with a limit of one. The candidate changed only that call's limit to eight.
The private baseline snapshot was derived from project HEAD 1c16871 plus the
then-current diagnostic additions, not from an unrelated historical benchmark.
See the saved baseline session source and prototype tests in batching-evidence;
the candidate is reproduced by changing handleCommandBatch(command, 1) to 8.
Source inventories bind both captured variants; production session.go is now
restored exactly to HEAD, with no batch helper or experimental public settings.

Four fresh isolated clusters, executed serially: baseline idle, candidate idle,
baseline loaded, candidate loaded. Each ran five ten-P secure 64-KiB read legs,
concurrency 16, 128 warmup and 4096 measured requests, GOGC=100 and GOMEMLIMIT=off.
Go 1.27.1, Linux ARM64 Docker VM on Apple M1 Max, ten VM CPUs, Ceph v20.2.4.
No concurrent validation workloads ran. These are short separate-cluster runs,
not randomized interleaved paired samples or sustained application testing.

Loaded Go legs ran eight same-process CPU workers hashing fixed 4096-byte
buffers. Workers start before the resource snapshot and remain active through
warmup and measured reads; stop joins them after the final snapshot. The CPU
work has no deliberate per-iteration heap allocation. It is a synthetic CPU
contention probe, not a representative allocation-heavy application. The
resource CPU/GC/RSS totals include background work. Native brackets have no
equivalent same-process load and are unloaded context, not loaded parity.

## Results

Entries are medians of five run-level measurements, not pooled p99.

| Background workers | Batch limit | p99 ms | IOPS | Allocated bytes | GC pause ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 8.035228 | 8311 | 344936096 | 134.673 |
| 0 | 8 | 8.513520 | 8526 | 344943200 | 133.091 |
| 8 | 1 | 19.809042 | 3034 | 344898312 | 69.187 |
| 8 | 8 | 19.374333 | 3110 | 344905808 | 62.469 |

Batching's idle p99 median is 5.95% higher; loaded p99 is 2.19% lower. Loaded
sample ranges overlap (baseline 18.863-22.183 ms, candidate 18.994-20.736 ms).
Throughput medians change by roughly +2.6%/+2.5%; allocation volume is unchanged
to practical precision. There is no repeatable demonstrated allocation or
latency improvement sufficient to justify changing the production event loop.
This does not prove that every alternative form of read/write batching fails.

The background workload materially worsens read tails in both variants while
their aggregate GC pauses fall. Consequently GC pause totals alone do not
explain mixed-workload p99; application runnable competition remains relevant.
No cgroup throttling increments occurred in the twenty measured Go legs.
Separate traced legs and full timing arrays are retained but excluded from
these medians. Trace effects prevent treating them as qualification results.

## Evidence and reproduction

[summary.json](batching-evidence/summary.json) retains every measured sample.
Regenerate with:

```sh
node integration/p07/batching-summary.mjs docs/performance-p99-scheduler/batching-evidence
node --test integration/p07/batching-summary.test.mjs
```

Each of before-idle, after-idle, before-loaded and after-loaded retains report,
cgroup, source-before/after and original artifact identities. Full timing
arrays are gzip-compressed. Binary executables and raw trace binaries are
omitted from publication; their hashes remain in each original artifacts.sha256
and their originals remain in /tmp/rados-go-batch-{before,after}-{idle,loaded}.
The original artifact lists cannot be checked wholesale against published
directories because of these intentional omissions. PUBLISHED.sha256 binds
the actual published files. Source inventories describe historical measured
code, including the now-removed batching source; later summary validation and
report assertion repairs were not part of those measured builds.

Reproduce a ten-P diagnostic on a checkout containing the desired variant:

```sh
P07_SCHEDULER_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 P07_BACKGROUND_WORKERS=8 \
  P07_DIAGNOSTIC_DIR=/tmp/fresh-absolute-directory GOTOOLCHAIN=go1.27.1 \
  ./integration/p07/reproduce.sh
```

Set workers to zero for idle load. The current checkout is the restored
unbatched implementation, not the historical eight-command candidate. The
preliminary ABBA legs still run unloaded; only sweep and traced Go legs receive
the background-worker setting. Changing global GOMAXPROCS to two is not a
recommended deployment requirement for highly parallel host applications.

Validation: full repository race suite, focused session race repetitions,
background lifecycle race tests, Linux ARM64 benchmark build, Node regression
tests, shell syntax, exact production session restoration and published
summary regeneration all passed. At that checkpoint ReadInto and bounded scratch
reuse were deferred; their subsequent implementation and remaining validation
limits are recorded separately in READ_INTO_RESULTS.md.