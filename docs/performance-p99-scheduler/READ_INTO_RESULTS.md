# Caller-buffer reads: implementation and measurements

Status: ObjectRef.ReadInto and bounded secure receive reuse implemented and
tested. No global runtime defaults changed. Allocation volume and throughput
improved in the measured workload; stable p99 compliance and Phase 4 completion
remain unproven. No native parity or P12/P13 requalification claim.

This records the original one-slot implementation. The subsequent
[inventory experiment](INVENTORY_RESULTS.md) retains four charged idle slots
by default and reports a repeated host-specific p99 improvement.

## API and ownership

```go
destination := make([]byte, 64<<10)
count, info, err := object.ReadInto(ctx, 0, destination)
```

The requested length is len(destination). Successful short reads change only
destination[:count]; errors leave it entirely unchanged. The caller must not
access the buffer during the call. No writes occur after return. Cancellation
concurrent with the final synchronous copy can resolve as successful completion;
copying never continues in a background worker. ObjectInfo reports Version,
as with Read.

An internal borrowed completion keeps its receive lease through full reply
validation and copying. Retries and backoff resends release discarded borrows.
Ordinary Read keeps its owned handoff: caller-visible backing is never recycled.
Authentication, padding, epilogue, identity, attempt, result, operation and length
checks precede destination mutation.

Built-in secure transports retain at most one free remaining-record buffer per
connection, for record sizes 32 KiB through 128 KiB. Capacity is rounded to
8-KiB multiples and charged to the aggregate receive budget while active and
idle. Returning a slot clears its entire backing. Idle slots are discarded
when admission would otherwise fail; exact-size allocation remains the fallback
when rounding cannot fit. Lock order is scratch mutex then budget mutex; budget
admission unlocks before requesting eviction. Close retires the slot without
invalidating outstanding borrowed data, which releases without reentering the
closed slot. No unbounded sync.Pool or uncharged cache is added.

CRC, custom transports, small records and records over 128 KiB support ReadInto
through allocation/copy fallback without this reuse. Existing receive-envelope
exclusions still apply, including allocator rounding on nonpooled allocations,
metadata and caller-owned destinations.

## Live comparison

Two fresh isolated Ceph v20.2.4 clusters on the ten-CPU Linux ARM64 Docker VM,
Go 1.27.1, secure 64-KiB reads, concurrency 16. Each capture has five repetitions
with native-before, two Go API legs and native-after. Go order alternates:
Read first on 1/3/5, ReadInto first on 2/4. Both APIs use the same binary/source
and seeded objects; each leg has 128 warmup and 4096 measured reads. Every read
checks content. GOMAXPROCS=10, GOGC=100, GOMEMLIMIT=off. No concurrent validation
work ran during timing.

Loaded Go legs run eight same-process hashing workers, as in BATCHING_RESULTS.md.
Native legs remain unloaded context, not loaded parity. Each ReadInto worker
allocates one destination before warmup; that allocation is included in resource
totals, not timed read latencies. Resources cover warmup plus measured reads and
include background CPU. Entries are medians of five run-level measurements:

| Workers | API | p99 ms | IOPS | Allocated MB | Allocations | GC cycles | GC pause ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | Read | 7.976073 | 8317 | 345.135 | 427872 | 84 | 130.322 |
| 0 | ReadInto | 7.734015 | 11242 | 213.759 | 436199 | 48 | 85.228 |
| 8 | Read | 18.777972 | 3210 | 345.100 | 427890 | 75 | 65.864 |
| 8 | ReadInto | 18.280400 | 3839 | 223.367 | 436332 | 46 | 34.859 |

Allocated volume falls about 38.1% idle and 35.3% loaded; throughput medians
rise 35.2% and 19.6%. Allocation counts increase about 2% due to small lifetime
bookkeeping. Throughput improves in every pair, but p99 regresses in two of five
pairs under each load. Median p99 changes are only -3.0%/-2.6%, not pooled p99
or evidence of stable tail compliance. No cgroup throttling increments occurred
in the twenty measured Go legs. This option is retained for demonstrated byte
allocation and throughput benefits, not a solved latency gate. Sustained,
broader-concurrency and allocation-heavy host workloads remain unqualified.
Pool hit/miss rates are not instrumented here.

## Local codec check

BenchmarkSecureReceiveInto times secure decode, reservation and handoff;
the borrowed variant includes copying and zeroing. Encode/reset, setup and
destination allocation are excluded. Session/OSD routing and network are absent.
Three 100-iteration runs on macOS ARM64:

| Payload | Owned B/op | Borrowed B/op | Owned allocs/op | Borrowed allocs/op |
| ---: | ---: | ---: | ---: | ---: |
| 64 KiB | about 74244 | about 1355 | 10 | 12 |
| 4 MiB | about 4203013 | about 4203049 | 10 | 12 |

At 4 MiB the copy-only borrowed path is slower (about 0.92 ms versus 0.83 ms
in these short runs). Prefer ordinary Read when its owned result is suitable,
especially for larger reads. Serial reuse does not imply every concurrent live
record hits the single slot. Raw local output is in read-into-evidence.

## Validation and evidence

Tests cover lease retention/exactly-once release, actual secure reuse,
authentication-failure wiping, retained ordinary backing, tight-budget fallback,
idle eviction, concurrent release/Close, generation retirement, actual Session
publication before cancellation/Stop, borrowed backoff resend, both EAGAIN
levels, malformed/server/cancellation errors, short/empty reads, overflow and
public admission errors. Full race suite, diagnostic-tag tests, vet, Linux ARM64
build and strict Node tests passed. Independent review found no blocking defect.
Combined live reconnect-with-borrow testing and P12/P13 qualification remain
future work; existing reconnect tests still pass.

[summary.json](read-into-evidence/summary.json) retains all pairs. Captures keep
source inventories, resource/cgroup reports and original artifact hashes. Full
timing arrays are gzip-compressed. Executables and raw traces are omitted with
hashes retained; PUBLISHED.sha256 covers actual published files. Four tests were
expanded after capture; later formatting/report assertions/docs were not part
of measured builds. Captured implementation behavior is unchanged. Existing
phase reports are not overwritten or recertified.
source-final-validated.sha256 separately identifies the formatted source after
the complete test additions; it is not a new live qualification identity.

```sh
node integration/p07/read-into-summary.mjs docs/performance-p99-scheduler/read-into-evidence
node --test integration/p07/read-into-summary.test.mjs
GOTOOLCHAIN=go1.27.1 go test ./internal/msgr -run '^$' \
  -bench '^BenchmarkSecureReceiveInto$' -benchmem -benchtime=100x -count=3
P07_SCHEDULER_SWEEP=1 P07_SWEEP_FIXED_PROCS=10 P07_READ_INTO_COMPARE=1 \
  P07_BACKGROUND_WORKERS=8 P07_DIAGNOSTIC_DIR=/tmp/fresh-absolute-directory \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

Use zero workers for idle comparison. Traced legs and preliminary unloaded
ABBA legs are retained but excluded from the paired table.