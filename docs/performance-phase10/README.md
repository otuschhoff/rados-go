# Phase 10: Current-Source Syscall Attribution

Date: 2026-10-03. Verdict: **no production change**. The attribution experiment
is complete; no additional safe small optimization has causal local evidence
and independent full-client benefit. Native CPU parity remains open. There is
no TCP/runtime tuning, batching delay, crypto backend or PGO adoption.

## Contract And Reproduction

The [serial driver](../../integration/p07/syscalls.mjs) freezes a two-round
plan before capture. Three cells are readcache 4-KiB read/c1, readcache 64-KiB
read/c16 and test-3x 1-MiB write/c16. For each cell, separate fresh-process
baseline, strace and perf probes each have two mirrored Go/native repetitions:
36 legs total. These are excluded attribution, not qualifying timing.

Both clients use the existing byte-matched fixture collectors, warmed connected
state, >=1-second AND 1,000-operation warmup and >=8-second AND 10,000-operation
measurement. Go 1.27.1 is built with explicit `-pgo=off` and CGO0; P10,
GOGC100, GOMEMLIMIToff and CPU affinity 0-9 are fixed. Native logging/retry
limitations are unchanged. Normal qualification minima remain unchanged.

Run from the repository root with the existing five private `P07_PARITY_*`
deployment path variables, only on the already approved fixture deployment:

```sh
node integration/p07/syscalls.mjs /absolute/fresh/private-root p07-parity-sys-fresh-prefix
node integration/p07/syscalls.mjs analyze /absolute/private-root /absolute/private-root/fresh-reanalysis.json
node --test integration/p07/syscalls.test.mjs
```

The driver retains private exclusive files, source/tool/build/library pins,
per-command exit data, actual secure MON/OSD modes, health/warning continuity
and every-worker placement. It checks byte-exact reads/final contents and
cleanup/NotFound through the collectors; only absent-before-seeding fixtures
are owned. Captures stop on any unsafe/incomplete evidence. Existing files,
failed attempts and unfavorable measurements are never replaced.

Collector output adds lossless string-valued realtime start/end nanoseconds
around the measured phase. Go/native boundaries are after resource baselines
and before sampler stop/final verification. Relative monotonic elapsed time
must agree within 200 ms; clock mismatch fails analysis. Setup, warmup and
cleanup stay outside the trace window. External trace time is not library CPU.

strace uses per-thread timestamps/durations and raw addresses for read/write/
send/receive arguments: no payload dump. Socket lifetimes distinguish socket
I/O from regular-file RSS/log I/O. Cross-boundary calls and thread-termination
censored parked calls are explicitly excluded from completed counts, not
invented as successes. Termination censorship requires a successful exit after
the measurement window. A 96-byte write is a control candidate, not a proven
ACK; keepalive or other control ambiguity remains explicit. Blocked syscall
duration is never treated as CPU time.

perf records 199-Hz software CPU samples, period-one software context-switch
events and raw frame-pointer callchains on the realtime clock. Decoding retains
leaf CPU attribution and per-thread context-switch traces; raw callchains remain
available. Recorder/decoder diagnostics are pinned and reported loss/truncation
is rejected. Native stripped symbols remain unknown, not guessed AES routines.
Scheduler tracepoints were unavailable; software context-switch observations
are not runnable-state traces or a causal wakeup source. No host policy was
changed to enable profiling.

Offline reproduction pins all executing analyzer dependencies, requires the
exact complete independent process/namespace population, verifies binary/raw
artifacts, mode/health/placement evidence and operation counts, and recomputes
metrics and attribution. Fresh exclusive output is required. Profiles, traces,
credentials and private captures are not committed.

## Final Evidence

Root: `/root/proj/rados-go/phase10-syscalls-20261003212821`.
All 36 legs passed capture/fixture/guard checks with 2,294,809 measured
operations. `reanalysis.json` exactly equals `summary.json`. Binary SHA-256:

| Build | SHA-256 |
| --- | --- |
| Go | `1841d7f1811455fa9b805766e4b3f9b0bd8d89b335a0562959f6d3456fadd84d` |
| Native | `b91e069df1bb70d982d8d64c5ddcabb9f96625b0b0176ed3bd65ab2b830bbca9` |

Completed measured-window syscall counts below include only traced operations.
Socket writes include sendmsg; epoll combines epoll_wait/epoll_pwait. Futex
counts include all completed futex operations, not just wakes or waits.

| Cell/Client/Round | Operations | Socket Writes | Socket Reads | Futex | Epoll | 96-Byte Control Candidates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Small Go 1 | 16240 | 16301 | 32480 | 111544 | 46592 | 60 |
| Small Native 1 | 11155 | 11156 | 22312 | 115289 | 22310 | 1 |
| Small Native 2 | 10617 | 10618 | 21236 | 110631 | 21234 | 1 |
| Small Go 2 | 13979 | 14027 | 27956 | 106115 | 37420 | 47 |
| Read/c16 Go 1 | 138589 | 138863 | 212970 | 118754 | 62193 | 273 |
| Read/c16 Native 1 | 47365 | 47366 | 107537 | 283372 | 24385 | 1 |
| Read/c16 Native 2 | 45306 | 45307 | 107070 | 279305 | 22094 | 1 |
| Read/c16 Go 2 | 139251 | 139581 | 218842 | 129386 | 64637 | 329 |
| Write/c16 Go 1 | 10807 | 15077 | 20956 | 64993 | 28227 | 4239 |
| Write/c16 Native 1 | 10060 | 10065 | 19304 | 84009 | 15772 | 2 |
| Write/c16 Native 2 | 10250 | 10707 | 19250 | 84830 | 16072 | 1 |
| Write/c16 Go 2 | 10461 | 14040 | 19807 | 63474 | 28457 | 3086 |

Separate software context-switch counts per successful operation:

| Cell | Go Round 1 / 2 | Native Round 1 / 2 |
| --- | --- | --- |
| Small read | 6.492 / 7.126 | 4.997 / 5.018 |
| Concurrent read | 2.020 / 1.887 | 3.296 / 3.476 |
| Concurrent write | 9.996 / 9.224 | 4.567 / 4.485 |

Small-read Go CPU leaves include syscall entry, task-switch completion, locks,
select and work stealing, with no single dominant local site. Concurrent-read
Go AES-GCM decryption is 10.24-10.94% and memmove 6.80-7.66% of sampled
CPU periods. Concurrent-write Go AES-GCM encryption is 34.71-35.45%, memmove
14.18-15.53%, kernel page initialization 7.97-9.08% and kernel copy-from-iter
6.69-6.71%. Native write libcrypto DSO leaves are 25.18-25.60% but exact
crypto routines are stripped. These percentages describe the excluded profiles,
not independent CPU floors or a crypto implementation authorization.

Raw captures retain full baseline latency distributions and CPU/RSS metrics.
Baseline CPU/op in microseconds, Go round1/2 versus native round1/2:
small 265.95/274.95 versus 203.99/173.74; concurrent read 132.34/131.47
versus 147.64/152.52; concurrent write 1306.34/1286.64 versus
1101.86/1181.17. Baseline p99 in milliseconds is small Go 0.496/0.526,
native 0.453/0.380; read/c16 Go 1.126/1.293, native 1.131/1.048;
write/c16 Go 20.820/19.394, native 21.465/23.527. No confidence or parity
claim is attached to these two-round descriptive measurements.

## Reevaluation And No-Change Decision

1. Separate-ACK hypothesis is not supported on small reads: socket writes are
   about one per operation, with only 47-60 control candidates across 14-16k
   operations. Concurrent writes have 0.30-0.39 control candidates/op, but only
   5 and 76 same-socket write gaps below 200 microseconds out of about 15k/14k
   pairs. Neither write gaps nor control size proves queued ready-frame density.
2. Scheduling overhead exists, but differs by workload. Go has more context
   switches on small reads and writes, fewer on concurrent reads. Futex counts
   do not predict CPU costs or uniquely identify a redundant handoff. strace
   visibly perturbs throughput, particularly native concurrent reads, so its
   counts cannot establish an uninstrumented performance benefit.
3. The existing writer already sends a contiguous complete frame, permits one
   outstanding write and uses buffered completion, registered admission and
   bounded ACK piggyback/deadline handling. There is no demonstrated redundant
   split write to fix. Unconditionally buffering tasks or batching would widen
   cancellation/generation/storage-lifetime windows without a proven benefit.
4. No production candidate is retained. Thus candidate idle/host-load trials
   and independent benefit comparisons are not claimed or manufactured. A
   future candidate must first demonstrate causal local readiness/handoff
   evidence, deterministic tests, repeated benchmark benefit, then independent
   c1/concurrent/idle/load full-client latency/memory/correctness comparisons.

The phase exit permits this no-change verdict. It does not mean scheduling is
optimal or that unknown ACK/wakeup attribution is solved. Those unknowns prevent
speculative optimization; they are not relaxed parity gates. CGO-free and
standard-library crypto remain unchanged; AES remains watch-only. Phase 11
resource policy/endurance is next, with policy approval dependencies intact.

Retained incomplete roots `phase10-syscalls-20261003211710` and
`phase10-syscalls-20261003212207` stopped at actual strace termination and perf
event/leaf parser findings. They are not complete assessments or discarded
performance failures. Repairs were regression-tested against their actual raw
records, then a new full fixed source-bound run was captured.

Verification covers focused parser/population rejection tests, all adjacent
capture suites including strict C tests, full Go race/build/vet and pure-Go
dependency checks, minimum-toolchain collector compatibility, arm64 compilation,
and repeated deterministic registered-admission, buffered-completion,
control-priority/deadline/replay/cancellation and lease-lifetime checks.