# Linux Live Observation Handoff

Validated preparation capture: `/tmp/rados-go-linux-amd64-factorial-ready-v2`.
It contains the untagged `build_linuxamd64` executable and the required source
and binary manifests. Stage the complete preparation capture and the matching
source tree. For trace decoding, `--trace-debug-max-buffer-mb` accepts 16..1024
and defaults to 1024; this bounded decoder setting is separate from benchmark
runtime settings and is recorded in the capture.

## Status And Scope

The Linux offered-load observation lifecycle is implemented. The live wrapper
rejects absent observation artifacts and mismatched attempt counts. The
[2026-10-01 external Linux retest](LIVE_RETEST_20261001.md) completed primary
and separate observed captures in readcache and the authorized test-3x rerun.
During any capture, do not change
selected source files or binaries, prepare another build, or compete for its
CPUs. Markdown documentation is outside the wrapper's source selectors.

The commands below are operator instructions, not commands executed by this
documentation update. External execution requires operator access. This is not
qualification, matched native offered-load parity, or completion of every
performance-review recommendation.

## Implemented Observation Lifecycle

The Linux helper exposes:

```go
validateOfferedObservationConfig(env func(string) string) error
beginOfferedObservation(ctx context.Context) (context.Context, func() error, error)
withOfferedArrivalIndex(ctx context.Context, index int) context.Context
offeredObservedRead(ctx context.Context, index int, read func(context.Context) error) error
wrapOfferedObservedRead(read func(context.Context, int) error) func(context.Context, int) error
exportOfferedObservation(ctx context.Context) error
```

1. `main.run` validates observation before branch selection and client setup,
  including seed and closed-loop branches. Orphan labels and non-factorial
  observation are rejected; offered configuration rejects legacy timing and
  profile controls.
2. Only for observation, `runOffered` attaches the scheduled arrival index just
  before an attempted read. It is not a worker ID or execution counter. Warmup
  has no index; arrivals that never attempt a read have no read task.
3. `runOfferedDiagnostic` begins observation and passes its context to the
  scheduler. It wraps the callback only for observation. Primary execution has
  a no-op finish function, no trace or per-attempt collector, no callback
  wrapper, and no arrival-index context insertion.
4. The scheduler joins read workers before returning. The diagnostic then stops
  and joins background workers before finishing observation. Finish is
  idempotent, rejects active observed reads and surfaces trace writer errors.
  Deferred stop/finish calls provide cleanup safeguards.
5. For observation, the diagnostic closes the client and exports timing from
  the same context, joining finish, close and export errors into the failed
  report. `RequestTiming.Events()` copies under its mutex after shutdown.
  Finish stops the trace; export is exclusive and non-repeatable.
6. The report marks the leg instrumented; the wrapper uses separate labels and
  directories, checks timing-call count against attempted reads, then decodes
  the trace and runs stall analysis. Instrumented samples must not enter
  primary repetition aggregates.

`P07_OFFERED_OBSERVATION_DIR` must be a clean, absolute, nonexistent directory
whose parent already exists. The helper creates it with mode 0700 and writes
`trace.out` and `timing.json` with mode 0600. It requires
`P07_OFFERED_LOAD=1`, `P07_OFFERED_FACTORIAL=1`, and
`P07_OFFERED_OBSERVATION_LABEL=instrumented`. It never removes an existing path.

## Prepare And Stage

After current live captures finish, run from the repository root. Node, Go and a
shell are required. Preparation can run on macOS; it cross-compiles only, with no
cluster connection. The capture path must be absolute and nonexistent:

```sh
GOTOOLCHAIN=go1.27.1 sh integration/p07/live-cluster.sh --prepare \
  --capture /tmp/rados-go-linux-amd64-prepared-fresh
```

This produces `build_linuxamd64` with `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`
and `-trimpath`, without diagnostic build tags: production scratch inventory is
four slots and scratch counters are off. Observation is a runtime opt-in, not
a tagged build or qualification mode.

Stage the binary, the **same source tree**, and the entire passed preparation
directory onto Linux amd64. Measurement requires `--build-capture`. Before any
workload, it verifies a passed preparation summary, identical before/after source
manifests, and matching before/after binary SHA-256 values. Selected sources
include untracked `.go`, `.mjs`, `.sh`, `.c`, `go.mod` and `go.sum` files, not
Markdown. Preserve the JSON manifests' relative source paths and the preparation
directory's artifact layout. The build binary's original absolute path remains
in the `-o` argument of `build-go.exit.json`; do not rewrite this provenance.
Binding uses binary SHA-256, not the original absolute path, so a relocated user
copy of the binary and source tree is supported. Measurement records supplied
paths in `build-binding.json` and rechecks source/binary stability at finalization.

Linux runs require at least ten online processors and pin
`GOMAXPROCS=10 GOGC=100 GOMEMLIMIT=off`. Affinity is recorded from
`/proc/self/status`, but CPU quota is not currently captured. Online CPU count
alone does not prove ten CPUs are available. Do not tune library code to this
host. Use a separate physical client host from OSD hosts where possible; record
co-location, competing workloads and limits rather than assuming isolation.

On Linux, add `--build-native` to preparation to compile the existing native
driver with `cc -std=c11 -Wall -Wextra -Werror -O2 -pthread ... -ldl`. Execution
requires the externally installed librados runtime; pin the intended release
and library hash before measurement. No Docker or cluster setup is performed.
The native ABI is `native-benchmark CONF KEYRING POOL secure [ENTITY]`.
The optional entity defaults to `client.p07`. Native diagnostic mode accepts
`P07_OPERATIONS_PER_WORKER=256..4096`, default 256. Match the Go count explicitly
for direct invocation; the live wrapper's closed-loop context uses defaults.
The reported `library_api_version` is not the Ceph release version.

## Dedicated Pool And Seeds

The operator must create a dedicated pool and provision a credential scoped to
it, outside this wrapper. Both drivers default to `client.p07`; use `--entity`
to select another authorized client for Go and native closed-loop work.
Keep credentials and native private configuration **outside the
workspace**; in-workspace paths and external aliases resolving there are rejected.
Use a key **file** or Ceph keyring for Go and a Ceph keyring for native; do not send secret
values through chat or command arguments. No key content or key digest is
captured. Native config and keyring paths are redacted too.

Default measurements are read-only and require the preseeded 64 KiB objects
`p07-shared-read-0` through `p07-shared-read-15` with the benchmark's deterministic
payloads. Go validates every read's length and payload. Missing or mismatching
seeds fail, not silently reseed. `--seed` is the only write opt-in; it warns and
writes those sixteen named objects before measurement, without deleting objects
or creating pools. Do not use it in a shared pool.

## Run On The External Host

Set these local variables in the host's terminal. The paths must be absolute;
the capture paths must not exist. The monitor and FSID values must identify the
operator's external cluster. No host access is assumed here.

```sh
BIN=/absolute/staged/build_linuxamd64
BUILD_CAPTURE=/absolute/staged/rados-go-linux-amd64-prepared-fresh
GOTOOLCHAIN=go1.27.1
export GOTOOLCHAIN
MONITORS=192.0.2.10:3300,192.0.2.11:3300
FSID=00000000-0000-0000-0000-000000000001
KEYFILE=/absolute/private/client.p07.key
POOL=dedicated-p07

# Explicit initial seeding plus primary measurements; omit --seed thereafter.
sh integration/p07/live-cluster.sh --capture /absolute/fresh/p07-primary \
  --build-capture "$BUILD_CAPTURE" --binary "$BIN" \
  --monitors "$MONITORS" --fsid "$FSID" \
  --key-file "$KEYFILE" --pool "$POOL" --seed
primary_exit=$?
printf 'primary exit=%s; inspect /absolute/fresh/p07-primary/summary.json\n' "$primary_exit"

# Separate instrumented repetitions; no reseeding.
sh integration/p07/live-cluster.sh --capture /absolute/fresh/p07-observed \
  --build-capture "$BUILD_CAPTURE" --binary "$BIN" \
  --monitors "$MONITORS" --fsid "$FSID" \
  --key-file "$KEYFILE" --pool "$POOL" --observe
observed_exit=$?
printf 'observed exit=%s; inspect /absolute/fresh/p07-observed/summary.json\n' "$observed_exit"
```

Run these examples in an operator shell without `errexit`. Retain each exit
status and inspect its summary before interpreting results; do not use a blind
`&&` that skips later independent evidence on failure. Use `--seed` only for
explicit initialization of the dedicated pool, then omit it. Default execution
is read-only. Run commands serially, after current live work finishes.

Defaults are three repetitions of each factorial case `none,cpu,alloc,both`,
secure transport, 64 KiB reads, sixteen read workers, and 1000 offered ops/s.
`--repetitions`, `--cases` and `--rate 1000|2000|4000` are explicit controls.
The wrapper runs the existing binary directly with the factorial environment:
`P07_OFFERED_LOAD=1 P07_OFFERED_FACTORIAL=1 P07_OFFERED_CASE=...`, eight legacy
background-worker configuration slots, and fixed allocation rate 100. Cases
control actual CPU/allocation worker roles: `none` has neither, `cpu` has eight
CPU workers, `alloc` has eight allocation workers, and `both` has eight of each
(sixteen background goroutines, not eight combined workers). CPU workers hash
through warmup, issuance and drain; allocation workers wait on absolute timers
and allocate only during the eight-second issuance window. The allocation target
is 800 allocations/s at 64 KiB each, or 50 MiB/s, independent of read rate.
Eight retention slots per allocation worker bound synthetic retained payload to
4 MiB (8 workers x 8 slots x 64 KiB), excluding metadata and other process memory.
Check completed/missed allocations and lateness; targets are not guaranteed
delivered load. Only instrumented repetitions add observation variables.
Inherited benchmark/profile variables are not
passed through. Primary and instrumented work must use identical source,
runtime settings, object set, load rate and case selection, but their latency
samples must remain separate because instrumentation adds overhead.

For a serial primary sweep, use fresh directories for every rate and retain
failures while continuing independent captures. Run after seeding; change the
directory suffix for every new sweep:

```sh
sweep_exit=0
for rate in 1000 2000 4000; do
  capture_dir="/tmp/rados-go-linux-primary-${rate}-fresh"
  if sh integration/p07/live-cluster.sh --capture "$capture_dir" \
    --build-capture "$BUILD_CAPTURE" --binary "$BIN" \
    --monitors "$MONITORS" --fsid "$FSID" --key-file "$KEYFILE" \
    --pool "$POOL" --rate "$rate"; then
    run_exit=0
  else
    run_exit=$?
    sweep_exit=1
  fi
  printf 'rate=%s exit=%s; inspect %s/summary.json\n' "$rate" "$run_exit" "$capture_dir"
done
printf 'serial sweep aggregate exit=%s\n' "$sweep_exit"
# In an automation script, finish with: exit "$sweep_exit"
```

For a separate instrumented sweep, add `--observe` and use distinct
`/tmp/rados-go-linux-observed-${rate}-fresh` paths. Never run sweeps in parallel
or merge instrumented and primary latency samples.

Captures retain every invoked command's stdout, stderr, exit status/signal,
timestamps and allowed environment, even on benchmark failure. Failures do not
prevent later independent measurement cases from running. A failed seed skips
all reads. JSON stdout and `summary.json` expose aggregate failure. Metadata
includes uname, CPU/memory/affinity, tool versions, Go build info, source manifests
before/after, executable hashes before/after, and artifact hashes. Source changes
fail the capture. Credentials are excluded from source hashing; credential
contents are never read by the wrapper. Benchmark processes still necessarily
read their credential files. Arbitrary tool output is retained: treat captures
as private, since tool/cluster errors may disclose operational information.

## Operator Environment Evidence

The wrapper does not collect Ceph versions, cluster health/topology or cgroup
CPU quota. With an already authorized read-only operator identity and private
configuration outside the workspace, collect these before/after measurements.
No elevated privilege or provisioning command is required here; if access is
denied, record the gap rather than escalating privileges:

```sh
ceph --version
ceph versions --format json
ceph status --format json
ceph health detail --format json
ceph osd tree --format json
ceph osd df tree --format json
ceph osd pool stats "$POOL" --format json
```

Use the operator's existing private CLI configuration, not secret command-line
values. Retain tool and daemon versions; verify compatibility against the
repository's protocol/compatibility evidence and pin the Ceph release and
client/runtime versions across repetitions. Record monitor/OSD placement, health
changes and pool statistics so server activity is not mistaken for client
scheduler cost. These outputs are operationally private even without keys:
retain them in private evidence storage and review before publication. Do not
capture keyrings, `auth get` output or configuration containing secrets.

For cgroup v2, identify the benchmark process's actual cgroup using
`/proc/<benchmark-pid>/cgroup` and the host's cgroup mount mapping (for example,
`/proc/<benchmark-pid>/mountinfo`). Read `cpu.max`, `cpu.stat` and
`cpuset.cpus.effective` there, and inspect applicable ancestor limits:

```sh
# Replace <benchmark-pid> before running these operator placeholders.
cat '/proc/<benchmark-pid>/cgroup'
cat '/proc/<benchmark-pid>/mountinfo'
# Substitute the resolved process cgroup, not the mount root by assumption.
cat /actual/cgroup/path/cpu.max
cat /actual/cgroup/path/cpu.stat
cat /actual/cgroup/path/cpuset.cpus.effective
```

This is an operator check, not an automatic quota-capture feature. For cgroup
v1, retain corresponding process-controller quota/period and cpuset files.
Record before/after throttling counters and effective affinity, including
ancestor limits. Missing quota evidence remains a limitation. No host or cluster
commands were run for this documentation update.

## Trace Analysis

```sh
node integration/p07/trace-stall-summary.mjs --decode \
  /absolute/observation/trace.out /absolute/fresh/trace-debug.txt
node integration/p07/trace-stall-summary.mjs \
  /absolute/trace-debug.txt /absolute/observation/timing.json
```

The decoder probes actual successful high-level output using `-d=parsed` and
then `-d=1`. The checked local toolchain is Go 1.27.1, whose help uses named
`parsed`/`wire` modes. Raw `wire`/`-d=2` output is not interpreted as high-level
events. Retain the exact decoder and producer Go versions; an unsupported format
fails explicitly. Tests generate a real runtime trace using the selected Go
toolchain, not a guessed textual trace grammar.

Each attempted read creates a `p07/read` task and logs `p07/index`. Timing uses
the same task context with `msgr.WithRequestTiming`. Events include existing
`write_end`, `read_frame_end` and `frame_received` when present, plus
`read_enter`/`read_return`. Synchronization logs bracket trace emission in
monotonic elapsed nanoseconds; the JSON wall anchor retains UTC provenance.
The analyzer intersects these brackets and reports alignment uncertainty. It
uses BigInt for absolute trace timestamps before converting relative durations.

Output appends per-read and transport-interval durations, matching GC STW
overlap, and caller-goroutine runnable overlap. It also lists global GC STW and
runnable intervals. Failed calls can have missing transport stages; those
intervals remain absent. Duplicate stages/retries and unmatched tasks fail
validation. This is temporal correlation, **not** proof that summed durations
explain CPU use or that GC/scheduler activity caused latency. Caller runnable
time does not capture session reader/writer goroutine delay. Network/server
latency and protocol wait remain possible explanations.

## Native Comparison Gap

The current native benchmark is closed-loop. It has no absolute offered-arrival
schedule, bounded offered queue, deadline/drop accounting, or independent fixed
background factorial harness. Comparing its closed-loop p99 against Go offered
p99 would be unfair. A matched native offered-load implementation is not
present. A future native harness must match the absolute schedule, bounded
queue, deadline/drop and all-outcome accounting, warmup, independent fixed
CPU/allocation roles, object set, host constraints and failure retention before
any paired offered-load performance claim. That implementation and validation
remain a gap.

For separate closed-loop context, use the same preseeded objects and dedicated
pool, no background load, and the driver's 256 reads per worker plus eight
warmup reads. This is not an offered comparison or a qualification gate:

```sh
sh integration/p07/live-cluster.sh --capture /absolute/fresh/p07-closed \
  --build-capture "$BUILD_CAPTURE" --binary "$BIN" \
  --monitors "$MONITORS" --fsid "$FSID" \
  --key-file "$KEYFILE" --pool "$POOL" --closed-loop \
  --native-binary /absolute/staged/native-benchmark \
  --native-conf /absolute/private/ceph.conf \
  --native-keyring /absolute/private/client.p07.keyring
closed_exit=$?
printf 'closed-loop exit=%s; inspect /absolute/fresh/p07-closed/summary.json\n' "$closed_exit"
```

Use a passed `--build-native` preparation containing both matching binary hashes.
Verify native config points to the same monitors and FSID and use negotiated-mode
evidence separately before making transport claims. The wrapper does not inspect
private config or keyrings to prove cluster identity or negotiated transport.

## Deferred Checks

Do not run tests, builds or trace analysis competing with active live captures.
After they finish, the focused helper checks are:

```sh
node --test integration/p07/trace-stall-summary.test.mjs integration/p07/live-cluster.test.mjs
GOTOOLCHAIN=go1.27.1 go test -race integration/p07/benchmark/offered_observation_linux.go \
  integration/p07/benchmark/offered_observation_linux_test.go \
  integration/p07/benchmark/trace_writer.go
```

The explicit-file Go check executes the portable helper on macOS; a normal
package check on macOS intentionally excludes Linux files. Linux package tests
and real external-cluster execution remain operator checks; no external-host
result or qualification is established by this documentation update.