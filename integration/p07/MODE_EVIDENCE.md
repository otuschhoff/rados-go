# Phase0 Negotiated Mode Evidence

This opt-in diagnostic is independent of strict P07 report schemas, historical
reports, and certification. Requested configuration is never actual-mode evidence.
It does not change Go's secure-first CRC allowance or native mode selection.

## Isolated LIVE Capture

With Docker available, invoke the existing disposable-cluster harness from the
repository root. The output path must be absolute, must not exist (including a
dangling symlink), and must have an existing parent directory:

```sh
P07_MODE_DIAGNOSTIC_DIR=/tmp/rados-go-phase0-live-20260930 \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

Preflight runs before Docker discovery. This mode cannot be combined with the
read or resource diagnostic selectors. It creates a mode-0700 directory and
uses a private umask for logs and artifacts. Do not publish raw native logs or
stderr, or move them into documentation. The historical P07 report and
performance-phase0 documentation are untouched.

Before Docker discovery or any workload build, capture freezes all Git-tracked
regular files plus non-ignored untracked `.go` files, `go.sum`, and this audit
document. The current tracked repository is small (about 4 MiB), so no historical
evidence exclusions are needed. Selection uses `git ls-files -co
--exclude-standard -z`, not recursive directory archiving. The capture subtree is
excluded even when it is inside the checkout. Ignored untracked files are never
selected; symlinks, submodules, and missing selected files fail closed. Keep secrets
out of Git-tracked files and non-ignored Go source. The archive contains only this
selected source set, never the disposable cluster directory or generated keyrings.

`source-files.nul` is the sorted, NUL-delimited selection. The sorted canonical
`source-manifest.json` records SHA256 from `shasum -a 256` with byte-exact filenames
encoded as `path_hex`, allowing spaces and newlines without ambiguous records.
`workload-sources.tar.gz` is created with `tar --null -T`, extracted into a private
temporary `source` directory, and checked against a pre-archive manifest before any
build. All mode-capture builds and source-relative reads run there, including the
probe, native driver, validator, and qualification image JSON. `go.sum` is included
when Git-selected; mode builds use readonly modules and disable ambient workspace,
Go environment-file, and GOFLAGS overrides (including race, coverage, and custom
build flags). `inherited-go.env.json` records inherited `GOFLAGS`, `GOENV`,
`GOEXPERIMENT`, `GOWORK`, and `GOTOOLCHAIN` before normalization, with null for
unset variables. Nonempty `GOEXPERIMENT` is rejected at `toolchain-preflight`,
before source freezing, Go commands, or Docker discovery; setup stderr and final
status are preserved. Accepted builds explicitly export `GOEXPERIMENT=''`,
`GOFLAGS=-mod=readonly`, `GOENV=off`, and `GOWORK=off`; `GOTOOLCHAIN` is retained.
`go.env.json` records these effective settings, including `GOEXPERIMENT`.
External module cache contents are
not archived. The ordinary report-producing branch continues to use the original
checkout and report path.

`source-binding.sha256` binds the frozen manifest and archive before builds.
`compile-commands.jsonl` records each compiler invocation as exact argv and cwd,
including cross-build environment assignments and the native container compiler
script. `go.version`, `go.env.json` (only nonsecret toolchain/build settings),
`docker.version` (server version and Git
revision), `image.reference`, `image.index`, and `platform` retain build/runtime
provenance. `artifacts.sha256` records the archive, manifest, and all produced
executables, including the probe and native driver. These are local provenance
records, not authenticated attestations.

One secure Go seed process initializes the shared read objects and records its
own observer sidecar. Separate Go/native read processes then request secure and
CRC, each with 128 warmup and 4,096 measured reads. The Linux validator runs in
the probe mount and writes normalized native observations before pair validation.
Logging and observation make these mode diagnostics, not timing baselines or
benchmark parity claims.

`status-secure.json`, `status-crc.json`, and `status.json` record workload and
validator exits, expected and observed outcomes, timestamps, Ceph version,
qualification image digest source, and the pinned parser source. Secure is
expected to pass; CRC is expected to fail with Go's requested-CRC/actual-secure
mismatch for every monitor and OSD connection, native secure evidence for every
monitor connection, and native CRC evidence for every OSD connection. Both
services must be present in each sidecar, with the correct evidence source on
every connection; missing or mixed per-service modes fail classification. This
only classifies an expected rejection: strict pair validation still rejects the
CRC pair, including native requested-CRC/actual-secure monitors.
Neither actual mode is assumed: both are
determined from the live sidecars. Unexpected validation outcomes or workload
failures make the harness exit nonzero after preserving both modes' artifacts.

The local shell regression uses sample sidecars and does not invoke Docker or
run a live capture:

```sh
./integration/p07/mode_capture_test.sh
```

Outputs also retain each workload's stdout JSON and stderr, validator stdout and
stderr, private `native-<mode>.log`, normalized `native-<mode>-modes.json`,
`go-<mode>-modes.json`, seed stdout/stderr/status/sidecar, setup stdout/stderr,
matching binaries, a workload source archive and hashes, Git identity/status,
and librados path/package/hash. Seed or setup failure stops the run; such an
incomplete capture must not be treated as a valid mode pair.

An EXIT finalizer is installed as soon as the private capture directory exists;
HUP, INT, and TERM route through it with nonzero signal exit codes. It preserves
available binaries and nonsecret `native-seed.json`, hashes artifacts, and compares
both a fresh Git selection and SHA256 manifest of the original checkout against
the frozen snapshot. It also rechecks the extracted build sources and the
pre-build archive/manifest binding. Source drift or a failed final audit makes the run fail.
`source-files-after.nul` and `source-manifest-after.json` retain that comparison.
Already-produced seed/mode sidecars and native logs are retained on interrupted
runs through an explicit nonsecret artifact allowlist, not a cluster-directory copy.
`final-status.json` always records start/end timestamps, last stage, exit code,
failed count, and source-check result, including early toolchain or Docker setup
failure; these fields also update `status.json`. If setup never reaches mode
summary, `status.json` contains only finalizer status, not a valid mode pair.
Failures before a fresh directory can be created cannot leave capture artifacts.
Setup stdout/stderr are captured from the start; credential-generation command
output is suppressed rather than copied to these logs. Raw workload/native logs
remain private. SIGKILL, host loss, or unwritable storage cannot be finalized.

## Capture On An Existing Linux Ceph Environment

Use an existing cluster, pool, and client.p07 credentials with the normal P07
permissions. No container is needed. The native benchmark requires librados.so.2.
Set MONITORS, KEY_FILE (raw CephX key file), FSID, POOL, CEPH_CONF, and KEYRING
in the shell. Do not print keys or keyring contents. From the repository root:

```sh
set -eu
umask 077
capture=$(mktemp -d "${TMPDIR:-/tmp}/p07-mode-evidence.XXXXXX")
go build -o "$capture/go-benchmark" ./integration/p07/benchmark
go build -o "$capture/perf-mode-check" ./tools/perf-mode-check
cc -O2 -pthread integration/p07/native_benchmark.c -ldl -o "$capture/native-benchmark"
P07_MODE_EVIDENCE_FILE="$capture/go-modes.json" \
  "$capture/go-benchmark" -monitors "$MONITORS" -key-file "$KEY_FILE" \
  -fsid "$FSID" -pool "$POOL" -transport secure > "$capture/go.json"
P07_NATIVE_MODE_LOG="$capture/native-ms.log" \
  "$capture/native-benchmark" "$CEPH_CONF" "$KEYRING" "$POOL" secure \
  > "$capture/native.json"
"$capture/perf-mode-check" -go "$capture/go-modes.json" \
  -native-log "$capture/native-ms.log" -requested secure \
  -native-out "$capture/native-modes.json"
printf 'Capture directory: %s\n' "$capture"
```

For a CRC claim, use fresh paths and change both benchmark requests and
`-requested` to `crc`. Go can negotiate secure under its CRC allowance; that
requested/actual mismatch MUST fail validation. Do not relabel it as CRC.

## Evidence Sources

Go's internal context observer is copied into the monitor and OSD session
configurations. Each successful authenticated transport handoff, including
reconnects, emits only service, service ID, and the returned transport's
`AuthMetadata.Mode` through `NegotiatedMode`. The collector assigns a unique
connection-attempt identifier. Missing metadata becomes `unknown`. The sidecar
is created exclusively with mode 0600, and snapshotted after client shutdown.
The ordinary benchmark JSON is unchanged. The observer must be installed on the
first Connect call; it is not a retroactive snapshot of an already connected client.

Native capture configures `debug_ms=1/1`, `debug_auth=0/0`, and a fresh private log
before connect. Actual mode comes from the ProtocolV2 `ready entity=mon.*` or
`ready entity=osd.*` line, NOT from `ms_client_mode`, preferred/allowed modes,
or the prefix of `handle_auth_done` (which can still contain the old mode).
In Ceph commit `7f793731f1b39eb4f465e960113d2363c311b964`
[`src/msg/async/ProtocolV2.cc`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/msg/async/ProtocolV2.cc), `handle_auth_done` assigns
`auth_meta->con_mode` from decoded AUTH_DONE; signature verification precedes
`ready()`. `_conn_prefix` prints `ceph_con_mode_name(auth_meta->con_mode)` and
`ready()` prints peer identity. Debug level 1 captures these post-authentication
records without auth payloads, keys, or signature dumps. The parser extracts
only service, connection/protocol pointer identifiers plus line number, and mode;
it never copies full log lines, addresses, cookies, or credentials into JSON.
Use a normal low-debug configuration for all other Ceph subsystems, and treat
the raw log as private diagnostic material, not a publishable sanitized artifact.

## Fail-Closed Validation And Limits

`tools/perf-mode-check` accepts `-go FILE` plus either `-native FILE` or
`-native-log FILE -requested secure|crc`. `-native-out FILE` preserves normalized
native observations even if a mode claim fails. It rejects unknown modes,
requested/actual mismatches, mixed reconnect modes, missing monitor/OSD evidence,
duplicate identities, unsupported sources, and mixed Go/native requests.
JSON is strictly decoded; unknown fields and trailing JSON are rejected.

The native parser currently supports the Ceph ProtocolV2 ready-prefix format
verified against commit `7f793731f1b39eb4f465e960113d2363c311b964`.
The parser regression fixture includes that commit's complete prefix fields,
including compression handlers. Unsupported formats or absent records fail closed;
there is no configuration fallback. This is a local capture/provenance convention,
not cryptographic attestation: externally fabricated JSON or truncated logs cannot
be authenticated by the validator. Preserve fresh complete per-process raw logs,
benchmark exit status, binary/library versions, and run provenance alongside the
sidecars. A failed workload or failed capture is not a valid benchmark pair even
if the recorded mode subset matches. Logging/observation adds diagnostic overhead;
these runs are not uninstrumented timing baselines. No live negotiation was
performed as part of implementing this diagnostic.