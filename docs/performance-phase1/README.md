# Performance Phase 1

Status: complete for the scoped Phase 1 exit. Five independent audits are
complete, with no outstanding concrete findings after the final pass.
Scope is F1 only, following the [Phase 1 specification](../PERFORMANCE_SCALABILITY_REVIEW.md)
and [Phase 0 qualification boundaries](../performance-phase0/README.md).
This handoff does not change the original plan, public configuration, P12 gates,
or establish native parity or release qualification. See the [audit](AUDIT.md)
and [results and published evidence](RESULTS.md).

## Regression And Implementation

The initial deterministic count/byte saturation cases in
[backoff_control_test.go](../../internal/objecter/backoff_control_test.go)
were reported as expected failures before implementation: a required backoff ACK
used ordinary admission and stopped the wrapper when application budgets were
full. The discriminating result is ACK progress with the unrelated request still
pending, followed by its successful reply. Explicit fake-transport gates avoid
sleep-based scheduling. The final source-bound capture and focused passing
output are published in the [results](RESULTS.md); the original expected-failure
observation is regression history, not a failure reproduced on the final source.

The current [messenger control lane](../../internal/msgr/session_control.go)
and [session owner](../../internal/msgr/session.go) implement:

- Fixed internal reserves of 16 messages and 1 MiB logical retained bytes per
  session, additional to application budgets; accounting includes message-header
  bytes. `SessionSnapshot.ControlQueued` includes active controls, and
  `ControlRetainedBytes` reports their logical retained charge.
- Only version-1 OSD backoff ACKs enter this lane, without middle/data payloads.
  Controls do not consume application reply/in-flight slots. `SendControl`
  waits synchronously for transport write completion, not peer acknowledgment;
  cancellation after writing starts can return `ErrOutcomeUnknown`.
- Controls may overtake admitted but unsequenced application requests (`seq == 0`),
  never older sequence-assigned replay. The no-control selection fast path stays
  in admission order. The same authenticated transport and single writer retain
  sequence, nonce/counter, and encryption handling; no second wire channel exists.
- Generic completed controls are not replayed; incomplete generic `SendControl`
  work retains legacy replay behavior. Wrapper ACKs use
  `SendControlGeneration` and the authoritative, non-wire incoming
  `Message.TransportGeneration` stamp in [message.go](../../internal/msgr/message.go).
  Fault, renewal, partial/full reset, terminal failure, and Stop invalidate scoped
  controls. Obsolete ACKs must not replay on a new transport; internal
  `ErrControlInvalidated` is not a mutation-unknown outcome.

The [OSD wrapper](../../internal/objecter/osd_session.go) registers a block before
enqueueing its ACK under `mu`, maintaining registration-versus-admission exclusion
through `SubmitAdmitted`. Its FIFO worker clones ACK storage and bounds active plus
queued work to the same 16 messages/1 MiB including headers. Deadlines start at
enqueue, not dequeue. The receive dispatcher remains able to process overlapping
unblocks while writes wait. Generation reconciliation rejects stale input before
decode/capacity use and ignores obsolete completion failures; delayed duplicate
reset signals do not discard fresh ACKs. The messenger also drops an old incoming
unsolicited message if an inline fault advances its generation, rather than
restamping it. An already-matched application reply is delivered and its request
released BEFORE `queueControl` can fault and trigger that generation guard.
`TestSessionInlineFaultPreservesMatchedReply` protects this ordering; unsolicited
stale-input handling is unchanged.

Stop cancels the worker context and stops raw transport; dispatcher teardown joins
the ACK worker before closing notifications and `dispatcherDone`, which Stop
waits for. Custom raw transports optionally supply the scoped interface or
`SendControl`; otherwise the worker falls back to `Send`. Such transports must
honor context cancellation for deadlines and joining to work. The built-in reserve
guarantee is not a guarantee for an arbitrary ordinary-Send fallback.

Ordinary application saturation alone is not fail-stop. True control exhaustion,
ACK deadline failure, or a permanently stalled writer still requires fail-stop
for protocol correctness. The wrapper strips the ACK's `ErrOutcomeUnknown`
classification before reporting its failure; unrelated mutations get their own
started/not-started classification from the raw messenger, not from the ACK.

## Resource And Native Limits

Both wrapper and messenger can retain logical ACK copies: budget up to 2 MiB
across the two layers, plus one encoded frame (single control maximum at most
1 MiB logical charge), codec wire storage, metadata, queue backing storage, and
deadline machinery. There is one extra ACK-worker goroutine per OSD session.
These are logical accounting bounds, not a 1 MiB heap/RSS bound or a client-wide
memory bound. Socket progress is not guaranteed against a stalled peer/writer.

Native reference: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`,
[Objecter.cc](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.cc#L3835),
SHA-256 `ef60c3e95571be0dfb7ff83e7cbc6ae7d7f1119f4f2ee26443bfbd44475f057d`.
`handle_osd_backoff` at lines 3835 onward registers before ACK, preserves the
original map epoch and ACK priority (the default application priority), and submits
through `con->send_message` outside application operation-budget acquisition.
Go's prioritization of unsequenced controls is not proven equivalent to native
messenger scheduling and does not prove stall-free behavior. Source observations
are not native runtime performance evidence.

## Validation And Reproduction

Final validation passed on the final implementation source: ordinary `./...`,
race `./...`, `p12diagnostics` `./...`, vet `./...`, and the Linux ARM64
`benchmarkdiagnostic` build. Repeated focused control race checks passed at count
20; earlier touched-package checks passed at count 20. The fifth independent
review found no concrete findings, with 87 editor tests and focused race count 2
passing. These completed checks are recorded in the [audit](AUDIT.md).

Commands below reproduce the checks; they are not new executions by this
documentation-only finalization.
Run from the repository root, sequentially with no shared-terminal agent activity:

```sh
GOTOOLCHAIN=go1.27.1 go test ./internal/msgr ./internal/objecter \
  -run 'TestSessionInlineFault|TestSessionControl|TestSessionScopedControl|TestSessionDispatchSelection|TestBackoffControl' -count=1
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./internal/msgr ./internal/objecter \
  -run 'TestSessionInlineFault|TestSessionControl|TestSessionScopedControl|TestSessionDispatchSelection|TestBackoffControl' -count=20
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./internal/msgr ./internal/objecter
GOTOOLCHAIN=go1.27.1 go test ./...
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./...
GOTOOLCHAIN=go1.27.1 go test -tags p12diagnostics ./...
GOTOOLCHAIN=go1.27.1 go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=go1.27.1 \
  go build -tags benchmarkdiagnostic ./...
GOTOOLCHAIN=go1.27.1 go test ./internal/msgr -run '^$' \
  -bench '^BenchmarkSessionDispatchSelection$' -benchmem -benchtime=100ms -count=5
GOTOOLCHAIN=go1.27.1 go run ./tools/perf-baseline \
  -out /tmp/rados-go-phase1-baseline-final-20260930 -benchtime 100ms -count 5
```

Use a fresh, nonexisting absolute capture directory. The existing
[baseline runner](../../tools/perf-baseline/main.go) and
[capture support](../../internal/perfbaseline/collector.go) bind raw measurements
to the source manifest/diff, command exits, toolchain/environment, inputs, limits,
and artifact hashes. Capture prerequisites (including required tools and
historical inputs), deterministic checks, exact benchmark counts, and unchanged
source verification are mandatory; the runner exercised them in the final
passing capture. Preserve its inherited 37-cell suite and Phase 0 scopes:
placement/incrementals, queue batches, CRC/secure submission/replay fixtures, and
idle connection lifecycles. Secure fixture encoding is not negotiated live secure
mode; retain Phase 0's separate matched-secure mode evidence and CRC rejection.
Fixture lifecycle timings exclude sockets/authentication/OSD processing; idle
heap estimates are not RSS. Do not conflate dispatcher ns/op with entire-client
latency. Frozen no-control selection samples span 2.043-2.256 ns/op across all
three depths; they do not establish whole-client latency or unchanged performance.

The [published inventory](evidence/SHA256SUMS) covers 13 compact evidence files.
The [baseline manifest](evidence/baseline/manifest.json) records 185 after samples,
zero failures, and unchanged captured source. The
[comparison metadata](evidence/comparison/comparison.json) binds 185 before
samples from the `3b8be0ceaa8a4ba6042df0b26b1ce6f15a640418` archive and 30 new
dispatcher samples to the same host and five repetitions. See [RESULTS.md](RESULTS.md)
for medians, exact capture directories, exporter omissions, and source limits.
The manifest certifies the exact frozen dirty snapshot, not a later final commit;
these documentation edits change no measured code. Historical Phase 0 files
remain pinned and unrewritten. The
[P13 live harness](../../integration/p13/reproduce.sh) is optional additional
native/live coverage, not mandated for this deterministic Phase 1 exit. No live
Ceph rerun, P12/P13 recertification, or parity claim is made. Phase 2 is the next
phase; it is not implemented by this finalization.