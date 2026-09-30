# Phase 4 Audit

Status: repeated implementation audits found no remaining concrete source
findings. This does not clear the unresolved live secure-tail gate or authorize
a completion commit. [Results](RESULTS.md) preserve the misses.

## Repaired Findings

1. Queue-only accounting did not bound allocation before owner admission. Built-in
   CRC/secure decoding now reserves before payload allocation and carries a
   unique backing lease through active decode, read-ahead, queue, and results.
2. Buffered reply/unsolicited publication could release accounting before actual
   consumption. Buffered replies retain their lease until `take`; budgeted
   unsolicited handoff is unbuffered and keeps the owner queue charged.
3. Reconnect could overlap the retired reader with its replacement. Transport
   Close closes the connection, joins decode under the read mutex, and clears
   the reader; reads after closure reject with `ErrSessionClosed`.
4. Renewal goroutines accumulated across generations before dormant timers
   fired. Generation cancellation now interrupts both waiting and forwarding;
   authentication Close does not create a false renewal by closing its signal.
5. A root constructor test accepted platform MaxInt despite control-allowance
   overflow. The supported boundary is the minimum of platform MaxInt and
   `MaxUint64 / (3 * 96)`, and the test now pins that boundary.
6. Missing lease coverage was added for stale generations, custom-codec transport
   fallback, real secure auth-wrapper handoff, and concurrent copied-carrier
   release. Error, duplicate, terminal, consumption, reconnect, and Stop paths
   are exercised.
7. Hot-path accounting initially added allocation and locking overhead. Lease
   release now uses one mutex acquisition; lease/reader and the secure prelude
   share unique per-frame storage. Reader references are cleared after decoding.
   There is no pooling or reuse of application-visible backing.

## Coverage And Limits

- `receive_budget_test.go`, `receive_budget_codec_test.go`, and
  `receive_budget_lifecycle_test.go`: capacity, physical decode backing,
  ownership transfer, failure/release, and shutdown boundaries.
- `reader_retirement_test.go`: joined Close, cleared reader, rejected later
  reads, and replacement ordering.
- `session_renewal_test.go`: 64 retired generations, blocked forwarding,
  terminal cancellation, Stop, and authentication signal meaning.
- Root `receive_budget_integration_test.go`: MON/OSD/MGR-labeled configurations
  share the public client's budget; rejection precedes connector invocation;
  no active eviction; a slot releases only after concurrent Stop joins. This
  is not a live authenticated production-factory saturation test.
- `receive_budget_stress_test.go`: 16 real secure reconnects over net.Pipe,
  stalled delivery, retired readers, dormant renewal pumps, zero final ledger,
  and unchanged application-owned payload.
- Opt-in resource captures: fake and actual IPv4 TCP loopback, CRC and secure,
  1/16/64/128 sessions, three repetitions; separate stalled 4-MiB payloads.
  Ready sessions exclude authentication and do not characterize a large live
  Ceph topology. A separate two-slot test pins admission boundaries.

The final frozen implementation passed repeated focused race tests (20), the
full repository race suite, diagnostic-tag tests, vet, and Linux ARM64 diagnostic
build. The first opt-in frozen-source capture failed solely because the snapshot
had no Git metadata; its output is retained and the explicit-metadata rerun
passed. There is no source fix hidden behind that evidence-collection repair.

## Open Exit Finding

Stable native-ratio compliance and a rigorous no-regression conclusion for
secure read p99 are not established. Committed Phase 3 also misses in some legs;
the final implementation passes one two-leg diagnostic but misses its
confirmation. Short profiles do not identify receive-budget mutex work as a
dominant cost. Existing secure backing allocation and GC remain material.

Next task: agree and execute a repeated, source-bound, alternating before/after
comparison on unchanged topology with a predefined non-regression margin and
uncertainty assessment. Preserve native denominator variation and all failures.
If it demonstrates a new regression, repair the controlling receive path and
repeat. If it identifies only the inherited tail gap, record the maintainer's
explicit disposition before closing Phase 4. Do not weaken numerical guardrails,
disable GC in library code, shrink the reader, or claim parity from one pass.