# Phase 1 Audit And Handoff

Status: complete for the scoped Phase 1 exit. Five independent audits completed;
the fifth final pass found no outstanding concrete findings.
The [implementation handoff](README.md) supplies contracts, limits, provenance,
measurement scopes, and reproduction commands. Only F1 in the
[original specification](../PERFORMANCE_SCALABILITY_REVIEW.md) is in scope.

## Iterative Audit History

| Cycle | Findings | Resolution |
| --- | --- | --- |
| 1 | Two high: stale ACK failure selected ahead of reset; obsolete ACK replay across transport reset | Reset-aware completion handling and scoped invalidation implemented; deterministic regressions pass. |
| 2 | Two high: delayed reset discards fresh ACK; stale incoming consumes capacity. Dispatch selection also adds no-control overhead. | Authoritative generation reconciliation and early stale rejection implemented; no-control fast path restored and measured. |
| 3 | One: inline transport-control queue fault restamps old incoming with current generation | Messenger detects generation change and drops the old message; `TestSessionInlineFaultDoesNotRestampIncoming` added. Focused race repetition reported passing. |
| 4 | Matched application reply lost by the previous inline-fault guard | Matched reply completion moved BEFORE `queueControl` and its generation guard; `TestSessionInlineFaultPreservesMatchedReply` passes. Unsolicited stale-input behavior is unchanged. |
| 5 | No concrete findings | Final independent review completed; 87 editor tests and focused race count 2 passed. |

Final implementation validation passed: ordinary `go test ./...`, race
`go test -race ./...`, `go test -tags p12diagnostics ./...`, `go vet ./...`, and
the Linux ARM64 `benchmarkdiagnostic` build. Repeated focused messenger/objecter
control race checks passed at count 20, and earlier touched-package checks passed
at count 20. Final source-bound capture passed with 37 cells times five
repetitions (185 after samples), zero failures, and source unchanged throughout
capture. Same-host comparison has exactly 185 before samples plus 30 new
dispatcher samples. The compact publication contains 13 files plus its
[checksum inventory](evidence/SHA256SUMS), with all inventory hashes verified.
See [RESULTS.md](RESULTS.md) for evidence and source-binding limits. This
documentation-only finalization validates local links, test names, ASCII,
fences, and checksums; it does not rerun the implementation suites.

## Coverage Map

Exact names can be inspected without executing tests:

```sh
rg -n '^func (Test|Benchmark)' internal/msgr/session_control_test.go internal/objecter/backoff_control_test.go
```

Messenger coverage lives in
[session_control_test.go](../../internal/msgr/session_control_test.go):

| Contract | Exact test names |
| --- | --- |
| Separate application/control count, bytes, reply slots | `TestSessionControlBypassesApplicationBudgets`, `TestSessionControlReserves` |
| Ownership, priority, no-control path, assigned replay ordering | `TestSessionControlOwnershipAndPriority`, `TestSessionDispatchSelectionNoControls`, `TestSessionDispatchSelectionControls`, `TestSessionDispatchSelectionSaturatedReplay`, `TestSessionControlReconnectSequenceOrder` |
| Write completion, generic cancellation/unknown, completed-write replay | `TestSessionControlStartedCancellationAndStop`, `TestSessionControlRequiresWriteCompletionDespiteMatchingTID`, `TestSessionControlCompletedWriteIsNotReplayed`, `TestSessionControlReplayCancellationUnknown` |
| Restricted lane, terminal/reset cleanup, sequence exhaustion | `TestSessionControlRejectsUnrestrictedBypass`, `TestSessionControlTerminalAndResetRelease`, `TestSessionControlSequenceOverflowTerminal` |
| Scoped fault/renewal/reset/Stop, metadata and sequence gaps | `TestSessionScopedControlBoundaries`, `TestSessionControlGenerationMetadata`, `TestSessionScopedControlReconnectSequenceGap` |
| Inline fault must not restamp old incoming | `TestSessionInlineFaultDoesNotRestampIncoming` |
| Matched application reply delivered before inline ACK-queue fault guard | `TestSessionInlineFaultPreservesMatchedReply` |

OSD wrapper coverage lives in
[backoff_control_test.go](../../internal/objecter/backoff_control_test.go):

| Contract | Exact test names |
| --- | --- |
| Initial count/byte regression, unrelated reply survives | `TestBackoffControlProgressUnderApplicationSaturation` (`count`, `bytes`) |
| FIFO original ACK encoding and overlapping unblocks during stall | `TestBackoffControlStalledWriterProcessesOverlappingUnblocks` |
| Active plus queued reserves, enqueue deadline, fail-stop and join | `TestBackoffControlReserveIncludesActiveAndQueued`, `TestBackoffControlQueuedDeadlineStartsAtEnqueue`, `TestBackoffControlStalledWriterTimeoutAndStop` |
| Reset cancels obsolete ACK; no ACK crosses owner reset | `TestBackoffControlResetCancelsOldGeneration`, `TestBackoffControlIntegratedNoACKAcrossOwnerReset` |
| Stale deadline/fallback error selected before pending reset | `TestBackoffControlIntegratedStaleDeadlineAheadOfReset`, `TestBackoffControlFallbackPendingResetBeforeError` |
| Initial authoritative generation, delayed duplicate reset | `TestBackoffControlAuthoritativeInitialGeneration`, `TestBackoffControlAuthoritativeDelayedDuplicateReset` |
| Stale block capacity, reused unblock ID, pre-decode rejection | `TestBackoffControlAuthoritativeStaleBlockFullReserve`, `TestBackoffControlAuthoritativeStaleUnblockReusedID`, `TestBackoffControlAuthoritativeStaleRejectedBeforeDecode` |
| Generation advances during registration | `TestBackoffControlAuthoritativeAdvanceDuringRegistration` |

`BenchmarkSessionDispatchSelection` in the messenger test file isolates selection
with/without controls at depths 64/1024/4096. It does not exercise authentication,
transport writes, ACK latency, or full-client throughput. Existing registration
and admission behavior in [osd_session_test.go](../../internal/objecter/osd_session_test.go)
is covered by the passing touched-package and full-repository race suites.

## Exit Checklist

- Implemented: bounded internal reserves, no application reply slots, ordered
  replay-safe selection, FIFO wrapper handoff, enqueue deadlines, context-based
  shutdown/join, authoritative generation filtering, independent mutation outcomes.
- Passed: initial regression failed as expected before implementation; focused
  control race count 20 and earlier touched-package count 20 checks passed.
- Passed: final ordinary, race, diagnostic-tag, vet, and Linux ARM64 build checks,
  including existing admission/backoff lifecycle coverage.
- Complete: five independent audits, all concrete findings repaired and
  revalidated; fifth final pass has no outstanding concrete findings.
- Passed: source-bound baseline and same-host before/after comparison, dispatcher
  repetitions, and unchanged inherited Phase 0 workload scopes.
- Verified: published artifact hashes, passing statuses, effective limits,
  source binding, and pinned historical inputs. Historical inputs are provenance,
  not newly rerun integration qualification.

Deterministic fake transports suffice for this scoped exit. Live/native P13
is optional, and earlier harness failures are not passes. Neither microbenchmarks
nor optional integration coverage establishes parity or release certification.
Phase 1 is complete within these boundaries. Phase 2 is next; this task does not
start it or modify the original review, library code, configuration, or evidence.