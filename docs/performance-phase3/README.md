# Phase 3: Isolate Session Creation And Replacement

Status: complete within the Phase 3 F3/F9 scope after iterative implementation,
regression repairs and three independent implementation audit rounds. The final
round found no remaining concrete findings. See [audit and coverage](AUDIT.md)
and [results and source-bound evidence](RESULTS.md). Phase 4 is not implemented.

## Objecter Contract

Creation is coordinated per OSD ID. A pending attempt owns retirement of the old
session, the factory call, cleanup and result publication. Stop and factory work
run outside the client-wide mutex; unrelated cached OSDs remain accessible while
either operation is gated. The same target shares one attempt and its success or
error. A differing observed address/generation supersedes pending work rather
than installing a stale result. Same-OSD replacements are serialized; unrelated
OSDs can create independently.

Acquisition waiters have independent contexts. Read and command acquisition use
the cancelable helper; an individual cancellation does not stop another caller's
shared creation. The OSD factory API has no context argument, so even abandoned
work must be joined, not detached. A successful factory may populate the cache
after all callers cancel if its target remains current; Close still owns it.

Installation rechecks closure, usable current OSD state/address, generation and
supersession. Authoritative observations revoke pending work even when a stale
request returns early. Map-watcher invalidation also revokes pending creations.
Empty and unsupported address vectors on known OSDs are unavailable, not a
fallback to stale caller addresses. Completed waiters recheck cancellation,
closed state and target identity. Partial results are stopped on failure; nil
success results are rejected. Read/command paths reroute on `ErrStaleMap` within
their existing attempt/context bounds.

Initial availability still means session installation, not completed transport
authentication. Its callback precedes creation result publication and startup
of notification dispatch. Invalidation waits for that publication, preserving
availability-before-invalidation ordering without holding the global mutex.
Production transport reset/recovery callbacks retain their existing meaning and
may run before installation; this phase does not redefine them as ownership-
filtered events. Watch interruption remains after actual replacement teardown
and factory completion, as before; initial cold creation does not interrupt new
watches. Retired dispatchers cannot deliver watch events or notify completions
as the current installed owner.

## Manager Contract

Concurrent misses for the same active manager share one creation attempt. Its
context preserves initiating caller values but is detached from that caller's
cancellation and deadline. Waiters keep their own cancellation. The factory is
canceled when no waiters remain, the target is superseded, Close begins or the
attempt finishes. One canceled caller cannot cancel work needed by others.

Authoritative target observations abandon old attempts before any stale-request,
unavailable-target or cache return. An observed A-to-B-to-A transition cannot
revive A1: restored A starts fresh A2, and late A1 is stopped. Installation
rechecks current identity and closure. GID/name/address define target identity;
epoch-only changes preserve existing session reuse semantics. Factory failures
fan out to current waiters and subsequent calls may retry. Partial, losing and
replaced resources are stopped outside the global lock.

## Shutdown And Limits

Creation and invalidation workers register under the mutex before ownership is
released. Close prevents new admission, clears installed ownership, cancels
manager attempts, stops cached resources and joins tracked factories, loser
cleanup, retirement and dispatch workers. Concurrent Close callers join the same
completion. No late resource is installed after closure.

A factory, Stop method or observer that never returns can necessarily block
Close. The OSD factory cannot be canceled through its API; manager factories must
cooperate with context cancellation for prompt shutdown. Joining every created
resource does not imply a fixed shutdown deadline. External map/generation reads
are not an atomic snapshot and cannot reveal an entirely unobserved intermediate
target. Concurrent invalidation after acquisition can still stop a returned
session, as with the previous cached fast path. These are not new guarantees.

This phase does not bound aggregate connections, receive memory or target-change
fanout. Those remain Phase 4 work requiring separate measurements and approval
before default-policy changes. No live latency, native parity or renewed
P12/P13 qualification claim is made.

## Reproduction

Run from the repository root with Go 1.27.1:

```sh
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race \
  ./internal/objecter ./internal/mgr -run '^TestSession' -count=20 -timeout=180s
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./...
GOTOOLCHAIN=go1.27.1 go test -tags p12diagnostics ./...
GOTOOLCHAIN=go1.27.1 go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=go1.27.1 \
  go build -tags p12diagnostics -o /tmp/rados-go-phase3-linux-check \
  ./integration/p07/benchmark
GOTOOLCHAIN=go1.27.1 go run ./tools/perf-baseline \
  -out /tmp/rados-go-phase3-fresh-capture -benchtime 100ms -count 5
git diff --check
```

The capture requires a fresh directory and freezes its own source. For before
measurements, archive Phase 2 commit
`190ce58132d9ac841be226f69574c1b8983c5b2f` into a separate fresh directory and
run `go test ./internal/maps ./internal/msgr -run '^$' -bench
'^BenchmarkPerformance' -benchmem -benchtime=100ms -count=5` there. Match the
[recorded environment](evidence/comparison/comparison.json), host and toolchain;
run timing serially and preserve raw outputs and hashes. A new capture binds its
own source, not a historical publication or a future commit.