# Caller-buffer reads without global runtime tuning

Status: original design, followed by a limited secure implementation of
ObjectRef.ReadInto. See [current implementation and results](READ_INTO_RESULTS.md)
for exact scope and remaining validation gaps. Command batching was rejected.
The staged design below remains a broader validation target, not a claim that
all measurement and qualification gates have passed.

## Verified controlling path

Ordinary ObjectRef.Read already selects the owned OSD reply decoder and returns
the operation's data view without a second payload copy. SecureCodec.Read opens
AES-GCM records in place. The remaining large allocation is the authenticated
remaining-segments record backing, allocated once per frame. A ReadInto wrapper
that just calls Read and copies its result adds work without removing this
allocation. Changing the small segment-zero reconstruction is not a solution to
the dominant 64 KiB allocation volume.

## Proposed contract

Add ObjectRef.ReadInto(ctx, offset, destination) returning the byte count,
ObjectInfo and error. The requested length is len(destination); a successful
short read changes only destination[:n]. Validate limits before admission.
No goroutine may write into destination after the method returns, including
cancellation, shutdown, reconnect and replay paths. On validation/authentication
failure, leave destination unchanged. Document whether a server error also
leaves it unchanged; the recommended contract is yes. Preserve existing Read
ownership and semantics, including custom sessions/transports.

Reuse private authenticated receive backing, then copy verified operation data
into destination. Do not decrypt directly into caller memory: the AEAD record
also contains front/middle data, padding and epilogue, and Open may change its
destination on authentication failure. Do not expose any plaintext before all
record authentication, padding, epilogue, OSD operation/result and length checks.

## Ownership implementation

1. Introduce a built-in-only recyclable backing token, separate from byte-budget
   release. Default ownership transfer permanently disarms recycling. Existing
   Read must retain this transfer behavior so caller-visible slices never return
   to a pool. Custom owned-reply capability alone is not recycling capability.
2. Add an internal synchronous reply-consumption path. submitResult.take currently
   releases the receive lease before objecter decoding; that is too early for
   reuse. The new path must hold the token through complete OSD validation and
   copying, then release in a defer on every outcome. It must not return a view
   of recyclable backing. Avoid arbitrary callbacks in the session owner loop.
3. Make destination consumption occur on the submitting goroutine, before its
   return, never in the read pump or an asynchronous completion worker. If a
   cancellation wins before consumption, discard the reply without copying.
   If consumption has begun, finish it synchronously before returning. Define
   this cancellation/completion race consistently with existing Submit.
4. Start with at most one free scratch slot per built-in connection, capped at
   128 KiB, and nonblocking fallback to ordinary allocation when occupied.
   Larger records are not retained. A frame can be queued or consumed while
   the next read proceeds, so one slot does not eliminate all allocations.
   No sync.Pool: its unbounded retention/GC behavior is not a memory contract.
5. Account actual scratch capacity, including tag/rounding, while free, reading,
   queued and being consumed. Transfer the same reservation between states;
   do not release then retain hidden bytes. Maintain per-session as well as
   aggregate accounting. Free idle scratch before terminal saturation when
   useful, with an explicit lock order; never evict live/queued caller data.
6. On ordinary ownership transfer, remove backing from the scratch inventory
   and release its reservation at the existing caller handoff. On discarded
   replies, authenticate/validate as required, zero sensitive retained plaintext,
   and recycle only after the last consumer finishes. On Close/reconnect, join
   readers/consumers and retire each old-generation token exactly once.

Keep scratch metadata synchronized independently of session-owner commands;
do not add a global lock or change GOMAXPROCS, GOGC or GOMEMLIMIT. Lock ordering
and ledger transitions must be specified before the first pool implementation.
This design saves allocations, not copying CPU, and cannot isolate a library
from application-wide Go GC pauses.

## Implementation gates

Implement in separate reviewable stages, retaining existing Read as the fallback:

1. Internal consumption lifetime tests, without recycling or a public API.
2. One bounded recyclable slot and capacity ledger tests with a fake reader.
3. Built-in secure/CRC record reuse and tests proving existing owned Read never
   aliases reused backing. Keep custom transports untouched.
4. Objecter ReadInto validation/copy path, then the public method and docs only
   after its failure/cancellation contract is tested end to end.
5. Benchmarks with caller-allocated destinations created before measurement:
   compare Read/ReadInto at 64 KiB and 4 MiB, concurrency 1/16/64, ten Ps and
   background workers 0/4/8. Include CPU, allocation bytes/count, GC, RSS, pool
   hit/miss rates and individual latency distributions. Native loaded parity
   requires equivalent native-side CPU work; unloaded native is context only.

Required failure tests: invalid tag, bad padding, aborted epilogue, malformed
OSD front, inconsistent lengths, server error, oversize reply, EOF, cancellation
before admission/during receive/during completion, queued replies, stale
generation, reconnect/replay, Stop joins, concurrent reads, retained ordinary
Read slices, mixed Read/ReadInto and receive-budget exhaustion. Run race tests
and secure interoperability with the pinned Ceph source. Prove zero writes
after return with canary/reused destinations.

Accept only measured allocation reduction with no ownership, memory-envelope
or error-contract regression and repeatable mixed-workload latency improvement.
One passing p99 leg does not close Phase 4 or certify P12/P13. The bounded
secure implementation and public API are now retained on measured allocation
volume and throughput benefits; stable p99 improvement remains unproven.