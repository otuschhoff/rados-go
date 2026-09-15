# P06 Read-Only Object Path

Status: implementation and automated qualification complete.

## Delivered

- Dedicated CephX OSD service authorization with challenge, nonce, global-ID, connection-secret, downgrade, peer-identity, ticket-validity, and transcript-signature verification.
- Modern MOSDOp v8 read/stat encoding and bounded reply, redirect, operation-result, and MOSDBackoff decoding.
- Exact object PG plus raw-hash targeting, acting-primary routing, session reuse/invalidation, and immutable namespace, locator, and snapshot views.
- Context cancellation, stale-map refresh, primary remap, bounded redirects/retries, late-reply isolation, and session-scoped backoff block/ACK/unblock handling.
- Public ranged read and stat APIs with operation versions and stable wire-error classification.
- A cgo-disabled three-OSD real-cluster gate using native-written binary, empty, namespaced, and locator-keyed objects, including actual primary loss and remap.

## Certified Scope

The certified object path is read-only and replicated: head reads, ranged reads, stat, namespace and locator views, redirects, backoff, and bounded recovery. The API and wire codec carry explicit snapshot IDs, but real-cluster snapshot interoperability is not claimed by this phase. Mutation operations, mutation replay/outcome semantics, compound operations, metadata operations, enumeration, watches, and erasure-coded pools remain outside P06.

## Exit Gate

`make verify-p06-all` runs formatting, all cgo-disabled tests/builds, strict report verification, race/vet/static analysis, vulnerability checks, four target cross-builds, OSD reply/backoff fuzzing, and the pinned real-cluster workflow.

The checked-in report demonstrates byte-for-byte native object reads, ranged and empty reads, namespace/locator parity, stat metadata and nonzero operation versions, native-compatible missing-object behavior, and recovery after stopping the actual acting primary.