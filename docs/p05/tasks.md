# P05 Exact Placement

Status: implementation and automated qualification complete; fixture redistribution review remains pending.

## Delivered

- Exact Rjenkins object hashing, locator replacement, namespace separation, stable PG modulus, HASHPSPOOL seeds, and invalid pool-policy rejection.
- Bounded CRUSH map decoding for straw2 buckets, modern class metadata, replicated rules, Tentacle tunables, and fail-closed choose arguments.
- Exact fixed-point straw2 draws, OSD-domain firstn rule execution, retries, collision handling, OSD weights, and malformed graph rejection.
- Immutable OSDMap routing with raw, up, acting, up-primary, acting-primary, upmap, temp, liveness, and primary-affinity semantics. Upmap has native oracle coverage; temp and primary-affinity ordering have source-grounded synthetic tests.
- A pinned Ceph 20.2.4 corpus containing 512 CRUSH mappings plus 512 complete object-to-PG/acting-primary mappings replayed through 256-to-32 PG and OSD-out incremental map transitions and a native upmap-bearing OSDMap.

## Certified Scope

Replicated pools using Rjenkins object hashes, HASHPSPOOL, straw2 buckets, a class-agnostic TAKE/CHOOSE_FIRSTN(osd)/EMIT rule, OSD failure domain, and Tentacle tunables are supported. Device-class metadata is decoded, but class-constrained rules are rejected. Nonempty choose arguments, other bucket algorithms, other rule shapes, erasure placement, and legacy 64-bit name-map keys fail explicitly.

## Exit Gate

`make verify-p05-all` runs formatting, all cgo-disabled tests/builds, pinned corpus parity, strict manifest verification, corpus reproduction, race/vet/static analysis, vulnerability checks, cross-builds, and CRUSH decoder fuzzing.

The complete object-routing corpus has zero mismatches for actual PG, up/acting sets, and both primaries. The existing live P00 vector independently confirms `p00-smoke-object` maps to PG `2.8`, up/acting `[0,2,1]`, primary `0`.
