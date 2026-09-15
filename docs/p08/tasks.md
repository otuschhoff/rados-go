# P08 Metadata, Compound Operations, and Enumeration

Status: implementation and automated qualification complete.

## Delivered

- Direct binary-safe xattr reads, writes, removal, and ordered listing.
- Bounded, ordered OMAP pagination, keyed and header reads, plus set, key/range remove, clear, and compare operations.
- Single-use read and write builders with ordered per-operation results, defensive input ownership, assertions, comparisons, qualified FAILOK flags, and one-MOSDOp server-owned atomicity.
- Pool- and namespace-owned opaque object cursors, bounded full/range listing, continuation, comparison, end detection, and exact reversed-hash partition splitting.
- Raw-hash PG placement and PGNLS continuation across current OSD maps while preserving request identity within retries.
- Native librados interoperability and live three-OSD evidence for metadata, atomic failure, contention, namespaces, pagination, and disjoint partition scans whose union equals the full listing.

## Certified Scope

P08 certifies replicated-pool metadata and single-object compound semantics on Ceph 20.2.4, plus unfiltered object enumeration for the default or one selected namespace. A compound write is sent as one ordered MOSDOp with `RETURNVEC`; it is never decomposed into client-side requests. Public cursors are opaque, pool-owned, and valid only for their originating pool and namespace.

## Explicit P09 Exclusion

Object-class execution (`Exec`) is not implemented or certified by P08.

## Exit Gate

- `./integration/p08/reproduce.sh`
- `go run ./tools/p08-verify`
- `CGO_ENABLED=0 go test ./tools/p08-verify`
- `make verify-p08-all`

The checked-in report requires all six scenarios, all ten Go probe assertions, both native seed assertions, and all four native verification assertions to pass.