# P07 Mutation, Completion, and Benchmark Path

Status: implementation and automated qualification complete.

## Delivered

- Public mutation coverage for create, exclusive create, write, write-full, append, truncate, zero, remove, and post-mutation read validation.
- Completion and recovery semantics verified through ordered object versions, missing-object behavior, and append-once behavior across acting-primary remap.
- Admission and flush/shutdown behavior verified by explicit `Flush` and `Shutdown` success under live cluster conditions.
- Native differential evidence for native seed with Go mutation and native verification, plus Go write and native read parity checks.
- Full benchmark baseline capture for Go and native clients across secure and crc transports over fixed size, concurrency, and workload matrices.

## Certified Scope

P07 certifies replicated-pool mutation semantics, completion ordering, missing-object handling, remap durability for append-once, native/Go differential interoperability, and baseline benchmark capture. The benchmark output is a baseline record, not a parity or superiority claim.

## Explicit P08 Exclusions

- No xattr or OMAP operations.
- No single-object compound operation builders, assertions, compare operations, or per-sub-operation results.
- No object enumeration, cursors, partitions, or pagination.

## Exit Gate

- `./integration/p07/reproduce.sh`
- `go run ./tools/p07-verify`
- `CGO_ENABLED=0 go test ./tools/p07-verify`

The checked-in report demonstrates all eight scenarios passed, strict probe/native booleans, and exactly four benchmark runs with full 36-row matrices per run.
