# Phase 2: Reuse Immutable Placement State

Status: complete within the Phase 2 scope after implementation, repair, three
independent audits, repeated measurements, and final validation. No outstanding
concrete findings. See [results and provenance](RESULTS.md) and
[audit and coverage](AUDIT.md). Phase 3 remains unimplemented by this change.

## Contract

Each immutable OSDMap snapshot owns or shares a lazy CRUSH payload state.
`sync.Once` decodes and validates the graph and certifies each rule once. Errors
are stable; unsupported unused rules do not poison supported rules. The private
decoded graph cannot be obtained through the placement API. Execution weights,
permutations and result slices are call-local. Snapshot weights, affinity, upmap,
PG temporary mappings and primary/shard transformations remain outside the
shared graph and are applied for the current snapshot on every route.

Unchanged and exact equal-byte incremental CRUSH payloads share state. Changed
payloads create fresh state; full-map replacements always create fresh state.
Old snapshots remain independently usable. Exported mutable CRUSH `Map.Place`
continues certification and graph validation on every call, including malformed
bucket identity and item/weight shape checks. OSDMap cloning constructs fields
explicitly rather than copying locks. Snapshot equivalence ignores lazy caches
and verification/application provenance while retaining semantic map fields.

No epoch/PG result cache is needed: the repeated topology benchmark demonstrates
that decoded-state reuse removes whole-topology routing work. There is no global
placement cache retaining historical epochs. The graph remains live while
referenced by snapshots, then is collectible. Retained graph memory scales with
payload topology and decode limits, not with the number of routes. Snapshot
collections and CRUSH byte copies still have their preexisting clone costs;
client-wide aggregate snapshot/session retention remains a later-phase concern.

## Validation And Reproduction

Run from the repository root, using Go 1.27.1:

```sh
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./...
GOTOOLCHAIN=go1.27.1 go test -tags p12diagnostics ./...
GOTOOLCHAIN=go1.27.1 go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=go1.27.1 \
  go build -tags p12diagnostics -o /tmp/rados-go-phase2-linux-check \
  ./integration/p07/benchmark
GOTOOLCHAIN=go1.27.1 go test ./internal/crush ./internal/maps -race -count=2
GOTOOLCHAIN=go1.27.1 go test ./internal/maps -run '^$' \
  -bench '^BenchmarkPlacementReuse(Warm|ColdDecode)$' \
  -benchmem -benchtime=100ms -count=5
GOTOOLCHAIN=go1.27.1 go run ./tools/perf-baseline \
  -out /tmp/rados-go-phase2-fresh-capture -benchtime 100ms -count 5
git diff --check
```

The baseline runner requires a fresh output directory and freezes its measured
source. For the before measurements, archive commit
`b3594226e93a338a875d8edb8fa3fa99438f32c6` into a separate fresh directory and
run `go test ./internal/maps ./internal/msgr -run '^$' -bench
'^BenchmarkPerformance' -benchmem -benchtime=100ms -count=5` there. Use the
matching environment recorded in [comparison metadata](evidence/comparison/comparison.json),
same host/toolchain and serial benchmark execution. Preserve raw samples and
hashes. New captures bind their own source, not this publication or future commits.

This completion does not assert librados performance parity, live network p99,
large-cluster scalability, or renewed P12/P13 certification. Historical native
and live-cluster evidence remains scoped to its original source and mode.