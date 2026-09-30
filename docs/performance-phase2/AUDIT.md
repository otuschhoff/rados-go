# Phase 2 Audit

Three independent read-only audits were used. The first identified defects and
coverage gaps; all were repaired and checked before continuing. The second found
no concrete correctness bugs but correctly left repeated measurement pending.
The third reviewed measured source, artifacts, tests and gate requirements and
found no remaining concrete defects. It independently checked 456 frozen source
hashes, before-source identity, artifact bindings, environments and sample counts.

## Findings And Repairs

1. New `sync.Once` state made fixture value-copy/deep-equality assertions invalid.
   The incremental fixture now uses explicit snapshot cloning and semantic
   `Equivalent`, which excludes caches and does not copy locks. Focused fixture
   tests and vet pass; every current semantic OSDMap field is retained.
2. Positive mutable/immutable comparisons supplied nil OSD weights and compared
   empty placements. Tests now supply full-in weights, require two outputs and
   verify owned input and independent results. Nonempty golden routes prevent
   vacuous placement equality.
3. Mutable bucket item/weight shape mismatches could panic inside straw2
   selection. A regression reproduced the panic; graph validation now rejects
   mismatched arrays and invalid bucket keys/identity with `ErrPlacement`.
   Supported equal-length arrays continue to work.
4. Added missing cold incremental/concurrent routing, payload error transitions,
   full replacement, snapshot metadata, EC golden weights and output isolation
   coverage. Each repaired slice passed focused checks before wider validation.

## Coverage

CRUSH tests cover mutable/immutable equivalence, independent decoded input,
unsupported unused rules, missing rules/invalid replicas/nil receivers, defensive
mutable graph validation and 1,024 ordered golden mappings. The corpus uses the
checked-in P05 and P10 CRUSH payloads with full-in and OSD1-out weights.

OSDMap tests cover concurrent warmed routing; concurrent cold routing alongside
unchanged increment application; stable malformed errors; malformed-to-valid,
valid-to-malformed and equal-byte malformed transitions; fresh changed/full-map
state; equal/unchanged state sharing; cache-independent equivalence; returned
slice isolation; weights, affinity, OSD state, upmap/items, PG/primary temporary
mappings and replicated/EC primary shard selection. P05 snapshot routes and P10
golden weights use real checked-in payloads. P10 metadata fixtures are synthetic:
this is not a full native P10 OSDMap/object-route differential rerun.

The final audit ran focused race tests with count 2 and checked 87 selected test
results. Parent full race, diagnostic-tag, vet and Linux ARM64 build validation
passed. The source-bound runner also passed its mandatory prerequisites,
deterministic checks, unchanged-source verification and all benchmark commands.
See [selected verbose tests](evidence/comparison/focused-placement.stdout).

## Exit Reevaluation

Repeated routes neither decode nor globally certify/validate unchanged graphs.
Snapshot sharing never shares metadata-dependent route results. Defensive mutable
APIs, error caching, concurrent use, exact placement and slice isolation pass.
Five repeated measurements show greatly reduced routing allocation volume and
eliminate growth with unrelated topology. Cold first-use cost remains measured
and disclosed. No additional result cache was introduced. The scoped gate is
satisfied; unrelated performance/scalability findings remain for later phases.