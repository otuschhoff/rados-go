# Phase 2 Results

Status: complete within the Phase 2 placement scope. Measurements are descriptive
medians of five repetitions, not significance claims or native-runtime parity.
Same Apple M1 Max / macOS ARM64 host, Go 1.27.1, 100 ms benchmark duration,
matching environment and serial timing runs. Network I/O is excluded.

## Repeated Routing

Identical inherited `BenchmarkPerformancePlacement` cases before and after:

| Unrelated buckets | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1702 | 768.5 | 1352 | 40 | 18 | 5 |
| 64 | 14949 | 802.2 | 27376 | 40 | 167 | 5 |
| 1024 | 205900 | 839.2 | 445891 | 42 | 2106 | 5 |
| 4096 | 929217 | 842.8 | 1789809 | 51 | 8324 | 5 |

The inherited after benchmark includes one initialization amortized over all
iterations; therefore its B/op includes small cold contributions at large
topologies. It is not a pure warmed cost. The selected rule's subtree and exact
route are held constant while valid unrelated buckets increase. At 4096 buckets
the median routing time is about 1103 times lower. This is a fixture improvement,
not an end-to-end operation or tail-latency claim.

## Explicit Warm And Cold Scope

Warm initialization is outside the timer; the timed operation is full
`PlaceRawHash`. Cold initializes a fresh state each iteration and measures CRUSH
decode, graph validation, per-rule certification and one raw placement. Cold
excludes OSDMap wire decoding/construction and metadata transformations, so it is
not directly interchangeable with the inherited before full-route benchmark.

| Unrelated buckets | Warm ns/op | Warm B/op | Warm allocs/op | Cold ns/op | Cold B/op | Cold allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 742.3 | 40 | 5 | 1500 | 1656 | 18 |
| 64 | 804 | 40 | 5 | 11486 | 24144 | 161 |
| 1024 | 819.3 | 40 | 5 | 155068 | 390227 | 2096 |
| 4096 | 822.2 | 40 | 5 | 643939 | 1566236 | 8290 |

First use remains topology-dependent. Allocation volume includes transient
decoder/executor state and is not retained heap or RSS. The decoded graph and
per-rule errors remain retained while referencing snapshots live; equal payload
increments share this graph but still clone snapshot metadata and bytes. No
global epoch cache or client-wide aggregate memory bound is claimed. Explicit
warm/cold benchmarks are new after-only cases, not invented before counterparts.

All 37 inherited cells remain published. Unrelated timing medians also changed:
CRC 4 MiB initial submission increased from 2220839 to 2489836 ns/op (about 12%);
CRC 16-connection lifecycle increased from 831031 to 912596 ns/op (about 10%);
the small incremental fixture increased from 1410 to 1534 ns/op (about 9%).
These samples do not establish causes or general unchanged/improved performance.
No live cluster, socket timing, native runtime, p99, or P12/P13 qualification run
was repeated. Existing allocation budgets and regression guardrails are unchanged.

## Evidence And Provenance

- [Baseline manifest](evidence/baseline/manifest.json): passing capture at
  `/tmp/rados-go-phase2-baseline-final-20260930T163510Z`, 37 cells times five,
  exactly 185 after samples, zero failures, source verified after commands.
- [Source manifest](evidence/baseline/source-manifest.json): measured frozen
  dirty snapshot, not a later final commit or retroactive qualification.
- [Comparison metadata](evidence/comparison/comparison.json): private capture
  `/tmp/rados-go-phase2-comparison-Udn4yA`, before archive of Phase 1 HEAD
  `b359422`, before archive hash, matching environment, commands/exits/hashes.
- [Before raw output](evidence/comparison/before-performance.stdout): 185
  samples. [Warm/cold raw output](evidence/comparison/placement-cold-warm.stdout):
  40 samples. [Focused tests](evidence/comparison/focused-placement.stdout):
  selected passing verbose placement and incremental tests.
- [SHA256 inventory](evidence/SHA256SUMS): 13 published artifacts plus inventory.

The compact exporter excludes private archives, binary patches, binaries, logs
and stderr. It publishes allowlisted stdout, manifests and historical provenance
inputs. Historical files are explicitly pinned inputs, not reruns; original
Phase 0/1 and P07/P12 reports are unchanged. Documentation finalization does not
alter measured implementation. Reproduction commands and scope are in the
[contract](README.md); audit repairs and limitations are in [AUDIT](AUDIT.md).