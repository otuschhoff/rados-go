# Phase 4 Results

Status: bounded receive/fanout implementation verified; **phase blocked on
secure-read latency**. No completion commit, native parity, or renewed P12/P13
qualification. [Contract and reproduction](README.md); [audit](AUDIT.md).

## Memory Measurements

Pre-budget captures measured fake and IPv4 TCP loopback ready sessions in both
CRC/secure modes, at 1/16/64/128 connections with three repetitions (48 records).
At 128 connections, median live heap growth was 66.30/66.51 MiB for fake
CRC/secure and 67.36/67.55 MiB for loopback. Reader backing alone was 64 MiB.
Heap slopes were 543114-553386 bytes/connection; post-close median goroutine
delta was zero. These measurements explain fanout cost, not a live authenticated
cluster or a whole-client bound. RSS varied independently of post-GC heap.

Pre-budget stalled delivery retained eight 4-MiB messages (33554432 payload
bytes). Queue, read-ahead, and gated active decode reached 41943136 owned bytes;
Stop alone did not free the buffered incoming queue. Application retention after
draining was separately visible, and RSS stayed elevated after releasing payloads.
CRC prefix-only decoding allocated roughly 4 MiB plus its 512-KiB reader before
session queue admission. These are historical measurements of the unbudgeted
fixture path, not final envelope claims.

The final frozen-source budgeted capture contains another 48 fanout records:

| At 128 sessions | Median live heap delta (bytes) | Reader bytes | Post-close goroutine delta |
| --- | ---: | ---: | ---: |
| Fake CRC | 68366576 | 67108864 | 0 |
| Fake secure | 68594592 | 67108864 | 0 |
| TCP loopback CRC | 69454456 | 67108864 | 0 |
| TCP loopback secure | 69672840 | 67108864 | 0 |

The unchanged reader dominates idle growth. Capture records verify cleared
readers and a zero session/payload/control ledger after Stop. The default
256-session cap bounds this reader capacity at 128 MiB, separately from the
256-MiB receive-backing cap and 72-KiB control allowance. It is not an RSS cap.

Final stalled capture uses stricter test limits: shared 16 MiB, per-queue 9 MiB,
queue depth eight. Two 4-MiB bodies fit; the third fails the session observably.
CRC queued backing is 8388690 bytes and the sampled decode peak is 12583035;
secure values are 8388864 and 12583296 bytes. All three repetitions per mode
release internal storage terminally and reach a zero ledger after Stop. Peaks
are ledger samples at connection Read boundaries, not measured allocator/RSS
peaks. Application-retained output validity is tested separately; it is not
included in these stalled-consumer measurements.

## Live Secure Read

All eight diagnostics are retained, including misses. Each default diagnostic
is native/Go/Go/native (ABBA), 64-KiB read, concurrency 16, 128 warmup operations,
4096 measured operations per leg. The table conservatively divides both Go legs
by the faster native p99 from that run. It is an explicitly selected diagnostic
summary, not a rerun of the P12 comparator/certification procedure. The existing
8x regression threshold is unchanged; this is not native parity. Both raw native
legs are preserved, allowing other explicitly stated comparisons.

| Private diagnostic suffix | Source state | Go p99 ms, legs 1/2 | Ratios, legs 1/2 |
| --- | --- | ---: | ---: |
| secure-20260930T190418Z | Initial budget implementation | 8.559 / 8.474 | 8.3813 / 8.2979 |
| secure-before-20260930T191150Z | Committed Phase 3 | 8.732 / 9.090 | 8.2665 / 8.6054 |
| secure-final-20260930T191525Z | Reader/renewal repair | 9.113 / 8.455 | 8.6152 / 7.9928 |
| secure-before-repeat-20260930T192100Z | Committed Phase 3 repeat | 8.613 / 8.707 | 7.9091 / 7.9954 |
| secure-final-repeat-20260930T192138Z | Reader/renewal repair repeat | 9.519 / 8.797 | 9.1628 / 8.4685 |
| secure-optimized-20260930T192802Z | Release/reader-lease optimization | 8.720 / 9.394 | 8.1717 / 8.8033 |
| secure-prelude-20260930T193250Z | Final embedded-prelude implementation | 8.778 / 8.658 | 7.4908 / 7.3888 |
| secure-prelude-confirm-20260930T193618Z | Same final implementation, confirmation | 9.082 / 9.057 | 8.3023 / 8.2786 |

Final four Go legs have a descriptive median around 8.917 ms versus 8.720 ms
across the four committed-before legs, approximately 2.3% higher. These small,
separately clustered samples do not establish statistical significance,
equivalence, stable compliance, or a causal budget regression. A favorable
native denominator contributes to the passing run; its failed confirmation
cannot be discarded.

The final short allocation profile attributes about 88% of allocated bytes to
`SecureCodec.Read`. Short CPU profiles show syscall/AES/runtime costs, not a
dominant new budget lock. Confirmation default Go leg 1 reports 86 GC cycles,
151870703 ns cumulative GC pause, and p99 9.082 ms. Its separate `GOGC=off`
diagnostic reports zero cycles, p99 1.722 ms, and max RSS 355524608 bytes versus
20529152 in default leg 1. This supports an allocation/GC contribution but is a
different diagnostic execution, not a production fix or controlled attribution.
No global GC/GOMAXPROCS tuning was added.

Unique lease/reader/prelude co-location reduced budgeted secure decode allocation
count from nine to seven, matching unbudgeted count; final measured 74128 B/op.
Lease lifecycle median changed from 40.94 to 27.40 ns in the local probe. No
decode-time or live latency improvement is established by those microbenchmarks.

## Evidence And Validation

The compact evidence includes frozen-source baseline manifests and 185 inherited
benchmark samples; source-bound final validation; pre-budget resource/stalled
captures; final budgeted capture; and all default live legs plus separate tuning,
timing, CPU, and memory diagnostic results. Private binaries, source archives,
keys, cluster configs, and profiles are omitted; binary/profile hashes and build
IDs are provenance, not a public reproducibility bundle. Historical reports are
not rewritten. The P07 diagnostic script uses Ceph v20.2.4 source anchor
`7f793731f1b39eb4f465e960113d2363c311b964`, not the older P13 v20.2.0 anchor.

The early-return diagnostic branch does not capture the full native runtime
package/hash or server identity. Intermediate dirty-source labels describe the
recorded edit chronology; they do not supply complete per-run source archives.
Build IDs/hashes pin the measured binaries, while the final source manifest pins
the final frozen implementation. It cannot retroactively certify intermediate
binaries. Private omitted profiles are required to independently reproduce the
sampled profile attribution. All these limitations preclude a certification claim.

- [Baseline manifest](evidence/baseline/manifest.json) and
	[frozen source manifest](evidence/baseline/source-manifest.json): 185 inherited
	benchmark samples and source identity.
- [Validation metadata](evidence/validation/validation.json) and
	[current source check](evidence/validation/publication-source-check.json): exact
	commands/overrides, exits, hashes, and 239 matching Go/module files.
- [Focused race output](evidence/validation/budget-race.stdout),
	[full race](evidence/validation/full-race.stdout),
	[diagnostics](evidence/validation/diagnostics.stdout),
	[vet](evidence/validation/vet.stdout), and
	[Linux build](evidence/validation/linux-build.stdout).
- [Pre-budget idle](evidence/resources/before-resource.json),
	[pre-budget stalled](evidence/resources/before-stalled.json), and
	[final budgeted](evidence/resources/final-budgeted.json) resource records.
- [Initial helper failure](evidence/validation/budgeted-resource.stdout) and
	[successful explicit-metadata rerun](evidence/validation/budgeted-resource-with-git.stdout).
- [All live runs and conservative ratios](evidence/live/summary.json): complete
	default legs, separate diagnostic variants, timestamps and binary/profile
	hashes. Full timing traces are losslessly gzip-compressed; their uncompressed
	hashes are recorded, with no samples omitted.
- [Initial microbenchmarks](evidence/microbench/before.stdout) and
	[final prelude microbenchmarks](evidence/microbench/prelude.stdout): local probes,
	not a fully source-bound comparative live experiment.
- [SHA256 inventory](evidence/SHA256SUMS): all 181 published artifacts.

Final frozen capture: `/tmp/rados-go-phase4-baseline-final-20260930T194447Z`.
Final validation: `/tmp/rados-go-phase4-validation-rjWtTJ`. Repeated focused race
checks (20), full repository race, diagnostic-tag tests, vet, Linux ARM64 build,
and the explicit-Git-metadata budgeted capture all passed. The initial capture
helper failure is preserved. Inherited benchmark samples provide context, not
isolated new-path speedup or latency non-regression proof.

Phase closure remains blocked. The [open audit task](AUDIT.md#open-exit-finding)
must be resolved or explicitly dispositioned by the maintainer before marking
Phase 4 complete and making the requested completion commit.