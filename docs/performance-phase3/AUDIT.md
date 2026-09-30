# Phase 3 Audit

The first OSD regression reproduced blocked cached B access while A was gated
in Stop or its factory. The first manager regression observed 12 factory entries
for 16 simultaneous misses. Both now pass with unrelated cached progress and
exactly one factory entry per shared target attempt.

Three independent implementation audit rounds, plus targeted coverage
reevaluations, were used. Findings were reproduced or pinned with discriminating
tests, repaired, and checked before further work. The final implementation round
found no outstanding concrete findings and supported every scoped exit gate.

## Findings And Repairs

1. The initial regression fixture compared a slice-containing address struct.
   It now compares identities through the existing address selector.
2. The first objecter worker draft could loop back after fast completion and
   retry a failed factory instead of returning that attempt's error. Initiating
   callers now wait on their exact pending attempt; failures fan out unchanged.
3. Initial creation incorrectly interrupted newly registered watches. Only
   actual old-session replacement now collects interrupted watches. Existing
   watch overflow/ping/re-registration race tests exposed and verify this repair.
4. A manager deadline test read a plain factory counter after the caller could
   return while its worker still ran. The counter is now atomic; its retry-budget
   assertion is preserved. Full package race tests pass.
5. Manager stale-target early returns skipped abandonment, leaving A1 eligible
   after an observed A-to-B-to-A transition. Abandonment now uses authoritative
   target state before any early or cached return. Changed and unavailable
   observations both reject late A1 and retain fresh A2.
6. Objecter had the corresponding early-error ABA hole. Stale address and down
   observations now permanently supersede pending attempts; completed results
   and the map-watcher path recheck ownership and identity. Regressions failed
   before the fix and now pass repeatedly under race instrumentation.
7. Empty address vectors report absent rather than present-but-invalid. The
   initial target check skipped them. Known OSDs without usable addresses now
   revoke pending creation and return `ErrNoPrimary`; empty and legacy-only
   vector variants pass. Synthetic sources with no known OSD state retain their
   existing caller-provided address behavior.

## Coverage Map

Objecter coverage pins gated Stop/factory cross-OSD progress, one shared creation,
first/later waiter cancellation, stable failure fanout, partial/nil results,
factory/loser/invalidation Stop joining, concurrent Close, replacement Stop
versus Close, skipped factories after closure, canceled Read acquisition, changed
addresses and generations, observed ABA, early target errors, map-watcher-only
revocation and empty/unsupported address vectors. Blocked availability callbacks
pin ordering against invalidation and closure without blocking unrelated cached
access. Stale dispatchers are tested for both watch delivery and notify completion.

Manager coverage pins same-target fan-in, independent first/later cancellation,
all-waiter cancellation and retry, stable failure fanout, partial cleanup,
factory-context cancellation, target changes without another caller, observed
ABA including stale/unavailable observations, cached authoritative matches,
epoch-only reuse, canceled old attempts versus retries, factory/loser Stop and
invalidation joining, gated replacement progress and concurrent Close.

The focused source-bound race run contains 29 top-level tests repeated 20 times:
580 top-level passes and 1120 pass entries including subtests. All commands exit
zero. Earlier package races include objecter count 10, manager count 20, both
packages count 10, and final independent focused/full package count 3. Full
repository race, diagnostic-tag tests, vet and Linux ARM64 build pass on the
frozen measured source; editor diagnostics are clean.

Command acquisition uses the same directly tested cancellation helper as Read;
there is no separate gated-factory command integration test. No atomic external
map/generation API, unobserved-history guarantee or arbitrary reentrant observer
contract is added. Noncooperative custom lifecycle callbacks can block joining.
These limitations are explicit, not hidden performance or shutdown guarantees.

## Final Reevaluation

Factory and synchronous retirement work no longer hold the global objecter lock.
Same-target manager misses do not stampede; canceled waiters do not cancel shared
work needed by others. Installation rejects closed, stale and superseded work;
late and retired resources remain joined. Notification ownership, watch behavior
and installation event ordering are covered. Repeated package race tests and
full validation pass. The defined Phase 3 gate is satisfied without changing
aggregate memory defaults or implementing Phase 4.