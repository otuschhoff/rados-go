# P12 Public Release Documentation

Status: **qualification, candidate, signed reviews, and final P12 certification pending**.

This directory describes the public surface implemented through P11 and records
the release gates without treating documentation or automated AI review as a
release decision. No 24-hour soak, final P12 pass, independent human review, or
completed release is asserted.

## Non-Live Qualification

Run `make qualify-p12` to execute the fixed non-live release matrix and write
`docs/p12/qualification-report.json`. The report records each observed command,
toolchain, platform, timestamps, exit status, output hash and output, current
source hashes, prior-phase evidence hashes, runtime observations, and both sets
of release artifact hashes.

The runtime matrix executes cgo-disabled binaries on darwin/arm64,
darwin/amd64 through Rosetta, and linux/arm64 and linux/amd64 in the pinned Ceph
image. A missing execution platform is a failed qualification, not a skip.
P03-P11 verifiers run against the current tree; stale phase reports therefore
produce a schema-valid `failed` report and a nonzero command exit. P05 binds the
complete verifier-owned manifest and fixture set because that phase has no
single integration report.

A qualifying 24-hour harness run emits an immutable `candidate` report after
qualification, credential renewals, churn, benchmarks, and retained release
artifacts complete. The candidate binds the passed qualification and release
bytes, but always records `reviews` as `null`. Quick reports record both
`qualification` and `reviews` as `null` and remain non-certifying.

## Exact-Candidate Fuzz Evidence

P12 fuzz evidence runs every current fuzz target in the immutable shared Go
contract, in its fixed order, on a Darwin host with exactly Go 1.27.1. Run
`make fuzz-p12` for the non-certifying smoke profile of 60 seconds per target.
Run `make fuzz-p12-nightly` for the certifying profile of 10 minutes per target,
then run `make verify-p12-fuzz`. Profile durations cannot be overridden.

The runner replaces `docs/p12/fuzz-report.json` with observed timestamps,
commands, output, exit status, pass state, output SHA-256, and hashes for the
current source tree. A failed or interrupted campaign writes a complete failed
report and exits nonzero. The checked-in report is failed pending evidence; it
does not claim that fuzzing ran or passed. A candidate endurance run refuses to
start unless this file is a current, passed certifying report, and binds its
path, profile, status, and file hash. Quick endurance reports keep `fuzz` null.

Only after the candidate exists, complete exactly one accountable approval for each of
`security`, `distributed-systems`, `license/notices`, and `release-owner` in
`human-review.json`. Each approval is signed with an externally held Ed25519
private key authorized for that exact role by `reviewer-trust.json`. The
verifier never handles private keys. Print bytes to sign with
`CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go run ./tools/p12-verify -check-human-review docs/p12/human-review.json -print-review-payload ROLE`.
After inserting the detached base64 signature, run `make verify-p12-review` or
`make verify-p12` with `P12_REVIEWER_TRUST_SHA256` set to the policy digest
obtained through the protected external trust channel. Pending, unsigned,
duplicated, stale, predated, mismatched,
or unresolved critical/high review evidence blocks certification.

## Documents

- `compatibility.md`: exact qualified server matrix and narrower build targets.
- `troubleshooting.md`: configuration, connection, operation, and recovery diagnosis.
- `cgo-migration.md`: semantic migration from librados cgo wrappers.
- `dependencies.md`: production module graph and license audit.
- `review-template.md`: evidence-oriented review form.
- `automated-review.md`: limited automated AI documentation review record.
- `human-review.md`: human-review contract, hash-cycle boundary, and open gates.
- `human-review.json`: authoritative structured review record, currently pending and unsigned.
- `reviewer-trust.json`: role authorization policy, currently pending with no identities or keys.

## Implemented Through P11

| Phase | Capability delivered |
| --- | --- |
| P00 | Evidence pins, API inventory, compatibility target, threat model, native oracle |
| P01 | Module, bounded codecs, wire errors, API/lifecycle contracts |
| P02 | Messenger v2.1 CRC and secure framing/session core |
| P03 | CephX authentication, transcript security, ticket/session recovery |
| P04 | Monitor bootstrap/failover, FSID/maps, pool discovery |
| P05 | Exact object-to-PG/CRUSH placement for the certified profile |
| P06 | CephX OSD sessions, reads/stat, remap and bounded retry |
| P07 | Object mutations, versions, flush/shutdown, unknown-outcome handling |
| P08 | Xattrs, OMAP, atomic operations, cursor enumeration |
| P09 | Class execution, locks, watch/notify and recovery |
| P10 | Named/self-managed snapshots, EC subset, specialized object I/O |
| P11 | Manager/admin commands, pool/application admin, blocklist and inconsistency queries |

Phase reports are scoped evidence for the exact source hashes and disposable
clusters they record. They do not make a blanket compatibility or release claim.

## Known Limitations

- Only Ceph 20.2.4 is live-qualified. Ceph 20.2.0 is the source-semantics
  baseline, not a separately certified server. Earlier, future 20.x, and later
  major releases are unqualified.
- Messenger v1/v2.0, wire compression, no-auth production use, automatic CRC
  downgrade, Windows, and architectures other than amd64/arm64 are unsupported.
- Configuration and keyring loading supports explicit files, documented
  default paths, argument parsing, and environment overrides. It intentionally
  implements the frozen v1 subset rather than every native Ceph option.
- Qualification covers specific single-host disposable CRUSH and pool profiles.
  Multi-host failure domains, unencountered CRUSH algorithms/features, cache
  tiers, and other EC plugins/profiles are not certified.
- EC support is operation-specific. P10 qualified `k=2,m=1` jerasure behavior
  with overwrites disabled; partial overwrite, writesame, and OMAP restrictions
  are surfaced rather than emulated.
- Enumeration is paged but not a global snapshot or globally sorted during
  concurrent mutation. Watches are not durable logs, and locks do not provide
  fencing against unrelated writes.
- Cancellation cannot roll back a mutation accepted by an OSD. Callers must
  handle `ErrOutcomeUnknown` and must not blindly retry non-idempotent work.
- Administrative methods require explicit Ceph capabilities and include
  destructive operations. There is no production-cluster safety prompt in the
  public API.
- RBD, CephFS, RGW/S3, multi-object transactions, local caching, and drop-in
  source compatibility with cgo wrappers are out of scope.
- There is no published semantic-version stability promise, release artifact,
  benchmark parity claim, FIPS claim, or completed P12 release review.

## Remaining P12 Gates

- Re-run and bind the passed non-live P12 qualification report to the exact
  final candidate used by the certifying endurance harness.
- Produce the required reproducible 24-hour soak evidence.
- Complete accountable human security review of auth/framing and human
  distributed-systems review of replay/completion on the reviewed commit.
- Resolve all critical/high security and data-integrity findings or record an
  explicit disposition allowed by the release contract.
- Confirm release artifacts, notices, checksums, version/tag, and publication
  procedure on the final tree.