# P12 Release Artifacts

Status: tooling implemented; no release or P12 certification is claimed.

## Generate a Candidate

Supply a semantic version with a leading `v`:

```sh
CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go run ./tools/p12-release -root . -out dist -version vX.Y.Z
```

The command writes exactly these files:

- `rados-go-vX.Y.Z.tar.gz`: deterministic source archive.
- `rados-go-vX.Y.Z.zip`: deterministic Go module archive rooted at
  `github.com/otuschhoff/rados-go@vX.Y.Z/`.
- `rados-go-vX.Y.Z.spdx.json`: deterministic SPDX 2.3 JSON SBOM.
- `SHA256SUMS`: SHA-256 digests for the other three artifacts.

`make release-p12 P12_RELEASE_VERSION=vX.Y.Z` generates the set twice in
independent temporary directories and compares every byte. The project release
version is `v0.1.0`; supply another semantic version explicitly for a later
release.

## Archive Boundary

Both archives include the LGPL-2.1-only `LICENSE`, `THIRD_PARTY_NOTICES`, root
public package Go sources and tests, internal Go packages and tests, examples,
`README.md`, `SECURITY.md`, `go.mod`, and `go.sum`.

The module zip is an intentionally curated pure-Go distribution. It excludes
qualification clusters, native C or C++ oracles, live integration scripts,
reports, fixture-generation infrastructure, documentation evidence, and build
tools. Inputs must be regular files. Symlinks, absolute paths, non-clean paths,
path traversal, backslash paths, and case-insensitive duplicate paths are
rejected. The generated zip is reopened and validated before the command
returns successfully.

Archive entry order, permissions, timestamps, gzip metadata, zip metadata,
JSON ordering, SPDX creation time, and checksum ordering are fixed. The SPDX
document inventories every shipped file and the four audited reachable module
dependencies. Its `1970-01-01T00:00:00Z` creation time is a reproducibility
marker, not the wall-clock generation time.

## Certification Boundary

A quick harness run is diagnostic only:

```sh
make p12-quick
```

Its report must contain `release.performed=false`, `version=null`,
`reproducible=false`, and an empty artifact map. `make verify-p12` rejects this
report by default; `make verify-p12-harness` only validates its non-certifying
shape and source evidence.

The first stage is an explicit operator-run 24-hour candidate run on a controlled
Docker host:

```sh
make fuzz-p12-nightly
make verify-p12-fuzz
P12_RELEASE_VERSION=vX.Y.Z ./integration/p12/reproduce.sh
```

The first command requires a Darwin host and exact Go 1.27.1, and takes at
least 5 hours 10 minutes for the 31 sequential 10-minute targets, plus startup
and test overhead. The endurance harness refuses a candidate run unless that
exact current-source fuzz report verifies as passed and certifying. Only after
the soak qualifies does the harness run the release generator twice,
compare all outputs, stage an exact copy of the first output, and replace
`docs/p12/release-artifacts` with that four-file set. The candidate report binds
the fuzz report and that exact directory, each file name, and each file SHA-256.
Quick runs record fuzz as null and neither create nor change the retained
directory.

The second stage occurs only after the candidate file is immutable. Populate
the approved branch of `docs/p12/human-review.json` with four role-authorized
detached Ed25519 signatures over verifier-printed canonical payloads, then run
`make verify-p12`. Certification succeeds only when the candidate and all four
signatures validate against `docs/p12/reviewer-trust.json`.

The retained directory is generated evidence, so qualification and final
current-source maps explicitly exclude it to avoid hashing output that does not
exist until after qualification. The passed final report instead binds those
bytes through `release.path` and `release.artifacts`. Verification recomputes
all four hashes, parses `SHA256SUMS`, safely inspects both archives, compares
every archived source byte with the current releasable source set, and strictly
checks the SPDX identity, licenses, dependencies, file inventory, and hashes.
Qualification otherwise hashes every regular repository file outside `.git`
except the generated fuzz, qualification, detached human-review, and final
reports. This includes `.gitignore`, `.github/workflows/p00.yml`, and other
release controls.
The final harness and verifier use the same exact retained-directory exclusion
and generated-report exclusions and include `.gitignore` and the P00 workflow
in their curated source map. The final report separately binds the fuzz report
by path and SHA-256, and verification recursively validates its current source
map and every target observation.
Hosted CI runs cumulative non-live gates and does not stand in for this manual
24-hour gate, accountable human review, tagging, or publication.