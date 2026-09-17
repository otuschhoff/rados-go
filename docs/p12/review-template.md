# P12 Release Review Template

This template is completed separately by automation/AI and accountable humans.
An automated record cannot sign a human section or satisfy a required human gate.

The completed approval is recorded in `human-review.json`; this document is a
working form and does not itself satisfy a gate.

## Review Identity

- Required role: security / distributed-systems / license/notices / release-owner
- Reviewer identity and affiliation exactly as authorized in `reviewer-trust.json`:
- Authorized key ID and role:
- Passed qualification report SHA-256:
- Candidate `integration/p12/report.json` SHA-256:
- Release version and four artifact SHA-256 values:
- Review start and completion timestamps:
- Scope and excluded paths:
- Tools, versions, and commands:

## Evidence

- Unit/build/vet/module verification results:
- Live qualification report identities and source-hash checks:
- 24-hour soak report identity, duration, workload, and resource bounds:
- Dependency graph and license notices checked:
- Security areas inspected:
- Replay/completion/failure areas inspected:

## Findings

| ID | Severity | File/component | Reproduction or evidence | Required action | Disposition |
| --- | --- | --- | --- | --- | --- |

State explicitly when no findings were identified; do not interpret that as
proof of absence. Record unresolved questions and evidence gaps.

## Decision

- Decision: approved / reject / pending
- Conditions or blockers:
- Follow-up owner and due point:
- Auditable approval reference (required absolute HTTP(S) URI):
- Detached Ed25519 signature (base64; private key remains outside this repository):
- Unresolved critical findings (must be zero for approval):
- Unresolved high findings (must be zero for approval):

For human reviews, the reviewer must be a named accountable person and the
record must identify the exact reviewed tree. AI-generated text may prepare the
record but cannot populate the signature or approval decision.