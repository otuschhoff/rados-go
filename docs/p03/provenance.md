# P03 Provenance and Reproduction

Protocol behavior is grounded in Ceph `v20.2.0` commit
`69f84cc2651aa259a15bc192ddaabd3baba07489`; live qualification and direct
`ceph-dencoder` evidence use Ceph `v20.2.4` commit
`7f793731f1b39eb4f465e960113d2363c311b964` and the digest-pinned image in
`docs/p03/evidence.json`.

## Source Anchors

| Area | Ceph source |
| --- | --- |
| CephX exchange and ticket encoding | `src/auth/cephx/CephxProtocol.{h,cc}` |
| Client reply processing | `src/auth/cephx/CephxClientHandler.cc` |
| Server challenge and ticket issue | `src/auth/cephx/CephxServiceHandler.cc` |
| AES-128 and key envelopes | `src/auth/Crypto.cc` |
| AES256-CTS-HMAC-SHA384-192 | RFC 8009 and `github.com/otuschhoff/gokrb5/v8` |
| Messenger authentication transition | `src/msg/async/ProtocolV2.cc` |
| Mode policy | `src/auth/AuthRegistry.cc` |

`cephx-encoding-vectors.json` contains test instance zero for
`CephXServerChallenge`, `CephXResponseHeader`, `CephXTicketBlob`, and
`CryptoKey`, generated directly by pinned `ceph-dencoder`.
`crypto-vectors.json` contains synthetic CBC and challenge values independently
recomputed with OpenSSL. Its published canonical RFC 8009 AES256 vector is
executed by a standalone standard-library implementation of the RFC's KDF,
AES-CTS, and HMAC construction, independently of the pinned gokrb5 production
implementation. No fixture contains a production credential.

Run `make reproduce-p03-fixtures` for byte/value reproduction and
`make integration-p03` for the disposable real-monitor workflow. Both workflows
run natively on `linux/amd64` and `linux/arm64` in CI, where each report is
verified and retained as a platform-specific artifact. The monitor
store, monmap, generated keys, probe binary, network, and containers are all
temporary. The checked-in legacy zero-device CRUSH map only removes P04 map and
placement requirements from this P03 authentication gate.

The real-monitor workflow writes `integration-report.json` with the pinned
server identity and platform, run time, scenario outcomes, sanitized probe
result, and SHA-256 hashes of every compiled internal source and test input. The
probe performs identification through the production messenger session. The
P03 verifier recomputes those hashes so stale integration evidence fails closed.

Oliver Tuschhoff completed the accountable security and fixture-redistribution
review on 2026-09-14 and approved the reviewed P03 tree with no findings and the
fixtures for redistribution. Automated and agent audits remain supplementary.
