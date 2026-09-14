# P03 Security Review

## Credentials and Cryptography

Credentials are immutable values with copied key material. Formatting and
errors do not expose keys, tickets, connection secrets, challenges, or
transcript bytes. Key type and exact size are validated before use. Decryption
checks envelope version, magic or integrity tag, key usage, and all configured
size limits before returning plaintext.

AES-128 uses Ceph's fixed-IV CBC envelope and its bounded, non-oracular padding
removal; the decoded magic supplies the protocol's rejection check.
AES256 uses RFC 8009 AES256-CTS-HMAC-SHA384-192 through the maintained
`github.com/otuschhoff/gokrb5/v8` fork. Ceph key usages `0x03`, `0x04`, `0x05`,
`0x10`, `0x11`, and `0x12` are assigned to distinct protocol contexts.

## Negotiation and Transcript

The connector advertises secure mode by default. CRC mode requires explicit
`AllowCRC`; no no-auth mode is exposed. `AuthBadMethod` is classified as a
downgrade only when Ceph does not offer the requested method/mode intersection;
otherwise it is a credential rejection.

Transcript HMAC covers the exact pre-signature bytes in the required direction.
Secure framing begins with `AuthSignature`, and the same codec instance and
counters continue into session traffic. Server signature failure closes the
connection and cannot publish authenticated state.

## Ticket Lifecycle

A connector serializes complete connection attempts and retains global ID and tickets only after the complete authenticated
handshake succeeds. Reconnect snapshots an unexpired auth ticket, uses its
session key to decrypt renewed ticket blobs, and atomically replaces retained
state after signature verification. After auth-ticket expiry, reclaim identity
is cleared for the next handshake so Ceph can assign a fresh global ID. A failed reconnect leaves prior state
unchanged. Authorizer creation rejects and removes expired tickets. State and
secret-free metadata access are mutex protected and covered by race tests.

## Real Negative Cases

The disposable monitor test proves that a structurally valid wrong key is
rejected on wire and that a secure-only client rejects a monitor restricted to
CRC mode. Four-second real tickets exercise same-connector renewal and expiry.
Temporary keyrings are generated outside the repository and removed by traps.

## Human Review

Oliver Tuschhoff reviewed the content-addressed P03 tree on 2026-09-14,
including authentication and wire behavior, connector deadlines, ticket
lifecycle, replay and completion semantics, secrets and logging, licensing, and
the P03/P04 scope boundary. The review was approved with no findings, and
fixture redistribution was approved.
