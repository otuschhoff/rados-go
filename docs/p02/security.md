# P02 Threat and Security Notes

## Boundary

P02 processes attacker-controlled frame descriptors, lengths, padding, tags,
checksums, ciphertext, control transitions, sequence numbers, and message
metadata. Decoders validate counts, arithmetic, configured sizes, alignment,
reserved fields, CRCs or authentication tags, padding, and late status before
returning a frame. Secure plaintext is not exposed after authentication
failure, and nonce counters are consumed even by failed records.

Secure fixtures and tests use deterministic secrets only. They prove exact
codec behavior; they are not credentials, entropy, CephX evidence, transcript
authentication, key lifecycle evidence, or permission to use deterministic
secrets in production. P03 owns real credentials and authenticated secret
establishment. Logs and failures must not expose secrets or payloads.

## ACK and Completion

A messenger `Ack` only removes acknowledged outbound sequences from the replay
set and emits an acknowledgment event. It does not complete a submitted
transaction. Completion requires a matching response transaction ID. This is
covered by `TestSessionAckDoesNotCompleteAndIncomingSequenceRules`; P07 still
owns object-operation completion and durability semantics.

## Replay, Reset, and Cancellation

- Under `ReplayPending`, reconnect preserves original message sequence and
  transaction identity for eligible unacknowledged work. `FailPending` instead
  settles pending calls with a disconnect error.
- `SessionReconnectOK` trims sequences acknowledged by the peer before replay.
  Duplicate inbound sequences are dropped and gaps are surfaced as events.
- Partial reset preserves replayable request identity while clearing peer
  connection state. Full reset fails pending calls and clears cookies,
  sequences, transaction state, retained bytes, and replay state.
- Cancellation removes queued or in-flight bookkeeping and releases retained
  bytes. It stops local waiting and replay eligibility; P02 does not claim to
  undo remote work already observed by a peer.
- Connector failures retry immediately until `MaxReconnectAttempts` is
  exhausted. The current session implements no delay, jitter, or backoff.

## Context and Socket Contract

`Connector.Connect(ctx)` must honor the supplied session-lifetime context and
return a fresh, already authenticated `Transport` or an error. Session stop
cancels that context; late connector results are ignored and any returned
transport is closed. Connector implementations must not retain the context
beyond the call or substitute a detached background context.

P02 deliberately exposes no socket deadline mutation through `Transport`.
Cancellation must not call `SetDeadline`, `SetReadDeadline`, or
`SetWriteDeadline` on a shared connection because that could abort unrelated
operations. `Transport.Close` is the interruption mechanism for blocked frame
I/O; per-request cancellation is handled by the session owner.

The current peers are synthetic and unauthenticated at the Ceph protocol
level. These tests make no live authenticated Ceph connectivity claim.