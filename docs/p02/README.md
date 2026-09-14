# P02 Messenger Framing and Session Core

Status: **implementation present; qualification incomplete**.

P02 provides pure-Go messenger v2.1 banner, CRC and secure framing, control and
message payloads, and a bounded scripted session core. Its integration surface
is synthetic protocol peers. It has not authenticated to a live Ceph monitor
and makes no Ceph connectivity or interoperability claim. The checked-in
fixtures come from a source-derived independent oracle; they are not vectors
produced by executing Ceph's `FrameAssembler`.

## Records

- `architecture.md`: package ownership, resource bounds, and session state
  machine.
- `provenance.md`: pinned source anchors, fixture origin, and reproduction
  procedure.
- `security.md`: trust boundaries, deterministic-secret restrictions, ACK,
  replay, reset, cancellation, and connector contracts.
- `tasks.md`: implementation and exit-gate status, including current local
  validation truth.
- `task-handoff.md`: unresolved P02 qualification and the P03 boundary.

Run the non-fuzzing development gate with `make verify-p02`. The broader local
gate is `make verify-p02-all`; it adds race/static/vulnerability checks, pinned
fixture reproduction, and six 60-second fuzz campaigns. The nightly CI fuzz
gate runs the same six targets for five minutes each.

`make integration-p02` records the current integration boundary; it does not
contact a cluster. P02 remains incomplete until actual upstream-executed Ceph
`FrameAssembler` vectors are captured and compared, regardless of the current
fixture parity results.