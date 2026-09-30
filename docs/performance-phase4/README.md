# Phase 4: Bound Receive And Fanout Memory

Status: implementation and memory validation pass; **phase blocked on the secure
read latency exit gate**. No completion commit, native parity, or renewed P12/P13
certification is claimed. See [results](RESULTS.md) and [audit](AUDIT.md).
Phase 5 is not implemented.

## Approved Public Policy

The maintainer approved these finite defaults before implementation:

| Config field | Default | Option / CLI |
| --- | ---: | --- |
| `MaxSessions` | 256 | `max_sessions` / `--max-sessions` |
| `MaxReceiveBytes` | 268435456 (256 MiB) | `max_receive_bytes` / `--max-receive-bytes` |
| `MaxQueuedReceiveBytes` | 67108864 (64 MiB) | `max_queued_receive_bytes` / `--max-queued-receive-bytes` |

`Config{}` zero values select these defaults; they do not disable bounds.
The operation-timeout zero value retains its separate unlimited-time meaning.
`WithOption`, configuration files, `ParseArgs`, and `ParseEnv` recognize the
limits. Environment suffixes are `MAX_SESSIONS`, `MAX_RECEIVE_BYTES`, and
`MAX_QUEUED_RECEIVE_BYTES` under the explicitly selected prefix; the default
prefix is `GO_LIBRADOS_`. Values are positive decimal integers, with invalid,
zero, negative, and overflowing values rejected. `New` validates the effective
queue cap against the aggregate cap, allowing option setters in either order.
The session maximum also respects platform `int` and control-allowance overflow.

One client-wide budget is shared by MON, OSD, and MGR session configurations
and reused across Connect attempts. Admission precedes connector/transport
creation and remains charged during initialization, installation, retirement,
and terminal-but-not-yet-stopped sessions. Concurrent `Stop` callers join cleanup;
only joined cleanup releases the slot. At capacity, new admission is rejected.
There is no idle eviction, including no eviction of active operations, replay
obligations, watches, or sessions being installed. Larger deployments can select
higher positive limits, with proportionally larger reader/control allowances.

## Receive Envelope

For the built-in post-authentication receive path, default internal storage is:

- At most 256 MiB of charged receive backing, including active decode,
  read-ahead frames, owner-held frames, queued unsolicited messages, and buffered
  replies awaiting actual consumption.
- At most 72 KiB of fixed controls: `256 * 3 * 96` bytes. The three positions
  cover active decoder, read-ahead, and owner-held control frames.
- At most 128 MiB of built-in reader capacity: `256 * 512 KiB`. The reader
  size is unchanged; transport retirement joins active reads and clears the old
  reader before a production replacement constructs its reader.

This is an accounting/storage envelope, **not a whole-client heap or RSS cap**.
It excludes allocator size-class rounding, metadata, stacks/runtime memory,
pre-authentication decoding/auth state, decoded maps and service-owned state,
outbound storage, application-owned outputs, and allocations performed inside
custom codecs/transports before they return. Pre-authentication frames still
have the existing up-to-64-MiB limit, not a small aggregate handshake allowance.
The existing 320-MiB outbound allowance per session can nominally permit 80 GiB
across 256 sessions; this phase does not solve outbound or whole-client memory.

CRC reserves backing before payload allocation. Secure decoding reserves a
conservative peak before allocation, then accounts retained unique physical
backing, including padding/tags and the embedded prelude when retained. A
single private lease follows frame/message ownership. Copies do not multiply
the backing charge; concurrent copied-carrier release is idempotent. Custom
transport results are validated and detached into budgeted storage after return,
which cannot bound the custom implementation's prior allocation.

Actual consumer transfer releases the internal charge. A buffered result is
still internal until consumed. Budgeted unsolicited delivery hands off from the
owner's bounded queue through an unbuffered channel, not through an uncharged
buffered queue. Application-retained outputs remain valid and can grow without
bound after transfer; the application owns that resource decision. Outbound
clones do not carry receive ownership. Duplicate/stale/error/control/terminal
and Stop paths release their internal leases.

At aggregate decode or per-session unsolicited-queue saturation, the receiving
session fails terminally with an observable queue/budget error rather than
silently dropping required notifications, backoffs, or replies. Budget errors
are not retried through reconnect. This is bounded overload failure, not a claim
that operations always succeed under pressure. The session slot remains admitted
until `Stop` joins. A noncooperative connection Read can necessarily block that
join; this phase does not promise a fixed shutdown deadline.

Retired authentication renewal pumps have per-generation cancellation during
timer waiting and event forwarding. Fault, replacement, terminal failure, and
Stop cancel obsolete pumps. Auth Close does not close `RenewalDue`, which would
otherwise fabricate a renewal event.

## Reproduction

Run timing serially, without concurrent tests or reviewers, on the same host:

```sh
P4_RESOURCE_CAPTURE=1 P4_RESOURCE_OUT=/tmp/phase4-before-resource.json \
  GOTOOLCHAIN=go1.27.1 go test ./internal/msgr \
  -run '^TestPhase4ResourceCapture$' -count=1 -parallel=1 -timeout=180s
P4_BUDGETED_CAPTURE=1 P4_BUDGETED_OUT=/tmp/phase4-budgeted.json \
  GOTOOLCHAIN=go1.27.1 go test ./internal/msgr \
  -run '^TestPhase4Budgeted' -count=1 -parallel=1 -timeout=180s
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race \
  ./ ./internal/msgr ./internal/cephx \
  -run 'Test.*(Receive|ReadBudget|Renewal|Prelude)' -count=20 -timeout=180s
CGO_ENABLED=1 GOTOOLCHAIN=go1.27.1 go test -race ./...
GOTOOLCHAIN=go1.27.1 go test -tags p12diagnostics ./...
GOTOOLCHAIN=go1.27.1 go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=go1.27.1 \
  go build -tags p12diagnostics -o /tmp/phase4-linux-check ./integration/p07/benchmark
GOTOOLCHAIN=go1.27.1 go run ./tools/perf-baseline \
  -out /tmp/phase4-fresh-baseline -benchtime 100ms -count 5
P07_DIAGNOSTIC_DIR=/tmp/phase4-fresh-secure \
  GOTOOLCHAIN=go1.27.1 ./integration/p07/reproduce.sh
```

Resource helpers require Git metadata for the baseline HEAD field. In an
exported source snapshot, supply `GIT_DIR` and `GIT_WORK_TREE` explicitly and
bind results to the source manifest; HEAD alone does not identify dirty code.
Fake/loopback fanout measurements use ready sessions without protocol handshake
or authentication. Separate tests cover real auth-wrapper ownership and
client-wide service admission, not full live authenticated saturation.