# Project Specification: Pure-Go RADOS Client

Status: implementation contract; phases P00 through P11 are implemented and qualified.

## 1. Purpose

Build `rados-go`, an idiomatic Go library that lets Go applications and other libraries access Ceph RADOS directly: connect, discover pools, read and write objects, manipulate metadata, execute atomic object operations, and use the wider librados feature set incrementally.

This is a behavioral and wire-protocol port of the client functionality behind Ceph librados, not a line-by-line translation of its C/C++ implementation. It must communicate with unmodified Ceph monitors, OSDs, and, where required, managers.

**Pure Go is a hard requirement:** the distributed client builds and runs with `CGO_ENABLED=0`, without native librados, C/C++ libraries, dynamic-library loading, subprocesses, a gateway, or a companion proxy. A package that calls native librados without cgo does not satisfy this requirement. Standard Go runtime assembly and standard-library platform implementations are acceptable; a custom foreign-function bridge is not.

Native Ceph tools and a small native librados reference driver are allowed only in isolated development/test infrastructure. They must never become dependencies of the shipped module or its normal unit tests.

## 2. Compatibility Contract

### 2.1 Version Policy

- Interpret "v20+" as **Ceph release 20 and later**, initially the Tentacle 20.2 series. This is not the librados ABI version: the v20.2.0 public header identifies librados as 3.0.0 and explicitly distinguishes the two version numbers.
- Use Ceph `v20.2.0` as the initial reproducible source baseline, not a claim that it is the newest or recommended deployment patch. Phase P00 must resolve its commit SHA and select a maintained 20.2.x patch for integration testing, with immutable image digests.
- Pin a source revision, reference-driver build, server images, Go toolchains, and cluster configuration in the test manifest. Record them in every integration report.
- Support additional 20.x patches and later Ceph majors through negotiated capabilities and explicit certification. Do not advertise compatibility with an untested future release merely because its version is greater than 20.
- Maintain a compatibility matrix across server version, authentication mode, messenger revision, pool type, CRUSH features, Go version, and client OS/architecture.
- At each release, certify the selected supported 20.2.x patch and each later release claimed as supported. Add mixed-version upgrade testing before claiming support for that upgrade path.
- Initially support Linux and macOS clients on amd64 and arm64. Integration servers run on Linux; macOS development may use a Linux VM or remote disposable cluster. Windows is a later qualification target.
- P00 selects the minimum Go version from supported stable toolchains. CI tests that minimum and the latest stable Go; dependencies may not silently raise the minimum.

### 2.2 Compatibility Means Behavior, Not C ABI

Preserve RADOS object identity, wire encodings, permissions, error meaning, atomicity, ordering, completion semantics, and supported failure behavior. Use Go ownership, contexts, errors, and concurrency instead of C handles, manual buffer frees, and callback allocation APIs.

Inventory every public function in the pinned C header and the public operations in the C++ header. For each, record: source symbol, Go equivalent or intentional omission, phase, pool/version prerequisites, semantic differences, and conformance test. Functions representing C memory management need a documented Go equivalent, not artificial public wrappers. Deprecated and unsupported upstream APIs must be classified explicitly.

Release labels:

| Milestone | Required Outcome | Allowed Claim |
| --- | --- | --- |
| M0: protocol proof, through P04 | Authenticated secure monitor session and verified cluster identity/maps | Experimental connectivity only |
| M1: object alpha, through P07 | Replicated-pool object I/O with bounded failure handling | Experimental core RADOS client |
| M2: core beta, through P08 | Metadata, atomic operations, enumeration, resilience tests | Core API subset, subject to published matrix |
| M3: v1.0, through P12 | All v1-required families below, security review, qualification gates | Production-ready for the certified feature matrix |
| Broader parity, P13 onward | Remaining APIs and additional releases with conformance evidence | Only the individually verified extensions |

Do not describe M1 or M2 as a complete librados replacement. Any omission from the v1-required scope requires an explicit spec revision before release, not an undocumented implementation shortcut.

## 3. Functional Scope

| Family | v1.0 Requirement | Boundary or Constraint |
| --- | --- | --- |
| Connection/configuration | Explicit monitor seeds, client entity, cluster FSID check, credentials, supported config/keyring loading, reconnect, shutdown | Document supported configuration grammar and precedence; no promise to implement every Ceph option |
| Transport/authentication | Messenger v2.1, CephX, secure and explicitly enabled CRC sessions, IPv4/IPv6 address vectors | No automatic security downgrade; older messenger revisions are deferred |
| Discovery | Monitor failover, full/incremental OSD maps, pool name/ID resolution, CRUSH routing | Reject unsupported required features before sending object I/O |
| Object I/O | Create/exclusive create, stat with precise time/version, ranged read/write, write-full, append, truncate, zero, remove | Enforce sizes and offset arithmetic; respect pool restrictions |
| Object identity | Pool, object name, namespace, locator key, read snapshot and write snapshot context | Preserve bytes; no Unicode normalization or path semantics |
| Metadata | Get/set/remove/list xattrs; OMAP set/get/list/compare/remove/clear and range removal | Binary values and applicable binary keys; bounded pagination; pool constraints apply |
| Atomic operations | Single-object read/write operation builders, assertions, compare-extents, version checks, per-operation flags/results | One server-side compound operation, never client-side read-modify-write emulation |
| Enumeration | Pool/namespace iteration, cursor continuation and partitioning where mapped from the API | No global snapshot or globally sorted enumeration promise under concurrent mutation |
| Completion | Concurrent context-aware calls, documented ordering, write completion metadata, flush/drain | Go calls replace duplicated sync/aio entry points; no fake "safe" callbacks |
| Coordination | Watch/notify/ack, watcher status/listing where exposed, reconnect, shared/exclusive locks, renew/unlock/list/break | Locks use Ceph class methods; watches are not a durable event log |
| Snapshots | Pool snapshots and self-managed snapshot IDs/contexts, snapshot reads, rollback, metadata/listing | Respect upstream mode incompatibilities and pool limitations |
| Server-side classes | Generic class/method execution and compound-operation integration | Preserve method results; no automatic retry assumption for unknown methods |
| Administration | Cluster/pool statistics, monitor/manager/OSD/PG commands, pool create/delete, application metadata, blocklisting/session addresses | Explicit privileges and destructive APIs; no admin capability required for ordinary I/O |
| Specialized I/O | Writesame, server-side checksum operations, allocation hints, sparse/extent and clone/copy operations exposed by the inventory | Each operation needs its own semantic tests; no silent non-atomic emulation |
| Pool types | Replicated pools and a certified erasure-coded pool profile with supported operations | Servers perform replication/erasure coding; reject or propagate unsupported operations faithfully |

Deferred scope: messenger v1 and v2.0, optional wire compression, cache-tier/legacy operations, deprecated aliases without distinct behavior, service registration/status beacons, cluster-log subscriptions, and unqualified CRUSH features. List each deferred symbol explicitly in the API inventory. A release can support a modern cluster without implementing all historical cluster configurations.

Out of scope: a Ceph server, client-side replica management or Reed-Solomon coding, RBD image-format implementation, CephFS, RGW/S3, local caching with new consistency semantics, multi-object transactions, and drop-in source compatibility with existing cgo wrappers. The RADOS primitives may enable separate Go RBD or other higher-level libraries later.

## 4. Architecture

The public package should be named `rados`; choose the actual Go module import path before initialization. The following paths are planned ownership boundaries, not instructions to scaffold empty packages all at once.

| Package/Area | Responsibility |
| --- | --- |
| Root `rados` | Public client, pool views, object references, operations, errors, documentation |
| `internal/encoding` | Bounded Ceph binary encoders/decoders and versioned envelopes |
| `internal/protocol` | Message types, feature namespaces, opcodes, wire-level value types |
| `internal/msgr` | Framing, TCP sessions, queues, handshakes, sequencing, reconnect and keepalive |
| `internal/cephx` | Credential parsing, challenges, tickets, authorizers, renewal and session secrets |
| `internal/mon` | Bootstrap, monitor sessions, subscriptions, monitor commands and global identity |
| `internal/maps` | Immutable monitor/OSD/pool map snapshots and incremental application |
| `internal/crush` | Exact placement algorithms, tunables, hashes and rule evaluation |
| `internal/objecter` | Target selection, OSD sessions, request identities, inflight tracking, completion and remapping |
| `internal/mgr` | Manager map/session handling and supported manager operations |
| `internal/cls` | Encodings for supported built-in class clients, starting with locks |
| `internal/testutil` | Deterministic clocks, fake peers, fault scripts and fixture support |
| `integration` | Real-cluster conformance and failure tests, behind an explicit integration gate |
| `testdata`, `docs`, `tools` | Provenance-tagged fixtures, protocol decisions and isolated reference tooling |

Normal object I/O follows public API -> objecter -> map/placement lookup -> OSD messenger session. Monitors provide authentication, discovery and map updates; they do not proxy object data. Managers are used only for operations that require them.

Keep protocol mechanics separate from routing policy and public API behavior. Use small interfaces only at real substitution points such as transport dialing, clock, and test oracle. Do not mirror Ceph's C++ inheritance hierarchy or invent a general RPC framework.

### 4.1 Protocol and Placement Invariants

- Implement exact byte order, signedness, length prefixes, version/compat-version envelopes, feature-dependent layouts, checksums and alignment. Go struct layout and `gob` are not wire formats.
- Keep messenger feature bits distinct from Ceph message/client feature bits. Advertise only implemented, tested capabilities. Unknown optional fields may be skipped only where the wire contract explicitly permits it; unknown required semantics must fail closed.
- Bound frame, segment, message, collection and allocation sizes before allocation. Validate arithmetic overflow, truncation, nesting and malformed version envelopes. Do not expose unverified secure-frame plaintext to message handlers.
- Implement messenger v2.1 CRC and secure framing, including pre-auth framing, transcript authentication, message acknowledgments, session cookies, sequence counters and reset behavior. TCP reconnect is not equivalent to session resume.
- Use Go crypto primitives for CephX's exact wire constructions and messenger AES-GCM, including nonce derivation, direction separation and rollover protection. Messenger secure mode is not TLS; substituting `crypto/tls` would not interoperate.
- Track monitor authentication separately from OSD/manager service authorization. Renew tickets before expiry and test expiration, rejected authorizers, monitor failover and global-ID reclaim rules.
- Apply full and incremental maps atomically with FSID and epoch checks. On missing increments, obtain a usable full map. Prevent older snapshots from replacing newer state and bound retained map history.
- Reproduce object hashing, namespace/locator handling, stable PG mapping, CRUSH rules/tunables, device classes, weights, choose arguments, upmap/upmap-items and applicable primary-selection/temp overrides for the certified profile. Include newer primary/upmap variants when encountered in the target source/configuration.
- Validate the entire object -> PG -> up/acting set -> primary/shard calculation against upstream results. A matching CRUSH output alone does not establish correct routing.
- Never substitute a generic consistent-hashing or weighted-random library for CRUSH. A specialized pure-Go implementation may be reused only after exact differential validation and license review.
- Route to the supported primary and preserve map epochs, retry metadata, redirects, backoff messages and request identity as required by the pinned Objecter behavior. Do not implement client-side writes to all replicas or EC shards.

## 5. Go API and Semantics

Prefer concrete types: `Client`, immutable `Pool` views, `ObjectRef`, `ReadOp`, `WriteOp`, `ObjectInfo`, `OpResult`, `Watch`, and typed errors. Freeze exported signatures in a small API design review during P01; do not make wire types public for convenience.

- Every network operation accepts `context.Context`. `Connect`/dial is cancellable. Require an explicit operation timeout policy and finite dial/handshake timeouts; document how caller deadlines and configured defaults interact.
- `Client` and immutable pool/object views are concurrency-safe. Deriving a namespace, locator, or snapshot view must not mutate state used by another goroutine. Operation builders are single-use and not concurrency-safe; submission freezes their contents.
- Expose offset-based I/O with explicit lengths and checked 64-bit offsets. Optional `io.ReaderAt`/`io.WriterAt` adapters must follow Go EOF/short-I/O rules and describe how contexts are bound to the adapter.
- A native `Read` may return fewer bytes at object EOF; distinguish a missing object from an existing empty object. Do not silently split an atomic write-full or compound operation to bypass protocol limits. Streaming/chunked helpers must state their weaker atomicity.
- Return object versions with the operation result, not through a shared mutable "last version" field. Preserve nanosecond timestamps where supplied.
- Treat object names, locators and namespaces as byte-preserving Go strings, and metadata values as bytes. Apply upstream per-operation constraints explicitly, including NUL limitations where applicable; enumeration must preserve names returned by the server.
- Caller input buffers are immutable for the duration of a synchronous call and reusable on return. If requests remain queued or replayable after cancellation, internal ownership must prevent further access to caller buffers. Returned byte slices are caller-owned unless an explicit advanced API states otherwise.
- Use Go goroutines for concurrent calls over a shared asynchronous request engine. Add public futures only if needed for an inventoried semantic gap; do not duplicate every method into sync and async variants.
- Document per-session and per-object ordering precisely. Concurrent calls do not inherit an ordering from goroutine scheduling. Explicit ordered operations/barriers must map to verified Ceph semantics.
- Successful writes mean the completion condition verified for the pinned Ceph implementation and pool settings, not socket write completion or messenger ACK. Determine the modern commit/complete relationship from source and failure tests; stale C-header prose is insufficient. Do not promise survival beyond the cluster's configured durability guarantees.
- `Flush(ctx)` covers writes submitted before a defined sequence watermark and waits for their verified completion; it is not a multi-object transaction. Specify treatment of outstanding unknown outcomes and report errors rather than converting them to success.
- `Close()` is idempotent, rejects new calls, terminates internal workers, and settles pending callers without implying that interrupted writes were rolled back. Provide `Shutdown(ctx)` for bounded graceful drain followed by resource cleanup. A caller that requires successful writes must explicitly observe their results or a successful drain.
- Watch dispatch cannot block the network receive loop. Bound event queues; overflow or a broken watch must surface an error and possible event loss, never silent dropping. Acknowledgment remains explicit and timeout-aware.
- Lock APIs expose cookie/owner, mode, duration and renewal semantics. Do not imply that a lock provides fencing or universally prevents unrelated writes; higher-level coordination must use Ceph's actual enforcement and blocklisting semantics.

### 5.1 Errors and Retry Safety

Provide a structured error containing operation, relevant target, Ceph wire error code, classification and underlying cause. Support `errors.Is`/`errors.As` for not-found, exists, permission, unsupported feature, invalid argument, quota/full, conflict, timeout, closed and outcome-unknown conditions.

Ceph wire errno values are not necessarily the client's host OS errno numbers. Preserve the original signed result and translate explicitly on macOS and other platforms. Separate transport errors, OSD errors, compound sub-operation results, compare mismatch offsets and class-method return values. Keep partial useful results where upstream returns them, such as notify acknowledgments on timeout.

Cancellation stops waiting and prevents unsent work from being dispatched, but does not undo a mutation already accepted by an OSD. If a mutation may have executed and no definitive reply is available, return an outcome-unknown error that retains any context deadline/cancellation cause.

Retries must follow a documented state machine: not submitted, submitted, acknowledged at transport, completed by OSD, or outcome unknown. Preserve the relevant Ceph request identity and retry/replay metadata across supported reconnects and remaps. Never implement "retry every write on timeout". Appends, class methods, and even apparently idempotent writes under concurrent writers require special care. After identity/deduplication guarantees are lost, report ambiguity instead of resubmitting as a new operation. Do not promise exactly-once execution across arbitrary crashes.

Bound per-connection/global inflight requests, queued bytes, connection counts and retries. Backoff must use jitter and respect caller deadlines; blocked/full/paused pools must not cause tight loops. A request cancellation must not indiscriminately set a shared connection deadline and abort unrelated calls.

## 6. Go-Native Dependencies and Code Quality

Prefer the standard library: `net`, `net/netip`, `context`, `encoding/binary`, `encoding/json`, `io`, `bytes`, `crypto/aes`, `crypto/cipher`, `crypto/hmac`, appropriate hash primitives, `crypto/rand`, `hash/crc32`, `sync`, `time`, `errors`, `log/slog`, `testing`, and profiling tools.

Choose third-party libraries only where they remove substantial well-understood work. Candidates include `golang.org/x/sync` for bounded concurrency, optional OpenTelemetry adapters, or a maintained pure-Go compression implementation if compression is later enabled. These are candidates, not required dependencies. Verify protocol-specific CRC initialization/finalization and hash seeding against fixtures rather than trusting matching algorithm names.

Before adopting a dependency, record its purpose, license, maintenance/security posture, supported Go version, transitive graph, cgo/FFI behavior, and replacement cost. Keep telemetry exporters optional and out of the core dependency graph where practical. A generic INI parser is acceptable only if tested against the supported Ceph config/keyring grammar; do not silently misparse Ceph syntax.

Required engineering practices:

- Small cohesive packages, explicit ownership, bounded resource use and documented exported contracts. No hidden singleton client, background connection in `init`, or process-global logging/flag changes.
- Table-driven tests and real failure-path tests, deterministic clocks/random sources where appropriate, fuzzable pure codecs, and benchmarks before optimizing buffers or pooling.
- No unsafe zero-copy tricks in the initial implementation. Later exceptions require measured benefit, bounds/ownership evidence and review. Hand-written cryptographic primitives are prohibited.
- `gofmt`, `go vet`, pinned static analysis, `govulncheck`, module verification and dependency/license checks in CI. No unexplained ignored errors or broad lint suppressions.
- No panics from malformed network data or ordinary API misuse. No goroutine, socket, timer or buffer leaks after cancellation and shutdown.
- Logs/metrics cover connection state, auth renewal, map age/epoch, inflight work, retries, backoff, latency and failure categories. Never log credentials, tickets, session secrets or object payloads. Avoid raw object names and unbounded labels by default.
- Standard-library crypto does not by itself constitute a FIPS certification or a security review of this protocol implementation.

### 6.1 Licensing and Provenance

Choose the project license only after reviewing the licenses of the specific upstream files and any reused Go implementation. The pinned librados C header is LGPL-2.1-or-later; other Ceph files must be checked individually. A translated implementation is not automatically free of upstream obligations because it uses a different language.

Record whether each component is independently implemented from protocol facts, adapted, or directly ported, with source revision and applicable notices. Do not assume a clean-room claim or a permissive project license is appropriate. Resolve distribution obligations before copying/translating code or shipping. Generated fixtures and native oracle tooling also need provenance and redistribution review.

## 7. Verification Strategy

Use three independent evidence layers: byte-level fixtures from pinned upstream tools/source, behavior comparison with native librados, and tests against real Ceph clusters. Encoder/decoder round trips and a mock server written from the same assumptions cannot establish interoperability by themselves.

### 7.1 Test Infrastructure

P00 establishes an isolated Linux cluster runner with monitors, manager and enough OSDs/failure domains for the certified replicated and EC profiles. Use distinct read-only, read/write, namespace-restricted and administrative identities. Keep test secrets out of version control; synthetic protocol fixtures must not expose reusable credentials.

Use a separate native reference driver with structured requests/results for operations not covered by the CLI. Run equivalent operation histories in isolated namespaces/objects and compare bytes, errors, sub-results, versions and externally visible state, normalizing only documented nondeterminism. Transport captures are supplementary; encrypted traffic alone does not provide a semantic oracle.

Unit tests require no cluster. Integration tests require explicit selection and a validated disposable-cluster identity. Cluster mutation, daemon termination, network partitioning and pool deletion must never target a user's production cluster by default. CI can use native build tools in its oracle job while the Go client job remains cgo-disabled.

### 7.2 Mandatory Coverage

- Codec fixtures: all supported versions, empty/max/truncated fields, unknown compatible extensions, oversized allocation claims, integer overflow, CRC/tag corruption and aborted frames.
- Authentication: valid/wrong keys, wrong FSID, denied caps, restricted namespace, ticket expiry/renewal, altered handshake, required-secure rejection of CRC, nonce rollover protection and reconnect key separation.
- Routing: many object names, binary names, namespaces/locators, non-power-of-two PG counts, pool IDs, weight changes, device classes, CRUSH tunables, temp/upmap overrides, PG splits/merges and removed pools. Require zero mismatches for the declared profile.
- Data semantics: zero-byte and large objects, sparse ranges, short reads, truncate/append/write-full, concurrent conditional updates, compound-op failure atomicity, snapshot isolation/rollback and EC restrictions. Verify both native-write/Go-read and Go-write/native-read.
- Failure semantics: disconnect before send, mid-frame, after server mutation but before reply, monitor loss, primary change, OSD restart, stale maps, blocklisting, full/quota/paused pools, delayed/duplicate messages, canceled calls and shutdown races.
- Coordination: two independent clients, mixed native/Go locks and watches, lease renewal/expiry, watch re-registration, slow consumers, lost events, notify timeouts and partial acknowledgments.
- Long-running behavior: sustained map churn and credential renewal, repeated connect/close, bounded queues under unavailable OSDs, leak checks, and the race detector.

### 7.3 Gates and Measurements

Each change runs the smallest applicable tests first, then required CI gates. Once the module exists, the baseline commands are:

```sh
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go build ./...
go vet ./...
go mod verify
```

Run `go test -race ./...` in a separate supported CI environment: Go's race detector can require cgo and a C toolchain. This tooling exception does not relax the cgo-disabled production build requirement. Cross-build supported OS/architecture targets and audit the selected dependency graph for native imports/FFI; run representative binaries without Ceph client libraries installed. Static-analysis and vulnerability-tool versions are pinned in CI.

P01 creates exact package-scoped commands for unit, differential, fuzz and integration suites. New protocol decoders require checked-in fuzz seeds and at least a 60-second fuzz smoke run per affected target; nightly jobs fuzz protocol/state-machine targets for a longer fixed budget. Correctness-critical routing/auth/replay logic requires invariant and negative tests, not just a coverage percentage.

Before v1.0: zero known critical/high security findings without resolution, zero known data-integrity defects, all advertised conformance cases passing, no races, and a reproducible 24-hour soak on the qualification cluster covering auth renewal and recovery. Record resource budgets and verify no unbounded growth. Human security review of auth/framing and human distributed-systems review of replay/completion are mandatory gates.

Benchmark against native librados on the same cluster/configuration using 4 KiB, 64 KiB, 1 MiB and 4 MiB objects, concurrency 1/16/64, read/write/mixed workloads, and both certified transport modes. Record throughput, IOPS, p50/p95/p99 latency, CPU, allocations, resident memory and environment. P07 records the first baseline; P12 must meet numeric budgets approved from that baseline or obtain an explicit release waiver with evidence. No unmeasured performance-parity claim is allowed. Correctness may not be weakened to meet a benchmark.

## 8. LLM-Executable Implementation Phases

Phases are dependency-ordered milestones, not single prompts or context windows. Each numbered work item below is a candidate task and must be split further when it spans several independent behaviors. Infrastructure-dependent gates are blocked, not passed, when no real cluster/oracle is available.

### P00: Evidence, Scope and Oracle

Dependencies: none.

1. Pin Ceph source/commit, server images and reference toolchain; verify their availability. Review licensing and choose a module path and Go support policy.
2. Produce the C/C++ API inventory, compatibility matrix and protocol-source index. Select a concrete default replicated/EC/CRUSH profile and list unsupported features.
3. Establish the disposable cluster runner, native reference driver and fixture manifest format. Demonstrate a native write/read/delete and a reference object-to-PG/primary lookup.

Exit gate: the reference smoke test is reproducible from documented commands; every API family is classified; unresolved protocol questions and review owners are recorded. Deliver an initial threat model and license decision before implementation.

### P01: Minimal Module and Binary Primitives

Dependencies: P00.

1. Initialize the Go module, CI gates, public API/error/lifecycle decisions, and only the packages needed for this phase.
2. Implement bounded primitive codecs, versioned envelopes, entity/address types, feature namespaces and wire errno handling in separately tested increments.
3. Add independent fixtures, fuzz harnesses, provenance validation, and deterministic fake clock/transport support as needed.

Exit gate: cgo-disabled tests/build pass; primitive encodings match the pinned fixtures; truncation/overflow fuzz tests do not panic or exceed allocation limits. Public ownership, timeout and shutdown contracts are documented.

### P02: Messenger Framing and Session Core

Dependencies: P01.

1. Implement banner/feature negotiation and v2.1 CRC frame encoding/decoding, one frame family per task.
2. Implement message framing, read/write pumps, bounded queues, transaction dispatch, ACK/keepalive and scripted session transitions.
3. Implement v2.1 secure frame codecs using deterministic test secrets, plus disconnect/reset/reconnect handling independently from live authentication.

Exit gate: upstream-derived frame vectors pass; fake-peer fragmentation, corruption, replayed sequence, reset, queue saturation and cancellation tests pass. Protocol-only synthetic peers cannot be described as a connected Ceph client.

### P03: CephX and Secure Session Integration

Dependencies: P02.

1. Implement explicit credentials and the supported keyring subset, then CephX challenge/ticket exchanges and authorizer generation.
2. Integrate transcript authentication and connection-secret handling with messenger secure mode. Keep no-auth support, if useful for tests, inaccessible in the production configuration surface.
3. Implement bounded ticket renewal and identity/session recovery, with negative tests for each state transition.

Exit gate: authenticate a messenger session with a real pinned monitor using CephX and secure v2.1; invalid credentials and prohibited downgrades fail clearly. Synthetic and real expiry/reconnect tests pass before advancing. Security-sensitive code receives focused review.

### P04: Monitor Client and Map Lifecycle

Dependencies: P03.

1. Implement seed parsing, bounded DNS resolution, monitor selection/failover, FSID checks and subscriptions. Supported config precedence is defaults < explicitly loaded file < explicit programmatic options; environment loading is opt-in with documented placement in that order.
2. Decode monmap, full OSDMap and required pool/CRUSH data; add incremental updates in small version-specific tasks.
3. Publish immutable map snapshots, detect gaps/stale epochs, refresh maps and expose pool lookup plus a read-only monitor command.

Exit gate: a cgo-disabled program reaches M0, reports the correct cluster/pools, observes a real map change and recovers from monitor loss without admitting a different FSID. Full-map and incremental paths converge on equivalent snapshots.

### P05: Exact Placement

Dependencies: P04.

1. Implement object identity hashing and PG selection with independent vectors.
2. Implement the selected CRUSH bucket algorithms, rule execution, tunables, weights and choose arguments in individually differential-tested tasks.
3. Apply OSDMap up/acting and primary/shard overrides; add PG-count changes and unsupported-feature rejection.

Exit gate: zero object-to-PG/acting-primary mismatches against the pinned oracle across the certified placement corpus, including map transitions. Unsupported rules must fail explicitly, not produce a plausible but incorrect target.

### P06: Read-Only Object Path

Dependencies: P05.

1. Implement OSD service authorization, OSD request/reply codecs, inflight registration and result decoding.
2. Wire public stat and ranged read to primary routing, including namespace/locator views and operation versions.
3. Handle read cancellation, map refresh, redirects, backoff and bounded read recovery without adding mutation retries yet.

Exit gate: read native-written objects and metadata from a real cluster; byte-for-byte contents and error behavior match native librados. Corrupt/late replies, missing objects, stale maps and primary changes are tested.

### P07: Mutations, Completion and Recovery

Dependencies: P06.

1. Implement create, write, write-full, append, truncate, zero and remove as separate tested operations.
2. Implement request identity, resubmission/remap rules and completion states from pinned Objecter behavior; prove transport ACK cannot complete a write.
3. Add outcome-unknown errors, inflight bounds, flush watermark, drain/close semantics and failure injection around each mutation boundary. Record initial performance/resource baselines.

Exit gate: M1 passes mixed native/Go CRUD and deterministic disconnect/retry tests, including append without accidental duplication. Ambiguous outcomes are surfaced; completion/durability behavior is documented from source and real failure evidence.

### P08: Metadata, Transactions and Enumeration

Dependencies: P07.

1. Implement xattrs, then OMAP operations and pagination, each with reference tests for binary data and boundary/error behavior.
2. Implement single-object compound read/write builders, assertions, compare operations, flags and per-sub-operation results.
3. Implement object listing, namespaces, cursor continuation/partitioning and map-change behavior.

Exit gate: M2 passes cross-client compare-and-write contention, compound failure atomicity, metadata ordering/bounds and enumeration conformance. No client-side sequence of separate requests is presented as atomic.

### P09: Class Execution, Locks and Watch/Notify

Dependencies: P08.

1. Implement generic class execution, input/output/result preservation and conservative retry classification; add built-in lock class encodings separately.
2. Implement lock acquire/renew/release/list/break with two-client native/Go interoperability tests.
3. Implement watch registration, liveness, notify/ack, partial timeout results, re-registration and bounded callback/event dispatch.

Exit gate: mixed clients interoperate for all supported coordination operations during OSD restart and remap. Queue overflow, lost-watch state and ambiguous class execution are observable; shutdown does not leak callbacks or workers.

### P10: Snapshots, EC and Specialized Object Operations

Dependencies: P09.

1. Implement pool snapshots and self-managed snapshot contexts separately; test reads, rollback, removal and ordering/sequence validation.
2. Qualify EC routing/shard metadata, alignment and overwrite restrictions, with successful and rejected operations on the selected profile. Do not assume OMAP or every replicated-pool operation is available on EC pools.
3. Implement remaining inventoried v1 specialized operations in individual tasks: writesame, checksum, hints and applicable sparse/clone/copy APIs.

Exit gate: snapshot histories match the oracle; the replicated/EC capability matrix is backed by tests, including unsupported cases. Specialized operations preserve atomicity, partial-result and server-error semantics.

### P11: Administrative and Manager Operations

Dependencies: P10.

1. Implement manager map/session and failover where required, then command transport for monitor/manager/OSD/PG targets with bounded structured payloads.
2. Add cluster/pool statistics, pool administration, application metadata, session addresses and blocklisting in separate tasks.
3. Close gaps in the public API inventory and document every adapted, unsupported or deferred entry.

Exit gate: all v1-required inventory entries have passing conformance tests; ordinary I/O works with least-privilege caps; destructive tests run only against validated disposable resources. Manager loss does not unnecessarily break ordinary OSD I/O.

### P12: Production Qualification and v1.0

Dependencies: P11.

1. Run the complete certified version/platform/configuration matrix, chaos scenarios, fuzz campaigns, race tests and 24-hour soak; fix discovered defects in the owning phase/package.
2. Profile and optimize measured bottlenecks without changing wire/semantic contracts; evaluate the numeric budgets approved from P07.
3. Complete independent security and distributed-systems reviews, dependency/license audits, compiled examples, operational troubleshooting, migration guidance and reproducible release artifacts.

Exit gate: every requirement in Section 7 passes; the release includes the exact compatibility matrix, known limitations, API inventory, benchmark evidence and security reporting process. No skipped infrastructure-dependent test is counted as a pass.

### P13: Wider Parity and Future Ceph Releases

Dependencies: P12.

1. Diff the new pinned Ceph headers and protocol/source paths against the certified baseline; classify wire changes, security fixes and new API surface before coding.
2. Add later-release features, deferred APIs, additional placement profiles or optional transport modes one at a time with explicit compatibility/security decisions.
3. Run old/new/mixed-server conformance and upgrade tests; publish the new matrix and migration notes. Follow Go semantic versioning for public API changes.

Exit gate: each newly advertised capability has independent evidence and regression coverage on previously supported releases. Forward compatibility remains a tested property, not an open-ended promise.

## 9. Execution Contract for Each LLM Task

Do not ask an LLM to "implement librados" or an entire complex phase in one pass. Each task should own one codec, operation, state transition, or tightly related testable behavior, normally within one package and one focused test suite. Start with evidence and a failing check, then implement the smallest coherent change.

Use this task packet:

```text
Task ID: Pxx-Tyy
Goal: one observable behavior
Prerequisites: completed task IDs and required cluster/oracle access
Scope: allowed files/packages; explicitly excluded behavior
Evidence: pinned upstream revision, symbols, fixture IDs and provenance
Contract: inputs/outputs, wire versions, errors, ownership, state transitions
Invariants: security, ordering, atomicity and resource bounds to preserve
Discriminating test: exact command and expected failure before implementation
Deliverables: code, focused tests, fixtures and documentation updates
Acceptance: exact commands and observable assertions
Stop conditions: missing evidence, conflicting upstream behavior, required review
Handoff: changed files, actual test results, remaining risks and next task
```

Task rules:

1. Inspect the owning upstream path and a relevant test/fixture. Cite symbols and revisions in the task notes. Do not infer wire fields or feature bits from names alone.
2. Establish one falsifiable local hypothesis and its cheapest independent check. Implement only enough to exercise that check, then validate before expanding scope.
3. Keep unsupported production paths explicit. Never merge a stub that silently succeeds, fabricated fixture, native fallback, or guessed opcode to make a test pass.
4. Run package-scoped tests after each behavioral change, then the phase's conformance gate. Report commands actually run, with exits and environment; do not substitute a mock test for a required live-cluster result.
5. Update the API/compatibility inventory and any relevant state diagram or architecture decision. Do not reformat unrelated files or rewrite approved interfaces without a scoped design change.
6. Stop and record a blocker when a protocol contract cannot be verified. Resolve conflicts among docs, source and observed behavior using the pinned implementation and an independent reproducer; never quietly pick a convenient interpretation.
7. Require a separate review pass for crypto/transcript handling, parsing bounds, CRUSH math, mutation replay, completion and snapshot correctness. LLM self-review supplements but does not replace the required human release reviews.
8. Persist a short handoff with completed/pending tasks, fixture provenance and unresolved questions so the next session does not repeat discovery or assume unverified work is complete.

Independent codec and fixture tasks may be parallelized only after their shared wire-type contracts are frozen. Map/CRUSH work can be separated at that boundary; authentication/session changes and mutation replay changes should not proceed against unstable shared interfaces. Avoid concurrent edits to the public API or shared feature definitions.

## 10. Principal Risks and Decisions

| Risk | Required Mitigation |
| --- | --- |
| Public librados headers hide a large distributed client implementation | Port the protocol/Objecter behaviors, not just the exported functions; prove connectivity and routing early |
| Wire details and comments disagree or evolve | Pinned source, golden vectors, native oracle and recorded discrepancies |
| Placement works on a toy pool but fails on real maps | Differential full-routing corpus and fail-closed unsupported configurations |
| Retry duplicates a mutation or reports false success | Explicit identities/state machine, outcome-unknown errors and lost-reply fault tests |
| CephX/secure framing is subtly insecure | Standard primitives, exact vectors, negative tests and independent security review |
| Library consumes unbounded memory under network failure | Byte/request/connection budgets, backpressure and soak tests |
| EC/snapshot/class semantics differ from basic replicated I/O | Separate qualification matrices and per-operation conformance |
| cgo or native loading enters through dependencies | Dependency audit plus actual cgo-disabled binary execution without native Ceph libraries |
| "v20+" or "full parity" overstates support | Published finite certification matrix and symbol-level inventory |
| Upstream translation creates licensing obligations | File-level provenance and license decision before reuse/distribution |
| LLM produces plausible but unverified protocol code | Small evidence-backed task packets; blocked gates remain blocked |

This is a substantial distributed-systems client project. Do not estimate completion from the number of API wrappers or assume it fits in a few autonomous sessions. Re-estimate after P00's inventory, P03's live authentication proof, and P07's mutation/recovery proof, using completed tasks and measured review effort.

## 11. Upstream Reference Anchors

The public C header and messenger document below were consulted while drafting this specification. They establish API scope and messenger requirements, not an exhaustive implementation audit. The other entries are the pinned implementation starting points to inspect in P00 and the relevant tasks. Replace tag-only references with resolved commit links in the implementation evidence manifest.

- [Public librados C API, v20.2.0](https://github.com/ceph/ceph/blob/v20.2.0/src/include/rados/librados.h): exported operations, API adaptation inventory, ownership/completion caveats and ABI version distinction.
- [Public librados C++ API, v20.2.0](https://github.com/ceph/ceph/blob/v20.2.0/src/include/rados/librados.hpp): richer operation surface and semantics to inventory.
- [Messenger v2 protocol, v20.2.0](https://github.com/ceph/ceph/blob/v20.2.0/doc/dev/msgr2.rst): revision negotiation, CRC/secure framing, authentication, sessions and replay-related control flow.
- [Messenger implementation](https://github.com/ceph/ceph/tree/v20.2.0/src/msg/async): concrete v2 state transitions and framing behavior.
- [CephX client implementation](https://github.com/ceph/ceph/tree/v20.2.0/src/auth/cephx): challenges, tickets, authorizers and session material.
- [Monitor client](https://github.com/ceph/ceph/tree/v20.2.0/src/mon): monitor bootstrap, subscriptions and authentication integration; start with `MonClient`.
- [Objecter implementation](https://github.com/ceph/ceph/blob/v20.2.0/src/osdc/Objecter.cc): request targeting, retries, remapping, backoff and completion; inspect its header as well.
- [OSD map implementation](https://github.com/ceph/ceph/blob/v20.2.0/src/osd/OSDMap.cc): full/incremental map application and effective placement.
- [CRUSH implementation](https://github.com/ceph/ceph/tree/v20.2.0/src/crush): exact hashing, bucket/rule algorithms, tunables and encodings.
- [Librados implementation](https://github.com/ceph/ceph/tree/v20.2.0/src/librados): public-to-internal behavior and modern completion semantics.
- [Ceph message definitions](https://github.com/ceph/ceph/tree/v20.2.0/src/messages): message encodings and feature/version-dependent fields.
- [Ceph object classes](https://github.com/ceph/ceph/tree/v20.2.0/src/cls): lock and other class-client payload contracts.
- [Librados tests](https://github.com/ceph/ceph/tree/v20.2.0/src/test/librados): behavioral cases for the conformance suite, subject to per-file license review.

Do not use moving `latest` documentation as the sole authority for a pinned release. Do not interpret the existence of an upstream symbol as evidence that its behavior has already been implemented or qualified in Go.