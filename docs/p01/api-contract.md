# P01 Public API and Lifecycle Decision

Status: accepted for P01. The only implemented operational surface is the error
taxonomy; client methods remain unavailable until their owning phases.

## Frozen Initial Surface

The complete reviewed v1 signature catalog is in `public-api.md`. The subset
below highlights the lifecycle and core object path whose semantics are fixed
by this decision.

The initial client, immutable view, and core object-I/O surface is approved with
these signatures. Later phases may add methods without changing these:

```go
type SecurityMode uint8
const (
	SecurityModeSecure SecurityMode = iota
	SecurityModeCRC
)

type Config struct {
	Monitors         []string
	Entity           string
	ClusterFSID      string
	Key              []byte
	SecurityMode     SecurityMode
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	OperationTimeout time.Duration
}

func New(config Config) (*Client, error)
func (client *Client) Connect(ctx context.Context) error
func (client *Client) OpenPool(ctx context.Context, name string) (Pool, error)
func (client *Client) OpenPoolByID(ctx context.Context, id int64) (Pool, error)
func (client *Client) Flush(ctx context.Context) error
func (client *Client) Shutdown(ctx context.Context) error
func (client *Client) Close() error

func (pool Pool) WithNamespace(namespace string) Pool
func (pool Pool) WithLocator(locator string) Pool
func (pool Pool) WithReadSnapshot(id uint64) Pool
func (pool Pool) Object(name string) ObjectRef

func (object ObjectRef) Read(ctx context.Context, offset, length uint64) ([]byte, ObjectInfo, error)
func (object ObjectRef) Write(ctx context.Context, offset uint64, data []byte) (OpResult, error)
func (object ObjectRef) WriteFull(ctx context.Context, data []byte) (OpResult, error)
func (object ObjectRef) Stat(ctx context.Context) (ObjectInfo, error)
func (object ObjectRef) Remove(ctx context.Context) (OpResult, error)
func (object ObjectRef) ExecuteRead(ctx context.Context, operation *ReadOp) (OpResult, error)
func (object ObjectRef) ExecuteWrite(ctx context.Context, operation *WriteOp) (OpResult, error)
```

`ObjectInfo` carries size, nanosecond modification time, and object version.
`OpResult` carries the resulting object version and operation-specific results.
Builder append methods are added with their owning operation families; they do
not alter the submission signatures above. P01 does not add nonfunctional
placeholders for these types. Wire types remain internal.

`New` validates and copies configuration but performs no network I/O.
`Connect` is explicit and context-aware. A `Client` is safe for concurrent use.
Immutable pool and object views will copy view state; deriving one cannot mutate
a sibling. Operation builders are single-use, are not concurrency-safe, and are
frozen on submission.

`Close` is idempotent, rejects new work with `ErrClosed`, cancels workers, and
settles pending callers. It does not drain and does not imply rollback.
`Shutdown` stops admission, waits for the submission watermark to settle until
its context expires, and then performs the same cleanup as `Close`. Repeated or
concurrent lifecycle calls converge on one terminal closed state. `Flush` waits
for writes accepted before its watermark and reports unknown outcomes.

## Timeouts and Cancellation

Every network operation accepts a context. An earlier caller deadline wins over
a configured finite timeout, and a later explicit caller deadline also remains
unchanged. With no caller deadline, a positive operation timeout is applied.
`DefaultConfig` supplies 10 second dial, 15 second handshake, and 30 second
operation timeouts. A `Config` passed directly to `New` keeps an
`OperationTimeout` of zero as unlimited; negative durations are invalid.
Unlimited operations are still canceled by client close or shutdown.

For notify, a positive operation timeout is also sent to the OSD in whole
seconds and the local wait receives one additional second of grace when the
caller supplied no deadline. Zero sends a zero server timeout and creates no
local timer. An explicit caller deadline is never replaced by notify grace.

Cancellation prevents unsent work from being dispatched and stops the caller's
wait. It cannot undo a request accepted by an OSD. When execution may have
occurred but no definitive result exists, the returned error matches both
`ErrOutcomeUnknown` and the context cause. Cancellation of one request never
changes a shared socket deadline.

## Buffer Ownership

Inputs are caller-owned and treated as immutable for the duration of a
synchronous call. Implementations must copy any bytes retained for queuing,
retry, or replay before the call returns, including cancellation paths. Returned
byte slices are caller-owned and do not alias receive buffers. Object names,
namespaces, locators, and metadata preserve bytes; no Unicode normalization or
path interpretation is applied.

Decoders in `internal/encoding` do not retain input beyond the decoder lifetime.
Methods returning bytes return copies. Encoders return a detached result copy.
All input-derived lengths are checked against owner-supplied limits before
allocation.

## Errors

`OpError` carries operation, safe target text, signed Linux/Ceph errno, and an
underlying cause. `errors.As` recovers it and `errors.Is` classifies not-found,
exists, permission, unsupported, invalid argument, quota/full, conflict,
timeout, canceled, closed, and outcome-unknown conditions. Wire codes are never
converted through the host `syscall.Errno` namespace.

Targets used in errors and future logs must not expose credentials, tickets,
payloads, or unrestricted object names. Compound sub-operation results remain
separate from the top-level transport/OSD result.

## Additive Cluster Observation API

P13 adds `Client.SubscribeClusterChanges` without changing the frozen lifecycle
or object-I/O signatures. A caller selects MON events, OSD events, or both and
provides a nonzero bounded queue no larger than `MaxWatchQueue`. Subscribing
before `Connect` includes initial accepted maps; subscribing later observes only
future publications.

`ClusterChangeAuthoritative` means an ordered MonMap or OSDMap was accepted by
the client. `ClusterChangeObserved` means this client observed one monitor or
OSD session become available or unavailable; it does not assert authoritative
membership, quorum, daemon `up`, or cluster health. Sequence numbers are global
to the client and monotonic, but filtered subscribers may observe gaps.

Every delivered event owns its slices, maps, and pointed-to states. A slow
subscriber never blocks monitor or objecter work: queue overflow terminates
that subscription and reports `ErrSubscriptionOverflow`. Context cancellation,
explicit subscription close, and client close terminate delivery. Consumers
must select on `Events`, `Errors`, and `Done` rather than assume an error is
present for normal cancellation.
