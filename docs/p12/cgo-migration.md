# Migrating from cgo librados Wrappers

`go-librados` is a semantic RADOS client, not a source-compatible replacement
for a particular wrapper. Migrate behavior and ownership deliberately instead
of translating calls one for one.

| cgo/librados pattern | `go-librados` pattern | Important difference |
| --- | --- | --- |
| cluster handle plus `rados_connect` | `rados.New(Config)`, then `Connect(ctx)` | Construction performs no network I/O; contexts and finite defaults bound work. |
| `conf_read_file`, `conf_parse_env`, default paths | explicit `rados.Config` | No exported config/keyring loader or implicit environment/default path. |
| `rados_ioctx_t` | immutable `rados.Pool` value | Namespace, locator, and snapshot methods return derived values; retain the result. |
| object name plus ioctx on each call | `pool.Object(name)` | `ObjectRef` retains the immutable pool view. |
| caller-allocated read buffer | `Read` returns `[]byte` and `ObjectInfo` | Returned bytes are Go-owned; short reads at EOF are normal. |
| completion objects/callbacks | synchronous context-aware methods; concurrent goroutines | No public completion allocation/release API. Cancellation is not rollback. |
| `rados_aio_flush` | `Client.Flush(ctx)` | Waits for the accepted-write watermark and reports unknown outcomes. |
| read/write operation handles | `NewReadOp`/`NewWriteOp`, then `Execute*` | Builders are single-use, not concurrency-safe, and freeze on submission. |
| integer return codes and `errno` | sentinel errors plus `*rados.OpError` | Use `errors.Is`/`errors.As`; wire errno is Linux/Ceph-specific. |
| manual buffer/handle release | Go ownership and `Client.Close`/`Watch.Close` | No public deallocator; close long-lived resources explicitly. |
| watch callback | bounded event channel plus explicit `Ack` | Consume `Errors`/`Done`; overflow/interruption can imply event loss. |
| native shared-library deployment | ordinary pure-Go binary | Build with `CGO_ENABLED=0`; no librados package is loaded at runtime. |

## Migration Sequence

1. Inventory each wrapper call and its required caps, pool types, retry policy,
   ordering assumptions, callbacks, and buffer ownership.
2. Compare every operation with the exact compatibility matrix. Keep the old
   path for unqualified features rather than silently emulating them.
3. Replace process-global/default configuration with explicit secrets and
   monitor/FSID inputs. Do not pass keyring file contents as `Config.Key`.
4. Convert ioctx mutation into immutable views. Assign results such as
   `namespaced := pool.WithNamespace(namespace)`.
5. Replace return-code switches with `errors.Is` and retain `OpError.Code` only
   for diagnostics that require the original Ceph result.
6. Redesign asynchronous calls around contexts and goroutines. Define how the
   application reconciles `ErrOutcomeUnknown` before enabling retries.
7. Run dual-client interoperability in isolated namespaces: native-write/Go-read
   and Go-write/native-read, including versions, errors, snapshots, and failure
   transitions used by the application.
8. Build and run the migrated binary with `CGO_ENABLED=0` on every claimed
   client target without Ceph client libraries installed.

Do not infer operation ordering from goroutine scheduling, treat a messenger ACK
as durable object completion, or assume a lock fences unrelated writers.