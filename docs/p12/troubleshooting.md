# Operational Troubleshooting

## Before Connecting

Construct `rados.Config` explicitly. `Monitors`, `Entity`, and `Key` are
required; set `ClusterFSID` to prevent connection to the wrong cluster. Zero
timeouts select finite defaults: 10 seconds for dialing, 15 seconds for the
handshake, and 30 seconds for operations. Negative durations are invalid.

Monitor seeds may be hostnames, IPv4, bracketed IPv6, `v2:` endpoints, or
`dns-srv:<domain>`. Bare hosts use port 3300. Separate entries in `Monitors`;
do not pass an unparsed Ceph config `mon_host` string as one entry.

The `Key` value is the base64 CephX key itself, not a keyring document or file
path. The root package does not automatically read `/etc/ceph`, process
environment, or keyring files. Keep credentials out of logs and command lines.

## Classify Errors

Use `errors.Is` for stable decisions and `errors.As` for wire detail:

```go
var operationError *rados.OpError
switch {
case errors.Is(err, rados.ErrPermission):
	// Check entity caps and pool/namespace restrictions.
case errors.Is(err, rados.ErrTimeout):
	// Inspect caller and configured deadlines; do not assume the operation failed.
case errors.Is(err, rados.ErrOutcomeUnknown):
	// Reconcile state before retrying a mutation.
case errors.As(err, &operationError):
	log.Printf("operation=%s target=%s ceph_errno=%d", operationError.Op, operationError.Target, operationError.Code)
}
```

`OpError.Code` is a signed Linux/Ceph errno, not the host OS `syscall.Errno`.
Preserve both the typed classification and the original error when wrapping it.

## Common Failures

| Symptom | Checks and action |
| --- | --- |
| `New` returns `ErrInvalidArgument` | Require monitor seeds, exact entity, base64 key, valid FSID, nonnegative timeouts, and a known security mode. |
| Connect timeout | Verify v2 port 3300 reachability, DNS/IPv6 resolution, CephX entity/key, monitor quorum, caller deadline, and FSID. |
| Permission denied | Compare monitor, manager, and OSD caps with the operation and exact pool/namespace. Ordinary I/O does not require manager caps. |
| Unsupported operation | Check the exact matrix and pool type. Do not replace atomic server behavior with client-side read/modify/write. |
| `ErrOutcomeUnknown` | The mutation may have executed. Read/version-check or use an application idempotency record before retrying. |
| Watch interruption | Consume `Watch.Errors()` and `Watch.Done()`. Reconcile object state because events may have been lost. |
| Notify timeout | Inspect partial acknowledgments/timeouts in `NotifyReply`; a timeout does not erase useful partial results. |
| Enumeration repeats or changes | Persist the returned cursor, bound page sizes, and expect concurrent mutation to change the visible set. |
| Shutdown deadline | The graceful drain exceeded its context. Treat unresolved mutations according to their returned errors. |

## Production Practices

- Use least-privilege identities and separate administrative clients from data
  clients. Validate resource ownership and cluster FSID before destructive APIs.
- Set caller deadlines for every operation. A context cancellation stops the
  wait but cannot roll back accepted server work.
- Keep object names and payloads out of unrestricted logs. `OpError.Target` is
  bounded diagnostic text, not an authorization or audit record.
- Consume watch channels promptly and choose a bounded nonzero queue. A watch is
  not a durable event stream.
- Observe every write result and call `Shutdown(ctx)` when a bounded graceful
  drain is required. `Close` cleans up but does not imply rollback or drain.
- Escalate only with redacted diagnostics under [SECURITY.md](../../SECURITY.md).