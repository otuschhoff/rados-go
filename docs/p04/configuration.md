# P04 Monitor Configuration

`DefaultConfig` is deterministic and performs no I/O. It selects cluster
`ceph`, entity `client.admin`, secure messenger mode, a 10 second dial timeout,
a 15 second handshake timeout, and a 30 second operation timeout. Monitor
addresses and credentials must still be supplied before `New` can succeed.
Passing a `Config` directly to `New` does not apply the operation default:
`OperationTimeout: 0` means unlimited. This is a compatibility change from the
former zero-means-30-seconds behavior; callers wanting the policy default
should start from `DefaultConfig`.

`ParseConfig` accepts at most 1 MiB of the deliberately small Ceph
configuration grammar. It applies `[global]`, selects the resulting entity,
and then applies only that exact entity section. Other entity sections are
ignored. Blank lines, `#` and `;` comments, quoted values, and key names using
spaces or underscores are accepted. At most 256 properties may appear across
all sections. Includes, include directories, shell execution, environment
expansion, and default Ceph search paths are not supported.

The supported option names are `cluster`, `entity`/`name`, `mon_host`, `fsid`,
`key`, `keyring`, `ms_mode`, `dial_timeout`, `handshake_timeout`, and
`operation_timeout`, plus the Ceph-compatible `rados_osd_op_timeout` alias.
`ms_mode` is exactly `secure` or `crc`. Dial and handshake durations use Go
duration syntax and must be positive. `operation_timeout` uses Go duration
syntax and may be zero; `rados_osd_op_timeout` additionally accepts a bare
integer as seconds. Negative durations are invalid. When both operation names
occur in one section, `rados_osd_op_timeout` is applied last and wins. `Option`
returns the effective Go duration for either name, including `0s`. Malformed
known values return an error compatible with `ErrInvalidArgument`. Unknown file
properties are ignored.

Resource options are `max_sessions`, `max_receive_bytes`,
`max_queued_receive_bytes`, `max_backoffs` and `max_backoff_bytes`; corresponding
hyphenated command arguments and uppercase environment suffixes are recognized.
Values must be positive unsigned decimal without whitespace; counts must fit
the platform `int`. Zero struct fields select defaults: 256 sessions, 256 MiB
aggregate receive backing, 64 MiB queued receive per session, and 4,096 active
backoff IDs / 8 MiB logical backoff charge per OSD session. Effective queued
receive bytes cannot exceed aggregate receive bytes. Backoff overflow fail-stops
only its session before ACK; duplicate/replacement/unblock/reset accounting and
the non-RSS charging model are described in the
[resource policy](../performance-phase11/README.md). These logical policies are
not a whole-client memory or RSS guarantee.

`LoadConfig` is the bounded file form of `ParseConfig`. If the selected
configuration has a `keyring` and no direct `key`, it expands only `$cluster`
and `$name` in that path after both sections have been applied, then loads the
selected entity from that keyring. `LoadKeyring` accepts at most 1 MiB, uses
the internal CephX parser, and returns a caller-owned copy of the canonical
encoded key accepted by `New`. Errors and formatting never include key bytes.

Precedence is:

1. built-in defaults (`cluster=ceph`, `entity=client.admin`)
2. `ParseConfig` or `LoadConfig`
3. an explicit `ParseEnv` overlay
4. `WithOption` or `ParseArgs`

`ParseEnv("")` uses prefix `GO_LIBRADOS`; another argument selects that exact
prefix. Read suffixes are `CLUSTER`, `ENTITY`, `MON_HOST`, `KEYRING`,
`FSID`, `KEY`, `MS_MODE`, `DIAL_TIMEOUT`, `HANDSHAKE_TIMEOUT`, and
`OPERATION_TIMEOUT`, plus `MAX_SESSIONS`, `MAX_RECEIVE_BYTES`,
`MAX_QUEUED_RECEIVE_BYTES`, `MAX_BACKOFFS` and `MAX_BACKOFF_BYTES`.
There is no `RADOS_OSD_OP_TIMEOUT` environment alias. No
API reads process environment implicitly.

`ParseArgs` recognizes `--name`, `--id`, `--cluster`, `--mon-host`, `--fsid`,
`--key`, `--keyring`, `--ms-mode`, and the three timeout options in either
`--option=value` or `--option value` form. It also recognizes
`--rados-osd-op-timeout` with the alias syntax above. `--id=x` means `client.x`.
Unknown options and non-options are returned unmodified and unconsumed; `--`
returns itself and all following arguments. Parsing does not register
process-global flags.

`WithOption`, `ParseArgs`, and `ParseEnv` return independent configurations:
monitor slices, key bytes, and retained option maps are deep-copied. Unknown
programmatic options are retained for `Option` lookup but are inert and never
change `New` behavior. An explicit `keyring` option is loaded after that
overlay has finalized `cluster` and `entity`; within one argument or
environment overlay, a direct `key` takes precedence over `keyring`.

Monitor seeds accept IPv4, bracketed IPv6, hostnames, `v2:` endpoints with an
optional numeric nonce, and `dns-srv:<domain>`. Bare hosts use the messenger v2
port 3300. Mixed Ceph address vectors ignore their `v1:` entries; a standalone
v1 seed is rejected by resolution. Both source seeds and resolved addresses
are bounded by caller-provided limits.