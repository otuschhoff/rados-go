# P04 Monitor Configuration

The monitor bootstrap loader supports a deliberately small Ceph configuration
subset: `[global]` and the exact client entity section, with `cluster`,
`mon_host`, `keyring`, and `fsid` keys. Key names may use spaces or underscores.
The keyring path expands `$cluster` and `$name`. Includes, shell expansion, and
arbitrary Ceph options are not supported.

Precedence is:

1. built-in defaults (`cluster=ceph`, `entity=client.admin`)
2. an explicitly supplied configuration file
3. the environment, only when environment loading is explicitly enabled
4. explicit programmatic options

The opt-in environment names are `GO_LIBRADOS_CLUSTER`,
`GO_LIBRADOS_ENTITY`, `GO_LIBRADOS_MON_HOST`, `GO_LIBRADOS_KEYRING`, and
`GO_LIBRADOS_FSID`. The loader does not implicitly read process environment or
default Ceph paths.

Monitor seeds accept IPv4, bracketed IPv6, hostnames, `v2:` endpoints with an
optional numeric nonce, and `dns-srv:<domain>`. Bare hosts use the messenger v2
port 3300. Mixed Ceph address vectors ignore their `v1:` entries; a standalone
v1 seed is rejected by resolution. Both source seeds and resolved addresses
are bounded by caller-provided limits.