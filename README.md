# go-librados

`go-librados` is a pure-Go Ceph RADOS client implemented through **P08:
Metadata, Compound Operations and Enumeration**. Its qualified network scope is Ceph
v20.2.4 replicated pools: monitor discovery and maps, exact placement,
CephX-secured OSD sessions, ranged head reads, stat, namespace and locator
views, create/write/write-full/append/truncate/zero/remove, flush and graceful
shutdown, outcome-unknown classification, and bounded recovery from primary
changes, redirects, and backoff. It also includes binary-safe xattrs and OMAP,
atomic single-object compound operations, and cursor-based object enumeration.
Live snapshot interoperability is not yet claimed.

The implementation contract is [SPEC.md](SPEC.md). Phase evidence and
qualification are indexed under `docs/p00` through `docs/p08`.

Run the complete P08 qualification, including the pinned disposable cluster,
with:

```sh
make verify-p08-all
```

The P08 Docker workflow and its host requirements are documented in
[integration/README.md](integration/README.md). The separate P00 cephadm proof
requires an isolated Linux host or VM and explicit destructive-test opt-in:

```sh
P00_DISPOSABLE_CLUSTER=I_UNDERSTAND_THIS_DESTROYS_DATA make p00-smoke
```

No test secret is stored in this repository. Native Ceph libraries are limited
to the isolated oracle and cluster infrastructure under `integration/`.