# go-librados

`go-librados` is a planned pure-Go Ceph RADOS client. The repository is
currently at **P00: Evidence, Scope and Oracle**. It does not yet contain a Go
client and must not be described as interoperable.

The implementation contract is [SPEC.md](SPEC.md). P00 evidence and decisions
are indexed in [docs/p00/README.md](docs/p00/README.md).

Run the locally reproducible P00 checks with:

```sh
make verify-p00
```

Run the disposable-cluster proof only on an isolated Linux host or VM that
meets [integration/README.md](integration/README.md):

```sh
P00_DISPOSABLE_CLUSTER=I_UNDERSTAND_THIS_DESTROYS_DATA make p00-smoke
```

No test secret is stored in this repository. Native Ceph libraries are limited
to the isolated oracle and cluster infrastructure under `integration/`.