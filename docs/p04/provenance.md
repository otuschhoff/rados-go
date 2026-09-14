# P04 Provenance and Reproduction

P04 protocol behavior is grounded in Ceph v20.2.0 commit `69f84cc2651aa259a15bc192ddaabd3baba07489` and qualified against Ceph v20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964` using the digest-pinned images in `docs/p00/evidence.json`.

The three fixtures under `testdata/p04` are emitted directly by pinned `ceph-dencoder` from `MonMap`, `OSDMap`, and `OSDMap::Incremental` test instance zero. Their strict manifests bind bytes, source paths and hashes, generator command, image, and license status. Run `make reproduce-p04-fixtures` to regenerate and compare them.

`make integration-p04` creates three target-cluster monitors and one foreign-FSID monitor on a disposable Docker network. It cross-builds the probe with `CGO_ENABLED=0`, creates and reports pools, observes an incremental map change, compares it with a fresh full-map snapshot, removes the initially selected monitor, proves continued updates and commands through the remaining quorum, and authenticates to but rejects the foreign cluster. Credentials, stores, containers, and network are temporary.

The workflow supports native `linux/amd64` and `linux/arm64`. The checked-in report records a successful native arm64 run. Fixture redistribution review remains pending and is not represented as approved.
