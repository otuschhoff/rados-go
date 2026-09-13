# Integration Infrastructure

Integration code is isolated from the shipped Go module. It may use native Ceph
tools and librados; production code and ordinary unit tests may not.

## P00 host requirements

Use a disposable Linux VM with systemd, root access, Docker, at least 4 CPUs,
12 GiB RAM and 40 GiB free disk. Python 3 and an SSH daemon listening on port 22
are required for cephadm host management. Every exposed block device must have
a usable udev database record so `ceph-volume` can inventory the host; Linux
containers that cannot process virtual-disk udev events are not sufficient. The
runner creates three 6 GiB loop devices, bootstraps Ceph services and destroys
them on exit. Never run it on a Ceph host or a machine containing valuable
`/var/lib/ceph` state. Run from a clean, committed worktree so the report's
repository commit identifies the exact code under test.

macOS is supported as a future client platform, but Docker Desktop is not a
sufficient cephadm host because its Linux VM does not expose the required
systemd/raw-device lifecycle. Start a dedicated Linux VM and run:

The host must also resolve the numeric Ceph UID and GID embedded in the pinned
image. Some recent distributions require a local `ceph` system account for
cephadm's numeric ownership operations; the runner checks this before bootstrap
and reports the required IDs.

```sh
sudo env P00_DISPOSABLE_CLUSTER=I_UNDERSTAND_THIS_DESTROYS_DATA make p00-smoke
```

Successful reports are written to `integration/reports/` and contain no keys.
The report must validate against `integration/manifest.schema.json`.