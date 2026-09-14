# P02 Handoff

Status: **P02 qualification pending; do not mark complete**.

## Before Closing P02

1. Rerun `make reproduce-p02` to completion in the pinned image. The previous
   package download/build was canceled, and no verified `openssl-devel` NEVRA
   and RPM checksum were recorded, so fixture reproduction is unresolved.
2. Generate vectors by executing upstream Ceph `FrameAssembler` at the pinned
   source and compare them byte-for-byte. The current source-derived independent
   oracle does not invoke `FrameAssembler` and cannot close this gate.
3. Record successful `make verify-p02-all` results, including all six
   60-second fuzz targets, or record and resolve each failure.
4. Confirm the scheduled CI run completes all six five-minute fuzz targets.
5. Preserve the explicit boundary that all session peers are synthetic and no
   live authenticated Ceph connectivity has been demonstrated.

## P03 Boundary

P03 owns CephX credential parsing, challenge/ticket exchanges, authorizers,
transcript authentication, connection-secret establishment, renewal, and
security downgrade rejection. It must provide a connector that honors the
session context and returns a freshly authenticated transport with fresh crypto
counters while leaving replay/session identity with `Session`.

The P03 exit gate requires authentication to a real pinned monitor using CephX
and secure messenger v2.1, plus clear failure for invalid credentials and
prohibited downgrade. No-auth or deterministic-secret paths may remain only as
inaccessible test facilities. P02 ACKs remain transport replay signals, not
operation completion or durability evidence.