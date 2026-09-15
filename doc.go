// Package rados provides a pure-Go client for Ceph RADOS.
//
// The current read-only object path supports explicit CephX monitor bootstrap,
// replicated-pool primary routing, ranged reads, stat metadata, immutable
// namespace/locator/snapshot views, and bounded recovery qualified on Ceph 20.2.4.
package rados
