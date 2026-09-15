// Package rados provides a pure-Go client for Ceph RADOS.
//
// The current object path supports explicit CephX monitor bootstrap,
// replicated-pool primary routing, ranged reads, stat metadata, mutations,
// completion metadata, flush/drain, outcome-unknown errors, immutable
// namespace/locator/snapshot views, and bounded recovery qualified on Ceph 20.2.4.
package rados
