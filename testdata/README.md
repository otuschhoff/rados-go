# Protocol Fixtures

No protocol fixtures are checked in during P00. Every fixture added from P01
onward must have a sidecar manifest that validates against
`manifest.schema.json`. Synthetic secrets must be non-reusable and explicitly
marked. Captures containing real credentials, tickets or session secrets must
never be committed.

`generator.command` must be sufficient to reproduce the fixture in the pinned
environment. `source.paths` lists every upstream file used to interpret it.
`license.reviewed_by` must name a human reviewer before redistribution.