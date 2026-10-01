# Experiment Cache

Ordinary Git contains the [experiment catalog](experiments.json),
[high-level verdicts](EXPERIMENT_VERDICTS.md), seven compact report tables under
`compact/`, result documents, and the analysis and archive tools. These retain
1,009 published reports without copying request arrays or traces into Git.

Full published evidence is preserved in seven external archives. The portable
[archive index](archive-index.json) binds their names, sizes, content manifests
and SHA-256 hashes. [Storage metadata](cache-storage.json) identifies the verified
local copy. It is not a remote backup: copy the complete cache directory to your
chosen durable storage before removing either local evidence copy. Neither the
original expanded directories nor historical manifests have been deleted or
rewritten. Existing deep evidence links in result documents resolve after restore.

## Verify And Restore

From the repository root, with Go 1.27.1:

```sh
GOTOOLCHAIN=go1.27.1 go run ./tools/experiment-cache verify \
  --index docs/performance-p99-scheduler/archive-index.json \
  --cache-dir /path/to/cache

GOTOOLCHAIN=go1.27.1 go run ./tools/experiment-cache restore \
  --index docs/performance-p99-scheduler/archive-index.json \
  --cache-dir /path/to/cache --dest /fresh/restored-evidence

node integration/p07/publish-factorial.mjs --verify \
  /fresh/restored-evidence/factorial-stall-evidence
```

Use `--bundles factorial-stall-evidence` to restore only one investigation.
Other bundle-specific analysis commands are in the catalog. To resolve historical
relative documentation links, place restored bundles under the corresponding
ignored evidence directory, without overwriting existing evidence. Reanalysis
does not require running Ceph. Rerunning workloads does.

Restore requires a nonexistent destination. It validates selected archives
before extraction, reserves that directory exclusively, and uses rooted file
operations. The directory is visible while extraction proceeds; do not consume
it until the command succeeds. Publication is not a filesystem crash-durability
guarantee, and the cache parent should be trusted rather than writable by hostile
processes. Archive packing stages the whole package before exclusive publication.

## Historical Guarantees

All 12,538 restored files match their archived bytes. All 12,531 published
checksum entries passed their original inventories. Available batching,
ReadInto, inventory and scale summaries reproduced exactly; newer request-path
and factorial publication verifiers passed. The initial scheduler nontrace
summaries reproduced, but omitted historical raw traces prevent regenerating
six trace analyses. Omitted executables and unavailable historical source
versions remain explicitly documented, not replaced with current code.

The catalog distinguishes a base commit from dirty measured sources. The final
checkpoint commit must not be substituted for earlier source manifests. Verdicts
are experiment-specific; correctness passes and successful archive verification
do not close performance qualification or establish native parity.

## Linux Transfer

Transfer the source checkout normally. Historical archives are optional for new
measurements, but required for detailed historical reanalysis. Rebuild a source-
bound preparation capture on Linux using the
[live-cluster handoff](LINUX_LIVE_OBSERVATION_HANDOFF.md). Copy the archive index
alongside the full archives when moving the cache to another storage location.