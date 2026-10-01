import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { parseManifest, readBytes, sha256, summarize, verifyArtifacts } from './request-path-summary.mjs';

function publishedFiles(directory, prefix = '') {
  return fs.readdirSync(path.join(directory, prefix), { withFileTypes: true }).flatMap(entry => {
    const relative = path.posix.join(prefix, entry.name);
    assert.ok(!entry.isSymbolicLink(), 'publication must not contain symlinks');
    return entry.isDirectory() ? publishedFiles(directory, relative) : [relative];
  }).filter(filename => filename !== 'PUBLISHED.sha256').sort();
}

export function writeInventory(directory) {
  const entries = publishedFiles(directory).map(filename => `${sha256(fs.readFileSync(path.join(directory, filename)))}  ${filename}`);
  fs.writeFileSync(path.join(directory, 'PUBLISHED.sha256'), `${entries.join('\n')}\n`);
}

export function verifyPublication(directory) {
  const inventory = parseManifest(fs.readFileSync(path.join(directory, 'PUBLISHED.sha256'), 'utf8'));
  assert.deepEqual([...inventory.keys()], publishedFiles(directory), 'published inventory coverage');
  for (const [filename, digest] of inventory) assert.equal(sha256(fs.readFileSync(path.join(directory, filename))), digest, `published hash: ${filename}`);
  const sourceMap = JSON.parse(fs.readFileSync(path.join(directory, 'source-availability.json')));
  const identities = new Map();
  for (const entry of sourceMap.entries) {
    if (!entry.published) {
      assert.equal(entry.availability, 'unavailable_historical_version');
      assert.ok(entry.reason);
      continue;
    }
    assert.equal(entry.availability, 'verified_matching_source_bytes');
    assert.equal(entry.published, `source/${entry.sha256}.gz`);
    assert.equal(sha256(readBytes(directory, `source/${entry.sha256}`)), entry.sha256, 'published source hash');
    identities.set(`${entry.capture}:${entry.filename}`, entry.sha256);
  }
  for (const load of ['idle', 'alloc']) {
    const captureDirectory = path.join(directory, `profile-before-${load}`);
    assert.ok(fs.existsSync(path.join(captureDirectory, 'availability.json')), 'profile publication requires availability map');
    verifyArtifacts(captureDirectory);
    const expected = [...parseManifest(readBytes(captureDirectory, 'source-before.sha256').toString())];
    assert.deepEqual(sourceMap.entries.filter(entry => entry.capture === `profile-before-${load}`).map(entry => [entry.filename, entry.sha256]), expected, 'profile source availability coverage');
  }
  const result = summarize(directory, { published: true });
  assert.deepEqual(result, JSON.parse(fs.readFileSync(path.join(directory, 'summary.json'))), 'published summary reproducibility');
  for (const capture of result.captures) {
    const mapped = sourceMap.entries.filter(entry => entry.capture === capture.name);
    assert.deepEqual(mapped.map(({ filename, sha256: digest }) => ({ filename, sha256: digest })), capture.sources, 'source availability coverage');
  }
  const provenance = JSON.parse(fs.readFileSync(path.join(directory, 'comparator/provenance.json')));
  for (const entry of provenance.production_hashes_reverified) assert.equal(entry.reverified_sha256, sha256(readBytes(directory, `source/${entry.expected}`)));
  for (const entry of provenance.measurement_tree_files) assert.equal(entry.sha256, sha256(readBytes(directory, `source/${entry.sha256}`)));
  const supplemental = JSON.parse(fs.readFileSync(path.join(directory, 'supplemental-originals.json')));
  for (const entry of supplemental.filter(entry => entry.published)) assert.equal(sha256(readBytes(directory, entry.published.replace(/\.gz$/, ''))), entry.sha256);
  return { published_files: inventory.size, captures: result.captures.length, rows: result.rows.length,
    original_artifact_entries: result.captures.reduce((total, capture) => total + capture.artifact_verification.length, 0),
    source_entries: sourceMap.entries.length, available_source_entries: identities.size,
    unavailable_source_entries: sourceMap.entries.filter(entry => !entry.published).length,
    production_hashes_reverified: provenance.production_hashes_reverified.length,
    measurement_tree_files: provenance.measurement_tree_files.length };
}

export function publish(rawRoot, workspace, baseline, destination) {
  assert.ok(!fs.existsSync(destination), 'publication destination must be fresh');
  const summary = summarize(rawRoot);
  const originalProvenanceFile = path.join(rawRoot, 'rados-go-p99-next-baseline-reconstruction.json');
  const originalProvenance = fs.readFileSync(originalProvenanceFile);
  const reconstruction = JSON.parse(originalProvenance);
  assert.equal(reconstruction.status, 'verified');
  const production = reconstruction.productionHashes.map(entry => {
    const actual = sha256(fs.readFileSync(path.join(baseline, entry.file)));
    assert.equal(actual, entry.expected, `reconstructed old production hash: ${entry.file}`);
    for (const capture of summary.captures.filter(capture => capture.name.includes('-before-'))) {
      assert.equal(capture.sources.find(source => source.filename === entry.file)?.sha256, actual);
    }
    return { ...entry, reverified_sha256: actual };
  });
  assert.equal(production.length, 3);
  const measurement = reconstruction.measurementFiles.map(entry => {
    const workspaceHash = sha256(fs.readFileSync(path.join(workspace, entry.file)));
    const baselineHash = sha256(fs.readFileSync(path.join(baseline, entry.file)));
    assert.equal(workspaceHash, baselineHash, `measurement tree identity: ${entry.file}`);
    assert.equal(workspaceHash, entry.workspaceHash, `measurement historical identity: ${entry.file}`);
    return { filename: entry.file, sha256: workspaceHash };
  });
  assert.equal(measurement.length, 38);
  const extraMeasurement = measurement.filter(entry => !summary.measurement_sources.some(([filename]) => filename === entry.filename));
  assert.equal(extraMeasurement.length, 4);
  for (const [filename, digest] of summary.measurement_sources) assert.equal(measurement.find(entry => entry.filename === filename)?.sha256, digest);
  fs.mkdirSync(destination, { recursive: true });
  const supplemental = [];
  function writeJSON(relative, value) {
    const target = path.join(destination, relative);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, `${JSON.stringify(value, null, 2)}\n`);
  }
  function compressed(relative, bytes) {
    const target = path.join(destination, `${relative}.gz`);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, gzipSync(bytes, { level: 9, mtime: 0 }));
    return `${relative}.gz`;
  }
  function original(relative, filename, label) {
    if (!fs.existsSync(filename)) {
      supplemental.push({ original: filename, label, publication: 'unavailable', sha256: null });
      return;
    }
    const bytes = fs.readFileSync(filename);
    supplemental.push({ original: filename, label, sha256: sha256(bytes), published: compressed(relative, bytes) });
  }
  function capture(name, directory) {
    const verified = verifyArtifacts(directory);
    const manifest = fs.readFileSync(path.join(directory, 'artifacts.sha256'));
    compressed(`${name}/artifacts.sha256`, manifest);
    const entries = verified.map(entry => {
      const available = entry.verification === 'verified_present_bytes';
      const omit = ['benchmark', 'native-benchmark'].includes(entry.filename);
      if (available && !omit) compressed(`${name}/${entry.filename}`, readBytes(directory, entry.filename));
      return { filename: entry.filename, sha256: entry.sha256,
        original_verification: available ? 'verified_present_bytes' : 'unavailable',
        actual_original_sha256: available ? entry.sha256 : null,
        publication: available && !omit ? 'gzip_original' : 'omitted',
        ...(available && !omit ? { published: `${entry.filename}.gz` } : {
          reason: available ? 'Executable omitted to avoid duplicating large binaries; actual original bytes verified before omission.' : 'Original unavailable at publication; historical manifest identity only.' }) };
    });
    writeJSON(`${name}/availability.json`, { original_directory: directory, original_manifest_sha256: sha256(manifest),
      policy: 'Original manifest unchanged; gzip is lossless and deterministic. Image reference retained, container image not bundled. No omitted binary is treated as republished or reproducible bytes.', entries });
    const listed = new Set(parseManifest(manifest.toString()).keys());
    for (const filename of fs.readdirSync(directory).sort()) {
      if (filename !== 'artifacts.sha256' && !listed.has(filename) && fs.statSync(path.join(directory, filename)).isFile()) {
        original(`${name}/supplemental/${filename}`, path.join(directory, filename), 'original capture file not listed in historical artifact manifest');
      }
    }
  }
  for (const entry of summary.captures) {
    const directory = path.join(rawRoot, `rados-go-p99-next-${entry.name}-v1`);
    capture(entry.name, directory);
    original(`${entry.name}/run.log`, `${directory}.log`, 'original live harness log');
  }
  for (const load of ['idle', 'alloc']) {
    const directory = path.join(rawRoot, `rados-go-p99-next-before-${load}-v1`);
    capture(`profile-before-${load}`, directory);
    original(`profile-before-${load}/run.log`, `${directory}.log`, 'earlier closed-loop/profile context; not ABBA offered-load measurement');
  }
  const sourceAvailability = [];
  const blobs = new Map();
  const allSources = summary.captures.flatMap(captureEntry => captureEntry.sources.map(entry => ({ capture: captureEntry.name, ...entry })));
  for (const load of ['idle', 'alloc']) {
    const directory = path.join(rawRoot, `rados-go-p99-next-before-${load}-v1`);
    for (const [filename, digest] of parseManifest(readBytes(directory, 'source-before.sha256').toString())) allSources.push({ capture: `profile-before-${load}`, filename, sha256: digest });
  }
  for (const entry of allSources) {
    let bytes, origin;
    for (const tree of [baseline, workspace]) {
      const filename = path.join(tree, entry.filename);
      if (fs.existsSync(filename)) {
        const candidate = fs.readFileSync(filename);
        if (sha256(candidate) === entry.sha256) { bytes = candidate; origin = tree; break; }
      }
    }
    if (bytes && !blobs.has(entry.sha256)) blobs.set(entry.sha256, compressed(`source/${entry.sha256}`, bytes));
    sourceAvailability.push({ ...entry, availability: bytes ? 'verified_matching_source_bytes' : 'unavailable_historical_version',
      published: bytes ? blobs.get(entry.sha256) : null, verified_from: origin ?? null,
      ...(bytes ? {} : { reason: 'Neither reconstructed comparator nor final workspace matches this historical source hash; no replacement bytes invented.' }) });
  }
  for (const entry of extraMeasurement) {
    const bytes = fs.readFileSync(path.join(baseline, entry.filename));
    if (!blobs.has(entry.sha256)) blobs.set(entry.sha256, compressed(`source/${entry.sha256}`, bytes));
  }
  writeJSON('source-availability.json', { policy: 'Content-addressed original source blobs, verified against captured hashes; unavailable historical source versions explicitly retained.', entries: sourceAvailability });
  original('comparator/original-reconstruction.json', originalProvenanceFile, 'historical reconstruction audit, unchanged');
  for (const filename of ['rados-go-p99-next-baseline-audit.mjs', 'rados-go-p99-next-baseline-build-normal.log', 'rados-go-p99-next-baseline-build-p12diagnostics.log', 'rados-go-p99-next-baseline-race.log']) {
    original(`comparator/${filename}`, path.join(rawRoot, filename), 'original comparator reconstruction check');
  }
  const benchmarks = [
    ['encoding-before', 'reconstructed comparator encoding'], ['encoding-after', 'initial encoding intermediate'],
    ['encoding-final', 'final encoding; unchanged by ACK removal'], ['session-before', 'reconstructed comparator linear dispatch accounting'],
    ['session-after', 'initial O(1) accounting plus ACK experiment intermediate'],
    ['session-final', 'pre-removal ACK experiment confirmation; not final no-ACK source'],
    ['session-noack', 'final O(1) accounting, ACK coalescing removed'],
  ];
  for (const [name, label] of benchmarks) original(`microbench/${name}.txt`, path.join(rawRoot, `rados-go-${name}.txt`), label);
  for (const filename of ['request-path-summary.mjs', 'request-path-summary.test.mjs', 'publish-request-path.mjs']) {
    original(`tooling/${filename}`, path.join(workspace, 'integration/p07', filename), 'publication tooling snapshot; not historical capture source');
  }
  writeJSON('comparator/provenance.json', { original_audit_sha256: sha256(originalProvenance),
    production_hashes_reverified: production, measurement_tree_files: measurement, captured_measurement_count: 34,
    tree_measurement_count: 38, excluded_from_captured_inventory: extraMeasurement,
    exclusion_reason: 'Capture input selector includes Go/C/module files, reproduce.sh and summary MJS/tests, not markdown, report.schema.json or mode_capture_test.sh. These four were compared separately by the 38-file reconstruction audit.',
    fixture_hashes_reverified: reconstruction.fixtureHashes.map(entry => {
      const digest = sha256(fs.readFileSync(path.join(baseline, entry.file)));
      assert.equal(digest, entry.expected);
      return { filename: entry.file, sha256: digest };
    }),
    excluded_incompatible_tests: reconstruction.excludedTests,
    microbench_identity_limitations: 'Raw benchmark stdout has platform/package/CPU and repetitions but no contemporaneous source or binary hashes. Chronology labels are not cryptographic binding. Original binaries are not reconstructed from stdout.',
    unavailable_source_versions: sourceAvailability.filter(entry => entry.availability === 'unavailable_historical_version'),
    artifact_counts: summary.captures.map(entry => ({ capture: entry.name, original_manifest_entries: entry.artifact_verification.length,
      verified_original_bytes: entry.artifact_verification.filter(artifact => artifact.verification === 'verified_present_bytes').length,
      binary_sha256: entry.benchmark.sha256 })) });
  writeJSON('supplemental-originals.json', supplemental);
  const published = summarize(destination, { published: true });
  assert.deepEqual(published.rows, summary.rows, 'raw/published row equality');
  assert.deepEqual(published.comparisons, summary.comparisons, 'raw/published comparison equality');
  writeJSON('summary.json', published);
  writeInventory(destination);
  verifyPublication(destination);
  return published;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] === '--verify') {
    assert.equal(process.argv.length, 4, 'Usage: node publish-request-path.mjs --verify PUBLICATION');
    console.log(JSON.stringify(verifyPublication(process.argv[3])));
  } else {
    assert.equal(process.argv.length, 6, 'Usage: node publish-request-path.mjs RAW_ROOT WORKSPACE BASELINE FRESH_DESTINATION');
    const result = publish(...process.argv.slice(2));
    console.log(JSON.stringify({ captures: result.captures.length, rows: result.rows.length, outcomes: result.rows.reduce((total, row) => total + row.expected, 0) }));
  }
}