import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { gzipSync, gunzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { main, parseManifest, readOriginal, compareCases, verifyArtifacts } from './factorial-summary.mjs';

export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');

function files(root, prefix = '') {
  return fs.readdirSync(path.join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    assert.ok(!entry.isSymbolicLink(), 'publication cannot contain symlinks');
    const name = path.posix.join(prefix, entry.name);
    assert.ok(entry.isDirectory() || entry.isFile(), 'unsupported publication entry');
    return entry.isDirectory() ? files(root, name) : [name];
  }).sort();
}

function writeJSON(root, name, value) {
  const target = path.join(root, name);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, JSON.stringify(value, null, 2) + '\n');
}

function compressed(root, name, bytes) {
  assert.ok(typeof name === 'string' && !name.includes('\\') && !/[\x00-\x1f\x7f]/.test(name), 'unsafe publication path');
  assert.ok(name.split('/').every(part => part && part !== '.' && part !== '..'), 'unsafe publication path');
  const target = path.join(root, name + '.gz');
  assert.ok(!fs.existsSync(target), 'publication file must not be overwritten');
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const packed = gzipSync(bytes, { level: 9 });
  assert.deepEqual(gunzipSync(packed), bytes, 'gzip original-byte round trip');
  fs.writeFileSync(target, packed);
  return name + '.gz';
}

export function writeInventory(root) {
  const names = files(root).filter(name => name !== 'PUBLISHED.sha256');
  fs.writeFileSync(path.join(root, 'PUBLISHED.sha256'), names.map(name => `${sha256(fs.readFileSync(path.join(root, name)))}  ${name}\n`).join(''));
  return names.length;
}

export async function verifySourceArchive(root) {
  const expected = parseManifest((await readOriginal(root, 'source-before.sha256')).toString());
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'factorial-source-'));
  try {
    const archive = path.join(temporary, 'source.tar.gz');
    fs.writeFileSync(archive, await readOriginal(root, 'workload-sources.tar.gz'));
    const run = args => {
      const result = spawnSync('tar', args, { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 });
      assert.ifError(result.error);
      assert.equal(result.status, 0, `source archive tar: ${result.stderr}`);
      return result.stdout;
    };
    const names = run(['-tzf', archive]).trimEnd().split('\n');
    assert.equal(new Set(names).size, names.length, 'duplicate archive entry');
    for (const name of names) {
      assert.ok(!name.startsWith('/') && !name.includes('\\') && !/[\x00-\x1f\x7f]/.test(name), 'unsafe archive entry');
      assert.ok(name.split('/').every(part => part && part !== '.' && part !== '..'), 'unsafe archive path');
    }
    assert.deepEqual([...names].sort(), [...expected.keys()].sort(), 'archive/source inventory exact coverage');
    for (const line of run(['-tvzf', archive]).trimEnd().split('\n')) assert.ok(line.startsWith('-'), 'source archive must contain regular files only');
    const extracted = path.join(temporary, 'extracted');
    fs.mkdirSync(extracted);
    run(['-xzf', archive, '-C', extracted]);
    for (const [name, digest] of expected) {
      assert.ok(fs.lstatSync(path.join(extracted, name)).isFile(), 'source archive regular file');
      assert.equal(sha256(fs.readFileSync(path.join(extracted, name))), digest, `archive source hash: ${name}`);
    }
    return { files: expected.size, archive_sha256: sha256(fs.readFileSync(archive)), status: 'all_archived_source_bytes_verified' };
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

async function publicationSummary(root, captureNames) {
  const summary = await main(captureNames.map(name => path.join(root, name)));
  for (const capture of summary.captures) {
    const name = path.basename(capture.root);
    capture.root = name;
    for (const row of capture.rows) row.capture_root = name;
    capture.comparison = compareCases(capture.rows);
    capture.source.archive_content_verification = (await verifySourceArchive(path.join(root, name))).status;
  }
  summary.comparison = compareCases(summary.captures.flatMap(capture => capture.rows));
  return summary;
}

export async function verifyTraceBinding(root, summary) {
  const filename = path.join(root, 'trace-analysis/summary.json.gz');
  if (!fs.existsSync(filename)) return null;
  const json = name => JSON.parse(gunzipSync(fs.readFileSync(path.join(root, name))));
  const trace = json('trace-analysis/summary.json.gz');
  const before = json('trace-analysis/original-hashes-before.json.gz');
  const after = json('trace-analysis/original-hashes-after-analysis.json.gz');
  assert.deepEqual(before, after, 'trace analysis must preserve original hashes');
  const capture = summary.captures.find(entry => entry.complete && entry.measurement === 'instrumented');
  assert.ok(capture, 'trace summary requires completed instrumented capture');
  const manifest = await readOriginal(path.join(root, capture.root), 'artifacts.sha256');
  assert.equal(before['artifacts.sha256'], sha256(manifest), 'trace original manifest binding');
  for (const [name, digest] of parseManifest(manifest.toString(), { artifact: true })) assert.equal(before[name], digest, `trace original binding: ${name}`);
  const snapshots = ['tooling/trace-stall-summary.mjs.gz', 'tooling/final/trace-stall-summary.mjs.gz'];
  const boundSnapshot = snapshots.find(name => fs.existsSync(path.join(root, name)) && sha256(gunzipSync(fs.readFileSync(path.join(root, name)))) === trace.analyzer_sha256);
  assert.ok(boundSnapshot, 'trace analyzer hash requires actual matching published tool bytes');
  assert.equal(trace.analysis_status, 'complete', 'trace analysis status');
  assert.equal(trace.complete_rows, capture.rows.length, 'trace completed row count');
  assert.equal(trace.expected_rows, capture.rows.length, 'trace expected row count');
  assert.equal(trace.rows.length, capture.rows.length, 'trace actual row count');
  assert.deepEqual(trace.source_final_status, capture.final, 'trace original final status');
  const labels = new Set();
  for (const row of trace.rows) {
    assert.ok(!labels.has(row.label), 'duplicate trace row');
    labels.add(row.label);
    const original = capture.rows.find(entry => entry.filename === row.label + '.json');
    assert.ok(original, 'trace row must have original offered outcome row');
    assert.equal(row.analysis_status, 'complete', 'trace row analysis status');
    assert.equal(row.counts.attempted, original.counts.attempted, 'trace attempted population');
    assert.equal(row.latency.service_attempted.p99_ns, original.phase_percentiles.service_ns.attempted.p99_ns, 'trace offered service p99');
  }
  return { rows: trace.rows.length, analyzer_snapshot: boundSnapshot, analyzer_sha256: trace.analyzer_sha256 };
}

export async function verifyPublication(root) {
  const inventory = parseManifest(fs.readFileSync(path.join(root, 'PUBLISHED.sha256'), 'utf8'));
  assert.deepEqual([...inventory.keys()], files(root).filter(name => name !== 'PUBLISHED.sha256'), 'published inventory exact coverage');
  for (const [name, digest] of inventory) assert.equal(sha256(fs.readFileSync(path.join(root, name))), digest, `published hash: ${name}`);
  const provenance = JSON.parse(fs.readFileSync(path.join(root, 'publication.json')));
  for (const original of provenance.supplemental_originals) {
    const bytes = gunzipSync(fs.readFileSync(path.join(root, original.published)));
    assert.equal(sha256(bytes), original.sha256, `supplement original hash: ${original.published}`);
  }
  const summary = await publicationSummary(root, provenance.capture_names);
  assert.deepEqual(summary, JSON.parse(fs.readFileSync(path.join(root, 'summary.json'))), 'published summary reproducibility');
  const bindings = JSON.parse(fs.readFileSync(path.join(root, 'source-binding.json')));
  assert.deepEqual(bindings.captures, summary.captures.map(capture => ({ name: capture.root, ...capture.source })), 'source binding coverage');
  assert.equal(bindings.all_source_artifacts_available, true, 'all source bytes must be published');
  assert.equal(bindings.primary_observed_same_source_manifest, new Set(summary.captures.filter(capture => capture.complete).map(capture => capture.source.source_before_sha256)).size === 1, 'completed capture source identity');
  const p00Bytes = gunzipSync(fs.readFileSync(path.join(root, 'provenance/p00-evidence.json.gz')));
  const p00 = JSON.parse(p00Bytes);
  for (const binding of bindings.image_bindings) {
    assert.equal(binding.p00_evidence_sha256, sha256(p00Bytes), 'P00 evidence byte binding');
    const capture = summary.captures.find(entry => entry.root === binding.capture);
    assert.ok(capture && capture.rows.length, 'image binding requires measured capture');
    assert.equal(binding.architecture, capture.rows[0].environment.GOARCH, 'image binding architecture');
    assert.equal(binding.reference, `quay.io/ceph/ceph@${p00.images.qualification[binding.architecture]}`, 'published P00 image digest');
    assert.equal(binding.reference, (await readOriginal(path.join(root, capture.root), 'image.reference')).toString().trim(), 'published captured image');
  }
  assert.equal(bindings.image_bindings.length, summary.captures.filter(capture => capture.rows.length).length, 'image binding coverage');
  const traceBinding = await verifyTraceBinding(root, summary);
  return { published_files: inventory.size, captures: summary.captures.length,
    rows: summary.captures.reduce((total, capture) => total + capture.rows.length, 0),
    expected_arrivals: summary.captures.flatMap(capture => capture.rows).reduce((total, row) => total + row.counts.expected, 0),
    original_manifest_entries: summary.captures.reduce((total, capture) => total + capture.artifacts.manifest_entries, 0),
    available_originals_reverified: summary.captures.reduce((total, capture) => total + capture.artifacts.verified_original_bytes, 0),
    initially_verified_omitted_binaries: summary.captures.reduce((total, capture) => total + capture.artifacts.unavailable.length, 0), trace_binding: traceBinding };
}

export async function publish({ roots, destination, workspace, supplemental = [] }) {
  assert.ok(!fs.existsSync(destination), 'publication destination must be fresh');
  const original = await main(roots);
  const names = roots.map(root => path.basename(root));
  assert.equal(new Set(names).size, names.length, 'capture publication names must be unique');
  const archiveChecks = [];
  for (const root of roots) archiveChecks.push(await verifySourceArchive(root));
  const p00Filename = path.join(workspace, 'docs/p00/evidence.json');
  const p00Bytes = fs.readFileSync(p00Filename);
  const p00 = JSON.parse(p00Bytes);
  const imageBindings = [];
  for (const [index, capture] of original.captures.entries()) {
    if (!capture.rows.length) continue;
    const architecture = capture.rows[0].environment.GOARCH;
    assert.ok(capture.rows.every(row => row.environment.GOARCH === architecture), 'capture architecture changed');
    const reference = (await readOriginal(roots[index], 'image.reference')).toString().trim();
    const expected = `quay.io/ceph/ceph@${p00.images.qualification[architecture]}`;
    assert.equal(reference, expected, 'captured workload image must match P00 qualification digest');
    imageBindings.push({ capture: names[index], architecture, reference, p00_evidence_sha256: sha256(p00Bytes) });
  }
  fs.mkdirSync(destination, { recursive: true });
  const supplements = [];
  const addSupplement = (filename, publishedName, label) => {
    const bytes = fs.readFileSync(filename);
    supplements.push({ original: filename, sha256: sha256(bytes), published: compressed(destination, publishedName, bytes), label });
  };
  for (const [index, root] of roots.entries()) {
    const name = names[index];
    const capture = path.join(destination, name);
    const manifest = await readOriginal(root, 'artifacts.sha256');
    const entries = parseManifest(manifest.toString(), { artifact: true });
    compressed(capture, 'artifacts.sha256', manifest);
    const omitted = {};
    for (const [filename, digest] of entries) {
      const bytes = await readOriginal(root, filename);
      assert.equal(sha256(bytes), digest, 'publisher original digest');
      if (['benchmark', 'native-benchmark'].includes(filename)) {
        omitted[filename] = { sha256: digest, verified_initial: true,
          verification_record: 'publication.json: initial source capture validation plus publisher original digest verification',
          reason: 'Duplicate executable bytes omitted; original digest verified before omission, not reverified from published bytes.' };
      } else compressed(capture, filename, bytes);
    }
    writeJSON(capture, 'published-availability.json', { original_directory: root,
      original_manifest_sha256: sha256(manifest), omitted, source_archive: archiveChecks[index],
      policy: 'All nonbinary original artifacts losslessly gzip-compressed, including traces, timing, OSD snapshots, outcomes, counters and unchanged manifests.' });
    await verifyArtifacts(capture, { published: JSON.parse(fs.readFileSync(path.join(capture, 'published-availability.json'))) });
    assert.ok(fs.existsSync(root + '.log'), 'capture harness log required');
    addSupplement(root + '.log', `logs/${name}.log`, 'original harness log, including failed setup');
  }
  for (const filename of ['factorial-summary.mjs', 'factorial-summary.test.mjs', 'publish-factorial.mjs', 'publish-factorial.test.mjs', 'trace-stall-summary.mjs', 'trace-stall-summary.test.mjs']) {
    addSupplement(path.join(workspace, 'integration/p07', filename), `tooling/${filename}`, 'publication-time tooling snapshot, not capture source identity');
  }
  for (const entry of supplemental) addSupplement(entry.filename, entry.published, entry.label);
  addSupplement(p00Filename, 'provenance/p00-evidence.json', 'P00 qualification image manifest used for digest binding');
  const summary = await publicationSummary(destination, names);
  assert.deepEqual(summary.comparison.rates.map(rate => ({ ...rate, cases: Object.fromEntries(Object.entries(rate.cases).map(([key, value]) => [key, { ...value, captures: [] }])) })),
    original.comparison.rates.map(rate => ({ ...rate, cases: Object.fromEntries(Object.entries(rate.cases).map(([key, value]) => [key, { ...value, captures: [] }])) })), 'raw/published comparison equality');
  for (const [index, capture] of summary.captures.entries()) {
    assert.deepEqual(capture.rows.map(({ capture_root, ...row }) => row), original.captures[index].rows.map(({ capture_root, ...row }) => row), 'raw/published row equality');
  }
  writeJSON(destination, 'summary.json', summary);
  writeJSON(destination, 'source-binding.json', { captures: summary.captures.map(capture => ({ name: capture.root, ...capture.source })),
    image_bindings: imageBindings,
    all_source_artifacts_available: true, current_worktree_substitution: false,
    primary_observed_same_source_manifest: new Set(summary.captures.filter(capture => capture.complete).map(capture => capture.source.source_before_sha256)).size === 1,
    final_workspace_source_manifest: 'Coordinator-owned source-final manifest may be appended as a verified supplemental original; capture archives remain authoritative.' });
  writeJSON(destination, 'publication.json', { schema: 1, benchmark_claim: false, capture_names: names,
    initial_original_verification: original.captures.map(capture => ({ root: capture.root, artifacts: capture.artifacts })),
    supplemental_originals: supplements,
    microbench_identity_limitations: 'No contemporaneous source/binary hash was captured with microbench stdout. Publication hashes preserve stdout identity, not benchmark source binding.',
    validation_limitations: 'Analyzer/publication checks only. Latest normal, tagged, race and broader coordinator validation are not asserted here.' });
  writeInventory(destination);
  return await verifyPublication(destination);
}

export async function addSupplements(root, entries) {
  await verifyPublication(root);
  const provenanceBytes = fs.readFileSync(path.join(root, 'publication.json'));
  const inventoryBytes = fs.readFileSync(path.join(root, 'PUBLISHED.sha256'));
  const provenance = JSON.parse(provenanceBytes);
  const staging = fs.mkdtempSync(path.join(os.tmpdir(), 'factorial-supplements-'));
  const installed = [];
  try {
    for (const entry of entries) {
      assert.ok(!fs.existsSync(path.join(root, entry.published + '.gz')), 'supplement must be new');
      const bytes = fs.readFileSync(entry.filename);
      provenance.supplemental_originals.push({ original: entry.filename, sha256: sha256(bytes),
        published: compressed(staging, entry.published, bytes), label: entry.label });
    }
    for (const name of files(staging)) {
      fs.mkdirSync(path.dirname(path.join(root, name)), { recursive: true });
      fs.copyFileSync(path.join(staging, name), path.join(root, name), fs.constants.COPYFILE_EXCL);
      installed.push(name);
    }
    writeJSON(root, 'publication.json', provenance);
    writeInventory(root);
    return await verifyPublication(root);
  } catch (error) {
    for (const name of installed) fs.unlinkSync(path.join(root, name));
    fs.writeFileSync(path.join(root, 'publication.json'), provenanceBytes);
    fs.writeFileSync(path.join(root, 'PUBLISHED.sha256'), inventoryBytes);
    throw error;
  } finally {
    fs.rmSync(staging, { recursive: true, force: true });
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const execute = async () => {
    if (args[0] === '--verify' && args.length === 2) return verifyPublication(args[1]);
    assert.equal(args.length, 1, 'Usage: node publish-factorial.mjs CONFIG.json | --verify PUBLICATION');
    return publish(JSON.parse(fs.readFileSync(args[0], 'utf8')));
  };
  execute().then(result => console.log(JSON.stringify(result))).catch(error => { console.error(error.message); process.exitCode = 1; });
}