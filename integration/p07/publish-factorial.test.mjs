import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { gzipSync } from 'node:zlib';
import { sha256, writeInventory, verifySourceArchive, verifyPublication, verifyTraceBinding, publish, addSupplements } from './publish-factorial.mjs';

function temporary() { return fs.mkdtempSync(path.join(os.tmpdir(), 'factorial-publisher-test-')); }

test('source archive verifies actual bytes and exact inventory, including compressed publication', async () => {
  const root = temporary();
  try {
    fs.mkdirSync(path.join(root, 'tree'));
    fs.writeFileSync(path.join(root, 'tree/source.go'), 'package fixture\n');
    fs.writeFileSync(path.join(root, 'source-before.sha256'), `${sha256(Buffer.from('package fixture\n'))}  source.go\n`);
    assert.equal(spawnSync('tar', ['-czf', path.join(root, 'workload-sources.tar.gz'), '-C', path.join(root, 'tree'), 'source.go']).status, 0);
    assert.equal((await verifySourceArchive(root)).files, 1);
    for (const name of ['source-before.sha256', 'workload-sources.tar.gz']) {
      fs.writeFileSync(path.join(root, name + '.gz'), gzipSync(fs.readFileSync(path.join(root, name))));
      fs.unlinkSync(path.join(root, name));
    }
    assert.equal((await verifySourceArchive(root)).status, 'all_archived_source_bytes_verified');
    fs.writeFileSync(path.join(root, 'source-before.sha256.gz'), gzipSync(Buffer.from(`${'0'.repeat(64)}  source.go\n`)));
    await assert.rejects(verifySourceArchive(root), /archive source hash/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('source archive rejects unbound extra files and symlink entries', async () => {
  const root = temporary();
  try {
    fs.writeFileSync(path.join(root, 'source.go'), 'source');
    fs.writeFileSync(path.join(root, 'extra.go'), 'extra');
    fs.writeFileSync(path.join(root, 'source-before.sha256'), `${sha256(Buffer.from('source'))}  source.go\n`);
    assert.equal(spawnSync('tar', ['-czf', path.join(root, 'workload-sources.tar.gz'), '-C', root, 'source.go', 'extra.go']).status, 0);
    await assert.rejects(verifySourceArchive(root), /coverage/);
    fs.unlinkSync(path.join(root, 'source.go'));
    fs.symlinkSync('extra.go', path.join(root, 'source.go'));
    assert.equal(spawnSync('tar', ['-czf', path.join(root, 'workload-sources.tar.gz'), '-C', root, 'source.go']).status, 0);
    await assert.rejects(verifySourceArchive(root), /regular files/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('trace handoff binds exact original hashes, analyzer bytes and service population', async () => {
  const root = temporary();
  try {
    for (const directory of ['observed', 'trace-analysis', 'tooling']) fs.mkdirSync(path.join(root, directory));
    const digest = sha256(Buffer.from('trace fixture'));
    const manifest = Buffer.from(`${digest}  ./trace.out\n`);
    fs.writeFileSync(path.join(root, 'observed/artifacts.sha256.gz'), gzipSync(manifest));
    const analyzer = Buffer.from('export const fixture = true;\n');
    fs.writeFileSync(path.join(root, 'tooling/trace-stall-summary.mjs.gz'), gzipSync(analyzer));
    const hashes = { 'artifacts.sha256': sha256(manifest), 'trace.out': digest };
    for (const name of ['original-hashes-before.json', 'original-hashes-after-analysis.json']) fs.writeFileSync(path.join(root, 'trace-analysis', name + '.gz'), gzipSync(JSON.stringify(hashes)));
    const trace = { analysis_status: 'complete', complete_rows: 1, expected_rows: 1,
      analyzer_sha256: sha256(analyzer), source_final_status: { status: 'diagnostic-failed' },
      rows: [{ label: 'observed-1', analysis_status: 'complete', counts: { attempted: 1 }, latency: { service_attempted: { p99_ns: 42 } } }] };
    const summary = { captures: [{ root: 'observed', complete: true, measurement: 'instrumented', final: trace.source_final_status,
      rows: [{ filename: 'observed-1.json', counts: { attempted: 1 }, phase_percentiles: { service_ns: { attempted: { p99_ns: 42 } } } }] }] };
    const write = () => fs.writeFileSync(path.join(root, 'trace-analysis/summary.json.gz'), gzipSync(JSON.stringify(trace)));
    write();
    assert.equal((await verifyTraceBinding(root, summary)).rows, 1);
    trace.rows[0].latency.service_attempted.p99_ns = 43;
    write();
    await assert.rejects(verifyTraceBinding(root, summary), /service p99/);
    trace.rows[0].latency.service_attempted.p99_ns = 42;
    write();
    fs.writeFileSync(path.join(root, 'tooling/trace-stall-summary.mjs.gz'), gzipSync(Buffer.from('changed analyzer')));
    await assert.rejects(verifyTraceBinding(root, summary), /matching published tool bytes/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('publication inventory detects tampering and unexpected files before interpreting summary', async () => {
  const root = temporary();
  try {
    fs.writeFileSync(path.join(root, 'raw.json.gz'), gzipSync(Buffer.from('{}\n')));
    assert.equal(writeInventory(root), 1);
    fs.appendFileSync(path.join(root, 'raw.json.gz'), 'tamper');
    await assert.rejects(verifyPublication(root), /published hash/);
    writeInventory(root);
    fs.writeFileSync(path.join(root, 'extra'), 'extra');
    await assert.rejects(verifyPublication(root), /coverage/);
    fs.unlinkSync(path.join(root, 'extra'));
    fs.symlinkSync('raw.json.gz', path.join(root, 'link'));
    assert.throws(() => writeInventory(root), /symlinks/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('publisher round trip retains setup failures, source bytes, logs and supplements', async () => {
  const root = temporary();
  try {
    const capture = path.join(root, 'setup-failed');
    fs.mkdirSync(capture);
    const source = Buffer.from('package fixture\n');
    const manifest = `${sha256(source)}  source.go\n`;
    fs.writeFileSync(path.join(root, 'source.go'), source);
    assert.equal(spawnSync('tar', ['-czf', path.join(capture, 'workload-sources.tar.gz'), '-C', root, 'source.go']).status, 0);
    const originals = {
      'source-before.sha256': manifest, 'source-after.sha256': manifest, 'source-files.nul': 'source.go\0',
      'source-snapshot-before.txt': 'source.go: OK\n', 'source-snapshot-check.txt': 'source.go: OK\n',
      'source-head': 'fixture\n', 'scheduler-status': 'diagnostic-failed\n',
      'final-status.json': JSON.stringify({ status: 'diagnostic-failed', measurement: 'primary', benchmark_claim: false, exit_code: 2, failed_legs: 0, source_check: 'unchanged' }),
      benchmark: 'verified executable fixture',
    };
    for (const [name, bytes] of Object.entries(originals)) fs.writeFileSync(path.join(capture, name), bytes);
    const entries = fs.readdirSync(capture).map(name => `${sha256(fs.readFileSync(path.join(capture, name)))}  ./${name}\n`);
    fs.writeFileSync(path.join(capture, 'artifacts.sha256'), entries.join(''));
    fs.writeFileSync(capture + '.log', 'missing P00 image manifest\n');
    const destination = path.join(root, 'publication');
    const workspace = path.resolve(import.meta.dirname, '../..');
    const result = await publish({ roots: [capture], destination, workspace });
    assert.equal(result.rows, 0);
    assert.equal(result.initially_verified_omitted_binaries, 1);
    assert.equal(result.captures, 1);
    assert.ok(fs.existsSync(path.join(destination, 'setup-failed/workload-sources.tar.gz.gz')));
    assert.ok(fs.existsSync(path.join(destination, 'logs/setup-failed.log.gz')));
    fs.writeFileSync(path.join(root, 'coordinator.txt'), 'coordinator result\n');
    await assert.rejects(addSupplements(destination, [{ filename: path.join(root, 'coordinator.txt'), published: '../escape', label: 'unsafe' }]), /unsafe publication path/);
    await assert.rejects(addSupplements(destination, [
      { filename: path.join(root, 'coordinator.txt'), published: 'coordinator/partial.txt', label: 'staged but not published' },
      { filename: path.join(root, 'missing'), published: 'coordinator/missing.txt', label: 'unavailable source' },
    ]), /ENOENT/);
    assert.ok(!fs.existsSync(path.join(destination, 'coordinator/partial.txt.gz')));
    assert.equal((await verifyPublication(destination)).captures, 1);
    assert.equal((await addSupplements(destination, [{ filename: path.join(root, 'coordinator.txt'), published: 'coordinator/result.txt', label: 'independent final validation' }])).captures, 1);
    fs.appendFileSync(path.join(destination, 'coordinator/result.txt.gz'), 'tamper');
    await assert.rejects(verifyPublication(destination), /published hash/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});