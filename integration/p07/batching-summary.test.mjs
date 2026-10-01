import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { summarize } from './batching-summary.mjs';

function fixture(context, mutate = () => {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'batching-summary-test-'));
  context.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const load of ['idle', 'loaded']) for (const variant of ['before', 'after']) {
    const directory = path.join(root, `${variant}-${load}`);
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, 'scheduler-status'), 'passed');
    for (const phase of ['before', 'after']) fs.writeFileSync(path.join(directory, `source-${phase}.sha256`), 'identity');
    for (let repeat = 1; repeat <= 5; repeat++) {
      const report = { implementation: 'go', transport: 'secure', environment: { gomaxprocs: 10 }, diagnostic: { background_cpu_workers: load === 'idle' ? 0 : 8, warmup_operations: 128, resource_scope: 'warmup_and_measured_reads', object_set: 'p07-shared-read-0..15' }, rows: [{ size_bytes: 65536, concurrency: 16, workload: 'read', operations: 4096, p99_ns: repeat * 1e6, iops: 100 }], resources: { allocated_bytes: 1000, gc_pause_ns: 1e6 } };
      mutate(report);
      fs.writeFileSync(path.join(directory, `sweep-${repeat}-procs-10.json`), JSON.stringify(report));
    }
  }
  return root;
}

test('summarizes all samples and medians', context => {
  const result = summarize(fixture(context));
  assert.equal(result.results.length, 4);
  assert.equal(result.results[0].samples.length, 5);
  assert.equal(result.results[0].median_p99_ms, 3);
});
test('rejects wrong operation count', context => {
  assert.throws(() => summarize(fixture(context, report => report.rows[0].operations = 1)), /wrong workload/);
});
test('rejects wrong parallelism', context => {
  assert.throws(() => summarize(fixture(context, report => report.environment.gomaxprocs = 2)), /wrong workload/);
});
test('rejects missing metrics', context => {
  assert.throws(() => summarize(fixture(context, report => delete report.resources.gc_pause_ns)), /invalid metric/);
});
test('rejects source drift', context => {
  const root = fixture(context);
  fs.writeFileSync(path.join(root, 'before-idle/source-after.sha256'), 'changed');
  assert.throws(() => summarize(root), /source changed/);
});
test('rejects mismatched methodology', context => {
  assert.throws(() => summarize(fixture(context, report => report.diagnostic.warmup_operations = 0)), /wrong workload/);
});
test('allows zero GC pause', context => {
  assert.equal(summarize(fixture(context, report => report.resources.gc_pause_ns = 0)).results[0].median_gc_pause_ms, 0);
});
test('rejects coerced numeric strings', context => {
  assert.throws(() => summarize(fixture(context, report => report.rows[0].p99_ns = '1000')), /invalid metric/);
});