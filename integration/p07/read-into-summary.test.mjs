import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { summarizeReadInto } from './read-into-summary.mjs';

function fixture(context, mutate = () => {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'read-into-summary-test-'));
  context.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const load of ['idle', 'loaded']) {
    const directory = path.join(root, load);
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, 'scheduler-status'), 'passed');
    for (const phase of ['before', 'after']) fs.writeFileSync(path.join(directory, `source-${phase}.sha256`), 'identity');
    for (const api of ['read', 'read_into']) for (let repeat = 1; repeat <= 5; repeat++) {
      const report = { implementation: 'go', transport: 'secure', environment: { gomaxprocs: 10 }, diagnostic: { background_cpu_workers: load === 'idle' ? 0 : 8, read_api: api, warmup_operations: 128, resource_scope: 'warmup_and_measured_reads', object_set: 'p07-shared-read-0..15' }, rows: [{ size_bytes: 65536, concurrency: 16, workload: 'read', operations: 4096, p99_ns: repeat * 1e6, iops: 100 }], resources: { allocated_bytes: 1000, allocations: 10, gc_cycles: 0, gc_pause_ns: 0 } };
      mutate(report);
      fs.writeFileSync(path.join(directory, `sweep-${repeat}-procs-10${api === 'read' ? '' : '-into'}.json`), JSON.stringify(report));
    }
  }
  return root;
}

test('retains all pairs and allows zero GC metrics', context => {
  const result = summarizeReadInto(fixture(context));
  assert.equal(result.results.length, 4);
  assert.equal(result.results[0].samples.length, 5);
  assert.equal(result.results[0].medians.p99_ns, 3e6);
});
test('rejects mislabeled API', context => {
  assert.throws(() => summarizeReadInto(fixture(context, report => report.diagnostic.read_api = 'other')), /wrong workload/);
});
test('rejects mismatched worker load', context => {
  assert.throws(() => summarizeReadInto(fixture(context, report => report.diagnostic.background_cpu_workers = 2)), /wrong workload/);
});
test('rejects missing allocation count', context => {
  assert.throws(() => summarizeReadInto(fixture(context, report => delete report.resources.allocations)), /invalid metric/);
});
test('rejects changed sources', context => {
  const root = fixture(context);
  fs.writeFileSync(path.join(root, 'idle/source-after.sha256'), 'changed');
  assert.throws(() => summarizeReadInto(root), /source changed/);
});