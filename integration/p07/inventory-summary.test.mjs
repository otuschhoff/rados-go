import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { summarizeInventory } from './inventory-summary.mjs';

function fixture(context, mutate = () => {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'inventory-summary-test-'));
  context.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const load of ['idle', 'cpu', 'alloc', 'default-confirm']) {
    const directory = path.join(root, load);
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, 'scheduler-status'), 'passed');
    for (const phase of ['before', 'after']) fs.writeFileSync(path.join(directory, `source-${phase}.sha256`), load === 'default-confirm' ? 'final-source' : 'source');
    for (const [slots, window] of load === 'default-confirm' ? [[0,16]] : [[1,16],[2,16],[4,16],[1,8],[1,4]]) for (let repeat = 1; repeat <= 5; repeat++) {
      const report = { implementation: 'go', transport: 'secure', environment: { gomaxprocs: 10, go_version: 'go1.27.1', GOOS: 'linux', GOARCH: 'arm64' }, diagnostic: { read_api: 'read_into', operations_per_worker: 1024, scratch_slots: slots, admission_window: window, background_cpu_workers: load === 'idle' ? 0 : 8, background_allocations: ['alloc','default-confirm'].includes(load), warmup_operations: 128, resource_scope: 'warmup_and_measured_reads', object_set: 'p07-shared-read-0..15', scratch: { hits: 10000, misses: 6512, bypasses: 0, shared_receive_retained_bytes: 221184 } }, rows: [{ size_bytes: 65536, concurrency: 16, workload: 'read', operations: 16384, p50_ns: 1, p95_ns: 2, p99_ns: repeat * 100, iops: 100, elapsed_ns: 100000 }], resources: { allocated_bytes: 1000, allocations: 10, cpu_user_ns: 1, cpu_system_ns: 1, max_rss_bytes: 10000, gc_cycles: 0, gc_pause_ns: 0 } };
      mutate(report);
      const filename = load === 'default-confirm' ? `default-${repeat}` : `inventory-${repeat}-slots-${slots}-window-${window}`;
      fs.writeFileSync(path.join(directory, `${filename}.json`), JSON.stringify(report));
      for (const phase of ['before','after']) fs.writeFileSync(path.join(directory, `${filename}-cpu.stat-${phase}`), 'nr_throttled 0\nthrottled_usec 0\n');
    }
  }
  return root;
}

test('retains all eighty samples and comparisons', context => {
  const result = summarizeInventory(fixture(context));
  assert.equal(result.results.length, 16);
  assert.equal(result.comparisons.length, 12);
  assert.equal(result.results[0].medians.p99_ns, 300);
});
test('rejects short capture', context => {
  assert.throws(() => summarizeInventory(fixture(context, report => report.rows[0].operations = 4096)), /wrong workload/);
});
test('rejects mismatched allocation mode', context => {
  assert.throws(() => summarizeInventory(fixture(context, report => report.diagnostic.background_allocations = true)), /wrong workload/);
});
test('rejects incorrect scratch totals', context => {
  assert.throws(() => summarizeInventory(fixture(context, report => report.diagnostic.scratch.hits = 1)), /wrong receive count/);
});
test('rejects missing resource metric', context => {
  assert.throws(() => summarizeInventory(fixture(context, report => delete report.resources.max_rss_bytes)), /invalid metric/);
});
test('rejects cross-load source drift', context => {
  const root = fixture(context);
  for (const phase of ['before','after']) fs.writeFileSync(path.join(root, `cpu/source-${phase}.sha256`), 'changed');
  assert.throws(() => summarizeInventory(root), /variant source mismatch/);
});
test('rejects absent cgroup counter', context => {
  const root = fixture(context);
  fs.writeFileSync(path.join(root, 'idle/inventory-1-slots-1-window-16-cpu.stat-after'), 'nr_throttled 0\n');
  assert.throws(() => summarizeInventory(root), /missing throttle counter/);
});