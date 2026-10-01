import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { analyzeOutcomes, analyzeReport, cgroupDelta, parseManifest, sha256, verifyArtifacts } from './request-path-summary.mjs';
import { writeInventory } from './publish-request-path.mjs';

function fixture() {
  return { expected: 100, admitted: 99, attempted: 99, success: 98, timeouts: 1,
    overload: 1, errors: 0, canceled: 0, slo_failures: 2, success_p99_ns: 10,
    all_outcome_p99_ns: 500, outcomes: Array.from({ length: 100 }, (_, index) => ({
      kind: index < 98 ? 'success' : index === 98 ? 'timeout' : 'overload',
      latency_ns: index < 98 ? 10 : 500, admitted: index !== 99,
      attempted: index !== 99, deadline_miss: index === 98 })) };
}

test('preserves failures and represents infinite failure-aware p99 explicitly', () => {
  const result = analyzeOutcomes(fixture());
  assert.equal(result.failures, 2);
  assert.equal(result.actual_all_outcome_p99_ns, 500);
  assert.equal(result.failure_aware_p99_ns, null);
  assert.equal(result.failure_aware_p99_status, 'infinite_failures');
  assert.equal(result.distribution['overload:within_deadline'], 1);
  assert.equal(result.deadline_misses, 1);
});

for (const [name, mutate] of [
  ['dropped failure', report => report.outcomes.pop()],
  ['wrong failure count', report => report.overload++],
  ['wrong SLO count', report => report.slo_failures--],
  ['wrong actual all-outcome p99', report => report.all_outcome_p99_ns = 10],
  ['wrong success p99', report => report.success_p99_ns = 9],
]) test(`rejects ${name}`, () => {
  const report = fixture();
  mutate(report);
  assert.throws(() => analyzeOutcomes(report));
});

test('BigInt cgroup counters preserve deltas above safe integer range', () => {
  assert.deepEqual(cgroupDelta('nr_throttled 9007199254740993\nthrottled_usec 0',
    'nr_throttled 9007199254740994\nthrottled_usec 9007199254740999'),
  { nr_throttled: '1', throttled_usec: '9007199254740999' });
  assert.throws(() => cgroupDelta('nr_throttled 2\nthrottled_usec 0', 'nr_throttled 1\nthrottled_usec 0'));
});

test('manifest rejects duplicate and unsafe paths', () => {
  const digest = 'a'.repeat(64);
  assert.throws(() => parseManifest(`${digest}  one\n${digest}  one`));
  assert.throws(() => parseManifest(`${digest}  ../one`));
  assert.equal(parseManifest(`${digest}  one`).get('one'), digest);
});

function reportFixture() {
  const outcomes = Array.from({ length: 8000 }, () => ({ kind: 'success', latency_ns: 10,
    admitted: true, attempted: true, deadline_miss: false }));
  const workers = Array.from({ length: 8 }, () => ({ expected: 800, completed: 800, missed: 0,
    bytes: 800 * 65536, hash_iterations: 1, max_lateness_ns: 0 }));
  return { implementation: 'go', transport: 'secure',
    environment: { GOOS: 'linux', GOARCH: 'arm64', go_version: 'go1.27.1', gomaxprocs: 10 },
    resources: { cpu_user_ns: 1, cpu_system_ns: 1, allocated_bytes: 1, allocations: 1, max_rss_bytes: 1, gc_cycles: 0, gc_pause_ns: 0 },
    diagnostic: { warmup_operations: 128, resource_scope: 'warmup_issuance_drain_background_join_and_harness',
      object_set: 'p07-shared-read-0..15', background_cpu_workers: 8, read_api: 'read_into', scratch_slots: 4,
      admission_window: 16, background_allocations: true },
    rows: [{ size_bytes: 65536, concurrency: 16, workload: 'read', operations: 8000, bytes: 8000 * 65536,
      elapsed_ns: 8000000000, p50_ns: 0, p95_ns: 0, p99_ns: 10, iops: 1000, throughput_bytes_per_second: 1000 * 65536 }],
    offered_load: { expected: 8000, admitted: 8000, attempted: 8000, success: 8000, timeouts: 0, overload: 0,
      errors: 0, canceled: 0, slo_failures: 0, success_p99_ns: 10, all_outcome_p99_ns: 10,
      config: { rate_ops_per_second: 1000, issuance_window_ns: 8000000000, deadline_ns: 500000000,
        delivery_lag_limit_ns: 50000000, workers: 16, queue_capacity: 128, allocations_per_worker_per_second: 100,
        gomaxprocs: 10, gogc: 100, gomemlimit: 'off' },
      scratch_counters_enabled: false, rows_operations_basis: 'expected_arrivals; bytes and throughput count successful reads only',
      success_bytes: 8000 * 65536, success_ops_per_issuance_second: 1000, success_bytes_per_issuance_second: 1000 * 65536,
      success_bytes_per_window_and_drain_second: 1000 * 65536, total_window_drain_ns: 8000000000,
      max_delivery_lag_ns: 0, delivery_invalid: false, failed: false, outcomes,
      background: { expected: 6400, completed: 6400, missed: 0, bytes: 6400 * 65536, expected_bytes: 6400 * 65536,
        allocation_window_ns: 8000000000, cpu_active_ns: 8000000000, hash_iterations: 8, max_lateness_ns: 0,
        delivery_invalid: false, workers } } };
}

test('validates complete workload and background accounting', () => {
  assert.equal(analyzeReport(reportFixture(), 1000).background.completed, 6400);
});

for (const [name, mutate] of [
  ['wrong workload rate', report => report.offered_load.config.rate_ops_per_second = 2000],
  ['wrong scratch default', report => report.diagnostic.scratch_slots = 8],
  ['missing background work', report => report.offered_load.background.completed--],
  ['wrong background bytes', report => report.offered_load.background.workers[0].bytes--],
  ['wrong background invalid flag', report => report.offered_load.background.delivery_invalid = true],
  ['wrong deadline classification', report => report.offered_load.outcomes[0].deadline_miss = true],
  ['wrong row p99', report => report.rows[0].p99_ns++],
  ['wrong resource count', report => report.resources.allocations = -1],
]) test(`report rejects ${name}`, () => {
  const report = reportFixture();
  mutate(report);
  assert.throws(() => analyzeReport(report, 1000));
});

test('compressed originals are hash verified and omissions require explicit maps', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'request-path-test-'));
  try {
    const digest = sha256('original');
    fs.writeFileSync(path.join(directory, 'artifacts.sha256'), `${digest}  sample\n${digest}  benchmark\n`);
    fs.writeFileSync(path.join(directory, 'sample.gz'), gzipSync('original', { mtime: 0 }));
    const entries = [
      { filename: 'sample', sha256: digest, publication: 'gzip_original', original_verification: 'verified_present_bytes', actual_original_sha256: digest },
      { filename: 'benchmark', sha256: digest, publication: 'omitted', reason: 'binary intentionally omitted', original_verification: 'verified_present_bytes', actual_original_sha256: digest },
    ];
    fs.writeFileSync(path.join(directory, 'availability.json'), JSON.stringify({ entries }));
    assert.equal(verifyArtifacts(directory)[1].verification, 'unavailable_original_manifest_identity_only');
    fs.writeFileSync(path.join(directory, 'sample.gz'), gzipSync('tampered'));
    assert.throws(() => verifyArtifacts(directory), /artifact hash/);
  } finally {
    fs.rmSync(directory, { recursive: true });
  }
});

test('publication inventory includes maps and originals but not itself', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'request-path-inventory-test-'));
  try {
    fs.mkdirSync(path.join(directory, 'capture'));
    fs.writeFileSync(path.join(directory, 'capture/availability.json'), '{}\n');
    fs.writeFileSync(path.join(directory, 'capture/artifacts.sha256.gz'), gzipSync('original manifest'));
    writeInventory(directory);
    const content = fs.readFileSync(path.join(directory, 'PUBLISHED.sha256'), 'utf8');
    const entries = parseManifest(content);
    assert.deepEqual([...entries.keys()], ['capture/artifacts.sha256.gz', 'capture/availability.json']);
    assert.equal(entries.get('capture/availability.json'), sha256('{}\n'));
    writeInventory(directory);
    assert.equal(fs.readFileSync(path.join(directory, 'PUBLISHED.sha256'), 'utf8'), content);
  } finally {
    fs.rmSync(directory, { recursive: true });
  }
});

test('entirely failed request rows retain all failures and infinite percentile', () => {
  const report = fixture();
  report.admitted = 0;
  report.attempted = 0;
  report.success = 0;
  report.timeouts = 0;
  report.overload = 100;
  report.slo_failures = 100;
  report.success_p99_ns = 0;
  report.all_outcome_p99_ns = 500;
  report.outcomes = Array.from({ length: 100 }, () => ({ kind: 'overload', latency_ns: 500,
    admitted: false, attempted: false, deadline_miss: false }));
  const result = analyzeOutcomes(report);
  assert.equal(result.failures, 100);
  assert.equal(result.success_p99_ns, null);
  assert.equal(result.failure_aware_p99_status, 'infinite_failures');
});