import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm, symlink, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { validateRow, parseRowName, matrixNames, percentile, parseManifest, parseCPUStat, summarizeCgroup, verifyArtifacts, readOriginal, compareCases, summarizeCapture } from './factorial-summary.mjs';

function fixture(loadCase = 'none', { setup = false, instrumented = false } = {}) {
  const cpu = ['cpu', 'both'].includes(loadCase) ? 8 : 0;
  const alloc = ['alloc', 'both'].includes(loadCase) ? 8 : 0;
  const workers = Array.from({ length: cpu + alloc }, (_, index) => ({ role: index < cpu ? 'cpu' : 'alloc',
    expected: index < cpu ? 0 : 800, completed: index < cpu || setup ? 0 : 800, missed: index < cpu || !setup ? 0 : 800,
    bytes: index < cpu || setup ? 0 : 800 * 65536, hash_iterations: index < cpu && !setup ? 6 : 0,
    warmup_hash_iterations: index < cpu && !setup ? 1 : 0, window_hash_iterations: index < cpu && !setup ? 3 : 0,
    drain_hash_iterations: index < cpu && !setup ? 2 : 0, max_lateness_ns: 0 }));
  const background = { cpu_workers: cpu, allocation_workers: alloc, workers, allocation_window_ns: 8e9,
    cpu_active_ns: cpu && !setup ? 8e9 + 100 : 0, expected_bytes: alloc * 800 * 65536, max_lateness_ns: 0, delivery_invalid: setup && alloc > 0 };
  for (const field of ['expected', 'completed', 'missed', 'bytes', 'hash_iterations', 'warmup_hash_iterations', 'window_hash_iterations', 'drain_hash_iterations']) background[field] = workers.reduce((sum, worker) => sum + worker[field], 0);
  const outcomes = Array.from({ length: 8000 }, (_, index) => {
    const scheduled = index * 1e6;
    return { scheduled_ns: scheduled, enqueued_ns: setup ? -1 : scheduled + 10,
      worker_start_ns: setup ? -1 : scheduled + 30, read_start_ns: setup ? -1 : scheduled + 60,
      returned_ns: setup ? -1 : scheduled + 100, delivery_delay_ns: setup ? -1 : 10, queue_delay_ns: setup ? -1 : 20,
      dispatch_ns: setup ? -1 : 30, service_ns: setup ? -1 : 40, latency_ns: setup ? 0 : 100,
      kind: setup ? 'canceled' : 'success', admitted: !setup, attempted: !setup, deadline_miss: false };
  });
  const success = setup ? 0 : 8000;
  const result = { config: { factorial: true, instrumented, load_case: loadCase, cpu_workers: cpu, allocation_workers: alloc,
    rate_ops_per_second: 1000, issuance_window_ns: 8e9, deadline_ns: 5e8, delivery_lag_limit_ns: 5e7, workers: 16,
    queue_capacity: 128, allocations_per_worker_per_second: 100, gomaxprocs: 10, gogc: 100, gomemlimit: 'off' },
    expected: 8000, admitted: success, attempted: success, success, timeouts: 0, overload: 0, errors: 0, canceled: setup ? 8000 : 0,
    slo_failures: setup ? 8000 : 0, success_p99_ns: setup ? 0 : 100, all_outcome_p99_ns: setup ? 0 : 100,
    max_delivery_lag_ns: setup ? 0 : 10, delivery_invalid: setup, failed: setup, queue_high_water_sampled: setup ? 0 : 1,
    timing_methodology: setup ? '' : 'monotonic; -1 means phase not reached',
    rows_operations_basis: setup ? 'expected_arrivals; setup failed before issuance' : 'expected_arrivals; bytes and throughput count successful reads only',
    scratch_counters_enabled: false, resource_limitations: setup ? 'setup failed; resource deltas unavailable' : 'process-wide deltas include warmup, background and harness',
    warmup_ns: setup ? 0 : 1000, actual_issuance_ns: setup ? 0 : 8e9, total_window_drain_ns: setup ? 0 : 8e9,
    drain_ns: 0, success_bytes: success * 65536, success_ops_per_issuance_second: success / 8,
    success_bytes_per_issuance_second: success * 65536 / 8, success_bytes_per_window_and_drain_second: success * 65536 / 8, background, outcomes };
  if (setup) result.failure_reason = 'setup: fixture';
  return { implementation: 'go', transport: 'secure', environment: { GOOS: 'linux', GOARCH: 'amd64', go_version: 'go1.27.1', gomaxprocs: 10 },
    diagnostic: { read_api: 'read_into', scratch_slots: 4, admission_window: 16, warmup_operations: 128,
      ...(cpu ? { background_cpu_workers: cpu } : {}), ...(alloc ? { background_allocations: true } : {}),
      resource_scope: 'warmup_issuance_drain_background_join_and_harness', object_set: 'p07-shared-read-0..15' },
    resources: { cpu_user_ns: 0, cpu_system_ns: 0, allocations: 0, allocated_bytes: setup ? 0 : 800000, max_rss_bytes: 0 },
    rows: [{ size_bytes: 65536, concurrency: 16, workload: 'read', operations: 8000, bytes: success * 65536, elapsed_ns: setup ? 0 : 8e9,
      throughput_bytes_per_second: success * 65536 / 8, iops: success / 8, p50_ns: 0, p95_ns: 0, p99_ns: setup ? 0 : 100 }], offered_load: result };
}

function meta(loadCase = 'none', instrumented = false) {
  return { rate: 1000, loadCase, instrumented, repeat: 1 };
}

function reconcile(report) {
  const result = report.offered_load;
  const outcomes = result.outcomes;
  for (const [kind, field] of Object.entries({ success: 'success', timeout: 'timeouts', overload: 'overload', error: 'errors', canceled: 'canceled' })) result[field] = outcomes.filter(outcome => outcome.kind === kind).length;
  for (const field of ['admitted', 'attempted']) result[field] = outcomes.filter(outcome => outcome[field]).length;
  result.slo_failures = outcomes.filter(outcome => outcome.kind !== 'success' || outcome.deadline_miss).length;
  result.success_p99_ns = percentile(outcomes.filter(outcome => outcome.kind === 'success').map(outcome => outcome.latency_ns)) ?? 0;
  result.all_outcome_p99_ns = percentile(outcomes.map(outcome => outcome.latency_ns));
  result.max_delivery_lag_ns = Math.max(0, ...outcomes.map(outcome => outcome.delivery_delay_ns));
  result.delivery_invalid = result.max_delivery_lag_ns > 5e7;
  result.failed = result.slo_failures > 0 || result.delivery_invalid || result.background.delivery_invalid;
  result.success_bytes = result.success * 65536;
  result.success_ops_per_issuance_second = result.success / 8;
  result.success_bytes_per_issuance_second = result.success_bytes / 8;
  result.success_bytes_per_window_and_drain_second = result.success_bytes / (result.total_window_drain_ns / 1e9);
  Object.assign(report.rows[0], { bytes: result.success_bytes, iops: result.success / 8, throughput_bytes_per_second: result.success_bytes / 8, p99_ns: result.all_outcome_p99_ns });
}

test('matrix filenames have exactly 60 primary and 12 observed legs', () => {
  assert.equal(matrixNames().length, 60);
  assert.equal(matrixNames(true).length, 12);
  for (const name of [...matrixNames(), ...matrixNames(true)]) assert.equal(parseRowName(name).filename, name);
  for (const name of ['observed-4-case-none-rate-2000.json', 'observed-1-case-none-rate-1000.json', 'factorial-0-case-none-rate-1000.json', 'factorial-1-case-neither-rate-1000.json', '../factorial-1-case-none-rate-1000.json']) assert.throws(() => parseRowName(name));
});

test('nearest-rank p99 preserves empty and exact rank semantics', () => {
  assert.equal(percentile([]), null);
  assert.equal(percentile(Array.from({ length: 100 }, (_, index) => index + 1)), 99);
  assert.equal(percentile([2, 1]), 2);
});

test('diagnostic zero fields use producer omitempty semantics', () => {
  const report = fixture();
  assert.equal(validateRow(report, meta()).counts.success, 8000);
  report.diagnostic.background_cpu_workers = 0;
  assert.throws(() => validateRow(report, meta()), /omitempty/);
  delete report.diagnostic.background_cpu_workers;
  report.diagnostic.background_allocations = false;
  assert.throws(() => validateRow(report, meta()), /omitempty/);
});

test('all roles validate independent hashes, allocation totals and retention', () => {
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
    const row = validateRow(fixture(loadCase), meta(loadCase));
    assert.equal(row.counts.success, 8000);
    assert.equal(row.worst_attempts.length, 50);
    assert.equal(row.failure_aware.status, 'finite');
    assert.equal(row.allocated_bytes_per_success, 100);
    assert.equal(row.background.retained_bytes_inferred_at_join, ['alloc', 'both'].includes(loadCase) ? 8 * 8 * 65536 : 0);
    assert.ok(row.worst_attempts.every(outcome => outcome.latency_ns === outcome.phase_sum_ns));
    assert.equal(row.largest_phase_samples.service_ns.index, 0);
    assert.equal(row.largest_phase_samples.service_ns.service_ns, 40);
  }
});

test('setup rows retain missing timing, zero CPU work and infinity failure-aware p99', () => {
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
    const row = validateRow(fixture(loadCase, { setup: true }), meta(loadCase));
    assert.equal(row.setup_failure, true);
    assert.equal(row.failure_aware.p99_ns, null);
    assert.equal(row.failure_aware.status, 'infinity');
    assert.equal(row.phase_percentiles.service_ns.attempted.count, 0);
    assert.equal(row.phase_percentiles.service_ns.attempted.p99_ns, null);
    assert.equal(row.worst_attempts.length, 0);
    assert.equal(row.largest_phase_samples.service_ns, null);
  }
});

test('local contract mutations are rejected', () => {
  const mutations = [
    report => report.offered_load.config.gogc = 99,
    report => report.offered_load.config.instrumented = true,
    report => report.offered_load.outcomes[0].service_ns++,
    report => report.offered_load.outcomes[0].scheduled_ns++,
    report => report.offered_load.success--,
    report => report.offered_load.success_p99_ns++,
    report => report.offered_load.success_bytes++,
    report => report.offered_load.failed = true,
    report => report.offered_load.queue_high_water_sampled = 129,
    report => report.offered_load.background.hash_iterations++,
    report => report.offered_load.background.workers[0].role = 'alloc',
    report => report.offered_load.outcomes[0].deadline_miss = true,
    report => report.diagnostic.scratch = {},
    report => report.resources.allocated_bytes = Number.MAX_SAFE_INTEGER + 1,
  ];
  for (const mutate of mutations) {
    const report = fixture('both');
    mutate(report);
    assert.throws(() => validateRow(report, meta('both')));
  }
});

test('failed SLOs affect infinity ranking without erasing success-only p99', () => {
  const report = fixture();
  for (const outcome of report.offered_load.outcomes.slice(0, 81)) outcome.kind = 'error';
  reconcile(report);
  const row = validateRow(report, meta());
  assert.equal(row.failure_aware.status, 'infinity');
  assert.equal(row.success_p99_ns, 100);
  assert.equal(row.kind_counts.error.attempted, 81);
  report.offered_load.outcomes[80].kind = 'success';
  reconcile(report);
  assert.equal(validateRow(report, meta()).failure_aware.status, 'finite');
});

test('nonadmission and admitted nonattempts retain different phase populations', () => {
  const report = fixture();
  Object.assign(report.offered_load.outcomes[0], { kind: 'overload', admitted: false, attempted: false, enqueued_ns: -1, worker_start_ns: -1,
    read_start_ns: -1, returned_ns: 10, delivery_delay_ns: 10, queue_delay_ns: -1, dispatch_ns: -1, service_ns: -1, latency_ns: 10 });
  const second = report.offered_load.outcomes[1];
  Object.assign(second, { kind: 'timeout', attempted: false, read_start_ns: -1, returned_ns: second.scheduled_ns + 5e8,
    dispatch_ns: -1, service_ns: -1, latency_ns: 5e8, deadline_miss: true });
  reconcile(report);
  const row = validateRow(report, meta());
  assert.equal(row.phase_percentiles.delivery_delay_ns.reached.count, 8000);
  assert.equal(row.phase_percentiles.queue_delay_ns.reached.count, 7999);
  assert.equal(row.phase_percentiles.service_ns.reached.count, 7998);
  assert.equal(row.counts.nonattempted, 2);
  assert.equal(row.counts.deadline_misses, 1);
  second.dispatch_ns = 0;
  assert.throws(() => validateRow(report, meta()), /nonattempted sentinel/);
});

test('late successful return is an SLO failure and delivery failure is independent', () => {
  const report = fixture();
  const outcome = report.offered_load.outcomes[0];
  Object.assign(outcome, { returned_ns: 5e8, latency_ns: 5e8, service_ns: 5e8 - 60, deadline_miss: true });
  reconcile(report);
  const row = validateRow(report, meta());
  assert.equal(row.counts.success, 8000);
  assert.equal(row.counts.slo_failures, 1);
  assert.equal(row.failed, true);
  assert.equal(row.delivery_invalid, false);
});

test('pre-arrival cancellation has missing delivery and a clamped legacy lifetime', () => {
  const report = fixture();
  const outcome = report.offered_load.outcomes[1];
  Object.assign(outcome, { kind: 'canceled', admitted: false, attempted: false, enqueued_ns: -1, worker_start_ns: -1,
    read_start_ns: -1, returned_ns: 0, delivery_delay_ns: -1, queue_delay_ns: -1, dispatch_ns: -1, service_ns: -1, latency_ns: 0 });
  reconcile(report);
  const row = validateRow(report, meta());
  assert.equal(row.phase_percentiles.delivery_delay_ns.reached.count, 7999);
  assert.equal(row.kind_counts.canceled.nonattempted, 1);
  outcome.delivery_delay_ns = 0;
  assert.throws(() => validateRow(report, meta()), /rejection delivery/);
});

test('delivery threshold is strictly greater than 50ms, while deadline uses greater-or-equal', () => {
  const report = fixture();
  const outcome = report.offered_load.outcomes[0];
  for (const delay of [5e7, 5e7 + 1]) {
    Object.assign(outcome, { enqueued_ns: delay, worker_start_ns: delay + 20, read_start_ns: delay + 50,
      returned_ns: delay + 90, delivery_delay_ns: delay, latency_ns: delay + 90 });
    reconcile(report);
    assert.equal(validateRow(report, meta()).delivery_invalid, delay > 5e7);
  }
});

test('allocation misses and excessive lateness are validated but remain failed observations', () => {
  const report = fixture('alloc');
  const background = report.offered_load.background;
  background.workers[0].completed--;
  background.workers[0].missed++;
  background.workers[0].bytes -= 65536;
  background.completed--;
  background.missed++;
  background.bytes -= 65536;
  background.delivery_invalid = true;
  report.offered_load.failed = true;
  assert.equal(validateRow(report, meta('alloc')).failed, true);
  background.workers[1].hash_iterations = 1;
  assert.throws(() => validateRow(report, meta('alloc')));
});

const stat = base => `usage_usec ${base}\nuser_usec ${base}\nsystem_usec ${base}\nnr_periods ${base}\nnr_throttled ${base}\nthrottled_usec ${base}\n`;

test('cgroups retain exact BigInt deltas and report nonzero throttling', () => {
  const row = summarizeCgroup(stat('90071992547409930'), stat('90071992547409933'), 'max 100000\n', 'max 100000\n');
  assert.equal(row.delta.usage_usec, '3');
  assert.equal(row.throttling_observed, true);
  assert.equal(parseCPUStat(stat('0')).usage_usec, 0n);
  assert.throws(() => summarizeCgroup(stat('3'), stat('2'), 'max 100000', 'max 100000'), /decreased/);
  assert.throws(() => summarizeCgroup(stat('0'), stat('0'), '1000 100000', '1000 100000'), /quota/);
  assert.throws(() => parseCPUStat(stat('0') + 'usage_usec 1\n'), /duplicate/);
});

const sha = bytes => createHash('sha256').update(bytes).digest('hex');

test('manifest parser anchors exact digest records and rejects unsafe/duplicate paths', () => {
  const hash = 'a'.repeat(64);
  assert.equal(parseManifest(`${hash}  ./report.json\n`, { artifact: true }).get('report.json'), hash);
  for (const line of [`prefix${hash}  ./x\n`, `${hash}  ./../x\n`, `${hash}  /x\n`, `${hash}  ./x\n${hash}  x\n`, `${hash}  ./x\\y\n`]) assert.throws(() => parseManifest(line));
  assert.throws(() => parseManifest(`${hash}  x\n`, { artifact: true }));
});

test('raw and gzip-fallback reports hash identical original bytes and explicit omissions stay unverified', async () => {
  const root = await mkdtemp(join(tmpdir(), 'factorial-unit-'));
  try {
    const bytes = Buffer.from('{"fixture":true}\n');
    const binary = Buffer.from([0, 255, 17]);
    await writeFile(join(root, 'report.json.gz'), gzipSync(bytes));
    await writeFile(join(root, 'benchmark'), binary);
    await writeFile(join(root, 'artifacts.sha256'), `${sha(bytes)}  ./report.json\n${sha(binary)}  ./benchmark\n`);
    assert.deepEqual(await readOriginal(root, 'report.json'), bytes);
    assert.equal((await verifyArtifacts(root)).verified_original_bytes, 2);
    await rm(join(root, 'benchmark'));
    await assert.rejects(verifyArtifacts(root), /unavailable/);
    const published = { omitted: { benchmark: { sha256: sha(binary), verified_initial: true, verification_record: 'publisher-record-1' } } };
    const verified = await verifyArtifacts(root, { published });
    assert.equal(verified.all_original_artifacts_reverified, false);
    assert.equal(verified.unavailable.length, 1);
    published.omitted.benchmark.sha256 = '0'.repeat(64);
    await assert.rejects(verifyArtifacts(root, { published }), /digest/);
    await writeFile(join(root, 'report.json.gz'), gzipSync(Buffer.from('tampered')));
    await assert.rejects(verifyArtifacts(root, { published: null }), /SHA256/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('comparison uses repeated run-level medians, excludes observed/incomplete rows and describes interaction', () => {
  const rows = ['none', 'cpu', 'alloc', 'both'].flatMap((loadCase, caseIndex) => {
    const base = validateRow(fixture(loadCase), meta(loadCase));
    return Array.from({ length: 10 }, (_, repeat) => ({ ...base, capture_root: repeat < 5 ? 'v3' : 'v4', all_outcome_p99_ns: [100, 200, 300, 500][caseIndex] + repeat }));
  });
  rows.push({ ...rows[0], instrumented: true, all_outcome_p99_ns: 1e9 });
  rows.push({ ...rows[0], capture_complete: false, all_outcome_p99_ns: 1e9 });
  const result = compareCases(rows);
  assert.equal(result.rates[0].cases.none.runs, 10);
  assert.equal(result.rates[0].cases.none.run_level_medians.all_outcome_p99_ns, 104.5);
  assert.equal(result.rates[0].descriptive_interaction_all_outcome_p99_ns, 100);
  assert.match(result.interpretation, /not causal/);
});

test('failure-aware run median keeps infinities in rank without emitting non-JSON numbers', () => {
  const finite = validateRow(fixture(), meta());
  const infinite = validateRow(fixture('none', { setup: true }), meta());
  const finiteMajority = compareCases([finite, finite, infinite]).rates[0].cases.none;
  assert.equal(finiteMajority.run_level_medians.failure_aware_p99_ns, 100);
  assert.equal(finiteMajority.run_level_medians.failure_aware_p99_status, 'finite');
  const infiniteMajority = compareCases([finite, infinite, infinite]).rates[0].cases.none;
  assert.equal(infiniteMajority.run_level_medians.failure_aware_p99_ns, null);
  assert.equal(infiniteMajority.run_level_medians.failure_aware_p99_status, 'infinity');
  assert.equal(infiniteMajority.totals.expected, 24000);
});

test('capture qualification failure is retained independently of valid workload outcomes', () => {
  const row = { ...validateRow(fixture(), meta()), exits: { benchmark: 0, docker: 0, capture: 1 } };
  const result = compareCases([row]).rates[0].cases.none;
  assert.equal(result.failed_runs, 0);
  assert.equal(result.capture_failed_runs, 1);
  assert.equal(result.totals.success, 8000);
});

async function writeCapture(root, files, { compressReports = false } = {}) {
  const records = [];
  for (const [name, data] of Object.entries(files)) {
    await mkdir(dirname(join(root, name)), { recursive: true });
    const bytes = Buffer.isBuffer(data) ? data : Buffer.from(typeof data === 'string' ? data : JSON.stringify(data) + '\n');
    if (compressReports && /^(factorial|observed)-.*\.json$/.test(name)) {
      await rm(join(root, name), { force: true });
      await writeFile(join(root, name + '.gz'), gzipSync(bytes));
    } else await writeFile(join(root, name), bytes);
    records.push(`${sha(bytes)}  ./${name}\n`);
  }
  await writeFile(join(root, 'artifacts.sha256'), records.join(''));
}

function captureFixture() {
  const label = 'factorial-1-case-none-rate-1000';
  const source = `${sha(Buffer.from('source fixture'))}  integration/p07/benchmark/offered_load.go\n`;
  return {
    'source-before.sha256': source,
    'source-after.sha256': source,
    'source-files.nul': Buffer.from('integration/p07/benchmark/offered_load.go\0'),
    'source-snapshot-before.txt': 'integration/p07/benchmark/offered_load.go: OK\n',
    'source-snapshot-check.txt': 'integration/p07/benchmark/offered_load.go: OK\n',
    'workload-sources.tar.gz': Buffer.from('archive bytes are hashed; tar content verification is explicitly external'),
    'source-head': 'fixture-head\n',
    'scheduler-status': 'diagnostic-failed\n',
    'final-status.json': { measurement: 'primary', benchmark_claim: false, source_check: 'unchanged', exit_code: 1, failed_legs: 1, status: 'diagnostic-failed' },
    [`${label}.json`]: fixture('none', { setup: true }),
    [`${label}-benchmark-exit-code`]: '1\n',
    [`${label}-exit-code`]: '1\n',
    [`${label}-capture-exit-code`]: '1\n',
    [`${label}-cpu.stat-before`]: stat('0'),
    [`${label}-cpu.stat-after`]: stat('0'),
    [`${label}-cpu.max-before`]: 'max 100000\n',
    [`${label}-cpu.max-after`]: 'max 100000\n',
  };
}

test('root summary preserves incomplete failure capture, checks source bindings and final leg exits', async () => {
  const root = await mkdtemp(join(tmpdir(), 'factorial-root-unit-'));
  try {
    const files = captureFixture();
    await writeCapture(root, files);
    const summary = await summarizeCapture(root);
    assert.equal(summary.complete, false);
    assert.equal(summary.missing_rows.length, 59);
    assert.equal(summary.rows[0].setup_failure, true);
    assert.equal(summary.comparison.rates.length, 0);
    assert.equal(summary.source.current_worktree_not_consulted, true);
    assert.equal(summary.source.archive_content_verification, 'pending_external_tar_verification');
    files['final-status.json'].failed_legs = 0;
    await writeCapture(root, files);
    await assert.rejects(summarizeCapture(root), /failed-leg count/);
    files['final-status.json'].failed_legs = 1;
    files['source-after.sha256'] = files['source-before.sha256'].replace(/^[a-f0-9]/, digit => digit === '0' ? '1' : '0');
    await writeCapture(root, files);
    await assert.rejects(summarizeCapture(root), /manifests differ/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('completed root must contain every matrix row and cannot disguise a missing capture', async () => {
  const root = await mkdtemp(join(tmpdir(), 'factorial-matrix-unit-'));
  try {
    const files = captureFixture();
    Object.assign(files['final-status.json'], { status: 'diagnostic-completed', exit_code: 0, failed_legs: 0 });
    files['scheduler-status'] = 'diagnostic-completed\n';
    await writeCapture(root, files);
    await assert.rejects(summarizeCapture(root), /matrix is incomplete/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('nested observation artifacts are not treated as top-level matrix rows', async () => {
  const root = await mkdtemp(join(tmpdir(), 'factorial-nested-unit-'));
  try {
    const files = captureFixture();
    files['observed-1-case-none-rate-2000-observation/timing.json'] = { schema: 1, label: 'instrumented', calls: [] };
    await writeCapture(root, files);
    assert.equal((await summarizeCapture(root)).rows.length, 1);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('publication compression preserves identical row arrays and original source identity', async () => {
  const root = await mkdtemp(join(tmpdir(), 'factorial-compression-unit-'));
  try {
    const files = captureFixture();
    await writeCapture(root, files);
    const raw = await summarizeCapture(root);
    await writeCapture(root, files, { compressReports: true });
    const compressed = await summarizeCapture(root);
    assert.deepEqual(compressed.rows, raw.rows);
    assert.deepEqual(compressed.source, raw.source);
    assert.deepEqual(compressed.comparison, raw.comparison);
    assert.equal(compressed.artifacts.verified_original_bytes, raw.artifacts.verified_original_bytes);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('manifest verification rejects extra files, symlink escapes and ambiguous compression', async () => {
  const root = await mkdtemp(join(tmpdir(), 'factorial-path-unit-'));
  const outside = await mkdtemp(join(tmpdir(), 'factorial-outside-unit-'));
  try {
    const bytes = Buffer.from('original');
    await writeFile(join(root, 'report.json'), bytes);
    await writeFile(join(root, 'artifacts.sha256'), `${sha(bytes)}  ./report.json\n`);
    await writeFile(join(root, 'extra'), 'unmanifested');
    await assert.rejects(verifyArtifacts(root), /unmanifested/);
    await rm(join(root, 'extra'));
    await writeFile(join(root, 'report.json.gz'), gzipSync(bytes));
    await assert.rejects(verifyArtifacts(root), /ambiguous/);
    await rm(join(root, 'report.json.gz'));
    await rm(join(root, 'report.json'));
    await writeFile(join(outside, 'report.json'), bytes);
    await symlink(join(outside, 'report.json'), join(root, 'report.json'));
    await assert.rejects(verifyArtifacts(root), /escapes root/);
  } finally {
    await rm(root, { recursive: true, force: true });
    await rm(outside, { recursive: true, force: true });
  }
});