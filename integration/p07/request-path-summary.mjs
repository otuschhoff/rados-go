import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';

export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
export function readBytes(directory, filename) {
  const target = path.join(directory, filename);
  return fs.existsSync(target) ? fs.readFileSync(target) : gunzipSync(fs.readFileSync(`${target}.gz`));
}
const text = (directory, filename) => readBytes(directory, filename).toString('utf8');
const json = (directory, filename) => JSON.parse(text(directory, filename));

export function parseManifest(content) {
  const entries = new Map();
  for (const line of content.trim().split(/\r?\n/)) {
    const match = /^([a-f0-9]{64})  (.+)$/.exec(line);
    assert.ok(match && !entries.has(match[2]), 'invalid or duplicate manifest entry');
    assert.ok(!path.isAbsolute(match[2]) && !match[2].split('/').includes('..'), 'unsafe manifest path');
    entries.set(match[2], match[1]);
  }
  return entries;
}

export function verifyArtifacts(directory) {
  const entries = parseManifest(text(directory, 'artifacts.sha256'));
  const availability = fs.existsSync(path.join(directory, 'availability.json')) ? json(directory, 'availability.json') : null;
  if (availability) assert.deepEqual(availability.entries.map(entry => [entry.filename, entry.sha256]), [...entries]);
  const results = [];
  for (const [filename, expected] of entries) {
    const present = fs.existsSync(path.join(directory, filename)) || fs.existsSync(path.join(directory, `${filename}.gz`));
    const record = availability?.entries.find(entry => entry.filename === filename);
    if (record) {
      assert.ok(['verified_present_bytes', 'unavailable'].includes(record.original_verification), 'original verification status');
      assert.equal(record.actual_original_sha256, record.original_verification === 'verified_present_bytes' ? expected : null, 'original verified hash');
    }
    if (present) {
      assert.equal(sha256(readBytes(directory, filename)), expected, `artifact hash: ${filename}`);
      if (record) assert.equal(record.publication, 'gzip_original');
    } else if (record) {
      assert.equal(record.publication, 'omitted');
      assert.ok(record.reason, `omission reason: ${filename}`);
    }
    results.push({ filename, sha256: expected, verification: present ? 'verified_present_bytes' : 'unavailable_original_manifest_identity_only',
      original_verification: record?.original_verification ?? (present ? 'verified_present_bytes' : 'unavailable'),
      actual_original_sha256: record?.actual_original_sha256 ?? (present ? expected : null) });
  }
  return results;
}

function integer(value, name) {
  assert.ok(Number.isSafeInteger(value) && value >= 0, `${name}: expected nonnegative safe integer`);
  return value;
}

export function percentile(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const value = sorted[Math.ceil(sorted.length * 0.99) - 1];
  return Number.isFinite(value) ? value : null;
}

export function analyzeOutcomes(offered) {
  const counts = { success: 0, timeout: 0, overload: 0, error: 0, canceled: 0 };
  let admitted = 0, attempted = 0, deadlineMisses = 0;
  const successes = [], all = [], actual = [], distribution = {};
  assert.ok(Array.isArray(offered.outcomes), 'missing outcomes');
  for (const outcome of offered.outcomes) {
    assert.ok(Object.hasOwn(counts, outcome.kind), `unknown outcome kind: ${outcome.kind}`);
    integer(outcome.latency_ns, 'outcome latency');
    for (const key of ['admitted', 'attempted', 'deadline_miss']) assert.equal(typeof outcome[key], 'boolean', key);
    assert.ok(!outcome.attempted || outcome.admitted, 'attempted without admission');
    counts[outcome.kind]++;
    admitted += Number(outcome.admitted);
    attempted += Number(outcome.attempted);
    deadlineMisses += Number(outcome.deadline_miss);
    if (offered.config) assert.equal(outcome.deadline_miss, outcome.latency_ns >= offered.config.deadline_ns, 'deadline classification');
    const bucket = `${outcome.kind}:${outcome.deadline_miss ? 'deadline_miss' : 'within_deadline'}`;
    distribution[bucket] = (distribution[bucket] ?? 0) + 1;
    if (outcome.kind === 'success') successes.push(outcome.latency_ns);
    all.push(outcome.kind === 'success' && !outcome.deadline_miss ? outcome.latency_ns : Infinity);
    actual.push(outcome.latency_ns);
  }
  const expected = integer(offered.expected, 'expected');
  assert.equal(offered.outcomes.length, expected, 'outcome count');
  for (const [key, count] of Object.entries({ success: counts.success, timeouts: counts.timeout,
    overload: counts.overload, errors: counts.error, canceled: counts.canceled, admitted, attempted })) {
    assert.equal(integer(offered[key], key), count, `${key} count`);
  }
  const failures = expected - counts.success;
  const sloFailures = offered.outcomes.filter(outcome => outcome.kind !== 'success' || outcome.deadline_miss).length;
  assert.equal(integer(offered.slo_failures, 'slo_failures'), sloFailures, 'SLO count');
  const successP99 = percentile(successes), allP99 = percentile(all);
  assert.equal(offered.success_p99_ns, successP99 ?? 0, 'success p99');
  assert.equal(offered.all_outcome_p99_ns, percentile(actual) ?? 0, 'actual all-outcome p99');
  return { counts, failures, slo_failures: sloFailures, deadline_misses: deadlineMisses, distribution,
    failure_fraction: expected ? failures / expected : null,
    success_p99_ns: successP99, actual_all_outcome_p99_ns: percentile(actual), failure_aware_p99_ns: allP99,
    failure_aware_p99_status: allP99 === null ? (expected ? 'infinite_failures' : 'empty') : 'finite' };
}

export function cgroupDelta(beforeText, afterText) {
  function parse(content) {
    const fields = new Map();
    for (const line of content.trim().split(/\r?\n/)) {
      const match = /^(\S+)\s+(\d+)$/.exec(line);
      assert.ok(match && !fields.has(match[1]), 'invalid cgroup counter');
      fields.set(match[1], BigInt(match[2]));
    }
    return fields;
  }
  const before = parse(beforeText), after = parse(afterText), delta = {};
  assert.deepEqual([...before.keys()].sort(), [...after.keys()].sort(), 'cgroup fields');
  for (const [key, value] of before) {
    assert.ok(after.get(key) >= value, `decreased cgroup ${key}`);
    delta[key] = (after.get(key) - value).toString();
  }
  for (const key of ['nr_throttled', 'throttled_usec']) assert.ok(Object.hasOwn(delta, key), 'missing throttle counter');
  return delta;
}

export function analyzeReport(report, rate) {
  assert.equal(report.implementation, 'go');
  assert.equal(report.transport, 'secure');
  assert.deepEqual(report.environment, { GOOS: 'linux', GOARCH: 'arm64', go_version: 'go1.27.1', gomaxprocs: 10 });
  const offered = report.offered_load;
  assert.deepEqual(offered.config, { rate_ops_per_second: rate, issuance_window_ns: 8000000000,
    deadline_ns: 500000000, delivery_lag_limit_ns: 50000000, workers: 16, queue_capacity: 128,
    allocations_per_worker_per_second: 100, gomaxprocs: 10, gogc: 100, gomemlimit: 'off' });
  assert.deepEqual(report.diagnostic, { warmup_operations: 128, resource_scope: 'warmup_issuance_drain_background_join_and_harness',
    object_set: 'p07-shared-read-0..15', background_cpu_workers: 8, read_api: 'read_into',
    scratch_slots: 4, admission_window: 16, background_allocations: true });
  assert.equal(offered.expected, rate * 8);
  assert.equal(offered.scratch_counters_enabled, false);
  assert.equal(offered.rows_operations_basis, 'expected_arrivals; bytes and throughput count successful reads only');
  const outcomes = analyzeOutcomes(offered);
  assert.equal(report.rows.length, 1);
  const row = report.rows[0];
  for (const [key, expected] of Object.entries({ size_bytes: 65536, concurrency: 16, workload: 'read',
    operations: rate * 8, bytes: offered.success * 65536, elapsed_ns: offered.total_window_drain_ns,
    p50_ns: 0, p95_ns: 0, p99_ns: outcomes.actual_all_outcome_p99_ns,
    iops: offered.success / 8, throughput_bytes_per_second: offered.success * 65536 / 8 })) assert.equal(row[key], expected, key);
  for (const key of ['success_bytes', 'success_ops_per_issuance_second', 'success_bytes_per_issuance_second', 'success_bytes_per_window_and_drain_second']) {
    const expected = { success_bytes: row.bytes, success_ops_per_issuance_second: row.iops,
      success_bytes_per_issuance_second: row.throughput_bytes_per_second,
      success_bytes_per_window_and_drain_second: row.bytes / (row.elapsed_ns / 1e9) }[key];
    assert.ok(Math.abs(offered[key] - expected) <= Math.max(1, expected) * 1e-12, key);
  }
  for (const value of Object.values(report.resources)) integer(value, 'resource');
  for (const key of ['cpu_user_ns', 'cpu_system_ns', 'allocated_bytes', 'allocations', 'max_rss_bytes', 'gc_cycles', 'gc_pause_ns']) integer(report.resources[key], key);
  const background = offered.background;
  for (const key of ['expected', 'completed', 'missed', 'bytes', 'hash_iterations', 'max_lateness_ns', 'cpu_active_ns']) integer(background[key], `background ${key}`);
  assert.equal(background.expected, 6400);
  assert.equal(background.expected_bytes, 6400 * 65536);
  assert.equal(background.allocation_window_ns, 8000000000);
  assert.equal(background.workers.length, 8);
  for (const worker of background.workers) {
    for (const value of Object.values(worker)) integer(value, 'background worker');
    assert.equal(worker.expected, 800);
    assert.equal(worker.completed + worker.missed, 800);
    assert.equal(worker.bytes, worker.completed * 65536);
  }
  for (const key of ['expected', 'completed', 'missed', 'bytes', 'hash_iterations']) assert.equal(background[key], background.workers.reduce((total, worker) => total + worker[key], 0), `background ${key}`);
  assert.equal(background.completed + background.missed, 6400);
  assert.equal(background.max_lateness_ns, Math.max(...background.workers.map(worker => worker.max_lateness_ns)));
  assert.equal(typeof background.delivery_invalid, 'boolean');
  assert.equal(background.delivery_invalid, background.missed !== 0 || background.max_lateness_ns > offered.config.delivery_lag_limit_ns, 'background invalid classification');
  assert.equal(offered.delivery_invalid, offered.max_delivery_lag_ns > offered.config.delivery_lag_limit_ns);
  assert.equal(offered.failed, offered.delivery_invalid || background.delivery_invalid || outcomes.slo_failures > 0 || Boolean(offered.failure_reason));
  return { ...outcomes, expected: offered.expected, admitted: offered.admitted, attempted: offered.attempted,
    failed: offered.failed, delivery_invalid: offered.delivery_invalid, background_invalid: background.delivery_invalid,
    valid: !offered.failed, failure_reason: offered.failure_reason ?? null,
    max_delivery_lag_ns: offered.max_delivery_lag_ns, config: offered.config, diagnostic: report.diagnostic,
    background, resources: report.resources, iops: row.iops, elapsed_ns: row.elapsed_ns,
    success_bytes: row.bytes, success_bytes_per_window_and_drain_second: offered.success_bytes_per_window_and_drain_second };
}

function median(values) {
  if (!values.length) return null;
  const sorted = values.map(value => value ?? Infinity).sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  const value = sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  return Number.isFinite(value) ? value : null;
}

function aggregate(rows) {
  const totals = {};
  for (const key of ['success', 'timeout', 'overload', 'error', 'canceled']) totals[key] = rows.reduce((total, row) => total + row.counts[key], 0);
  return { rows: rows.length, failed_rows: rows.filter(row => row.failed).length,
    delivery_invalid_rows: rows.filter(row => row.delivery_invalid).length,
    background_invalid_rows: rows.filter(row => row.background_invalid).length,
    expected: rows.reduce((total, row) => total + row.expected, 0), counts: totals,
    slo_failures: rows.reduce((total, row) => total + row.slo_failures, 0),
    background_completed: rows.reduce((total, row) => total + row.background.completed, 0),
    background_missed: rows.reduce((total, row) => total + row.background.missed, 0),
    background_bytes: rows.reduce((total, row) => total + row.background.bytes, 0),
    median_success_p99_ns: median(rows.map(row => row.success_p99_ns)),
    median_actual_all_outcome_p99_ns: median(rows.map(row => row.actual_all_outcome_p99_ns)),
    median_failure_aware_p99_ns: median(rows.map(row => row.failure_aware_p99_ns)),
    infinite_failure_aware_p99_rows: rows.filter(row => row.failure_aware_p99_status === 'infinite_failures').length,
    median_iops: median(rows.map(row => row.iops)) };
}

export function summarize(root, { published = false } = {}) {
  const captures = [], rows = [];
  let measurementReference;
  for (const series of ['offered', 'noack']) for (const variant of ['before-a', 'after-a', 'after-b', 'before-b']) {
    const name = `${series}-${variant}`;
    const directory = path.join(root, published ? name : `rados-go-p99-next-${name}-v1`);
    if (published) assert.ok(fs.existsSync(path.join(directory, 'availability.json')), 'published capture requires availability map');
    const artifacts = verifyArtifacts(directory);
    const source = text(directory, 'source-before.sha256');
    assert.equal(text(directory, 'source-after.sha256'), source, `${name} source changed`);
    const sources = parseManifest(source);
    const head = text(directory, 'source-head').trim();
    assert.match(head, /^[a-f0-9]{40}$/);
    const measurement = [...sources].filter(([filename]) => filename.startsWith('integration/p07/'));
    assert.equal(measurement.length, 34, 'captured measurement inventory');
    if (measurementReference) assert.deepEqual(measurement, measurementReference, 'measurement source mismatch');
    else measurementReference = measurement;
    const status = json(directory, 'final-status.json');
    assert.equal(status.status, 'diagnostic-failed');
    assert.equal(status.exit_code, 1);
    assert.equal(status.benchmark_claim, false);
    assert.equal(status.source_check, 'unchanged');
    assert.equal(text(directory, 'scheduler-status').trim(), 'diagnostic-failed');
    const build = text(directory, 'go-buildinfo.txt');
    for (const pattern of [/go1\.27\.1/, /GOOS=linux/, /GOARCH=arm64/, /CGO_ENABLED=0/, /-trimpath=true/]) assert.match(build, pattern);
    assert.ok(!build.includes('-tags='), 'offered-load binary must use the untagged production counter mode');
    assert.ok(build.includes(`vcs.revision=${head}`), 'build/source revision mismatch');
    for (const filename of ['benchmark', 'go-buildinfo.txt', 'source-before.sha256', 'source-after.sha256', 'source-head', 'scheduler-status', 'final-status.json']) assert.ok(artifacts.some(entry => entry.filename === filename), `missing manifest identity: ${filename}`);
    const captureRows = [];
    const labels = Array.from({ length: 5 }, (_, index) => [1000, 2000, 4000].map(rate => `offered-${index + 1}-rate-${rate}.json`)).flat();
    assert.deepEqual([...new Set(fs.readdirSync(directory).map(filename => filename.replace(/\.gz$/, '')).filter(filename => /^offered-\d+-rate-\d+\.json$/.test(filename)))].sort(), labels.sort(), 'capture row inventory');
    for (let repeat = 1; repeat <= 5; repeat++) for (const rate of [1000, 2000, 4000]) {
      const label = `offered-${repeat}-rate-${rate}`;
      assert.ok(artifacts.some(entry => entry.filename === `${label}.json`), 'outcome file missing from original manifest');
      const row = analyzeReport(json(directory, `${label}.json`), rate);
      const exits = {};
      for (const kind of ['benchmark', 'capture']) {
        exits[kind] = Number(text(directory, `${label}-${kind}-exit-code`).trim());
        assert.equal(exits[kind], row.failed ? 1 : 0, `${label} ${kind} exit`);
      }
      assert.equal(Number(text(directory, `${label}-exit-code`).trim()), row.failed ? 1 : 0, `${label} exit`);
      const delta = cgroupDelta(text(directory, `${label}-cpu.stat-before`), text(directory, `${label}-cpu.stat-after`));
      for (const phase of ['before', 'after']) {
        assert.equal(text(directory, `${label}-cpu.max-${phase}`).trim(), 'max 100000');
        assert.equal(text(directory, `${label}-cpuset.cpus.effective-${phase}`).trim(), '0-9');
      }
      captureRows.push({ series, variant, label, repeat, rate, ...row, exits, cgroup_deltas: delta });
    }
    assert.equal(status.failed_legs, captureRows.filter(row => row.failed).length, 'failed legs');
    rows.push(...captureRows);
    captures.push({ name, status, source_head: text(directory, 'source-head').trim(),
      source_manifest_sha256: sha256(source), sources: [...sources].map(([filename, digest]) => ({ filename, sha256: digest })),
      buildinfo_sha256: sha256(build), benchmark: artifacts.find(entry => entry.filename === 'benchmark'),
      artifact_verification: artifacts, all_rows: aggregate(captureRows), valid_only: aggregate(captureRows.filter(row => row.valid)) });
  }
  const baselineSources = captures.filter(capture => capture.name.includes('-before-'));
  for (const capture of baselineSources.slice(1)) assert.deepEqual(capture.sources, baselineSources[0].sources, 'baseline source identity');
  for (const series of ['offered', 'noack']) {
    const sameBuild = captures.filter(capture => capture.name.startsWith(`${series}-after-`));
    assert.deepEqual(sameBuild[0].sources, sameBuild[1].sources, 'candidate source identity');
    assert.equal(sameBuild[0].benchmark.sha256, sameBuild[1].benchmark.sha256, 'candidate binary identity');
  }
  for (const capture of baselineSources) assert.equal(capture.benchmark.sha256, baselineSources[0].benchmark.sha256, 'baseline binary identity');
  const comparisons = [];
  for (const series of ['offered', 'noack']) for (const rate of [1000, 2000, 4000]) {
    const selected = rows.filter(row => row.series === series && row.rate === rate);
    const groups = {};
    for (const scope of ['all_rows', 'valid_only']) {
      const scoped = selected.filter(row => scope === 'all_rows' || row.valid);
      groups[scope] = Object.fromEntries(['before', 'after'].map(side => [side, aggregate(scoped.filter(row => row.variant.startsWith(side)))]));
    }
    comparisons.push({ series, rate, ...groups });
  }
  return { scope: 'ABBA offered-load diagnostic; all failed captures retained; rate-capped throughput is not capacity; no native comparison or qualification',
    measurement_sources: measurementReference, captures, rows, comparisons };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(summarize(process.argv[2], { published: process.argv.includes('--published') }), null, 2));
}