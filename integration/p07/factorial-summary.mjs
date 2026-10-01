import { createReadStream } from 'node:fs';
import { readFile, readdir, realpath } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { resolve, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createGunzip } from 'node:zlib';

const CASES = ['none', 'cpu', 'alloc', 'both'];
const PHASES = ['delivery_delay_ns', 'queue_delay_ns', 'dispatch_ns', 'service_ns'];
const WINDOW = 8_000_000_000;
const DEADLINE = 500_000_000;
const LAG = 50_000_000;
const HASH_FIELDS = ['hash_iterations', 'warmup_hash_iterations', 'window_hash_iterations', 'drain_hash_iterations'];
const SUM_FIELDS = ['expected', 'completed', 'missed', 'bytes', ...HASH_FIELDS];

function requireThat(condition, message) {
  if (!condition) throw new Error(message);
}

function equal(actual, expected, label) {
  requireThat(actual === expected, `${label}: expected ${expected}, got ${actual}`);
}

function integer(value, label, minimum = 0) {
  requireThat(Number.isSafeInteger(value) && value >= minimum, `${label}: invalid safe integer`);
  return value;
}

function boolean(value, label) {
  requireThat(typeof value === 'boolean', `${label}: expected boolean`);
}

export function percentile(values, percent = 99) {
  requireThat(percent > 0 && percent <= 100, 'invalid percentile');
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.ceil(percent * sorted.length / 100) - 1];
}

function median(values) {
  if (!values.length || values.some(value => value === null)) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function failureMedian(rows) {
  if (!rows.length) return { p99_ns: null, status: 'empty' };
  const value = median(rows.map(row => row.failure_aware.status === 'infinity' ? Infinity : row.failure_aware.p99_ns));
  return Number.isFinite(value) ? { p99_ns: value, status: 'finite' } : { p99_ns: null, status: 'infinity' };
}

export function parseRowName(filename) {
  const match = /^(factorial|observed)-([1-5])-case-(none|cpu|alloc|both)-rate-(1000|2000|4000)\.json$/.exec(filename);
  requireThat(match, `invalid matrix filename: ${filename}`);
  const [, family, repeatText, loadCase, rateText] = match;
  const repeat = Number(repeatText);
  const rate = Number(rateText);
  requireThat(family !== 'observed' || (repeat <= 3 && rate === 2000), `invalid observed matrix filename: ${filename}`);
  return { filename, repeat, loadCase, rate, instrumented: family === 'observed' };
}

export function matrixNames(instrumented = false) {
  const names = [];
  for (let repeat = 1; repeat <= (instrumented ? 3 : 5); repeat++) {
    for (const rate of instrumented ? [2000] : [1000, 2000, 4000]) {
      for (const loadCase of CASES) names.push(`${instrumented ? 'observed' : 'factorial'}-${repeat}-case-${loadCase}-rate-${rate}.json`);
    }
  }
  return names;
}

function validateBackground(background, config, setup) {
  const cpu = config.cpu_workers;
  const alloc = config.allocation_workers;
  equal(background.cpu_workers, cpu, 'background.cpu_workers');
  equal(background.allocation_workers, alloc, 'background.allocation_workers');
  requireThat(Array.isArray(background.workers), 'background.workers: expected array');
  equal(background.workers.length, cpu + alloc, 'background worker count');
  equal(background.allocation_window_ns, WINDOW, 'background allocation window');
  integer(background.cpu_active_ns, 'background.cpu_active_ns');
  if (!cpu || setup) equal(background.cpu_active_ns, 0, 'inactive CPU duration');
  const sums = Object.fromEntries(SUM_FIELDS.map(field => [field, 0]));
  let maximum = 0;
  let retainedBytes = 0;
  background.workers.forEach((worker, index) => {
    const role = index < cpu ? 'cpu' : 'alloc';
    equal(worker.role, role, `background worker ${index} role`);
    for (const field of SUM_FIELDS) sums[field] += integer(worker[field], `worker ${index}.${field}`);
    maximum = Math.max(maximum, integer(worker.max_lateness_ns, 'worker lateness'));
    equal(worker.hash_iterations, worker.warmup_hash_iterations + worker.window_hash_iterations + worker.drain_hash_iterations, 'worker hash phase sum');
    equal(worker.expected, role === 'alloc' ? 800 : 0, 'worker expected allocations');
    equal(worker.completed + worker.missed, worker.expected, 'worker allocation sum');
    equal(worker.bytes, worker.completed * 65536, 'worker allocation bytes');
    if (role === 'alloc') {
      for (const field of HASH_FIELDS) equal(worker[field], 0, 'allocation-only hashes');
      retainedBytes += Math.min(8, worker.completed) * 65536;
    } else {
      equal(worker.max_lateness_ns, 0, 'CPU-only allocation lateness');
      if (!setup) requireThat(worker.warmup_hash_iterations >= 1, 'CPU worker initial warmup hash missing');
    }
    if (setup) {
      equal(worker.completed, 0, 'setup allocation completion');
      for (const field of HASH_FIELDS) equal(worker[field], 0, 'setup hash iterations');
    }
  });
  for (const field of SUM_FIELDS) equal(background[field], sums[field], `background.${field} worker sum`);
  equal(background.expected, alloc * 800, 'background expected allocations');
  equal(background.expected_bytes, background.expected * 65536, 'background expected bytes');
  equal(background.max_lateness_ns, maximum, 'background maximum lateness');
  equal(background.delivery_invalid, background.missed > 0 || maximum > LAG, 'background delivery_invalid');
  return { ...background, retained_bytes_inferred_at_join: retainedBytes, retention_slots_per_allocation_worker: 8, retention_is_inferred_not_measured: true };
}

function population(outcomes, predicate, field) {
  const values = outcomes.filter(predicate).map(outcome => outcome[field]);
  return { count: values.length, p99_ns: percentile(values) };
}

export function validateRow(report, expectedMeta) {
  const meta = typeof expectedMeta === 'string' ? parseRowName(expectedMeta) : expectedMeta;
  const { rate, loadCase, instrumented } = meta;
  requireThat([1000, 2000, 4000].includes(rate) && CASES.includes(loadCase) && typeof instrumented === 'boolean', 'invalid expected row metadata');
  const result = report.offered_load;
  requireThat(result && result.config, 'missing offered_load config');
  const config = result.config;
  const cpu = ['cpu', 'both'].includes(loadCase) ? 8 : 0;
  const alloc = ['alloc', 'both'].includes(loadCase) ? 8 : 0;
  const expectedConfig = { factorial: true, instrumented, load_case: loadCase, cpu_workers: cpu, allocation_workers: alloc,
    rate_ops_per_second: rate, issuance_window_ns: WINDOW, deadline_ns: DEADLINE, delivery_lag_limit_ns: LAG,
    workers: 16, queue_capacity: 128, allocations_per_worker_per_second: 100, gomaxprocs: 10, gogc: 100, gomemlimit: 'off' };
  for (const [field, value] of Object.entries(expectedConfig)) equal(config[field], value, `config.${field}`);
  equal(report.implementation, 'go', 'implementation');
  equal(report.transport, 'secure', 'transport');
  equal(report.environment.GOOS, 'linux', 'GOOS');
  requireThat(['arm64', 'amd64'].includes(report.environment.GOARCH), 'unsupported GOARCH');
  equal(report.environment.gomaxprocs, 10, 'environment.gomaxprocs');
  requireThat(typeof report.environment.go_version === 'string' && report.environment.go_version.length > 0, 'missing Go version');
  const diagnostic = report.diagnostic;
  for (const [field, value] of Object.entries({ read_api: 'read_into', scratch_slots: 4, admission_window: 16,
    warmup_operations: 128,
    resource_scope: 'warmup_issuance_drain_background_join_and_harness', object_set: 'p07-shared-read-0..15' })) equal(diagnostic[field], value, `diagnostic.${field}`);
  equal(diagnostic.background_cpu_workers, cpu || undefined, 'diagnostic.background_cpu_workers omitempty');
  equal(diagnostic.background_allocations, alloc > 0 ? true : undefined, 'diagnostic.background_allocations omitempty');
  requireThat(diagnostic.scratch == null, 'scratch counters must be absent');
  equal(result.scratch_counters_enabled, false, 'scratch counters enabled');
  requireThat(typeof result.resource_limitations === 'string' && result.resource_limitations.length > 0, 'missing resource scope limitations');
  const setup = result.rows_operations_basis === 'expected_arrivals; setup failed before issuance';
  if (setup) requireThat(result.failure_reason?.startsWith('setup: '), 'setup failure reason missing');
  else {
    equal(result.rows_operations_basis, 'expected_arrivals; bytes and throughput count successful reads only', 'operations basis');
    requireThat(typeof result.timing_methodology === 'string' && result.timing_methodology.includes('-1'), 'missing timing methodology');
  }
  equal(result.expected, rate * 8, 'expected arrivals');
  requireThat(Array.isArray(result.outcomes), 'outcomes must be an array');
  equal(result.outcomes.length, result.expected, 'outcome count');
  const counts = { admitted: 0, attempted: 0, success: 0, timeouts: 0, overload: 0, errors: 0, canceled: 0, slo_failures: 0, deadline_misses: 0, nonattempted: 0 };
  let maximumDelivery = 0;
  const kindCounts = {};
  result.outcomes.forEach((outcome, index) => {
    for (const field of ['scheduled_ns', 'enqueued_ns', 'worker_start_ns', 'read_start_ns', 'returned_ns', ...PHASES]) integer(outcome[field], `outcome ${index}.${field}`, -1);
    integer(outcome.latency_ns, 'outcome latency');
    for (const field of ['admitted', 'attempted', 'deadline_miss']) boolean(outcome[field], field);
    equal(outcome.scheduled_ns, Math.floor(index * 1_000_000_000 / rate), 'arrival schedule');
    requireThat(['success', 'timeout', 'overload', 'error', 'canceled'].includes(outcome.kind), `unknown outcome kind ${outcome.kind}`);
    requireThat(!outcome.attempted || outcome.admitted, 'attempt requires admission');
    if (['success', 'error'].includes(outcome.kind)) requireThat(outcome.attempted, 'success/error requires attempted read');
    if (outcome.kind === 'overload') requireThat(!outcome.admitted && !outcome.attempted, 'overload cannot be admitted');
    if (outcome.kind === 'timeout') requireThat(outcome.admitted, 'timeout requires admission');
    const byKind = kindCounts[outcome.kind] ??= { count: 0, admitted: 0, attempted: 0, nonattempted: 0, deadline_misses: 0 };
    byKind.count++;
    byKind.admitted += Number(outcome.admitted);
    byKind.attempted += Number(outcome.attempted);
    byKind.nonattempted += Number(!outcome.attempted);
    byKind.deadline_misses += Number(outcome.deadline_miss);
    counts.admitted += Number(outcome.admitted);
    counts.attempted += Number(outcome.attempted);
    counts.nonattempted += Number(!outcome.attempted);
    counts.deadline_misses += Number(outcome.deadline_miss);
    counts[{ success: 'success', timeout: 'timeouts', overload: 'overload', error: 'errors', canceled: 'canceled' }[outcome.kind]]++;
    counts.slo_failures += Number(outcome.kind !== 'success' || outcome.deadline_miss);
    equal(outcome.deadline_miss, outcome.latency_ns >= DEADLINE, 'deadline_miss');
    if (setup) {
      equal(outcome.kind, 'canceled', 'setup outcome kind');
      equal(outcome.admitted, false, 'setup admitted');
      equal(outcome.attempted, false, 'setup attempted');
      equal(outcome.latency_ns, 0, 'setup legacy latency');
      for (const field of ['enqueued_ns', 'worker_start_ns', 'read_start_ns', 'returned_ns', ...PHASES]) equal(outcome[field], -1, 'setup timing sentinel');
      return;
    }
    requireThat(outcome.returned_ns >= 0, 'missing outcome return');
    equal(outcome.latency_ns, Math.max(0, outcome.returned_ns - outcome.scheduled_ns), 'arrival latency');
    if (outcome.admitted) {
      requireThat(outcome.enqueued_ns >= outcome.scheduled_ns && outcome.worker_start_ns >= outcome.enqueued_ns, 'admission timestamp order');
      equal(outcome.delivery_delay_ns, outcome.enqueued_ns - outcome.scheduled_ns, 'delivery phase');
      equal(outcome.queue_delay_ns, outcome.worker_start_ns - outcome.enqueued_ns, 'queue phase');
      requireThat(outcome.returned_ns >= outcome.worker_start_ns, 'worker return timestamp order');
    } else {
      for (const field of ['enqueued_ns', 'worker_start_ns', 'queue_delay_ns']) equal(outcome[field], -1, 'nonadmitted sentinel');
      equal(outcome.delivery_delay_ns, outcome.returned_ns < outcome.scheduled_ns ? -1 : outcome.latency_ns, 'rejection delivery');
      if (outcome.returned_ns < outcome.scheduled_ns) equal(outcome.kind, 'canceled', 'pre-arrival cancellation');
    }
    if (outcome.attempted) {
      requireThat(outcome.read_start_ns >= outcome.worker_start_ns && outcome.returned_ns >= outcome.read_start_ns, 'read timestamp order');
      equal(outcome.dispatch_ns, outcome.read_start_ns - outcome.worker_start_ns, 'dispatch phase');
      equal(outcome.service_ns, outcome.returned_ns - outcome.read_start_ns, 'service phase');
      equal(outcome.latency_ns, PHASES.reduce((sum, phase) => sum + outcome[phase], 0), 'phase identity');
    } else for (const field of ['read_start_ns', 'dispatch_ns', 'service_ns']) equal(outcome[field], -1, 'nonattempted sentinel');
    maximumDelivery = Math.max(maximumDelivery, outcome.delivery_delay_ns);
  });
  for (const field of ['admitted', 'attempted', 'success', 'timeouts', 'overload', 'errors', 'canceled', 'slo_failures']) equal(result[field], counts[field], `result.${field}`);
  counts.expected = result.expected;
  equal(counts.success + counts.timeouts + counts.overload + counts.errors + counts.canceled, result.expected, 'all outcome kinds reconcile');
  equal(result.max_delivery_lag_ns, maximumDelivery, 'maximum delivery lag');
  equal(result.delivery_invalid, setup || maximumDelivery > LAG, 'delivery invalid');
  integer(result.queue_high_water_sampled, 'queue high water');
  requireThat(result.queue_high_water_sampled <= 128, 'queue sample exceeds capacity');
  requireThat(result.queue_high_water_sampled <= counts.admitted, 'queue sample exceeds total admissions');
  const background = validateBackground(result.background, config, setup);
  equal(result.failed, result.delivery_invalid || counts.slo_failures > 0 || background.delivery_invalid || Boolean(result.failure_reason), 'failed flag');
  for (const field of ['warmup_ns', 'actual_issuance_ns', 'total_window_drain_ns', 'drain_ns']) integer(result[field], field);
  requireThat(result.total_window_drain_ns >= result.actual_issuance_ns, 'drain precedes issuance completion');
  equal(result.drain_ns, Math.max(0, result.total_window_drain_ns - WINDOW), 'drain duration');
  if (setup) for (const field of ['warmup_ns', 'actual_issuance_ns', 'total_window_drain_ns', 'drain_ns', 'queue_high_water_sampled']) equal(result[field], 0, 'setup duration/queue');
  const successValues = result.outcomes.filter(outcome => outcome.kind === 'success').map(outcome => outcome.latency_ns);
  const allValues = result.outcomes.map(outcome => outcome.latency_ns);
  equal(result.success_p99_ns, percentile(successValues) ?? 0, 'success p99');
  equal(result.all_outcome_p99_ns, percentile(allValues), 'all outcome p99');
  equal(result.success_bytes, counts.success * 65536, 'success bytes');
  equal(result.success_ops_per_issuance_second, counts.success / 8, 'window IOPS');
  equal(result.success_bytes_per_issuance_second, result.success_bytes / 8, 'window bytes/sec');
  const totalThroughput = result.total_window_drain_ns > 0 ? result.success_bytes / (result.total_window_drain_ns / 1e9) : 0;
  requireThat(Math.abs(result.success_bytes_per_window_and_drain_second - totalThroughput) <= Math.max(1e-8, Math.abs(totalThroughput) * 1e-12), 'total throughput mismatch');
  equal(report.rows.length, 1, 'row count');
  for (const [field, value] of Object.entries({ size_bytes: 65536, concurrency: 16, workload: 'read', operations: result.expected, bytes: result.success_bytes,
    elapsed_ns: result.total_window_drain_ns, throughput_bytes_per_second: result.success_bytes / 8, iops: counts.success / 8,
    p50_ns: 0, p95_ns: 0, p99_ns: result.all_outcome_p99_ns })) equal(report.rows[0][field], value, `rows[0].${field}`);
  for (const field of ['cpu_user_ns', 'cpu_system_ns', 'allocations', 'allocated_bytes', 'max_rss_bytes']) integer(report.resources[field], `resources.${field}`);
  for (const field of ['gc_cycles', 'gc_pause_ns']) if (report.resources[field] != null) integer(report.resources[field], `resources.${field}`);
  const good = result.outcomes.filter(outcome => outcome.kind === 'success' && !outcome.deadline_miss);
  const rank = Math.ceil(0.99 * result.expected);
  const failureP99 = good.length < rank ? { p99_ns: null, status: 'infinity' } : { p99_ns: [...good].sort((left, right) => left.latency_ns - right.latency_ns)[rank - 1].latency_ns, status: 'finite' };
  const phasePercentiles = {};
  for (const phase of PHASES) phasePercentiles[phase] = {
    reached: population(result.outcomes, outcome => outcome[phase] >= 0, phase),
    attempted: population(result.outcomes, outcome => outcome.attempted, phase),
    success: population(result.outcomes, outcome => outcome.kind === 'success', phase),
  };
  const worstAttempts = result.outcomes.map((outcome, index) => ({ index, ...outcome })).filter(outcome => outcome.attempted)
    .sort((left, right) => right.latency_ns - left.latency_ns || left.index - right.index).slice(0, 50)
    .map(outcome => ({ ...outcome, phase_sum_ns: PHASES.reduce((sum, phase) => sum + outcome[phase], 0),
      dominant_phases: PHASES.filter(phase => outcome[phase] === Math.max(...PHASES.map(field => outcome[field]))) }));
  const dominantCounts = Object.fromEntries(PHASES.map(phase => [phase, worstAttempts.filter(outcome => outcome.dominant_phases.includes(phase)).length]));
  const largestPhaseSamples = Object.fromEntries(PHASES.map(phase => {
    let selected = null;
    result.outcomes.forEach((outcome, index) => {
      if (outcome[phase] >= 0 && (!selected || outcome[phase] > selected[phase])) selected = { index, ...outcome };
    });
    return [phase, selected];
  }));
  return { ...meta, setup_failure: setup, failed: result.failed, failure_reason: result.failure_reason ?? null, counts, kind_counts: kindCounts,
    success_p99_ns: result.success_p99_ns, all_outcome_p99_ns: result.all_outcome_p99_ns, failure_aware: failureP99,
    phase_percentiles: phasePercentiles, phase_quantiles_are_not_additive: true, worst_attempts: worstAttempts, dominant_worst_attempt_counts: dominantCounts,
    largest_phase_samples: largestPhaseSamples, largest_phase_tie_assignment: 'earliest arrival index', environment: report.environment,
    queue_high_water_sampled: result.queue_high_water_sampled, queue_sample_may_underestimate: true, delivery_invalid: result.delivery_invalid,
    max_delivery_lag_ns: maximumDelivery, durations: Object.fromEntries(['warmup_ns', 'actual_issuance_ns', 'total_window_drain_ns', 'drain_ns'].map(field => [field, result[field]])),
    background, resources: report.resources, allocated_bytes_per_expected_arrival: report.resources.allocated_bytes / result.expected,
    allocated_bytes_per_attempt: counts.attempted ? report.resources.allocated_bytes / counts.attempted : null,
    allocated_bytes_per_success: counts.success ? report.resources.allocated_bytes / counts.success : null,
    allocation_scope: result.resource_limitations, success_bytes: result.success_bytes };
}

function safeName(name) {
  requireThat(typeof name === 'string' && name.length > 0 && !name.includes('\\') && !/[\x00-\x1f\x7f]/.test(name), 'unsafe manifest path');
  const clean = name.startsWith('./') ? name.slice(2) : name;
  requireThat(!clean.startsWith('/') && clean.split('/').every(part => part && part !== '.' && part !== '..'), `unsafe manifest path: ${name}`);
  return clean;
}

export function parseManifest(text, { artifact = false } = {}) {
  const entries = new Map();
  for (const line of text.split('\n')) {
    if (!line) continue;
    const match = /^([a-f0-9]{64}) ([ *])(.+)$/.exec(line);
    requireThat(match, `invalid SHA256 manifest line: ${line}`);
    requireThat(!artifact || match[3].startsWith('./'), 'artifact manifest path must begin ./');
    const name = safeName(match[3]);
    requireThat(!entries.has(name), `duplicate manifest path: ${name}`);
    entries.set(name, match[1]);
  }
  requireThat(entries.size > 0, 'empty SHA256 manifest');
  return entries;
}

export function parseCPUStat(text) {
  const result = {};
  for (const line of text.trim().split('\n')) {
    const match = /^([a-z_]+) ([0-9]+)$/.exec(line);
    requireThat(match && !Object.hasOwn(result, match[1]), 'invalid or duplicate cpu.stat counter');
    result[match[1]] = BigInt(match[2]);
  }
  for (const field of ['usage_usec', 'user_usec', 'system_usec', 'nr_periods', 'nr_throttled', 'throttled_usec']) requireThat(Object.hasOwn(result, field), `missing cpu.stat ${field}`);
  return result;
}

export function summarizeCgroup(beforeText, afterText, beforeMax, afterMax) {
  for (const value of [beforeMax, afterMax]) requireThat(/^max [1-9][0-9]*\n?$/.test(value), 'cgroup CPU quota must be unlimited');
  equal(beforeMax.trim(), afterMax.trim(), 'CPU quota changed');
  const before = parseCPUStat(beforeText);
  const after = parseCPUStat(afterText);
  equal(Object.keys(before).sort().join(','), Object.keys(after).sort().join(','), 'cgroup counter set changed');
  const delta = {};
  for (const field of Object.keys(before)) {
    requireThat(after[field] >= before[field], `cgroup counter decreased: ${field}`);
    delta[field] = (after[field] - before[field]).toString();
  }
  return { quota: beforeMax.trim(), delta, throttling_observed: BigInt(delta.nr_throttled) > 0n || BigInt(delta.throttled_usec) > 0n,
    throttling_is_reported_not_rejected: true };
}

async function existingPath(root, name) {
  safeName(name);
  const absolute = resolve(root, name);
  try {
    const canonical = await realpath(absolute);
    requireThat(canonical === root || canonical.startsWith(root + sep), `artifact escapes root: ${name}`);
    return absolute;
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

async function originalStream(root, name) {
  const raw = await existingPath(root, name);
  if (raw) return createReadStream(raw);
  const compressed = await existingPath(root, name + '.gz');
  requireThat(compressed, `artifact unavailable: ${name}`);
  const input = createReadStream(compressed);
  const output = createGunzip();
  input.on('error', error => output.destroy(error));
  return input.pipe(output);
}

export async function readOriginal(root, name) {
  root = await realpath(root);
  const chunks = [];
  for await (const chunk of await originalStream(root, name)) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function originalHash(root, name) {
  const hash = createHash('sha256');
  for await (const chunk of await originalStream(root, name)) hash.update(chunk);
  return hash.digest('hex');
}

async function inventory(root, prefix = '') {
  const names = [];
  for (const entry of await readdir(resolve(root, prefix), { withFileTypes: true })) {
    const name = prefix + entry.name;
    safeName(name);
    if (entry.isDirectory()) names.push(...await inventory(root, name + '/'));
    else {
      requireThat(entry.isFile() || entry.isSymbolicLink(), `unsupported artifact: ${name}`);
      await existingPath(root, name);
      names.push(name);
    }
  }
  return names;
}

export async function verifyArtifacts(root, { published = null } = {}) {
  root = await realpath(root);
  const entries = parseManifest((await readOriginal(root, 'artifacts.sha256')).toString('utf8'), { artifact: true });
  requireThat(!entries.has('artifacts.sha256'), 'artifact manifest cannot hash itself');
  const omitted = published?.omitted ?? {};
  requireThat(!published || (typeof published === 'object' && !Array.isArray(published)), 'published must be availability metadata');
  const verified = [];
  const unavailable = [];
  for (const [name, sha256] of entries) {
    if (Object.hasOwn(omitted, name)) {
      requireThat(['benchmark', 'native-benchmark'].includes(name), 'only original benchmark binaries may be omitted');
      requireThat(!(await existingPath(root, name)) && !(await existingPath(root, name + '.gz')), 'omitted artifact is available');
      equal(omitted[name].sha256, sha256, 'omission digest');
      equal(omitted[name].verified_initial, true, 'omission requires initial publisher verification');
      requireThat(typeof omitted[name].verification_record === 'string' && omitted[name].verification_record.length > 0, 'omission verification record required');
      unavailable.push({ name, sha256, status: 'publisher_verified_initially_not_reverified', verification_record: omitted[name].verification_record });
    } else {
      equal(await originalHash(root, name), sha256, `artifact SHA256 ${name}`);
      verified.push(name);
    }
  }
  for (const name of Object.keys(omitted)) requireThat(entries.has(name), `omission not in original manifest: ${name}`);
  const permittedExtras = new Set(['artifacts.sha256', 'artifacts.sha256.gz']);
  if (published) permittedExtras.add('published-availability.json');
  for (const name of await inventory(root)) {
    requireThat(entries.has(name) || (name.endsWith('.gz') && entries.has(name.slice(0, -3))) || permittedExtras.has(name), `unmanifested artifact: ${name}`);
    if (name.endsWith('.gz') && entries.has(name.slice(0, -3)) && !entries.has(name)) requireThat(!(await existingPath(root, name.slice(0, -3))), `ambiguous raw and compressed artifact: ${name}`);
  }
  return { manifest_entries: entries.size, verified_original_bytes: verified.length, unavailable, all_original_artifacts_reverified: unavailable.length === 0 };
}

export function compareCases(rows) {
  const primary = rows.filter(row => !row.instrumented && row.capture_complete !== false);
  const rates = [];
  for (const rate of [...new Set(primary.map(row => row.rate))].sort((left, right) => left - right)) {
    const cases = {};
    for (const loadCase of CASES) {
      const selected = primary.filter(row => row.rate === rate && row.loadCase === loadCase);
      const totals = {};
      for (const field of ['expected', 'admitted', 'attempted', 'success', 'timeouts', 'overload', 'errors', 'canceled', 'slo_failures', 'deadline_misses', 'nonattempted']) totals[field] = selected.reduce((sum, row) => sum + row.counts[field], 0);
      const failureAwareMedian = failureMedian(selected);
      const medians = { all_outcome_p99_ns: median(selected.map(row => row.all_outcome_p99_ns)), success_p99_ns: median(selected.map(row => row.success_p99_ns)),
        failure_aware_p99_ns: failureAwareMedian.p99_ns, failure_aware_p99_status: failureAwareMedian.status };
      for (const phase of PHASES) for (const pop of ['reached', 'attempted', 'success']) medians[`${phase}_${pop}_p99_ns`] = median(selected.map(row => row.phase_percentiles[phase][pop].p99_ns));
      const dominant = Object.fromEntries(PHASES.map(phase => [phase, selected.reduce((sum, row) => sum + row.dominant_worst_attempt_counts[phase], 0)]));
      const phaseCounts = Object.fromEntries(PHASES.map(phase => [phase, Object.fromEntries(['reached', 'attempted', 'success'].map(pop => [pop,
        selected.reduce((sum, row) => sum + row.phase_percentiles[phase][pop].count, 0)]))]));
      for (const field of ['expected', 'completed', 'missed', 'bytes', ...HASH_FIELDS]) totals[`background_${field}`] = selected.reduce((sum, row) => sum + row.background[field], 0);
      cases[loadCase] = { runs: selected.length, captures: [...new Set(selected.map(row => row.capture_root))].filter(Boolean).sort(), failed_runs: selected.filter(row => row.failed).length,
        capture_failed_runs: selected.filter(row => row.exits ? row.exits.capture !== 0 : row.failed).length,
        delivery_invalid_runs: selected.filter(row => row.delivery_invalid).length, background_invalid_runs: selected.filter(row => row.background.delivery_invalid).length,
        phase_population_counts: phaseCounts,
        infinity_failure_aware_runs: selected.filter(row => row.failure_aware.status === 'infinity').length, totals, run_level_medians: medians, dominant_worst_attempt_counts: dominant };
    }
    const medians = CASES.map(loadCase => cases[loadCase].run_level_medians.all_outcome_p99_ns);
    rates.push({ rate, cases, descriptive_interaction_all_outcome_p99_ns: medians.every(value => value !== null) ? medians[3] - medians[1] - medians[2] + medians[0] : null });
  }
  return { rates, interpretation: 'Descriptive differences of run-level total-latency p99 medians only; not causal effects or statistical significance. Phase quantiles have different populations and must not be added. Instrumented and incomplete captures excluded. Dominant-stage ties count in each tied stage.' };
}

export async function summarizeCapture(root, { published = null } = {}) {
  root = await realpath(root);
  const artifacts = await verifyArtifacts(root, { published });
  const text = async name => (await readOriginal(root, name)).toString('utf8');
  const json = async name => JSON.parse(await text(name));
  const final = await json('final-status.json');
  requireThat(['primary', 'instrumented'].includes(final.measurement), 'invalid final measurement');
  equal(final.benchmark_claim, false, 'benchmark claim');
  equal(final.source_check, 'unchanged', 'captured source check');
  const sourceBefore = await readOriginal(root, 'source-before.sha256');
  const sourceAfter = await readOriginal(root, 'source-after.sha256');
  requireThat(sourceBefore.equals(sourceAfter), 'source before/after manifests differ');
  const sourceEntries = parseManifest(sourceBefore.toString('utf8'));
  const sourceFiles = (await readOriginal(root, 'source-files.nul')).toString('utf8').split('\0');
  equal(sourceFiles.pop(), '', 'source-files must be NUL terminated');
  equal(new Set(sourceFiles.map(safeName)).size, sourceFiles.length, 'duplicate source-files path');
  equal([...sourceEntries.keys()].sort().join('\0'), sourceFiles.map(safeName).sort().join('\0'), 'source file set binding');
  for (const name of ['source-snapshot-before.txt', 'source-snapshot-check.txt']) {
    const checked = new Set();
    for (const line of (await text(name)).trimEnd().split('\n')) {
      const match = /^(.+): OK$/.exec(line);
      requireThat(match, `source snapshot check failed: ${name}`);
      const pathName = safeName(match[1]);
      requireThat(sourceEntries.has(pathName) && !checked.has(pathName), 'invalid source snapshot check path');
      checked.add(pathName);
    }
    equal(checked.size, sourceEntries.size, 'source snapshot check count');
  }
  const sourceTarSHA = await originalHash(root, 'workload-sources.tar.gz');
  const entries = parseManifest(await text('artifacts.sha256'), { artifact: true });
  equal(entries.get('workload-sources.tar.gz'), sourceTarSHA, 'source archive artifact binding');
  const instrumented = final.measurement === 'instrumented';
  const wanted = matrixNames(instrumented);
  const actual = [...entries.keys()].filter(name => /^(factorial|observed)-[^/]*\.json$/.test(name) && !/-osd-/.test(name));
  for (const name of actual) {
    const meta = parseRowName(name);
    equal(meta.instrumented, instrumented, 'mixed measurements');
    requireThat(wanted.includes(name), `unexpected matrix leg: ${name}`);
  }
  const missing = wanted.filter(name => !actual.includes(name));
  integer(final.failed_legs, 'final failed legs');
  integer(final.exit_code, 'final exit code');
  requireThat(!missing.length || (final.status === 'diagnostic-failed' && final.exit_code !== 0), 'completed capture matrix is incomplete');
  const rows = [];
  let failedLegs = 0;
  for (const name of wanted.filter(name => actual.includes(name))) {
    const row = validateRow(await json(name), name);
    const label = name.slice(0, -5);
    const exits = {};
    for (const [key, suffix] of [['benchmark', 'benchmark-exit-code'], ['docker', 'exit-code'], ['capture', 'capture-exit-code']]) {
      const raw = await text(`${label}-${suffix}`);
      requireThat(/^(0|[1-9][0-9]*)\n?$/.test(raw), 'invalid exit-code artifact');
      exits[key] = integer(Number(raw.trim()), `${key} exit`);
    }
    requireThat(exits.capture <= 1, 'capture qualification exit must be 0 or 1');
    equal(exits.benchmark === 0, !row.failed, 'benchmark exit and failed flag');
    if (exits.benchmark !== 0) equal(exits.docker, exits.benchmark, 'docker must propagate benchmark failure');
    if (row.failed || exits.docker !== 0) equal(exits.capture, 1, 'failed leg qualification');
    failedLegs += Number(exits.capture !== 0);
    row.cgroup = summarizeCgroup(await text(`${label}-cpu.stat-before`), await text(`${label}-cpu.stat-after`), await text(`${label}-cpu.max-before`), await text(`${label}-cpu.max-after`));
    if (instrumented && !row.setup_failure) {
      const timing = await json(`${label}-observation/timing.json`);
      equal(timing.label, 'instrumented', 'observation label');
      equal(timing.calls.length, row.counts.attempted, 'observation attempted calls');
      const indices = new Set();
      const report = await json(name);
      for (const call of timing.calls) {
        integer(call.index, 'observation index');
        requireThat(!indices.has(call.index) && report.offered_load.outcomes[call.index]?.attempted, 'invalid/duplicate observation call index');
        indices.add(call.index);
        integer(call.begin_ns, 'observation begin');
        requireThat(integer(call.end_ns, 'observation end') >= call.begin_ns, 'observation end precedes begin');
      }
      row.observation_calls = indices.size;
    }
    rows.push({ ...row, capture_root: root, capture_complete: missing.length === 0, exits });
  }
  equal(final.failed_legs, failedLegs, 'final failed-leg count');
  requireThat((final.exit_code === 0) === (final.status === 'diagnostic-completed'), 'final exit/status inconsistent');
  requireThat(['diagnostic-completed', 'diagnostic-failed'].includes(final.status), 'invalid final status');
  if (failedLegs > 0) requireThat(final.exit_code !== 0, 'failed legs require nonzero final exit');
  equal((await text('scheduler-status')).trim(), final.status, 'scheduler/final status');
  return { root, measurement: final.measurement, complete: missing.length === 0, missing_rows: missing, final, artifacts,
    source: { files: sourceEntries.size, source_before_sha256: createHash('sha256').update(sourceBefore).digest('hex'),
      source_head: (await text('source-head')).trim(), before_after_identical: true, frozen_snapshot_checks: 'passed', workload_sources_tar_gz_sha256: sourceTarSHA,
      archive_content_verification: 'pending_external_tar_verification', current_worktree_not_consulted: true }, rows, comparison: compareCases(rows) };
}

export async function main(roots) {
  requireThat(roots.length >= 1, 'usage: node factorial-summary.mjs capture1 [capture2 observation optional-failure-capture...]');
  const captures = [];
  for (const root of roots) {
    const canonical = await realpath(root);
    let published = null;
    const availability = await existingPath(canonical, 'published-availability.json');
    if (availability) published = JSON.parse(await readFile(availability, 'utf8'));
    captures.push(await summarizeCapture(canonical, { published }));
  }
  requireThat(new Set(captures.map(capture => capture.root)).size === captures.length, 'duplicate capture roots');
  return { schema: 1, benchmark_claim: false, captures, comparison: compareCases(captures.flatMap(capture => capture.rows)) };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).then(summary => process.stdout.write(JSON.stringify(summary, null, 2) + '\n')).catch(error => {
    process.stderr.write(error.message + '\n');
    process.exitCode = 1;
  });
}