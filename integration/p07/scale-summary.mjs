import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const workloads = [['idle', 65536, 0, false], ['alloc', 65536, 8, true],
  ['small', 4096, 0, false], ['large', 1048576, 0, false]];
const concurrencies = [16, 32, 64];
const slots = [4, 8];
const repeats = [1, 2, 3, 4, 5];
const retainedLimit = 256 * 1024 * 1024;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const median = values => [...values].sort((left, right) => left - right)[2];

function requireValue(condition, message) {
  if (!condition) throw new Error(message);
}

function number(value, name, { integer = false, positive = false } = {}) {
  requireValue(typeof value === 'number' && Number.isFinite(value) &&
    (positive ? value > 0 : value >= 0) && (!integer || Number.isSafeInteger(value)),
  `${name}: expected ${positive ? 'positive' : 'nonnegative'} finite ${integer ? 'safe integer' : 'number'}`);
  return value;
}

function same(actual, expected, name) {
  requireValue(JSON.stringify(actual) === JSON.stringify(expected), `${name}: mismatch`);
}

function text(directory, filename) {
  return fs.readFileSync(path.join(directory, filename), 'utf8');
}

function json(directory, filename) {
  try { return JSON.parse(text(directory, filename)); }
  catch (error) { throw new Error(`${directory}/${filename}: ${error.message}`); }
}

function manifest(content, name) {
  const entries = new Map();
  for (const line of content.trim().split(/\r?\n/)) {
    const match = /^([a-fA-F0-9]{64})\s+\*?(.+)$/.exec(line);
    requireValue(match && !entries.has(match[2]), `${name}: invalid or duplicate manifest entry`);
    entries.set(match[2], match[1].toLowerCase());
  }
  return entries;
}

function cgroup(directory, label) {
  function stat(phase) {
    const fields = new Map();
    for (const line of text(directory, `${label}-cpu.stat-${phase}`).trim().split(/\r?\n/)) {
      const match = /^(\S+)\s+([0-9]+)$/.exec(line);
      requireValue(match && !fields.has(match[1]), `${label}: invalid decimal cpu.stat ${phase}`);
      fields.set(match[1], BigInt(match[2]));
    }
    return fields;
  }
  const before = stat('before');
  const after = stat('after');
  const deltas = {};
  for (const key of ['nr_throttled', 'throttled_usec']) {
    requireValue(before.has(key) && after.has(key), `${label}: missing cgroup counter ${key}`);
    requireValue(after.get(key) >= before.get(key), `${label}: decreased cgroup counter ${key}`);
    deltas[key] = (after.get(key) - before.get(key)).toString();
  }
  const quota = text(directory, `${label}-cpu.max-before`).trim();
  requireValue(/^(max|[0-9]+)\s+[0-9]+$/.test(quota), `${label}: invalid cpu.max`);
  const cpuset = text(directory, `${label}-cpuset.cpus.effective-before`).trim();
  requireValue(/^[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*$/.test(cpuset), `${label}: invalid cpuset`);
  return { cpu_max: quota, cpuset_cpus_effective: cpuset, deltas,
    zero_throttling: deltas.nr_throttled === '0' && deltas.throttled_usec === '0' };
}

function work(report, size, concurrency, operations, implementation, label) {
  same(report.implementation, implementation, `${label} implementation`);
  same(report.transport, 'secure', `${label} transport`);
  requireValue(Array.isArray(report.rows) && report.rows.length === 1, `${label}: expected exactly one row`);
  const row = report.rows[0];
  for (const [key, expected] of Object.entries({ size_bytes: size, concurrency,
    workload: 'read', operations, bytes: size * operations })) same(row[key], expected, `${label} ${key}`);
  for (const key of ['elapsed_ns', 'p50_ns', 'p95_ns', 'p99_ns']) {
    number(row[key], `${label} ${key}`, { integer: true, positive: true });
  }
  for (const key of ['throughput_bytes_per_second', 'iops']) number(row[key], `${label} ${key}`, { positive: true });
  requireValue(row.p50_ns <= row.p95_ns && row.p95_ns <= row.p99_ns, `${label}: unordered percentiles`);
  requireValue(report.resources && typeof report.resources === 'object' && !Array.isArray(report.resources), `${label}: missing resources`);
  for (const key of ['cpu_user_ns', 'cpu_system_ns', 'allocations', 'allocated_bytes', 'max_rss_bytes',
    ...(implementation === 'go' ? ['gc_cycles', 'gc_pause_ns'] : [])]) {
    if (implementation === 'native' && ['allocations', 'allocated_bytes'].includes(key) && report.resources[key] === null) continue;
    number(report.resources[key], `${label} resources.${key}`, { integer: true, positive: key === 'max_rss_bytes' });
  }
  for (const [key, value] of Object.entries(report.resources)) {
    if (implementation === 'native' && ['allocations', 'allocated_bytes'].includes(key) && value === null) continue;
    number(value, `${label} resources.${key}`, { integer: true });
  }
  const diagnostic = report.diagnostic;
  requireValue(diagnostic && typeof diagnostic === 'object', `${label}: missing diagnostic`);
  same(diagnostic.resource_scope, 'warmup_and_measured_reads', `${label} resource_scope`);
  same(diagnostic.warmup_operations, 8 * concurrency, `${label} warmup_operations`);
  const prefix = size === 65536 ? 'p07-shared-read-' : `p07-shared-read-size-${size}-`;
  same(diagnostic.object_set, `${prefix}0..${concurrency - 1}`, `${label} object_set`);
  return row;
}

function identity(directory, name) {
  const source = text(directory, 'source-before.sha256');
  same(text(directory, 'source-after.sha256'), source, `${name} source changed`);
  const sources = manifest(source, `${name} source`);
  const head = text(directory, 'source-head').trim();
  requireValue(/^[a-f0-9]{40}$/.test(head), `${name}: invalid source head`);
  same(text(directory, 'scheduler-status').trim(), 'passed', `${name} scheduler-status`);
  const build = text(directory, 'go-buildinfo.txt').trim().split(/\r?\n/);
  requireValue(/: go1\.27\.1$/.test(build[0]), `${name}: build toolchain mismatch`);
  const settings = new Map();
  for (const line of build.slice(1)) {
    const match = /^\s*build\s+(\S+?)=(.*)$/.exec(line);
    if (match) {
      requireValue(!settings.has(match[1]), `${name}: duplicate build setting`);
      settings.set(match[1], match[2]);
    }
  }
  requireValue((settings.get('-tags') ?? '').replace(/^"|"$/g, '').split(/[ ,]+/).includes('p12diagnostics'), `${name}: missing p12diagnostics build tag`);
  same(settings.get('GOOS'), 'linux', `${name} build GOOS`);
  same(settings.get('GOARCH'), 'arm64', `${name} build GOARCH`);
  const artifacts = manifest(text(directory, 'artifacts.sha256'), `${name} artifacts`);
  const binaryHash = artifacts.get('benchmark');
  requireValue(binaryHash, `${name}: missing benchmark artifact hash`);
  const binary = path.join(directory, 'benchmark');
  const present = fs.existsSync(binary);
  if (present) same(hash(fs.readFileSync(binary)), binaryHash, `${name} benchmark binary hash`);
  return { source, sources: [...sources].map(([filename, sha256]) => ({ filename, sha256 })),
    source_manifest_sha256: hash(source), head, build: ['go1.27.1', ...build.slice(1)].join('\n'),
    benchmark_sha256: binaryHash, binary_verification: present ? 'verified_present_bytes' : 'manifest_identity_only_binary_absent' };
}

export function summarize(directory) {
  requireValue(typeof directory === 'string' && directory.length > 0 && fs.existsSync(directory) && fs.statSync(directory).isDirectory(),
    'Usage: node scale-summary.mjs ROOTDIR (existing directory containing idle, alloc, small, large)');
  const samples = [];
  const native = [];
  const identities = [];
  let reference;
  for (const [name, size, workers, allocations] of workloads) {
    const capture = path.join(directory, name);
    const current = identity(capture, name);
    if (reference) {
      for (const key of ['source', 'head', 'build', 'benchmark_sha256']) same(current[key], reference[key], `${name} cross-capture ${key}`);
    } else reference = current;
    identities.push({ workload: name, benchmark_sha256: current.benchmark_sha256, binary_verification: current.binary_verification });
    const methodology = json(capture, 'scale-methodology.json');
    same(methodology.kind, 'read-scale-diagnostic', `${name} methodology kind`);
    same(methodology.benchmark_claim, false, `${name} benchmark_claim`);
    same(methodology.matched_native, false, `${name} matched_native`);
    for (const [key, expected] of Object.entries({ size_bytes: size, concurrencies, scratch_slots: slots,
      operations_per_worker: 1024, measured_legs: 30 })) same(methodology.go?.[key], expected, `${name} methodology go.${key}`);
    for (const [key, expected] of Object.entries({ size_bytes: 65536, concurrency: 16, operations: 4096,
      scope: 'shorter unmatched context' })) same(methodology.native_context?.[key], expected, `${name} native_context.${key}`);
    const expectedLabels = repeats.flatMap(repeat => concurrencies.flatMap(concurrency => slots.map(scratchSlots =>
      `scale-${repeat}-concurrency-${concurrency}-slots-${scratchSlots}.json`)));
    const found = fs.readdirSync(capture).filter(filename => /^scale-.*\.json$/.test(filename) && filename !== 'scale-methodology.json').sort();
    same(found, [...expectedLabels].sort(), `${name} scale labels (missing or extra leg)`);
    for (const repeat of repeats) for (const concurrency of concurrencies) for (const scratchSlots of slots) {
      const label = `scale-${repeat}-concurrency-${concurrency}-slots-${scratchSlots}`;
      const report = json(capture, `${label}.json`);
      const row = work(report, size, concurrency, 1024 * concurrency, 'go', label);
      for (const [key, expected] of Object.entries({ GOOS: 'linux', GOARCH: 'arm64', go_version: 'go1.27.1', gomaxprocs: 10 })) {
        same(report.environment?.[key], expected, `${label} environment.${key}`);
      }
      const diagnostic = report.diagnostic;
      for (const [key, expected] of Object.entries({ read_api: 'read_into', operations_per_worker: 1024,
        scratch_slots: scratchSlots, admission_window: concurrency })) same(diagnostic[key], expected, `${label} ${key}`);
      same(diagnostic.background_cpu_workers === undefined ? 0 : diagnostic.background_cpu_workers, workers, `${label} background_cpu_workers`);
      same(diagnostic.background_allocations === undefined ? false : diagnostic.background_allocations, allocations, `${label} background_allocations`);
      const scratch = diagnostic.scratch;
      requireValue(scratch && typeof scratch === 'object' && !Array.isArray(scratch), `${label}: missing scratch counters`);
      for (const key of ['hits', 'misses', 'bypasses', 'shared_receive_retained_bytes']) number(scratch[key], `${label} scratch.${key}`, { integer: true });
      for (const [key, value] of Object.entries(scratch)) number(value, `${label} scratch.${key}`, { integer: true });
      const expectedReplies = BigInt(row.operations + diagnostic.warmup_operations);
      const eligible = BigInt(scratch.hits) + BigInt(scratch.misses);
      if (size === 65536) requireValue(eligible >= expectedReplies, `${label}: missing eligible replies`);
      else requireValue(eligible === 0n && BigInt(scratch.bypasses) >= expectedReplies, `${label}: fallback hits/misses or missing bypasses`);
      requireValue(scratch.shared_receive_retained_bytes <= retainedLimit, `${label}: retained exceeds global 256 MiB budget`);
      samples.push({ workload: name, label, repeat, size_bytes: size, concurrency, scratch_slots: scratchSlots,
        operations: row.operations, bytes: row.bytes, elapsed_ns: row.elapsed_ns, p50_ns: row.p50_ns,
        p95_ns: row.p95_ns, p99_ns: row.p99_ns, iops: row.iops,
        throughput_bytes_per_second: row.throughput_bytes_per_second,
        allocated_bytes: report.resources.allocated_bytes, allocations: report.resources.allocations,
        resources: report.resources, scratch_hits: scratch.hits, scratch_misses: scratch.misses,
        hit_fraction: size === 65536 ? Number(BigInt(scratch.hits)) / Number(eligible) : null,
        bypasses: scratch.bypasses, retained_bytes: scratch.shared_receive_retained_bytes,
        cgroup: cgroup(capture, label) });
    }
    const expectedNative = repeats.flatMap(repeat => ['before', 'after'].map(phase => `sweep-${repeat}-native-${phase}.json`));
    same(fs.readdirSync(capture).filter(filename => /^sweep-.*-native-.*\.json$/.test(filename)).sort(), expectedNative.sort(), `${name} native labels`);
    for (const repeat of repeats) for (const phase of ['before', 'after']) {
      const label = `sweep-${repeat}-native-${phase}`;
      const report = json(capture, `${label}.json`);
      const row = work(report, 65536, 16, 4096, 'native', label);
      native.push({ workload: name, repeat, phase, matched: false, size_bytes: 65536, concurrency: 16,
        operations: 4096, p99_ns: row.p99_ns, iops: row.iops, cgroup: cgroup(capture, label) });
    }
  }
  const groups = [];
  const pairs = [];
  const aggregates = [];
  for (const [name] of workloads) {
    const aggregate = { workload: name, paired_repeats: 15, p99_wins_slots_8: 0, iops_wins_slots_8: 0, joint_wins_slots_8: 0 };
    for (const concurrency of concurrencies) {
      for (const scratchSlots of slots) {
        const group = samples.filter(sample => sample.workload === name && sample.concurrency === concurrency && sample.scratch_slots === scratchSlots);
        const medians = {};
        for (const key of ['p99_ns', 'iops', 'allocated_bytes', 'allocations', 'bypasses', 'retained_bytes']) medians[key] = median(group.map(sample => sample[key]));
        medians.hit_fraction = group[0].hit_fraction === null ? null : median(group.map(sample => sample.hit_fraction));
        groups.push({ workload: name, size_bytes: group[0].size_bytes, concurrency, scratch_slots: scratchSlots, sample_count: 5, medians });
      }
      const pair = { workload: name, concurrency, paired_repeats: 5, p99_wins_slots_8: 0, iops_wins_slots_8: 0, joint_wins_slots_8: 0,
        p99_ties: 0, iops_ties: 0 };
      for (const repeat of repeats) {
        const pairSamples = samples.filter(sample => sample.workload === name && sample.concurrency === concurrency && sample.repeat === repeat);
        const four = pairSamples.find(sample => sample.scratch_slots === 4);
        const eight = pairSamples.find(sample => sample.scratch_slots === 8);
        const p99Win = eight.p99_ns < four.p99_ns;
        const iopsWin = eight.iops > four.iops;
        pair.p99_wins_slots_8 += Number(p99Win);
        pair.iops_wins_slots_8 += Number(iopsWin);
        pair.joint_wins_slots_8 += Number(p99Win && iopsWin);
        pair.p99_ties += Number(eight.p99_ns === four.p99_ns);
        pair.iops_ties += Number(eight.iops === four.iops);
      }
      for (const key of ['p99_wins_slots_8', 'iops_wins_slots_8', 'joint_wins_slots_8']) aggregate[key] += pair[key];
      pairs.push(pair);
    }
    aggregates.push(aggregate);
  }
  return { kind: 'read-scale-summary', matched_native: false,
    source: { head: reference.head, manifest_sha256: reference.source_manifest_sha256, files: reference.sources },
    benchmark_sha256: reference.benchmark_sha256, captures: identities,
    contract: { implementation: 'go', transport: 'secure', GOOS: 'linux', GOARCH: 'arm64',
      go_version: 'go1.27.1', gomaxprocs: 10, read_api: 'read_into', scratch_slots: slots,
      admission_window: 'concurrency', operations_per_worker: 1024, warmup_operations: '8 * concurrency',
      resource_scope: 'warmup_and_measured_reads', global_receive_budget_bytes: retainedLimit,
      loads: workloads.map(([workload, size_bytes, background_cpu_workers, background_allocations]) =>
        ({ workload, size_bytes, background_cpu_workers, background_allocations })) },
    sample_count: samples.length, samples, groups, pairs, aggregates,
    throttling: { go_legs: samples.length, go_legs_with_nonzero_delta: samples.filter(sample => !sample.cgroup.zero_throttling).length,
      native_legs: native.length, native_legs_with_nonzero_delta: native.filter(sample => !sample.cgroup.zero_throttling).length },
    native_context: { matched: false, scope: 'shorter unmatched context; work scope validated only', sample_count: native.length, samples: native },
    caveats: ['One host; closed-loop reads; five repeats per size/concurrency/slot group.',
      'Repeat-paired slot wins are descriptive, not significance tests or cross-host estimates.',
      'Native context is unmatched (64 KiB, concurrency 16, 4096 operations); no native ratios or baseline comparisons.',
      'Source hashes identify captured manifests, not a verification of current live repository contents.',
      'Absent publication binaries use original manifest identity only; no absent binary bytes are verified.',
      'Retained bytes are a snapshot checked against the global 256 MiB receive budget, not a peak or a per-transport slot bound.'] };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    requireValue(process.argv.length === 3, 'Usage: node scale-summary.mjs ROOTDIR (containing idle, alloc, small, large)');
    console.log(JSON.stringify(summarize(process.argv[2]), null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}