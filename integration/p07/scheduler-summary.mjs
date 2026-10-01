import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

const directory = process.argv[2];
if (!directory) throw new Error('usage: node scheduler-summary.mjs <capture-directory>');
const read = name => readFileSync(join(directory, name), 'utf8');
const json = name => JSON.parse(read(name));
const finite = (value, name, positive = false) => {
  if (!Number.isFinite(value) || value < 0 || (positive && value === 0)) throw new Error(`invalid metric: ${name}`);
  return value;
};
const reportFor = (name, implementation, parallelism) => {
  const report = json(name);
  const result = report.rows?.[0];
  if (report.implementation !== implementation || report.transport !== 'secure' ||
      report.rows?.length !== 1 || result.operations !== 4096 || result.size_bytes !== 65536 ||
      result.concurrency !== 16 || result.workload !== 'read' ||
      (parallelism !== undefined && report.environment?.gomaxprocs !== parallelism)) throw new Error(`scope mismatch: ${name}`);
  finite(result.p99_ns, `${name}:p99`, true);
  finite(result.iops, `${name}:iops`, true);
  if (implementation === 'go') {
    for (const key of ['gc_cycles', 'gc_pause_ns', 'max_rss_bytes', 'allocated_bytes']) finite(report.resources?.[key], `${name}:${key}`);
  }
  return report;
};
const median = values => [...values].sort((left, right) => left - right)[Math.floor(values.length / 2)];
const percentile = (values, fraction) => [...values].sort((left, right) => left - right)[Math.floor((values.length - 1) * fraction)];
const nanoseconds = value => {
  const match = value.match(/^(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$/);
  if (!match) throw new Error(`invalid timestamp: ${value}`);
  return BigInt(Date.parse(match[1] + match[3])) * 1000000n + BigInt((match[2] || '').padEnd(9, '0'));
};
const counters = name => {
  const values = Object.fromEntries(read(name).trim().split('\n').map(line => {
    const [key, value] = line.split(/\s+/);
    return [key, Number(value)];
  }));
  for (const key of ['nr_throttled', 'throttled_usec']) finite(values[key], `${name}:${key}`);
  return values;
};
const rows = [];
for (const parallelism of [2, 4, 6, 10]) {
  const repetitions = [];
  for (const repeat of [1, 2, 3, 4, 5]) {
    const label = `sweep-${repeat}-procs-${parallelism}`;
    const report = reportFor(`${label}.json`, 'go', parallelism);
    const result = report.rows[0];
    const native = ['before', 'after'].map(side => reportFor(`sweep-${repeat}-native-${side}.json`, 'native').rows[0].p99_ns);
    const before = counters(`${label}-cpu.stat-before`);
    const after = counters(`${label}-cpu.stat-after`);
    for (const key of ['nr_throttled', 'throttled_usec']) finite(after[key] - before[key], `${label}:${key} delta`);
    repetitions.push({ repeat, p99_ns: result.p99_ns, iops: result.iops,
      gc_cycles: report.resources.gc_cycles, gc_pause_ns: report.resources.gc_pause_ns,
      max_rss_bytes: report.resources.max_rss_bytes, allocated_bytes: report.resources.allocated_bytes,
      native_p99_ns: native, ratio_to_faster_native: result.p99_ns / Math.min(...native),
      cpu_max: read(`${label}-cpu.max-before`).trim(),
      cpuset: read(`${label}-cpuset.cpus.effective-before`).trim(),
      nr_throttled_delta: after.nr_throttled - before.nr_throttled,
      throttled_usec_delta: after.throttled_usec - before.throttled_usec });
  }
  rows.push({ parallelism, median_p99_ns: median(repetitions.map(value => value.p99_ns)),
    median_iops: median(repetitions.map(value => value.iops)),
    median_gc_pause_ns: median(repetitions.map(value => value.gc_pause_ns)), repetitions });
}

const traces = [];
for (const parallelism of [2, 10]) {
  const label = `trace-procs-${parallelism}`;
  if (!existsSync(join(directory, `${label}-events.txt`))) continue;
  const report = reportFor(`${label}.json`, 'go', parallelism);
  const events = read(`${label}-events.txt`);
  const clock = events.match(/Sync .*?Trace=(\d+) Mono=\d+ Wall=(\S+)/);
  if (!clock) throw new Error(`missing clock mapping: ${label}`);
  const wallOffset = nanoseconds(clock[2]) - BigInt(clock[1]);
  const pending = new Map();
  const ranges = [];
  let unmatchedEnds = 0;
  let activeRanges = 0;
  for (const line of events.split('\n')) {
    if (line.includes('RangeActive ')) activeRanges++;
    const match = line.match(/Range(Begin|End) Time=(\d+) Name="([^"]+)" Scope=([^ ]+)/);
    if (!match) continue;
    const [, kind, time, name, scope] = match;
    const key = `${name} ${scope}`;
    const at = BigInt(time) + wallOffset;
    if (kind === 'Begin') {
      if (pending.has(key)) throw new Error(`duplicate range begin: ${label}:${key}`);
      pending.set(key, at);
    }
    else if (pending.has(key)) {
      const start = pending.get(key);
      pending.delete(key);
      if (at < start) throw new Error(`negative range: ${label}`);
      ranges.push({ name, start, end: at, duration_ns: Number(at - start) });
    } else unmatchedEnds++;
  }
  const gcNames = [...new Set(ranges.filter(range => /GC/.test(range.name)).map(range => range.name))];
  const gc = gcNames.map(name => {
    const durations = ranges.filter(range => range.name === name).map(range => range.duration_ns);
    return { name, count: durations.length, total_ns: durations.reduce((total, value) => total + value, 0),
      p99_ns: percentile(durations, 0.99), max_ns: Math.max(...durations) };
  });
  const pauses = ranges.filter(range => range.name.startsWith('stop-the-world (GC'));
  const timing = json(`${label}-timing.json`);
  if (!Array.isArray(timing) || timing.length !== 4096) throw new Error(`incomplete request timings: ${label}`);
  const identities = new Set();
  const requests = timing.map(request => {
    const first = request.find(event => event.stage === 'read_enter');
    const last = request.find(event => event.stage === 'read_return');
    if (!first || !last) throw new Error(`missing read boundary: ${label}`);
    const start = nanoseconds(first.at), end = nanoseconds(last.at);
    if (end <= start || !Number.isSafeInteger(first.transaction_id) || identities.has(first.transaction_id)) throw new Error(`invalid request interval or identity: ${label}`);
    identities.add(first.transaction_id);
    let pauseOverlap = 0n;
    for (const pause of pauses) {
      const overlapStart = start > pause.start ? start : pause.start;
      const overlapEnd = end < pause.end ? end : pause.end;
      if (overlapEnd > overlapStart) pauseOverlap += overlapEnd - overlapStart;
    }
    return { transaction_id: first.transaction_id, duration_ns: Number(end - start),
      gc_stw_overlap_ns: Number(pauseOverlap) };
  }).sort((left, right) => right.duration_ns - left.duration_ns);
  const slowest = requests.slice(0, Math.ceil(requests.length * 0.01));
  traces.push({ parallelism, report, clock_mapping: clock[0], gc,
    range_coverage: { active_at_boundary: activeRanges, unmatched_ends: unmatchedEnds,
      unfinished_ranges: pending.size, complete: activeRanges === 0 && unmatchedEnds === 0 && pending.size === 0 },
    requests: requests.length, slowest_one_percent: slowest,
    slowest_with_gc_stw_overlap: slowest.filter(request => request.gc_stw_overlap_ns > 0).length,
    median_slowest_gc_stw_fraction: median(slowest.map(request => request.gc_stw_overlap_ns / request.duration_ns)),
    limitations: 'Instrumented execution differs materially in GC count/live heap. Global STW overlap is temporal, not causal attribution; assists sum across goroutines. Read-enter/read-return scope differs slightly from outer benchmark timer. No request-to-goroutine assist attribution. Only completed ranges are summarized; non-complete range_coverage means overlap may be underestimated.' });
}

const sourceBefore = read('source-before.sha256');
if (sourceBefore !== read('source-after.sha256')) throw new Error('source changed during capture');
const summary = { schema_version: 1, source_head: read('source-head').trim(),
  source_inventory_sha256: createHash('sha256').update(sourceBefore).digest('hex'),
  method: 'Single isolated cluster, five serial repetitions, ascending/descending P order, native brackets, GOGC=100/GOMEMLIMIT=off. Trace runs separate from qualification samples.',
  status: 'diagnostic only; no Phase 4 closure, library-default change, parity or certification', rows, traces };
console.log(JSON.stringify(summary, null, 2));