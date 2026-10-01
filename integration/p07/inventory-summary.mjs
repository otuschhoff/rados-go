import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const variants = [[1, 16], [2, 16], [4, 16], [1, 8], [1, 4]];
const number = (value, positive = false) => {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || (positive && value === 0)) throw Error('invalid metric');
  return value;
};

function readSample(directory, filename, load, slots, window) {
  const report = JSON.parse(fs.readFileSync(path.join(directory, `${filename}.json`)));
  const row = report.rows?.[0];
  const diagnostic = report.diagnostic;
  if (report.implementation !== 'go' || report.transport !== 'secure' || report.environment?.gomaxprocs !== 10 || report.environment?.go_version !== 'go1.27.1' || report.environment?.GOOS !== 'linux' || report.environment?.GOARCH !== 'arm64' || report.rows?.length !== 1 || row.size_bytes !== 65536 || row.concurrency !== 16 || row.workload !== 'read' || row.operations !== 16384 || diagnostic?.read_api !== 'read_into' || diagnostic.operations_per_worker !== 1024 || (diagnostic.scratch_slots ?? 0) !== slots || diagnostic.admission_window !== window || (diagnostic.background_cpu_workers ?? 0) !== (load === 'idle' ? 0 : 8) || (diagnostic.background_allocations ?? false) !== (load === 'alloc' || load === 'default-confirm') || diagnostic.warmup_operations !== 128 || diagnostic.resource_scope !== 'warmup_and_measured_reads' || diagnostic.object_set !== 'p07-shared-read-0..15') throw Error('wrong workload');
  const sample = {
    p50_ns: number(row.p50_ns, true), p95_ns: number(row.p95_ns, true), p99_ns: number(row.p99_ns, true),
    iops: number(row.iops, true), elapsed_ns: number(row.elapsed_ns, true),
    allocated_bytes: number(report.resources?.allocated_bytes, true), allocations: number(report.resources?.allocations, true),
    cpu_user_ns: number(report.resources?.cpu_user_ns), cpu_system_ns: number(report.resources?.cpu_system_ns),
    max_rss_bytes: number(report.resources?.max_rss_bytes, true),
    gc_cycles: number(report.resources?.gc_cycles), gc_pause_ns: number(report.resources?.gc_pause_ns),
  };
  if (slots !== 0) {
    const counters = diagnostic.scratch;
    sample.hits = number(counters?.hits);
    sample.misses = number(counters?.misses);
    sample.bypasses = number(counters?.bypasses);
    sample.shared_receive_retained_bytes = number(counters?.shared_receive_retained_bytes);
    if (!Number.isSafeInteger(sample.hits) || !Number.isSafeInteger(sample.misses) || sample.hits + sample.misses !== 16512) throw Error('wrong receive count');
    sample.hit_fraction = sample.hits / (sample.hits + sample.misses);
  }
  const counterFile = phase => {
    const entries = fs.readFileSync(path.join(directory, `${filename}-cpu.stat-${phase}`), 'utf8').trim().split('\n').map(line => line.trim().split(/\s+/));
    return Object.fromEntries(entries.map(([key, value]) => [key, value]));
  };
  const before = counterFile('before'), after = counterFile('after');
  for (const key of ['nr_throttled', 'throttled_usec']) {
    if (!/^\d+$/.test(before[key] ?? '') || !/^\d+$/.test(after[key] ?? '')) throw Error('missing throttle counter');
    const delta = BigInt(after[key]) - BigInt(before[key]);
    if (delta < 0n || delta > BigInt(Number.MAX_SAFE_INTEGER)) throw Error('invalid throttle delta');
    sample[`${key}_delta`] = Number(delta);
  }
  return sample;
}

function checkSource(directory) {
  if (fs.readFileSync(path.join(directory, 'scheduler-status'), 'utf8').trim() !== 'passed') throw Error('capture failed');
  const before = fs.readFileSync(path.join(directory, 'source-before.sha256'));
  if (!before.equals(fs.readFileSync(path.join(directory, 'source-after.sha256')))) throw Error('source changed');
  return before;
}

export function summarizeInventory(root) {
  const results = [];
  let source;
  for (const load of ['idle', 'cpu', 'alloc', 'default-confirm']) {
    const directory = path.join(root, load);
    const capturedSource = checkSource(directory);
    if (load !== 'default-confirm') {
      if (source && !source.equals(capturedSource)) throw Error('variant source mismatch');
      source = capturedSource;
    }
    for (const [slots, window] of load === 'default-confirm' ? [[0, 16]] : variants) {
      const samples = [];
      for (let repeat = 1; repeat <= 5; repeat++) {
        const filename = load === 'default-confirm' ? `default-${repeat}` : `inventory-${repeat}-slots-${slots}-window-${window}`;
        samples.push({ repeat, ...readSample(directory, filename, load, slots, window) });
      }
      const medians = {};
      for (const key of Object.keys(samples[0]).filter(key => key !== 'repeat')) medians[key] = samples.map(sample => sample[key]).sort((first, second) => first - second)[2];
      results.push({ load, slots: slots || 'production-default', window, samples, medians });
    }
  }
  const comparisons = [];
  for (const load of ['idle', 'cpu', 'alloc']) {
    const baseline = results.find(result => result.load === load && result.slots === 1 && result.window === 16);
    for (const candidate of results.filter(result => result.load === load && result !== baseline)) {
      comparisons.push({ load, slots: candidate.slots, window: candidate.window,
        p99_improved_repeats: candidate.samples.filter((sample, index) => sample.p99_ns < baseline.samples[index].p99_ns).length,
        iops_improved_repeats: candidate.samples.filter((sample, index) => sample.iops > baseline.samples[index].iops).length,
        median_p99_change_fraction: candidate.medians.p99_ns / baseline.medians.p99_ns - 1,
        median_iops_change_fraction: candidate.medians.iops / baseline.medians.iops - 1 });
    }
  }
  return { scope: 'same-cluster rotated diagnostics, admission included; native unloaded shorter context; no qualification', results, comparisons };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) console.log(JSON.stringify(summarizeInventory(process.argv[2]), null, 2));