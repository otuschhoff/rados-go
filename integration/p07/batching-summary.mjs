import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function summarize(root) {
  const results = [];
  for (const load of ['idle', 'loaded']) {
    for (const variant of ['before', 'after']) {
      const directory = path.join(root, `${variant}-${load}`);
      if (fs.readFileSync(path.join(directory, 'scheduler-status'), 'utf8').trim() !== 'passed') throw Error('capture failed');
      if (!fs.readFileSync(path.join(directory, 'source-before.sha256')).equals(fs.readFileSync(path.join(directory, 'source-after.sha256')))) throw Error('source changed');
      const samples = [];
      for (let repeat = 1; repeat <= 5; repeat++) {
        const report = JSON.parse(fs.readFileSync(path.join(directory, `sweep-${repeat}-procs-10.json`)));
        const row = report.rows[0];
        const workers = load === 'idle' ? 0 : 8;
        if (report.implementation !== 'go' || report.transport !== 'secure' || report.environment.gomaxprocs !== 10 || report.rows.length !== 1 || row.size_bytes !== 65536 || row.concurrency !== 16 || row.workload !== 'read' || row.operations !== 4096 || (report.diagnostic.background_cpu_workers ?? 0) !== workers || report.diagnostic.warmup_operations !== 128 || report.diagnostic.resource_scope !== 'warmup_and_measured_reads' || report.diagnostic.object_set !== 'p07-shared-read-0..15') throw Error('wrong workload');
        const positive = [row.p99_ns, row.iops, report.resources.allocated_bytes];
        if (positive.some(value => typeof value !== 'number' || !Number.isFinite(value) || value <= 0) || typeof report.resources.gc_pause_ns !== 'number' || !Number.isFinite(report.resources.gc_pause_ns) || report.resources.gc_pause_ns < 0) throw Error('invalid metric');
        const sample = { p99_ms: row.p99_ns / 1e6, iops: row.iops, allocated_bytes: report.resources.allocated_bytes, gc_pause_ms: report.resources.gc_pause_ns / 1e6 };
        samples.push(sample);
      }
      const median = key => samples.map(sample => sample[key]).sort((first, second) => first - second)[2];
      results.push({ load, variant, samples, median_p99_ms: median('p99_ms'), median_iops: median('iops'), median_allocated_bytes: median('allocated_bytes'), median_gc_pause_ms: median('gc_pause_ms') });
    }
  }
  return { scope: 'diagnostic; fresh clusters, not interleaved paired qualification', results };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(summarize(process.argv[2]), null, 2));
}