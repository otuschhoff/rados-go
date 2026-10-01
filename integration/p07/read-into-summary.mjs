import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function summarizeReadInto(root) {
  const results = [];
  for (const load of ['idle', 'loaded']) {
    const directory = path.join(root, load);
    if (fs.readFileSync(path.join(directory, 'scheduler-status'), 'utf8').trim() !== 'passed') throw Error('capture failed');
    if (!fs.readFileSync(path.join(directory, 'source-before.sha256')).equals(fs.readFileSync(path.join(directory, 'source-after.sha256')))) throw Error('source changed');
    for (const api of ['read', 'read_into']) {
      const samples = [];
      for (let repeat = 1; repeat <= 5; repeat++) {
        const suffix = api === 'read' ? '' : '-into';
        const prefix = path.join(directory, `sweep-${repeat}-procs-10${suffix}`);
        const report = JSON.parse(fs.readFileSync(`${prefix}.json`));
        const row = report.rows?.[0];
        const diagnostic = report.diagnostic;
        if (report.implementation !== 'go' || report.transport !== 'secure' || report.environment?.gomaxprocs !== 10 || report.rows?.length !== 1 || row.size_bytes !== 65536 || row.concurrency !== 16 || row.workload !== 'read' || row.operations !== 4096 || diagnostic?.read_api !== api || (diagnostic.background_cpu_workers ?? 0) !== (load === 'idle' ? 0 : 8) || diagnostic.warmup_operations !== 128 || diagnostic.resource_scope !== 'warmup_and_measured_reads' || diagnostic.object_set !== 'p07-shared-read-0..15') throw Error('wrong workload');
        const values = { p99_ns: row.p99_ns, iops: row.iops, allocated_bytes: report.resources?.allocated_bytes, allocations: report.resources?.allocations, gc_cycles: report.resources?.gc_cycles, gc_pause_ns: report.resources?.gc_pause_ns };
        for (const [key, value] of Object.entries(values)) {
          if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || (['p99_ns', 'iops', 'allocated_bytes', 'allocations'].includes(key) && value === 0)) throw Error('invalid metric');
        }
        samples.push({ repeat, ...values });
      }
      const medians = {};
      for (const key of Object.keys(samples[0]).filter(key => key !== 'repeat')) medians[key] = samples.map(sample => sample[key]).sort((first, second) => first - second)[2];
      results.push({ load, api, samples, medians });
    }
  }
  return { scope: 'alternating same-cluster diagnostic pairs; unloaded native context only; no qualification', results };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) console.log(JSON.stringify(summarizeReadInto(process.argv[2]), null, 2));