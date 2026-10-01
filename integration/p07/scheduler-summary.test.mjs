import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tool = fileURLToPath(new URL('./scheduler-summary.mjs', import.meta.url));
const report = (implementation, parallelism) => ({
  implementation, transport: 'secure', environment: { gomaxprocs: parallelism },
  resources: { gc_cycles: 1, gc_pause_ns: 100, max_rss_bytes: 1000, allocated_bytes: 1000 },
  rows: [{ operations: 4096, size_bytes: 65536, concurrency: 16, workload: 'read', p99_ns: 1000, iops: 100 }],
});

function fixture() {
  const directory = mkdtempSync(join(tmpdir(), 'scheduler-summary-test-'));
  const write = (name, value) => writeFileSync(join(directory, name), typeof value === 'string' ? value : JSON.stringify(value));
  write('source-head', 'fixture-head\n');
  write('source-before.sha256', 'fixture source inventory\n');
  write('source-after.sha256', 'fixture source inventory\n');
  for (const repeat of [1, 2, 3, 4, 5]) {
    for (const side of ['before', 'after']) write(`sweep-${repeat}-native-${side}.json`, report('native'));
    for (const parallelism of [2, 4, 6, 10]) {
      const label = `sweep-${repeat}-procs-${parallelism}`;
      write(`${label}.json`, report('go', parallelism));
      write(`${label}-cpu.max-before`, 'max 100000\n');
      write(`${label}-cpuset.cpus.effective-before`, '0-9\n');
      for (const side of ['before', 'after']) write(`${label}-cpu.stat-${side}`, 'nr_throttled 0\nthrottled_usec 0\n');
    }
  }
  return { directory, write };
}

for (const scenario of [
  { name: 'valid sweep', mutate: () => {}, passes: true },
  { name: 'native scope mismatch', mutate: write => write('sweep-1-native-before.json', report('go', 2)) },
  { name: 'missing counter', mutate: write => write('sweep-1-procs-2-cpu.stat-after', 'usage_usec 1\n') },
  { name: 'zero native denominator', mutate: write => {
    const invalid = report('native');
    invalid.rows[0].p99_ns = 0;
    write('sweep-1-native-before.json', invalid);
  } },
  { name: 'source mutation', mutate: write => write('source-after.sha256', 'changed\n') },
  { name: 'incomplete trace timings', mutate: write => {
    write('trace-procs-2.json', report('go', 2));
    write('trace-procs-2-events.txt', 'Sync Time=1000 Trace=1000 Mono=1000 Wall=2026-10-01T00:00:00Z\n');
    write('trace-procs-2-timing.json', []);
  } },
]) {
  test(scenario.name, () => {
    const { directory, write } = fixture();
    try {
      scenario.mutate(write);
      const result = spawnSync(process.execPath, [tool, directory], { encoding: 'utf8' });
      if (scenario.passes) {
        assert.equal(result.status, 0, result.stderr);
        assert.equal(JSON.parse(result.stdout).rows.length, 4);
      } else {
        assert.notEqual(result.status, 0);
        assert.equal(result.stdout, '');
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
}