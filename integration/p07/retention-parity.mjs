import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {fixtureName, validateParityHealth} from './parity.mjs';

export function retentionMetrics(report, implementation, cell, windows = 16) {
  assert.equal(report.implementation, implementation); assert.equal(report.transport, 'secure');
  assert.equal(report.windows.length, windows);
  const rss = [], allocated = [];
  for (const [index, window] of report.windows.entries()) {
    assert.equal(window.index, index); assert.equal(window.size_bytes, cell.size);
    assert.equal(window.concurrency, cell.concurrency); assert.equal(window.workload, cell.workload);
    assert.equal(window.operations, cell.operations * cell.concurrency);
    assert.equal(window.payload_verified, true); assert.equal(window.cleanup_verified, true);
    const allocation = window[implementation === 'go' ? 'heap_after_gc_bytes' : 'glibc_allocated_bytes'];
    assert(Number.isSafeInteger(allocation) && allocation > 0);
    assert(Number.isSafeInteger(window.rss_after_cleanup_bytes) && window.rss_after_cleanup_bytes > 0);
    rss.push(window.rss_after_cleanup_bytes); allocated.push(allocation);
  }
  const median = values => {
    const sorted = [...values].sort((first, second) => first - second), midpoint = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[midpoint] : (sorted[midpoint - 1] + sorted[midpoint]) / 2;
  };
  const trend = values => {
    const baseline = median(values.slice(4, 8)), final = median(values.slice(-4));
    const budget = Math.max(16 * 1048576, baseline * .10);
    return {baseline_bytes: baseline, final_bytes: final, growth_bytes: final - baseline, budget_bytes: budget, bounded_growth: final - baseline <= budget};
  };
  assert(windows >= 12);
  return {rss: trend(rss), allocated: trend(allocated), rss_samples: rss, allocation_samples: allocated, allocation_metric: implementation === 'go' ? 'post_gc_go_heap' : 'partial_glibc_allocations', limitation: 'Bounded-window growth budget, not an endurance or equal-live-heap claim'};
}

export function captureRetentionParity(output, prepared, namespace) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(namespace) && namespace.length <= 64);
  const env = process.env;
  for (const key of ['P07_PARITY_CONFIG', 'P07_PARITY_KEY', 'P07_PARITY_KEYRING', 'P07_PARITY_MONITORS_FILE', 'P07_PARITY_FSID_FILE']) assert(env[key], key);
  fs.mkdirSync(output, {mode: 0o700});
  const write = (name, value) => fs.writeFileSync(path.join(output, name), JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});
  const hash = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const affinity = JSON.parse(fs.readFileSync(path.join(prepared, 'methodology.json'))).affinity;
  const environment = Object.fromEntries(['PATH', 'HOME', 'GOTOOLCHAIN', 'GOCACHE', 'TMPDIR'].filter(key => env[key]).map(key => [key, env[key]]));
  const runtime = {GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'};
  const execute = (name, command, args, extra = {}) => {
    const result = spawnSync(command, args, {env: {...environment, ...runtime, ...extra}, encoding: 'utf8', timeout: 3600000, maxBuffer: 32 << 20});
    for (const stream of ['stdout', 'stderr']) fs.writeFileSync(path.join(output, `${name}.${stream}`), result[stream] ?? '', {flag: 'wx', mode: 0o600});
    write(`${name}.exit.json`, {command, args: args.map(value => [env.P07_PARITY_KEY, env.P07_PARITY_KEYRING, env.P07_PARITY_CONFIG].includes(value) ? '<private-path>' : value), status: result.status, signal: result.signal, error: result.error?.message});
    assert.equal(result.status, 0, `${name} failed: ${result.stderr}`);
    return result.stdout;
  };
  const sources = [...new Set(execute('source-files', 'git', ['ls-files', '-co', '--exclude-standard']).trim().split('\n'))].filter(file => /(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sourcePins = () => sources.map(file => ({file, sha256: hash(file)}));
  const beforeSources = sourcePins(); write('sources-before.json', beforeSources);
  const binaries = {go: path.join(output, 'go-benchmark'), native: path.join(output, 'native-retention')}, checker = path.join(output, 'mode-check');
  execute('build-go', 'go', ['build', '-o', binaries.go, './integration/p07/benchmark']);
  execute('build-native', 'gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', 'integration/p07/native_retention.c', '-ldl', '-o', binaries.native]);
  execute('build-mode-check', 'go', ['build', '-o', checker, './tools/perf-mode-check']);
  const pins = Object.fromEntries(Object.entries(binaries).map(([name, file]) => [name, hash(file)])); write('binaries.json', pins);
  const library = fs.realpathSync('/lib64/librados.so.2'), libraryPin = hash(library); write('library.json', {path: library, sha256: libraryPin});
  const monitors = fs.readFileSync(env.P07_PARITY_MONITORS_FILE, 'utf8').trim().split(/\s+/).map(host => `${host}:3300`).join(',');
  const fsid = fs.readFileSync(env.P07_PARITY_FSID_FILE, 'utf8').trim();
  const args = pool => ['-monitors', monitors, '-fsid', fsid, '-key-file', env.P07_PARITY_KEY, '-entity', 'client.amakura', '-pool', pool, '-transport', 'secure'];
  const ceph = (name, command) => JSON.parse(execute(name, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, ...command, '--format', 'json']));
  const health = name => {
    return validateParityHealth(ceph(name, ['status']), fsid);
  };
  const placement = (name, pool, cell) => Array.from({length: cell.concurrency}, (_, worker) => {
    const report = ceph(`${name}-w${worker}`, ['osd', 'map', pool, fixtureName(cell, worker), namespace]);
    assert(report.pgid && report.acting?.length && Number.isInteger(report.acting_primary));
    return {pg: report.pgid, acting: report.acting, primary: report.acting_primary};
  });
  const host = name => write(name, Object.fromEntries(['cpu.stat', 'cpu.max', 'cpuset.cpus.effective', 'memory.events'].map(file => [file, fs.readFileSync(`/sys/fs/cgroup/${file}`, 'utf8')])));
  const cells = [{size: 1048576, concurrency: 1, workload: 'write', operations: 1024}, {size: 4194304, concurrency: 16, workload: 'write', operations: 256}, {size: 65536, concurrency: 16, workload: 'read', operations: 4096}];
  const windows = 16, captures = [], failures = [];
  write('methodology.json', {namespace, affinity, runtime, cells, windows, order: ['native', 'go', 'go', 'native'], pools: ['test-3x', 'readcache'], budget: 'median final four minus median windows 5..8 <= max(16MiB, 10% baseline), separately for RSS and each implementation-specific allocation metric', warmup_windows_excluded: 4, status: 'bounded retention diagnostics, not native performance parity or endurance qualification'});
  const leg = (name, implementation, pool, cell, count, probe = false) => {
    const extra = {P07_PARITY_NAMESPACE: namespace, P07_MATRIX_SIZE: String(cell.size), P07_MATRIX_CONCURRENCY: String(cell.concurrency), P07_MATRIX_WORKLOAD: cell.workload, P07_MATRIX_OPERATIONS_PER_WORKER: String(cell.operations)};
    if (implementation === 'go') { extra.P07_RETENTION_WINDOWS = String(count); extra.P07_READ_INTO = '1'; if (probe) extra.P07_MODE_EVIDENCE_FILE = path.join(output, `${name}.modes.json`); }
    else if (probe) extra.P07_NATIVE_MODE_LOG = path.join(output, `${name}.modes.log`);
    const command = implementation === 'go' ? args(pool) : [env.P07_PARITY_CONFIG, env.P07_PARITY_KEYRING, pool, String(count)];
    const report = JSON.parse(execute(name, 'taskset', ['-c', affinity, binaries[implementation], ...command], extra));
    assert.equal(hash(binaries[implementation]), pins[implementation]);
    return report;
  };
  try {
    host('host-before.json'); const warnings = health('health-before');
    for (const pool of ['test-3x', 'readcache']) {
      const probe = {size: 4096, concurrency: 1, workload: 'write', operations: 256};
      leg(`${pool}-go-mode`, 'go', pool, probe, 6, true); leg(`${pool}-native-mode`, 'native', pool, probe, 6, true);
      execute(`${pool}-mode-check`, checker, ['-go', path.join(output, `${pool}-go-mode.modes.json`), '-native-log', path.join(output, `${pool}-native-mode.modes.log`), '-requested', 'secure', '-native-out', path.join(output, `${pool}-native.modes.json`)]);
      for (const cell of cells) {
        const label = `${pool}-${cell.size}-c${cell.concurrency}-${cell.workload}`, before = placement(`${label}-before`, pool, cell);
        write(`${label}-placement-before.json`, before);
        for (const [index, implementation] of ['native', 'go', 'go', 'native'].entries()) {
          assert.deepEqual(health(`${label}-l${index}-health`), warnings);
          const name = `${label}-l${index}-${implementation}`, report = leg(name, implementation, pool, cell, windows);
          const capture = {name, pool, cell, implementation, ...retentionMetrics(report, implementation, cell, windows)};
          captures.push(capture); write(`${name}.analysis.json`, capture);
          console.log(`${name}: ${windows} verified same-client windows`);
        }
        const after = placement(`${label}-after`, pool, cell); write(`${label}-placement-after.json`, after); assert.deepEqual(after, before);
      }
    }
    assert.deepEqual(health('health-after'), warnings);
  } catch (error) { failures.push({error: error.message}); }
  host('host-after.json'); const afterSources = sourcePins(); write('sources-after.json', afterSources);
  try { assert.deepEqual(afterSources, beforeSources); assert.equal(hash(library), libraryPin); } catch (error) { failures.push({error: error.message}); }
  write('summary.json', {status: failures.length ? 'failed' : 'bounded_retention_captured', captures, findings: captures.filter(capture => !capture.rss.bounded_growth || !capture.allocated.bounded_growth), failures});
  if (failures.length) throw Error(failures[0].error);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  assert.equal(process.argv.length, 5, 'usage: node retention-parity.mjs FRESH_OUTPUT PREPARED_ROOT NAMESPACE');
  captureRetentionParity(path.resolve(process.argv[2]), path.resolve(process.argv[3]), process.argv[4]);
}