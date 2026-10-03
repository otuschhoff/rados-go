import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {validateParityHealth} from './parity.mjs';

export function offeredParityMetrics(report, implementation, rate, budgetNS = 10000000) {
  assert.equal(report.implementation, implementation); assert.equal(report.transport, 'secure');
  const result = report.offered_load, config = result.config ?? result;
  assert.equal(config.rate_ops_per_second ?? config.rate, rate);
  assert.equal(config.issuance_window_ns ?? config.window_ns, 8000000000);
  assert.equal(config.deadline_ns, 500000000);
  assert.equal(config.workers, 16); assert.equal(config.queue_capacity, 128);
  if (implementation === 'go') {
    assert.equal(report.environment.gomaxprocs, 10);
    assert.equal(config.gomaxprocs, 10); assert.equal(config.gogc, 100); assert.equal(config.gomemlimit, 'off');
    assert.equal(config.load_case, 'none'); assert.equal(config.cpu_workers, 0); assert.equal(config.allocation_workers, 0);
    assert.equal(report.diagnostic.read_api, 'read_into');
  }
  assert.equal(result.expected, rate * 8); assert.equal(result.outcomes.length, result.expected);
  const counts = {success: 0, timeout: 0, overload: 0, error: 0, canceled: 0};
  const latency = [];
  let admitted = 0, attempted = 0, withinBudget = 0, maxDelivery = 0;
  for (const [index, outcome] of result.outcomes.entries()) {
    assert.equal(outcome.scheduled_ns, Math.floor(index * 1000000000 / rate));
    assert(Object.hasOwn(counts, outcome.kind)); counts[outcome.kind]++;
    assert(Number.isFinite(outcome.returned_ns) && outcome.returned_ns >= outcome.scheduled_ns);
    const elapsed = outcome.returned_ns - outcome.scheduled_ns;
    latency.push(elapsed);
    admitted += Number(outcome.admitted); attempted += Number(outcome.attempted);
    if (outcome.attempted) assert(outcome.admitted && outcome.read_start_ns >= outcome.worker_start_ns);
    if (!outcome.admitted) assert.equal(outcome.enqueued_ns, -1);
    const delivery = outcome.delivery_delay_ns;
    assert(Number.isFinite(delivery) && delivery >= 0); maxDelivery = Math.max(maxDelivery, delivery);
    if (outcome.kind === 'success' && elapsed <= budgetNS) withinBudget++;
  }
  for (const [kind, field] of Object.entries({success: 'success', timeout: 'timeouts', overload: 'overload', error: 'errors', canceled: 'canceled'})) assert.equal(counts[kind], result[field]);
  assert.equal(admitted, result.admitted); assert.equal(attempted, result.attempted);
  assert.equal(counts.error, 0, 'payload or API error'); assert.equal(counts.canceled, 0, 'setup/cancellation failure');
  latency.sort((first, second) => first - second);
  const p99 = latency[Math.ceil(latency.length * .99) - 1];
  assert.equal(result.all_outcome_p99_ns, p99);
  assert.equal(result.delivery_invalid, maxDelivery > 50000000);
  assert.equal(result.success_ops_per_issuance_second, counts.success / 8);
  const valid = !result.delivery_invalid;
  return {rate, budget_ns: budgetNS, valid, expected: result.expected, ...counts, all_outcome_p99_ns: p99, delivered_iops: counts.success / 8, within_budget_iops: withinBudget / 8, within_budget_mib_per_second: withinBudget / 8 / 16, max_delivery_lag_ns: maxDelivery, resources: report.resources};
}

export function captureOfferedParity(output, prepared, namespace) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(namespace));
  fs.mkdirSync(output, {mode: 0o700});
  const env = process.env;
  for (const key of ['P07_PARITY_CONFIG', 'P07_PARITY_KEY', 'P07_PARITY_MONITORS_FILE', 'P07_PARITY_FSID_FILE']) assert(env[key], key);
  const write = (name, value) => fs.writeFileSync(path.join(output, name), JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});
  const hash = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const method = JSON.parse(fs.readFileSync(path.join(prepared, 'methodology.json')));
  const affinity = method.affinity;
  const binaries = {go: path.join(output, 'go-benchmark'), native: path.join(output, 'native-offered')};
  const modeChecker = path.join(output, 'mode-check');
  const runtime = {GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'};
  const environment = Object.fromEntries(['PATH', 'HOME', 'GOTOOLCHAIN', 'GOCACHE', 'TMPDIR'].filter(key => env[key]).map(key => [key, env[key]]));
  const execute = (name, command, args, extra = {}, allowSLOFailure = false) => {
    const result = spawnSync(command, args, {env: {...environment, ...runtime, ...extra}, encoding: 'utf8', timeout: 600000, maxBuffer: 256 << 20});
    for (const stream of ['stdout', 'stderr']) fs.writeFileSync(path.join(output, `${name}.${stream}`), result[stream] ?? '', {flag: 'wx', mode: 0o600});
    write(`${name}.exit.json`, {command, args: args.map(value => [env.P07_PARITY_KEY, env.P07_PARITY_CONFIG].includes(value) ? '<private-path>' : value), status: result.status, signal: result.signal, error: result.error?.message});
    assert(result.status === 0 || allowSLOFailure && result.status === 1, `${name} failed: ${result.stderr}`);
    return result.stdout;
  };
  const sourceFiles = [...new Set(execute('source-files', 'git', ['ls-files', '-co', '--exclude-standard']).trim().split('\n'))].filter(file => /(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sourcePins = () => sourceFiles.map(file => ({file, sha256: hash(file)}));
  const beforeSources = sourcePins(); write('sources-before.json', beforeSources);
  execute('build-go', 'go', ['build', '-o', binaries.go, './integration/p07/benchmark']);
  execute('build-native-offered', 'gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', 'integration/p07/native_offered.c', '-ldl', '-o', binaries.native]);
  execute('build-mode-check', 'go', ['build', '-o', modeChecker, './tools/perf-mode-check']);
  const pins = Object.fromEntries(Object.entries(binaries).map(([name, file]) => [name, hash(file)])); write('binaries.json', pins);
  const monitors = fs.readFileSync(env.P07_PARITY_MONITORS_FILE, 'utf8').trim().split(/\s+/).map(host => `${host}:3300`).join(',');
  const fsid = fs.readFileSync(env.P07_PARITY_FSID_FILE, 'utf8').trim();
  const args = pool => ['-monitors', monitors, '-fsid', fsid, '-key-file', env.P07_PARITY_KEY, '-entity', 'client.amakura', '-pool', pool, '-transport', 'secure'];
  const health = name => {
    const report = JSON.parse(execute(name, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, 'status', '--format', 'json']));
    return validateParityHealth(report, fsid);
  };
  const placement = (name, pool) => Array.from({length: 16}, (_, worker) => {
    const report = JSON.parse(execute(`${name}-w${worker}`, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, 'osd', 'map', pool, `p07-shared-read-${worker}`, namespace, '--format', 'json']));
    assert(report.pgid && report.acting?.length && Number.isInteger(report.acting_primary));
    return {pg: report.pgid, acting: report.acting, primary: report.acting_primary};
  });
  const rates = [1000, 2000, 4000, 8000, 16000, 32000, 64000];
  write('methodology.json', {namespace, affinity, runtime, rates, workers: 16, queue: 128, window_ns: 8000000000, deadline_ns: 500000000, latency_budget_ns: 10000000, blocks_per_cell: 6, pools: ['test-3x', 'readcache'], matched_background: 'none', interpretation: 'fixed diagnostic offered-load sweep; overload/timeouts retained as SLO failures rather than discarded; delivery lag >50ms makes comparisons invalid; native cancellation completion drain is explicit; no parity qualification claim'});
  const blocks = [], failures = [], seededPools = [];
  const library = fs.realpathSync('/lib64/librados.so.2'), libraryPin = hash(library);
  write('library.json', {path: library, sha256: libraryPin});
  const host = name => write(name, Object.fromEntries(['cpu.stat', 'cpu.max', 'cpuset.cpus.effective', 'memory.events'].map(file => [file, fs.readFileSync(`/sys/fs/cgroup/${file}`, 'utf8')])));
  try {
    host('host-before.json');
    const warnings = health('health-before');
    for (const pool of ['test-3x', 'readcache']) {
      const before = placement(`${pool}-before`, pool); write(`${pool}-placement-before.json`, before);
      seededPools.push(pool);
      execute(`${pool}-seed`, 'taskset', ['-c', affinity, binaries.go, ...args(pool)], {P07_PARITY_NAMESPACE: namespace, P07_SEED_ONLY: '1', P07_READ_SIZE: '65536', P07_READ_CONCURRENCY: '16'});
      execute(`${pool}-go-mode-probe`, 'taskset', ['-c', affinity, binaries.go, ...args(pool)], {P07_PARITY_NAMESPACE: namespace, P07_MATRIX_SIZE: '4096', P07_MATRIX_CONCURRENCY: '1', P07_MATRIX_WORKLOAD: 'write', P07_MATRIX_OPERATIONS_PER_WORKER: '256', P07_MODE_EVIDENCE_FILE: path.join(output, `${pool}-go.modes.json`)});
      const probe = JSON.parse(execute(`${pool}-native-mode-probe`, 'taskset', ['-c', affinity, binaries.native, env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, pool, '1000'], {P07_PARITY_NAMESPACE: namespace, P07_NATIVE_MODE_LOG: path.join(output, `${pool}-native.modes.log`)}));
      offeredParityMetrics(probe, 'native', 1000);
      execute(`${pool}-mode-check`, modeChecker, ['-go', path.join(output, `${pool}-go.modes.json`), '-native-log', path.join(output, `${pool}-native.modes.log`), '-requested', 'secure', '-native-out', path.join(output, `${pool}-native.modes.json`)]);
      for (const rate of rates) for (let repetition = 1; repetition <= 6; repetition++) {
        const label = `${pool}-${rate}-r${repetition}`;
        assert.deepEqual(health(`${label}-health`), warnings);
        const order = repetition % 2 ? ['native', 'go', 'go', 'native'] : ['go', 'native', 'native', 'go'];
        const legs = order.map((implementation, index) => {
          const name = `${label}-l${index + 1}-${implementation}`;
          const extra = implementation === 'go' ? {P07_PARITY_NAMESPACE: namespace, P07_READ_DIAGNOSTIC: '1', P07_READ_INTO: '1', P07_BACKGROUND_WORKERS: '8', P07_OFFERED_LOAD: '1', P07_OFFERED_FACTORIAL: '1', P07_OFFERED_CASE: 'none', P07_OFFERED_RATE: String(rate)} : {P07_PARITY_NAMESPACE: namespace};
          const commandArgs = implementation === 'go' ? args(pool) : [env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, pool, String(rate)];
          const report = JSON.parse(execute(name, 'taskset', ['-c', affinity, binaries[implementation], ...commandArgs], extra, true));
          const metrics = offeredParityMetrics(report, implementation, rate);
          assert.equal(hash(binaries[implementation]), pins[implementation]);
          return {name, implementation, ...metrics};
        });
        const block = {pool, rate, repetition, order, legs}; blocks.push(block); write(`${label}.block.json`, block);
        console.log(`${label}: all offered outcomes accounted`);
      }
      const after = placement(`${pool}-after`, pool); write(`${pool}-placement-after.json`, after); assert.deepEqual(after, before);
    }
    assert.deepEqual(health('health-after'), warnings);
  } catch (error) { failures.push({error: error.message}); }
  for (const pool of seededPools) {
    try {
      const cleanup = JSON.parse(execute(`${pool}-cleanup`, 'taskset', ['-c', affinity, binaries.native, env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, pool, '1000'], {P07_PARITY_NAMESPACE: namespace, P07_PARITY_CLEANUP: '1'}));
      assert.equal(cleanup.payload_verified, true); assert.equal(cleanup.cleanup_verified, true); assert.equal(cleanup.owned_objects, 16);
    } catch (error) { failures.push({error: `${pool} cleanup: ${error.message}`}); }
  }
  host('host-after.json');
  const afterSources = sourcePins(); write('sources-after.json', afterSources);
  try { assert.deepEqual(beforeSources, afterSources); assert.equal(hash(library), libraryPin); } catch (error) { failures.push({error: error.message}); }
  write('summary.json', {status: failures.length ? 'failed' : 'diagnostic_outcomes_captured', blocks, failures});
  if (failures.length) throw Error(failures[0].error);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  assert.equal(process.argv.length, 5, 'usage: node offered-parity.mjs FRESH_OUTPUT PREPARED_ROOT NAMESPACE');
  captureOfferedParity(path.resolve(process.argv[2]), path.resolve(process.argv[3]), process.argv[4]);
}