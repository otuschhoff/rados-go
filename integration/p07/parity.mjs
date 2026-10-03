import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';

export const parityCells = [
  {size: 1048576, concurrency: 1, workload: 'write', operations: 1024},
  {size: 1048576, concurrency: 1, workload: 'mixed', operations: 1024},
  {size: 4194304, concurrency: 1, workload: 'write', operations: 1024},
  {size: 4194304, concurrency: 16, workload: 'write', operations: 256},
  {size: 65536, concurrency: 16, workload: 'read', operations: 4096},
  {size: 4096, concurrency: 1, workload: 'read', operations: 4096}
];

export function fixtureName(cell, worker) {
  return `p07-parity-${cell.size}-c${cell.concurrency}-${cell.workload}-w${worker}`;
}

export function validateParityHealth(report, fsid) {
  assert.equal(report.fsid, fsid);
  assert(['HEALTH_OK', 'HEALTH_WARN'].includes(report.health.status));
  assert(report.pgmap.pgs_by_state?.length);
  for (const state of report.pgmap.pgs_by_state) {
    assert.equal(state.state_name, 'active+clean');
    assert(Number.isSafeInteger(state.count) && state.count > 0);
  }
  assert.equal(report.pgmap.pgs_by_state.reduce((total, state) => total + state.count, 0), report.pgmap.num_pgs);
  return Object.keys(report.health.checks ?? {}).sort();
}

export function validateParity(report, implementation, cell, expectedOperations = cell.operations * cell.concurrency) {
  assert.equal(report.implementation, implementation);
  assert.equal(report.transport, 'secure');
  assert.equal(report.rows.length, 1);
  const row = report.rows[0];
  assert.equal(row.size_bytes, cell.size);
  assert.equal(row.concurrency, cell.concurrency);
  assert.equal(row.workload, cell.workload);
  assert(Number.isSafeInteger(expectedOperations) && expectedOperations > 0, 'expected operation population');
  assert.equal(row.operations, expectedOperations);
  assert.equal(row.bytes, row.operations * row.size_bytes);
  for (const field of ['elapsed_ns', 'p50_ns', 'p95_ns', 'p99_ns', 'iops']) assert(Number.isFinite(row[field]) && row[field] > 0, field);
  assert(row.p50_ns <= row.p95_ns && row.p95_ns <= row.p99_ns);
  assert.equal(row.parity.payload_verified, true);
  assert.equal(row.parity.cleanup_verified, true);
  for (const field of ['rss_before_bytes', 'rss_after_bytes', 'rss_after_cleanup_bytes']) assert(row.parity[field] > 0, field);
  const cpu = row.parity.measured_resources.cpu_user_ns + row.parity.measured_resources.cpu_system_ns;
  assert(Number.isFinite(cpu) && cpu > 0);
  if (implementation === 'go') assert.equal(report.environment.gomaxprocs, 10);
  return {...row, cpu_ns_per_operation: cpu / row.operations};
}

export function validateQualificationLeg(report, implementation, cell, evidence) {
  assert(evidence && typeof evidence === 'object', 'missing qualification evidence');
  const populations = evidence.operations_per_worker ?? Array.from({length: cell.concurrency}, () => cell.operations);
  assert(Array.isArray(populations) && populations.length === cell.concurrency, 'declared worker populations');
  assert(populations.every(count => Number.isSafeInteger(count) && count > 0), 'positive worker populations');
  const row = validateParity(report, implementation, cell, populations.reduce((total, count) => total + count, 0));
  assert(Number.isSafeInteger(evidence.round) && evidence.round >= 1, 'round identity');
  assert(typeof evidence.leg === 'string' && evidence.leg.length > 0, 'leg identity');
  assert(Number.isSafeInteger(evidence.seed) && evidence.seed >= 0, 'round seed');
  assert(Number.isSafeInteger(row.elapsed_ns) && row.elapsed_ns >= 60000000000, 'measured time minimum');
  assert(row.operations >= 100000, 'measured count minimum');
  for (const field of ['cpu_user_ns', 'cpu_system_ns']) {
    assert(Number.isSafeInteger(row.parity.measured_resources[field]) && row.parity.measured_resources[field] >= 0, 'measured CPU delta');
  }
  assert(Number.isSafeInteger(row.parity.measured_resources.cpu_user_ns + row.parity.measured_resources.cpu_system_ns), 'measured CPU sum');
  const measuredIOPS = row.operations * 1000000000 / row.elapsed_ns;
  assert(Math.abs(row.iops - measuredIOPS) <= Math.max(0.000001, measuredIOPS * 1e-9), 'measured throughput agreement');
  assert(Number.isSafeInteger(evidence.warmup?.elapsed_ns) && evidence.warmup.elapsed_ns >= 10000000000, 'warmup time minimum');
  assert(Number.isSafeInteger(evidence.warmup?.successful_operations) && evidence.warmup.successful_operations >= 10000, 'warmup count minimum');
  assert.equal(evidence.warmup.unexpected_failures, 0, 'warmup failures');
  assert.equal(evidence.warmup.censored, 0, 'warmup censoring');
  assert(Array.isArray(evidence.records) && evidence.records.length === row.operations, 'raw operation population');
  const identities = new Set(), ordinals = Array.from({length: cell.concurrency}, () => 0), workerEnds = Array.from({length: cell.concurrency}, () => 0), latencies = [];
  for (const record of evidence.records) {
    assert.equal(record.round, evidence.round, 'operation round');
    assert.equal(record.leg, evidence.leg, 'operation leg');
    assert(typeof record.operation_id === 'string' && record.operation_id.length > 0 && !identities.has(record.operation_id), 'operation identity');
    identities.add(record.operation_id);
    assert(Number.isSafeInteger(record.worker) && record.worker >= 0 && record.worker < cell.concurrency, 'operation worker');
    assert.equal(record.ordinal, ordinals[record.worker]++, 'worker ordinal');
    assert.equal(record.object, fixtureName(cell, record.worker), 'operation fixture');
    assert.equal(record.type, cell.workload === 'mixed' ? (record.ordinal % 2 ? 'write' : 'read') : cell.workload, 'operation type');
    assert(Number.isSafeInteger(record.start_ns) && record.start_ns >= 0, 'operation start');
    assert(Number.isSafeInteger(record.end_ns) && record.end_ns > record.start_ns && record.end_ns <= row.elapsed_ns, 'operation end');
    assert(record.start_ns >= workerEnds[record.worker], 'closed-loop worker operations overlap');
    workerEnds[record.worker] = record.end_ns;
    assert.equal(record.success, true, 'unexpected operation failure');
    assert.equal(record.error, null, 'operation error');
    assert.equal(record.timeout, false, 'operation timeout');
    assert.equal(record.censored, false, 'operation censoring');
    assert(Number.isSafeInteger(record.retry_count) && record.retry_count >= 0, 'retry count must be observed');
    assert(record.timeout_deadline_ns === null || (Number.isSafeInteger(record.timeout_deadline_ns) && record.timeout_deadline_ns >= record.end_ns), 'declared timeout deadline');
    latencies.push(record.end_ns - record.start_ns);
  }
  assert(ordinals.every((count, worker) => count === populations[worker]), 'worker operation population');
  latencies.sort((left, right) => left - right);
  for (const [field, quantile] of [['p50_ns', .5], ['p95_ns', .95], ['p99_ns', .99]]) {
    assert.equal(row[field], latencies[Math.ceil(latencies.length * quantile) - 1], 'raw quantile agreement');
  }
  return {...row, ...validateQualificationMemory(evidence.memory, row.elapsed_ns), evidence_status: 'leg_validated_not_matrix_qualification'};
}

export function validateQualificationMemory(memory, elapsedNS) {
  assert(Number.isSafeInteger(elapsedNS) && elapsedNS > 0, 'RSS measured interval');
  assert.equal(memory?.clock, 'measurement_relative_ns', 'RSS clock');
  assert.equal(memory.idle?.connected, true, 'connected idle baseline');
  assert.equal(memory.idle?.equally_warmed, true, 'equally warmed idle baseline');
  assert(Number.isSafeInteger(memory.interval_ns) && memory.interval_ns > 0 && memory.interval_ns <= 1000000000, 'predeclared RSS sampling interval at most one second');
  assert(Number.isSafeInteger(memory.idle?.at_ns) && memory.idle.at_ns <= 0 && memory.idle.at_ns >= -memory.interval_ns, 'immediately preceding idle baseline');
  assert(Number.isSafeInteger(memory.idle?.rss_bytes) && memory.idle.rss_bytes > 0, 'idle RSS');
  assert(Array.isArray(memory.samples) && memory.samples.length >= 2, 'interval RSS samples');
  let previous = -Infinity, peak = 0;
  for (const sample of memory.samples) {
    assert(Number.isSafeInteger(sample.at_ns) && sample.at_ns > previous, 'ordered RSS samples');
    assert(previous === -Infinity || sample.at_ns - previous <= memory.interval_ns * 2, 'RSS sampling gap');
    assert(Number.isSafeInteger(sample.rss_bytes) && sample.rss_bytes > 0, 'sampled RSS');
    if (sample.at_ns >= 0 && sample.at_ns <= elapsedNS) peak = Math.max(peak, sample.rss_bytes);
    previous = sample.at_ns;
  }
  assert(memory.samples[0].at_ns >= -memory.interval_ns && memory.samples[0].at_ns <= 0, 'RSS start coverage');
  assert(previous >= elapsedNS && previous <= elapsedNS + memory.interval_ns, 'RSS end coverage');
  assert(peak > 0, 'measured RSS peak');
  return {rss_idle_bytes: memory.idle.rss_bytes, rss_interval_peak_bytes: peak,
    rss_incremental_peak_bytes: Math.max(0, peak - memory.idle.rss_bytes)};
}

export function runParity(root, namespace, repetitions = 6) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(namespace) && namespace.length <= 64);
  assert(Number.isInteger(repetitions) && repetitions >= 1 && repetitions <= 30);
  const repository = process.cwd();
  const env = process.env;
  for (const name of ['P07_PARITY_CONFIG', 'P07_PARITY_KEY', 'P07_PARITY_KEYRING', 'P07_PARITY_MONITORS_FILE', 'P07_PARITY_FSID_FILE']) assert(env[name], `missing ${name}`);
  fs.mkdirSync(root, {mode: 0o700});
  const write = (name, value) => fs.writeFileSync(path.join(root, name), JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});
  const digest = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const runtime = {GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'};
  const environment = Object.fromEntries(['PATH', 'HOME', 'GOTOOLCHAIN', 'GOCACHE', 'TMPDIR'].filter(name => env[name]).map(name => [name, env[name]]));
  const commands = [], blocks = [], failures = [];
  const execute = (name, executable, args, extra = {}) => {
    const result = spawnSync(executable, args, {env: {...environment, ...runtime, ...extra}, encoding: 'utf8', timeout: 600000, maxBuffer: 32 << 20});
    for (const stream of ['stdout', 'stderr']) fs.writeFileSync(path.join(root, `${name}.${stream}`), result[stream] ?? '', {flag: 'wx', mode: 0o600});
    const entry = {name, executable, args: args.map(arg => [env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, env.P07_PARITY_KEYRING].includes(arg) ? '<private-path>' : arg), exit_status: result.status, signal: result.signal, error: result.error?.message};
    commands.push(entry); write(`${name}.exit.json`, entry);
    assert.equal(result.status, 0, `${name}: ${result.error?.message ?? result.stderr}`);
    return result.stdout;
  };
  const cpuLine = fs.readFileSync('/proc/self/status', 'utf8').split('\n').find(line => line.startsWith('Cpus_allowed_list:'));
  const available = cpuLine.split(':')[1].trim().split(',').flatMap(range => {
    const [start, end = start] = range.split('-').map(Number);
    return Array.from({length: end - start + 1}, (_, index) => start + index);
  });
  assert(available.length >= 10);
  const affinity = available.slice(0, 10).join(',');
  const binary = {go: path.join(root, 'go-benchmark'), native: path.join(root, 'native-benchmark')};
  const checker = path.join(root, 'mode-check');
  const monitors = fs.readFileSync(env.P07_PARITY_MONITORS_FILE, 'utf8').trim().split(/\s+/).map(host => `${host}:3300`).join(',');
  const fsid = fs.readFileSync(env.P07_PARITY_FSID_FILE, 'utf8').trim();
  const sourceFiles = [...new Set(execute('source-files', 'git', ['ls-files', '-co', '--exclude-standard']).trim().split('\n'))].filter(file => /(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sourcePins = () => sourceFiles.map(file => ({file, sha256: digest(path.join(repository, file))}));
  const beforeSources = sourcePins(); write('sources-before.json', beforeSources);
  execute('build-go', 'go', ['build', '-o', binary.go, './integration/p07/benchmark']);
  execute('build-native', 'gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', 'integration/p07/native_benchmark.c', '-ldl', '-o', binary.native]);
  execute('build-mode-check', 'go', ['build', '-o', checker, './tools/perf-mode-check']);
  const binaryPins = Object.fromEntries(Object.entries(binary).map(([name, file]) => [name, digest(file)]));
  write('binaries.json', binaryPins);
  const library = fs.realpathSync('/lib64/librados.so.2');
  const libraryPin = digest(library);
  write('native-library.json', {path: library, sha256: libraryPin, release: execute('ceph-version', 'ceph', ['--version']).trim()});
  const ceph = (name, args) => JSON.parse(execute(name, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, ...args, '--format', 'json']));
  const health = report => validateParityHealth(report, fsid);
  const host = name => write(name, Object.fromEntries(['cpu.stat', 'cpu.max', 'cpuset.cpus.effective', 'memory.events'].map(file => [file, fs.readFileSync(`/sys/fs/cgroup/${file}`, 'utf8')])));
  const mapping = (name, pool, cell) => Array.from({length: cell.concurrency}, (_, worker) => {
    const object = fixtureName(cell, worker), report = ceph(`${name}-w${worker}`, ['osd', 'map', pool, object, namespace]);
    assert(report.pgid && report.acting?.length && Number.isInteger(report.acting_primary));
    return {object, pg: report.pgid, acting: report.acting, primary: report.acting_primary};
  });
  const leg = (name, implementation, pool, cell, probe = false) => {
    const extra = {P07_PARITY_NAMESPACE: namespace, P07_MATRIX_SIZE: String(cell.size), P07_MATRIX_CONCURRENCY: String(cell.concurrency), P07_MATRIX_WORKLOAD: cell.workload, P07_MATRIX_OPERATIONS_PER_WORKER: String(cell.operations)};
    const args = implementation === 'go' ? ['-monitors', monitors, '-fsid', fsid, '-key-file', env.P07_PARITY_KEY, '-entity', 'client.amakura', '-pool', pool, '-transport', 'secure'] : [env.P07_PARITY_CONFIG, env.P07_PARITY_KEYRING, pool, 'secure', 'client.amakura'];
    if (implementation === 'go') { extra.P07_READ_INTO = '1'; if (probe) extra.P07_MODE_EVIDENCE_FILE = path.join(root, `${name}.modes.json`); }
    else if (probe) extra.P07_NATIVE_MODE_LOG = path.join(root, `${name}.modes.log`);
    const report = JSON.parse(execute(name, 'taskset', ['-c', affinity, binary[implementation], ...args], extra));
    assert.equal(digest(binary[implementation]), binaryPins[implementation]);
    return {name, implementation, ...validateParity(report, implementation, cell)};
  };
  write('methodology.json', {namespace, runtime, affinity, repetitions, cells: parityCells, pools: ['test-3x', 'readcache'], mode_probes_excluded: true, read_api: 'ReadInto/rados_read', status: 'diagnostic; no native parity acceptance claim', measurement: 'CPU barrier includes timed calls and read validation, excludes setup/warmup/final verification/cleanup; RSS at warm/timed-end/cleanup boundaries; Go heap explicitly post-GC, native retained heap not established', stopping: 'fixed sample; stop on failed correctness, changed placement or new health-warning category'});
  try {
    host('host-before.json');
    const warnings = health(ceph('health-before', ['status']));
    for (const pool of ['test-3x', 'readcache']) {
      const probe = {size: 4096, concurrency: 1, workload: 'write', operations: 256};
      leg(`${pool}-mode-go`, 'go', pool, probe, true);
      leg(`${pool}-mode-native`, 'native', pool, probe, true);
      execute(`${pool}-mode-check`, checker, ['-go', path.join(root, `${pool}-mode-go.modes.json`), '-native-log', path.join(root, `${pool}-mode-native.modes.log`), '-requested', 'secure', '-native-out', path.join(root, `${pool}-native.modes.json`)]);
      for (const cell of parityCells) {
        const label = `${pool}-${cell.size}-c${cell.concurrency}-${cell.workload}`;
        const before = mapping(`${label}-before`, pool, cell); write(`${label}-placement-before.json`, before);
        for (let repetition = 1; repetition <= repetitions; repetition++) {
          assert.deepEqual(health(ceph(`${label}-r${repetition}-health`, ['status'])), warnings);
          const order = repetition % 2 ? ['native', 'go', 'go', 'native'] : ['go', 'native', 'native', 'go'];
          const legs = order.map((implementation, index) => leg(`${label}-r${repetition}-l${index + 1}-${implementation}`, implementation, pool, cell));
          const block = {pool, cell, repetition, order, legs}; blocks.push(block); write(`${label}-r${repetition}.block.json`, block);
          console.log(`${label} block ${repetition}/${repetitions} verified`);
        }
        const after = mapping(`${label}-after`, pool, cell); write(`${label}-placement-after.json`, after); assert.deepEqual(after, before);
      }
    }
    assert.deepEqual(health(ceph('health-after', ['status'])), warnings);
  } catch (error) { failures.push({error: error.message}); }
  host('host-after.json');
  const afterSources = sourcePins(); write('sources-after.json', afterSources);
  try { assert.deepEqual(beforeSources, afterSources); } catch (error) { failures.push({error: error.message}); }
  try { assert.equal(digest(library), libraryPin); } catch (error) { failures.push({error: error.message}); }
  write('summary.json', {status: failures.length ? 'failed' : 'diagnostic_capture_passed', blocks, commands, failures});
  if (failures.length) throw Error(failures[0].error);
}

export function runQualificationSmoke(root, namespace) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(namespace) && namespace.length <= 64);
  const env = process.env;
  for (const name of ['P07_PARITY_CONFIG', 'P07_PARITY_KEY', 'P07_PARITY_KEYRING', 'P07_PARITY_MONITORS_FILE', 'P07_PARITY_FSID_FILE']) assert(env[name], `missing ${name}`);
  fs.mkdirSync(root, {mode: 0o700});
  const write = (name, value) => fs.writeFileSync(path.join(root, name), JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});
  const digest = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const runtime = {CGO_ENABLED: '0', GOTOOLCHAIN: 'go1.27.1', GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'};
  const environment = Object.fromEntries(['PATH', 'HOME', 'GOCACHE', 'TMPDIR'].filter(name => env[name]).map(name => [name, env[name]]));
  const execute = (name, executable, args, extra = {}, expectedStatus = 0) => {
    const result = spawnSync(executable, args, {env: {...environment, ...runtime, ...extra}, encoding: 'utf8', timeout: 960000, maxBuffer: 32 << 20});
    for (const stream of ['stdout', 'stderr']) fs.writeFileSync(path.join(root, `${name}.${stream}`), result[stream] ?? '', {flag: 'wx', mode: 0o600});
    write(`${name}.exit.json`, {executable, args: args.map(arg => [env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, env.P07_PARITY_KEYRING].includes(arg) ? '<private-path>' : arg), exit_status: result.status, signal: result.signal, error: result.error?.message});
    assert.equal(result.status, expectedStatus, `${name} unexpected exit; private output retained`);
    return result.stdout;
  };
  const cell = {size: 4096, concurrency: 1, workload: 'read'};
  const fsid = fs.readFileSync(env.P07_PARITY_FSID_FILE, 'utf8').trim();
  const monitors = fs.readFileSync(env.P07_PARITY_MONITORS_FILE, 'utf8').trim().split(/\s+/).map(host => `${host}:3300`).join(',');
  const ceph = (name, args) => JSON.parse(execute(name, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, ...args, '--format', 'json']));
  const sourceFiles = [...new Set(execute('source-files', 'git', ['ls-files', '-co', '--exclude-standard']).trim().split('\n'))].filter(file => /(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sourcePins = () => sourceFiles.map(file => ({file, sha256: digest(file)}));
  const sources = sourcePins(); write('sources-before.json', sources);
  const binaries = {go: path.join(root, 'go-benchmark'), native: path.join(root, 'native-qualification'), checker: path.join(root, 'mode-check')};
  const attempts = [], failures = [];
  write('methodology.json', {status: 'fixed Go/native integration smoke, not ABBA or qualification', pool: 'readcache', namespace, cell, runtime, affinity: '0-9', order: ['go', 'native'],
    round: 1, seed: 42, warmup_minimum_seconds: 10, warmup_minimum_operations: 10000, measured_minimum_seconds: 60, measured_minimum_operations: 100000,
    operation_timeout_seconds: 30, safety_deadline_seconds: 900, maximum_records_per_phase: 1000000,
    rss: '100ms interval samples and immediately preceding connected/warmed baseline; harness retention included', retries: 'unknown, not zero', observation: 'health/placement at leg boundaries only', stopping: 'fixed two legs; retain failed attempts; do not retry for a pass'});
  try {
    execute('build-go', 'go', ['build', '-o', binaries.go, './integration/p07/benchmark']);
    execute('build-native', 'gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', 'integration/p07/native_qualification.c', '-ldl', '-o', binaries.native]);
    execute('build-checker', 'go', ['build', '-o', binaries.checker, './tools/perf-mode-check']);
    const binaryPins = Object.fromEntries(Object.entries(binaries).map(([name, file]) => [name, digest(file)])); write('binaries.json', binaryPins);
    const library = fs.realpathSync('/lib64/librados.so.2'), libraryPin = digest(library);
    write('native-library.json', {path: library, sha256: libraryPin, release: execute('native-version', 'ceph', ['--version']).trim()});
    const warnings = validateParityHealth(ceph('health-before', ['status']), fsid);
    const placement = ceph('placement-before', ['osd', 'map', 'readcache', fixtureName(cell, 0), namespace]);
    assert(placement.pgid && placement.acting?.length && Number.isInteger(placement.acting_primary));
    for (const implementation of ['go', 'native']) {
      assert.deepEqual(validateParityHealth(ceph(`${implementation}-health-before`, ['status']), fsid), warnings);
      const name = `r1-${implementation}`, file = path.join(root, `${name}.attempt.json`);
      const extra = {P07_PARITY_NAMESPACE: namespace, P07_MATRIX_SIZE: '4096', P07_MATRIX_CONCURRENCY: '1', P07_MATRIX_WORKLOAD: 'read',
        P07_QUALIFICATION_FILE: file, P07_QUALIFICATION_ROUND: '1', P07_QUALIFICATION_SEED: '42', P07_QUALIFICATION_LEG: name};
      const args = implementation === 'go' ? ['-monitors', monitors, '-fsid', fsid, '-key-file', env.P07_PARITY_KEY, '-entity', 'client.amakura', '-pool', 'readcache', '-transport', 'secure'] : [env.P07_PARITY_CONFIG, env.P07_PARITY_KEYRING, 'readcache', 'secure', 'client.amakura'];
      if (implementation === 'go') { extra.P07_READ_INTO = '1'; extra.P07_MODE_EVIDENCE_FILE = path.join(root, 'go.modes.json'); }
      else extra.P07_NATIVE_MODE_LOG = path.join(root, 'native.modes.log');
      const fixtureArgs = ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, '-p', 'readcache', '-N', namespace];
      const collisionPayload = path.join(root, `${implementation}-collision-payload.bin`);
      fs.writeFileSync(collisionPayload, Buffer.alloc(4096, 0xa7), {flag: 'wx', mode: 0o600});
      execute(`${implementation}-collision-seed`, 'rados', [...fixtureArgs, 'put', fixtureName(cell, 0), collisionPayload]);
      try {
        const collision = {...extra, P07_QUALIFICATION_FILE: path.join(root, `${implementation}-collision.attempt.json`), P07_QUALIFICATION_LEG: `${implementation}-collision`};
        if (implementation === 'go') collision.P07_MODE_EVIDENCE_FILE = path.join(root, 'go-collision.modes.json');
        else collision.P07_NATIVE_MODE_LOG = path.join(root, 'native-collision.modes.log');
        execute(`${implementation}-collision`, 'taskset', ['-c', '0-9', binaries[implementation], ...args], collision, 1);
        const rejected = JSON.parse(fs.readFileSync(collision.P07_QUALIFICATION_FILE, 'utf8'));
        assert.equal(rejected.status, 'failed'); assert(rejected.error);
        assert.equal(rejected.attempt.payload_verified, false); assert.equal(rejected.attempt.cleanup_verified, true);
        assert.equal(rejected.attempt.measured.records?.length ?? 0, 0);
        const retained = path.join(root, `${implementation}-collision-retained.bin`);
        execute(`${implementation}-collision-get`, 'rados', [...fixtureArgs, 'get', fixtureName(cell, 0), retained]);
        fs.chmodSync(retained, 0o600);
        assert.deepEqual(fs.readFileSync(retained), fs.readFileSync(collisionPayload), 'pre-existing fixture must not be overwritten or removed');
      } finally {
        execute(`${implementation}-collision-cleanup`, 'rados', [...fixtureArgs, 'rm', fixtureName(cell, 0)]);
        execute(`${implementation}-collision-notfound`, 'rados', [...fixtureArgs, 'stat', fixtureName(cell, 0)], {}, 1);
        assert.match(fs.readFileSync(path.join(root, `${implementation}-collision-notfound.stderr`), 'utf8'), /\(2\) No such file or directory/);
      }
      execute(name, 'taskset', ['-c', '0-9', binaries[implementation], ...args], extra);
      assert.equal(fs.statSync(file).mode & 0o777, 0o600);
      const capture = JSON.parse(fs.readFileSync(file, 'utf8'));
      assert.equal(capture.status, `sustained_${implementation}_capture_unqualified`);
      assert.equal(capture.error, null);
      assert.deepEqual(capture.identity, {round: 1, leg: name, seed: 42});
      assert.equal(capture.attempt.payload_verified, true); assert.equal(capture.attempt.cleanup_verified, true);
      for (const [phase, seconds, count] of [['warmup', 10, 10000], ['measured', 60, 100000]]) {
        const value = capture.attempt[phase];
        assert(value.elapsed_ns >= seconds * 1000000000 && value.successful_operations >= count);
        assert.equal(value.unexpected_failures, 0); assert.equal(value.censored, 0);
        assert.equal(value.records.length, value.successful_operations);
        assert.equal(value.operations_per_worker.length, 1); assert.equal(value.operations_per_worker[0], value.records.length);
        const identities = new Set(); let preceding = 0;
        for (const [ordinal, record] of value.records.entries()) {
          assert.equal(record.round, 1); assert.equal(record.leg, name); assert.equal(record.ordinal, ordinal); assert.equal(record.worker, 0);
          assert.equal(record.object, fixtureName(cell, 0)); assert.equal(record.type, 'read');
          assert(!identities.has(record.operation_id)); identities.add(record.operation_id);
          assert(record.start_ns >= preceding && record.end_ns > record.start_ns && record.end_ns <= value.elapsed_ns); preceding = record.end_ns;
          assert.equal(record.success, true); assert.equal(record.error, null); assert.equal(record.timeout, false); assert.equal(record.censored, false);
          assert.equal(record.retry_count, null); assert(record.timeout_deadline_ns >= record.end_ns);
          assert(record.timeout_deadline_ns - record.start_ns <= 30000000000);
        }
      }
      const measured = capture.attempt.measured;
      const memory = validateQualificationMemory(capture.attempt.memory, measured.elapsed_ns);
      validateParity(capture.report, implementation, cell, measured.successful_operations);
      assert.throws(() => validateQualificationLeg(capture.report, implementation, cell, {...capture.identity, warmup: capture.attempt.warmup, records: measured.records, operations_per_worker: measured.operations_per_worker}), /retry count must be observed/);
      attempts.push({implementation, file: path.basename(file), warmup_operations: capture.attempt.warmup.successful_operations, measured_operations: measured.successful_operations, elapsed_ns: measured.elapsed_ns, rss_samples: capture.attempt.memory.samples.length, memory, collision_rejected_without_mutation: true, qualification_rejected: 'retry count must be observed'});
      assert.deepEqual(validateParityHealth(ceph(`${implementation}-health-after`, ['status']), fsid), warnings);
      const after = ceph(`${implementation}-placement-after`, ['osd', 'map', 'readcache', fixtureName(cell, 0), namespace]);
      for (const key of ['pgid', 'acting', 'acting_primary']) assert.deepEqual(after[key], placement[key]);
      assert.equal(digest(binaries[implementation]), binaryPins[implementation]);
    }
    execute('mode-check', binaries.checker, ['-go', path.join(root, 'go.modes.json'), '-native-log', path.join(root, 'native.modes.log'), '-requested', 'secure', '-native-out', path.join(root, 'native.modes.json')]);
    assert.equal(digest(library), libraryPin);
    assert.deepEqual(validateParityHealth(ceph('health-after', ['status']), fsid), warnings);
  } catch (error) { failures.push({error: error.message}); }
  const after = sourcePins(); write('sources-after.json', after);
  try { assert.deepEqual(after, sources); } catch (error) { failures.push({error: error.message}); }
  write('summary.json', {status: failures.length ? 'failed' : 'integration_smoke_passed_not_qualification', attempts, failures});
  if (failures.length) throw Error(failures[0].error);
  console.log(JSON.stringify({root, status: 'integration_smoke_passed_not_qualification', attempts}));
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  if (process.argv.length === 5 && process.argv[4] === '--qualification-smoke') runQualificationSmoke(path.resolve(process.argv[2]), process.argv[3]);
  else {
  assert.equal(process.argv.length, 4, 'usage: node parity.mjs FRESH_PRIVATE_OUTPUT NAMESPACE [--qualification-smoke]');
  runParity(path.resolve(process.argv[2]), process.argv[3]);
  }
}