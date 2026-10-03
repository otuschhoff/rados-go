import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {fixtureName, validateParity, validateParityHealth, validateQualificationRecords, validateQualificationMemory} from './parity.mjs';
import {createQualificationPlan, qualificationOrder, analyzeQualificationMetrics} from './qualification.mjs';

const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const digest = file => hash(fs.readFileSync(file));
const write = (root, name, value) => fs.writeFileSync(path.join(root, name), JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});

export function pgoExperimentPlan() {
  const training = [
    {id: 'small-read', pool: 'readcache', size: 4096, concurrency: 1, workload: 'read'},
    {id: 'concurrent-read', pool: 'readcache', size: 65536, concurrency: 16, workload: 'read'},
    {id: 'replicated-write', pool: 'test-3x', size: 1048576, concurrency: 1, workload: 'write'},
    {id: 'replicated-mixed', pool: 'test-3x', size: 1048576, concurrency: 16, workload: 'mixed'},
    {id: 'large-write', pool: 'readcache', size: 4194304, concurrency: 16, workload: 'write'},
    {id: 'large-read', pool: 'readcache', size: 4194304, concurrency: 1, workload: 'read'}
  ];
  const heldout = [training[0], training[1], {...training[3], id: 'mixed-heldout', concurrency: 1}];
  const statistics = createQualificationPlan({cells: heldout, rounds: 5, seed: 901, bootstrapSeed: 902});
  const body = {version: 1, consumer: './integration/p07/benchmark', toolchain: 'go1.27.1', training, heldout, statistics,
    runtime: statistics.runtime, phases: {warmup_seconds: 1, warmup_operations: 1000, measured_seconds: 8, measured_operations: 10000},
    profile_composition: 'one measured CPU profile per declared training cell; additive original CPU samples, no tuning or normalization',
    training_scope: 'measured window and sampler stop; warmup, setup, final verification, cleanup and record assembly excluded',
    heldout_order: 'seeded Go off/PGO ABBA; unchanged native comparator brackets each round',
    conditioning: 'fresh process and fresh fixture namespace per training/probe/heldout round',
    profile_usage: 'explicit -pgo=off baseline and -pgo=PINNED_PROFILE candidate; no default.pgo',
    probes: 'one excluded small-read correctness capture per Go build after profile freeze; no tuning',
    limitations: ['short diagnostic windows are not qualification', 'whole-process CPU and RSS include harness costs',
      'native retries unknown and debug_ms1/1 is instrumented', 'benchmark consumer is not a universal application profile'],
    stopping: 'fixed five rounds, no replacement or optional stopping; abort unsafe or incomplete capture and retain artifacts'};
  return {...body, plan_id: hash(JSON.stringify(body))};
}

export function pgoOrder(plan, cell, round) {
  return ['native', ...qualificationOrder(plan.statistics, cell.id, round).map(implementation => implementation === 'go' ? 'pgo' : 'off'), 'native'];
}

export function validatePGOCapture(capture, cell, identity, implementation) {
  assert.equal(capture?.status, `pgo_diagnostic_${implementation}_capture_unqualified`, 'explicit nonqualifying PGO capture');
  assert.equal(capture.error, null, 'capture failure');
  assert.deepEqual(capture.identity, identity, 'capture identity');
  assert.equal(capture.attempt?.payload_verified, true, 'byte-exact final payload');
  assert.equal(capture.attempt.cleanup_verified, true, 'fixture cleanup');
  for (const [name, elapsed, count] of [['warmup', 1000000000, 1000], ['measured', 8000000000, 10000]]) {
    const phase = capture.attempt[name];
    assert(Number.isSafeInteger(phase?.elapsed_ns) && phase.elapsed_ns >= elapsed, 'PGO phase duration');
    assert(phase.successful_operations >= count, 'PGO phase count');
    assert.equal(phase.unexpected_failures, 0); assert.equal(phase.censored, 0);
    assert.equal(phase.records?.length, phase.successful_operations, 'PGO counter agreement');
    const latencies = validateQualificationRecords(cell, {...identity, records: phase.records}, phase.elapsed_ns, phase.operations_per_worker, implementation === 'native');
    for (const record of phase.records) assert(Number.isSafeInteger(record.timeout_deadline_ns) && record.timeout_deadline_ns >= record.end_ns && record.timeout_deadline_ns <= record.start_ns + 30000000000, 'operation budget');
    if (name === 'measured') {
      const row = validateParity(capture.report, implementation, cell, phase.successful_operations);
      assert.equal(row.elapsed_ns, phase.elapsed_ns, 'measured clock agreement');
      latencies.sort((left, right) => left - right);
      for (const [name, fraction] of [['p50_ns', .5], ['p95_ns', .95], ['p99_ns', .99]]) assert.equal(row[name], latencies[Math.ceil(latencies.length * fraction) - 1], 'raw quantile agreement');
      const iops = row.operations * 1000000000 / row.elapsed_ns;
      assert(Math.abs(row.iops - iops) <= Math.max(1e-6, iops * 1e-9), 'throughput agreement');
      for (const name of ['cpu_user_ns', 'cpu_system_ns']) assert(Number.isSafeInteger(row.parity.measured_resources[name]) && row.parity.measured_resources[name] >= 0, 'CPU delta');
    }
  }
  assert.equal(capture.attempt.memory.interval_ns, 100000000, 'matched RSS cadence');
  const measured = capture.attempt.measured, row = capture.report.rows[0];
  const memory = validateQualificationMemory(capture.attempt.memory, measured.elapsed_ns);
  return {cpu_ns_per_operation: (row.parity.measured_resources.cpu_user_ns + row.parity.measured_resources.cpu_system_ns) / row.operations,
    p99_ns: row.p99_ns, successful_iops: row.iops, rss_incremental_peak_bytes: memory.rss_incremental_peak_bytes};
}

export function assessPGO(plan, rounds, build) {
  assert.deepEqual(plan, pgoExperimentPlan(), 'frozen PGO experiment changed');
  assert(build?.compiler_profile_used && /^[a-f0-9]{64}$/.test(build.profile_sha256), 'compiler profile proof');
  assert(/^[a-f0-9]{64}$/.test(build.off_sha256) && /^[a-f0-9]{64}$/.test(build.pgo_sha256) && build.off_sha256 !== build.pgo_sha256, 'distinct pinned builds');
  for (const key of ['off_bytes', 'pgo_bytes']) assert(Number.isSafeInteger(build[key]) && build[key] > 0, 'binary size');
  const processes = new Set();
  const converted = rounds.map(round => {
    const cell = plan.heldout.find(cell => cell.id === round.cell_id);
    assert(cell, 'unexpected held-out cell');
    assert.deepEqual(round.legs.map(leg => leg.variant), pgoOrder(plan, cell, round.round), 'held-out assignment');
    assert.equal(round.plan_id, plan.plan_id, 'held-out plan binding');
    for (const leg of round.legs) {
      assert(typeof leg.process_id === 'string' && leg.process_id.length > 0 && !processes.has(leg.process_id), 'independent held-out process');
      processes.add(leg.process_id);
      assert.equal(leg.binary_sha256, build[`${leg.variant}_sha256`], 'held-out build binding');
    }
    return {...round, plan_id: plan.statistics.plan_id, legs: round.legs.filter(leg => leg.variant !== 'native').map(leg => ({...leg, implementation: leg.variant === 'pgo' ? 'go' : 'native'}))};
  });
  const candidate = analyzeQualificationMetrics(plan.statistics, converted);
  const comparison = variant => ({status: 'descriptive_native_comparator_not_randomized', cells: plan.heldout.map(cell => ({cell, metrics: Object.fromEntries(Object.keys(plan.statistics.gates).map(metric => {
    const ratios = rounds.filter(round => round.cell_id === cell.id).map(round => {
      const average = selected => { const pair = round.legs.filter(leg => leg.variant === selected).map(leg => leg.metrics?.[metric]); return pair.length === 2 && pair.every(value => Number.isFinite(value) && value > 0 && (metric !== 'rss_incremental_peak_bytes' || selected !== 'native' || value >= plan.statistics.rss_resolution_bytes)) ? pair[0] / 2 + pair[1] / 2 : NaN; };
      return average(variant) / average('native');
    });
    const known = ratios.length === plan.statistics.rounds && ratios.every(value => Number.isFinite(value) && value > 0);
    return [metric, {status: known ? 'descriptive_only' : 'unknown', point: known ? Math.exp(ratios.reduce((total, ratio) => total + Math.log(ratio), 0) / ratios.length) : null,
      lower: null, upper: null, per_round_ratios: ratios.map(value => Number.isFinite(value) ? value : null)}];
  }))}))});
  const baselineNative = comparison('off'), pgoNative = comparison('pgo');
  const failed = candidate.cells.some(cell => Object.values(cell.metrics).some(metric => metric.status === 'failed'));
  return {status: candidate.status === 'invalid' ? 'invalid_assessment' : 'heldout_diagnostic_assessment', decision: failed ? 'reject' : 'defer',
    decision_scope: 'this pinned benchmark consumer/profile; do not enable production PGO',
    reason: failed ? 'At least one unchanged held-out gate is not established; reject adoption without claiming a causal PGO regression.' : 'Diagnostic and consumer-specific evidence cannot justify adoption or library-only/native parity; retain uncertainty.',
    candidate_vs_off: candidate, comparator_labels: {go: 'pgo', native: 'off'}, off_vs_native: baselineNative, pgo_vs_native: pgoNative,
    binary: {...build, size_ratio: build.pgo_bytes / build.off_bytes}, limitations: plan.limitations};
}

function validateModes(evidence, implementation) {
  assert.equal(evidence.implementation, implementation); assert.equal(evidence.requested, 'secure');
  for (const service of ['monitor', 'osd']) assert(evidence.connections.some(connection => connection.service === service), 'actual authenticated mode coverage');
  for (const connection of evidence.connections) { assert.equal(connection.actual, 'secure'); assert.equal(connection.source, implementation === 'go' ? 'go-auth-metadata' : 'ceph-ready-log'); }
}

export function runPGO(root, namespace) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(namespace) && namespace.length <= 40, 'fresh PGO namespace prefix');
  const env = process.env;
  for (const name of ['P07_PARITY_CONFIG', 'P07_PARITY_KEY', 'P07_PARITY_KEYRING', 'P07_PARITY_MONITORS_FILE', 'P07_PARITY_FSID_FILE']) assert(env[name], `missing ${name}`);
  fs.mkdirSync(root, {mode: 0o700});
  const plan = pgoExperimentPlan(); write(root, 'plan.json', plan);
  const base = Object.fromEntries(['PATH', 'HOME', 'GOCACHE', 'TMPDIR'].filter(name => env[name]).map(name => [name, env[name]]));
  const runtime = {CGO_ENABLED: '0', GOTOOLCHAIN: plan.toolchain, GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'};
  const commands = [], training = [], probes = [], rounds = [], failures = [];
  const execute = (name, executable, args, extra = {}) => {
    const result = spawnSync(executable, args, {env: {...base, ...runtime, ...extra}, encoding: 'utf8', timeout: 960000, maxBuffer: 128 << 20});
    for (const stream of ['stdout', 'stderr']) fs.writeFileSync(path.join(root, `${name}.${stream}`), result[stream] ?? '', {flag: 'wx', mode: 0o600});
    const entry = {name, executable, args: args.map(arg => [env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, env.P07_PARITY_KEYRING].includes(arg) ? '<private-path>' : arg), status: result.status, signal: result.signal, error: result.error?.message};
    commands.push(entry); write(root, `${name}.exit.json`, entry);
    assert.equal(result.status, 0, `${name} failed; private logs retained`);
    return result.stdout;
  };
  const files = [...new Set(execute('source-files', 'git', ['ls-files', '-co', '--exclude-standard']).trim().split('\n'))].filter(file => /(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sourcePins = () => files.map(file => ({file, sha256: digest(file)}));
  const sources = sourcePins(); write(root, 'sources-before.json', sources);
  const binaries = {off: path.join(root, 'go-off'), pgo: path.join(root, 'go-pgo'), native: path.join(root, 'native'), checker: path.join(root, 'mode-check')};
  const library = fs.realpathSync('/lib64/librados.so.2'), libraryPin = digest(library);
  const fsid = fs.readFileSync(env.P07_PARITY_FSID_FILE, 'utf8').trim();
  const monitors = fs.readFileSync(env.P07_PARITY_MONITORS_FILE, 'utf8').trim().split(/\s+/).map(host => `${host}:3300`).join(',');
  const ceph = (name, args) => JSON.parse(execute(name, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, ...args, '--format', 'json']));
  const pins = {}, profiles = [];
  let warnings, lastGoMode, build;
  const leg = (name, variant, cell, round, fixtureNamespace, profile = false) => {
    const implementation = variant === 'native' ? 'native' : 'go', identity = {round, leg: name, seed: plan.statistics.seed};
    const before = ceph(`${name}-health-before`, ['status']); assert.deepEqual(validateParityHealth(before, fsid), warnings);
    const placement = Array.from({length: cell.concurrency}, (_, worker) => ({worker, before: ceph(`${name}-w${worker}-placement-before`, ['osd', 'map', cell.pool, fixtureName(cell, worker), fixtureNamespace])}));
    for (const value of placement) assert(value.before.pgid && value.before.acting?.length && Number.isInteger(value.before.acting_primary));
    const file = path.join(root, `${name}.capture.json`), modeFile = path.join(root, `${name}.modes.${implementation === 'native' ? 'log' : 'json'}`);
    const extra = {P07_PGO_DIAGNOSTIC: '1', P07_PARITY_NAMESPACE: fixtureNamespace, P07_MATRIX_SIZE: String(cell.size), P07_MATRIX_CONCURRENCY: String(cell.concurrency),
      P07_MATRIX_WORKLOAD: cell.workload, P07_QUALIFICATION_FILE: file, P07_QUALIFICATION_ROUND: String(round), P07_QUALIFICATION_LEG: name, P07_QUALIFICATION_SEED: String(identity.seed)};
    if (implementation === 'go') { extra.P07_READ_INTO = '1'; extra.P07_MODE_EVIDENCE_FILE = modeFile; }
    else extra.P07_NATIVE_MODE_LOG = modeFile;
    if (profile) { extra.P07_PGO_PROFILE_FILE = path.join(root, `${name}.pprof`); profiles.push(extra.P07_PGO_PROFILE_FILE); }
    const args = implementation === 'go' ? ['-monitors', monitors, '-fsid', fsid, '-key-file', env.P07_PARITY_KEY, '-entity', 'client.amakura', '-pool', cell.pool, '-transport', 'secure'] : [env.P07_PARITY_CONFIG, env.P07_PARITY_KEYRING, cell.pool, 'secure', 'client.amakura'];
    assert.equal(digest(binaries[variant]), pins[variant]); assert.deepEqual(sourcePins(), sources); assert.equal(digest(library), libraryPin);
    execute(name, 'taskset', ['-c', '0-9', binaries[variant], ...args], extra);
    const capture = JSON.parse(fs.readFileSync(file, 'utf8'));
    const metrics = validatePGOCapture(capture, cell, identity, implementation);
    if (implementation === 'native') {
      execute(`${name}-mode-check`, binaries.checker, ['-go', lastGoMode, '-native-log', modeFile, '-requested', 'secure', '-native-out', path.join(root, `${name}.modes.json`)]);
      validateModes(JSON.parse(fs.readFileSync(path.join(root, `${name}.modes.json`))), 'native');
    } else { validateModes(JSON.parse(fs.readFileSync(modeFile)), 'go'); lastGoMode = modeFile; }
    const after = ceph(`${name}-health-after`, ['status']); assert.deepEqual(validateParityHealth(after, fsid), warnings);
    for (const value of placement) {
      value.after = ceph(`${name}-w${value.worker}-placement-after`, ['osd', 'map', cell.pool, fixtureName(cell, value.worker), fixtureNamespace]);
      for (const key of ['pgid', 'acting', 'acting_primary']) assert.deepEqual(value.after[key], value.before[key]);
    }
    assert.equal(digest(binaries[variant]), pins[variant]); assert.deepEqual(sourcePins(), sources); assert.equal(digest(library), libraryPin);
    const retained = {variant, implementation, process_id: name, capture: {file: path.basename(file), sha256: digest(file)}, modes: {file: `${name}.modes.json`, sha256: digest(path.join(root, `${name}.modes.json`))},
      metrics, binary_sha256: pins[variant], profile: profile ? {file: `${name}.pprof`, sha256: digest(extra.P07_PGO_PROFILE_FILE)} : null,
      measured_operations: capture.attempt.measured.successful_operations, measured_elapsed_ns: capture.attempt.measured.elapsed_ns,
      provenance: {source_sha256: hash(JSON.stringify(sources)), tool_sha256: digest('integration/p07/pgo.mjs'), library_sha256: libraryPin, runtime: plan.runtime, fsid, fixtureNamespace, health: {before, after}, placement}};
    write(root, `${name}.leg.json`, retained);
    console.log(`${name} verified (${retained.measured_operations} operations)`);
    return retained;
  };
  try {
    write(root, 'native-library.json', {path: library, sha256: libraryPin, release: execute('native-version', 'ceph', ['--version']).trim()});
    execute('build-off', 'go', ['build', '-gcflags=all=-d=pgodebug=1', '-pgo=off', '-o', binaries.off, plan.consumer]); pins.off = digest(binaries.off);
    execute('build-native', 'gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', 'integration/p07/native_qualification.c', '-ldl', '-o', binaries.native]); pins.native = digest(binaries.native);
    execute('build-checker', 'go', ['build', '-pgo=off', '-o', binaries.checker, './tools/perf-mode-check']);
    warnings = validateParityHealth(ceph('health-before', ['status']), fsid);
    for (const [index, cell] of plan.training.entries()) training.push({cell, leg: leg(`train-${cell.id}`, 'off', cell, 1, `${namespace}-t${index}`, true)});
    write(root, 'training.json', {composition: plan.profile_composition, training});
    const profile = path.join(root, 'training.pprof');
    execute('merge-profile', 'go', ['tool', 'pprof', '-proto', '-output', profile, binaries.off, ...profiles]); fs.chmodSync(profile, 0o600);
    execute('profile-top', 'go', ['tool', 'pprof', '-top', '-nodecount=40', binaries.off, profile]);
    const profilePin = digest(profile);
    execute('build-pgo', 'go', ['build', '-x', '-gcflags=all=-d=pgodebug=1', '-pgo=' + profile, '-o', binaries.pgo, plan.consumer]);
    const compilerLog = fs.readFileSync(path.join(root, 'build-pgo.stderr'), 'utf8');
    assert.match(compilerLog, /-pgoprofile=/, 'compiler must consume preprocessed PGO profile');
    pins.pgo = digest(binaries.pgo); assert.notEqual(pins.off, pins.pgo, 'PGO build must have distinct binary identity');
    build = {off_sha256: pins.off, pgo_sha256: pins.pgo, native_sha256: pins.native, profile_sha256: profilePin, compiler_profile_used: true, compiler_log_sha256: digest(path.join(root, 'build-pgo.stderr')),
      off_bytes: fs.statSync(binaries.off).size, pgo_bytes: fs.statSync(binaries.pgo).size, off_build_info: execute('build-info-off', 'go', ['version', '-m', binaries.off]), pgo_build_info: execute('build-info-pgo', 'go', ['version', '-m', binaries.pgo])};
    assert.match(build.off_build_info, /CGO_ENABLED=0/); assert.match(build.pgo_build_info, /CGO_ENABLED=0/);
    write(root, 'builds.json', build);
    for (const variant of ['off', 'pgo']) probes.push(leg(`probe-${variant}`, variant, plan.heldout[0], 1, `${namespace}-probe-${variant}`));
    write(root, 'probes.json', {excluded: true, no_tuning: true, probes});
    for (const [index, cell] of plan.heldout.entries()) for (let round = 1; round <= plan.statistics.rounds; round++) {
      const fixtureNamespace = `${namespace}-h${index}r${round}`, result = {cell_id: cell.id, round, namespace: fixtureNamespace, plan_id: plan.plan_id, legs: []};
      rounds.push(result);
      for (const [position, variant] of pgoOrder(plan, cell, round).entries()) result.legs.push(leg(`${cell.id}-r${round}-l${position + 1}-${variant}`, variant, cell, round, fixtureNamespace));
      write(root, `${cell.id}-r${round}.round.json`, result);
      assert.equal(digest(profile), profilePin);
    }
    assert.deepEqual(validateParityHealth(ceph('health-after', ['status']), fsid), warnings);
    write(root, 'assessment.json', assessPGO(plan, rounds, build));
  } catch (error) { failures.push({error: error.message}); }
  const after = sourcePins(); write(root, 'sources-after.json', after);
  try { assert.deepEqual(after, sources); assert.equal(digest(library), libraryPin); } catch (error) { failures.push({error: error.message}); }
  write(root, 'summary.json', {status: failures.length ? 'failed' : 'heldout_diagnostic_capture_complete', plan_id: plan.plan_id, training, probes, rounds, build, commands, failures});
  if (failures.length) throw Error(failures[0].error);
  console.log(JSON.stringify({root, status: 'heldout_diagnostic_capture_complete', decision: JSON.parse(fs.readFileSync(path.join(root, 'assessment.json'))).decision}));
}

export function reproducePGO(root, outputFile) {
  assert(!fs.existsSync(outputFile), 'assessment output must be fresh');
  const plan = JSON.parse(fs.readFileSync(path.join(root, 'plan.json'))), summary = JSON.parse(fs.readFileSync(path.join(root, 'summary.json')));
  assert.equal(summary.status, 'heldout_diagnostic_capture_complete', 'incomplete experiment');
  assert.equal(summary.plan_id, plan.plan_id, 'summary plan binding');
  validatePGOPhaseBindings(plan, summary);
  const load = reference => {
    assert(/^[a-z0-9.-]+$/.test(reference.file) && !['.', '..'].includes(reference.file), 'local PGO evidence reference');
    const file = path.join(root, reference.file); assert(fs.lstatSync(file).isFile() && !fs.lstatSync(file).isSymbolicLink());
    assert.equal(digest(file), reference.sha256, 'raw input pin');
    return JSON.parse(fs.readFileSync(file));
  };
  const toolPin = digest('integration/p07/pgo.mjs');
  for (const variant of ['off', 'pgo', 'native']) assert.equal(digest(path.join(root, variant === 'native' ? 'native' : `go-${variant}`)), summary.build[`${variant}_sha256`], 'build file pin');
  assert.equal(digest(path.join(root, 'training.pprof')), summary.build.profile_sha256, 'training profile pin');
  assert.equal(digest(path.join(root, 'build-pgo.stderr')), summary.build.compiler_log_sha256, 'compiler log pin');
  const sources = JSON.parse(fs.readFileSync(path.join(root, 'sources-before.json')));
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(root, 'sources-after.json'))), sources, 'source capture continuity');
  validatePGOToolPins(sources);
  const verifyLeg = (leg, cell, round) => {
    assert.equal(leg.provenance.tool_sha256, toolPin, 'executing PGO tool pin');
    assert.equal(leg.provenance.source_sha256, hash(JSON.stringify(sources)), 'source pin');
    assert.deepEqual(leg.provenance.runtime, plan.runtime, 'runtime binding');
    assert.deepEqual(validateParityHealth(leg.provenance.health.before, leg.provenance.fsid), validateParityHealth(leg.provenance.health.after, leg.provenance.fsid));
    assert.equal(leg.provenance.placement.length, cell.concurrency, 'worker placement population');
    for (const [worker, value] of leg.provenance.placement.entries()) { assert.equal(value.worker, worker); for (const key of ['pgid', 'acting', 'acting_primary']) assert.deepEqual(value.after[key], value.before[key]); }
    validateModes(load(leg.modes), leg.implementation);
    const capture = load(leg.capture);
    assert.deepEqual(validatePGOCapture(capture, cell, {round, leg: leg.process_id, seed: plan.statistics.seed}, leg.implementation), leg.metrics, 'recomputed raw metrics');
    if (leg.profile) {
      assert(/^[a-z0-9.-]+$/.test(leg.profile.file) && !['.', '..'].includes(leg.profile.file), 'local training profile reference');
      const profile = path.join(root, leg.profile.file);
      assert(fs.lstatSync(profile).isFile() && !fs.lstatSync(profile).isSymbolicLink(), 'regular training profile');
      assert.equal(digest(profile), leg.profile.sha256, 'training input pin');
    }
  };
  for (const {cell, leg} of summary.training) verifyLeg(leg, cell, 1);
  assert.equal(summary.training.length, plan.training.length, 'complete training composition');
  assert.deepEqual(summary.training.map(entry => entry.cell), plan.training, 'profile composition binding');
  for (const leg of summary.probes) verifyLeg(leg, plan.heldout[0], 1);
  assert.deepEqual(summary.probes.map(leg => leg.variant), ['off', 'pgo'], 'both correctness probes');
  for (const round of summary.rounds) for (const leg of round.legs) {
    assert.equal(leg.profile, null, 'held-out profiling forbidden');
    assert.equal(leg.provenance.fixtureNamespace, round.namespace, 'held-out conditioning');
    verifyLeg(leg, plan.heldout.find(cell => cell.id === round.cell_id), round.round);
  }
  const result = assessPGO(plan, summary.rounds, summary.build);
  fs.writeFileSync(outputFile, JSON.stringify(result, null, 2) + '\n', {flag: 'wx', mode: 0o600});
  return result;
}

export function validatePGOToolPins(sources) {
  for (const file of ['integration/p07/pgo.mjs', 'integration/p07/parity.mjs', 'integration/p07/qualification.mjs']) {
    const pin = sources.find(pin => pin.file === file);
    assert(pin && pin.sha256 === digest(file), 'executing PGO analyzer dependency pin');
  }
}

export function validatePGOPhaseBindings(plan, summary) {
  assert.deepEqual(summary.training.map(entry => entry.cell), plan.training, 'complete training composition');
  assert.deepEqual(summary.probes.map(leg => leg.variant), ['off', 'pgo'], 'both correctness probes');
  const processes = new Set(), namespaces = new Set();
  const verify = (leg, profiling, separateNamespace) => {
    assert(typeof leg.process_id === 'string' && leg.process_id.length > 0 && !processes.has(leg.process_id), 'independent experiment process');
    processes.add(leg.process_id);
    assert(['off', 'pgo', 'native'].includes(leg.variant), 'experiment build variant');
    assert.equal(leg.implementation, leg.variant === 'native' ? 'native' : 'go', 'experiment implementation');
    assert.equal(leg.binary_sha256, summary.build[`${leg.variant}_sha256`], 'experiment build binding');
    if (profiling) {
      assert.equal(leg.variant, 'off', 'non-PGO training');
      assert(leg.profile && /^[a-f0-9]{64}$/.test(leg.profile.sha256), 'required training profile');
    } else assert.equal(leg.profile, null, 'excluded probe or held-out profiling forbidden');
    if (separateNamespace) {
      const namespace = leg.provenance.fixtureNamespace;
      assert(typeof namespace === 'string' && namespace.length > 0 && !namespaces.has(namespace), 'independent phase namespace');
      namespaces.add(namespace);
    }
  };
  for (const {leg} of summary.training) verify(leg, true, true);
  for (const leg of summary.probes) verify(leg, false, true);
  for (const round of summary.rounds) {
    assert(typeof round.namespace === 'string' && round.namespace.length > 0 && !namespaces.has(round.namespace), 'independent held-out namespace');
    namespaces.add(round.namespace);
    for (const leg of round.legs) verify(leg, false, false);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  if (process.argv[2] === 'analyze') { assert.equal(process.argv.length, 5, 'usage: node pgo.mjs analyze PRIVATE_ROOT FRESH_ASSESSMENT'); console.log(JSON.stringify({decision: reproducePGO(path.resolve(process.argv[3]), path.resolve(process.argv[4])).decision})); }
  else { assert.equal(process.argv.length, 4, 'usage: node pgo.mjs FRESH_PRIVATE_ROOT FRESH_NAMESPACE_PREFIX'); runPGO(path.resolve(process.argv[2]), process.argv[3]); }
}