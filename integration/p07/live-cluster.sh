#!/bin/sh
set -eu
exec node --input-type=module - "$@" <<'NODE'
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';

const options = { entity: 'client.p07', repetitions: '3', rate: '1000', cases: 'none,cpu,alloc,both', 'trace-debug-max-buffer-mb': '1024' };
const switches = new Set(['prepare', 'seed', 'observe', 'closed-loop', 'build-native']);
const values = new Set(['entity', 'capture', 'build-capture', 'binary', 'monitors', 'fsid', 'key-file', 'pool', 'repetitions', 'rate', 'cases', 'native-binary', 'native-conf', 'native-keyring', 'trace-debug-max-buffer-mb']);
let capture, sourceBefore, binaryBefore, nativeBefore;
const results = [];
const root = process.cwd();
const privatePaths = [];
const privateIdentities = new Set();
function registerPrivate(file) {
  const resolved = fs.realpathSync(file);
  if (withinRoot(path.resolve(file)) || withinRoot(resolved)) throw new Error('credential files must be outside the workspace');
  const stat = fs.statSync(file);
  privateIdentities.add(`${stat.dev}:${stat.ino}`);
  privatePaths.push(file, resolved);
  if (file.startsWith(root + path.sep)) privatePaths.push(path.relative(root, file));
}
const redact = text => privatePaths.reduce((value, secret) => value.split(secret).join('<credential-path>'), text);
const hash = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const writeJSON = (name, value) => fs.writeFileSync(path.join(capture, name), JSON.stringify(value, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
const baseEnv = Object.fromEntries(['PATH', 'HOME', 'TMPDIR', 'GOCACHE', 'GOPATH', 'GOTOOLCHAIN', 'CC'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
const runtimeEnv = { GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off', P07_READ_DIAGNOSTIC: '1', P07_READ_INTO: '1', P07_READ_SIZE: '65536', P07_READ_CONCURRENCY: '16' };
function run(name, command, args = [], env = {}) {
  const started = new Date().toISOString();
  const result = spawnSync(command, args, { cwd: root, env: { ...baseEnv, ...env }, encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 });
  fs.writeFileSync(path.join(capture, `${name}.stdout`), redact(result.stdout ?? ''), { flag: 'wx', mode: 0o600 });
  fs.writeFileSync(path.join(capture, `${name}.stderr`), redact(result.stderr ?? ''), { flag: 'wx', mode: 0o600 });
  const entry = { name, command: redact(command), args: args.map(redact), environment: env, started, ended: new Date().toISOString(), exit_code: result.status, signal: result.signal, error: result.error?.message };
  writeJSON(`${name}.exit.json`, entry);
  results.push(entry);
  return result;
}
function requireSuccess(result, message) {
  if (result.error || result.status !== 0) throw new Error(message);
}
function withinRoot(file) {
  const relative = path.relative(root, file);
  return relative === '' || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
}
function snapshot() {
  const files = [];
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      if (['.git', 'node_modules', '.DS_Store'].includes(entry.name)) continue;
      const file = path.join(directory, entry.name);
      if (file === capture || file === options['build-capture'] || privatePaths.includes(file)) continue;
      if (entry.isSymbolicLink()) {
        const resolved = fs.realpathSync(file);
        const stat = fs.statSync(file);
        if (privatePaths.includes(resolved) || privateIdentities.has(`${stat.dev}:${stat.ino}`)) continue;
        throw new Error('source symlinks are not supported');
      }
      if (entry.isDirectory()) visit(file);
      else if (entry.isFile() && (/\.(go|mjs|sh|c)$/.test(file) || ['go.mod', 'go.sum'].includes(entry.name))) {
        const stat = fs.statSync(file);
        if (!privateIdentities.has(`${stat.dev}:${stat.ino}`)) files.push({ file: path.relative(root, file), sha256: hash(file) });
      }
    }
  }
  visit(root);
  return files.sort((left, right) => left.file.localeCompare(right.file));
}
function bindPreparation() {
  const directory = options['build-capture'];
  const read = name => JSON.parse(fs.readFileSync(path.join(directory, name), 'utf8'));
  const summary = read('summary.json');
  if (summary.schema !== 1 || summary.mode !== 'preparation' || summary.status !== 'passed' || summary.failed !== false ||
      !Array.isArray(summary.results) || !summary.results.some(entry => entry.name === 'build-go' && entry.exit_code === 0) ||
      summary.results.some(entry => entry.exit_code !== 0)) throw new Error('build capture must contain a passed preparation');
  const normalize = manifest => {
    if (!Array.isArray(manifest) || manifest.length === 0) throw new Error('invalid preparation source manifest');
    const names = new Set();
    for (const entry of manifest) {
      if (typeof entry.file !== 'string' || !entry.file || path.isAbsolute(entry.file) || path.normalize(entry.file) !== entry.file ||
          entry.file.split(path.sep).includes('..') || names.has(entry.file) || !/^[0-9a-f]{64}$/.test(entry.sha256)) throw new Error('invalid preparation source manifest');
      names.add(entry.file);
    }
    return JSON.stringify(manifest.map(({ file, sha256 }) => ({ file, sha256 })).sort((left, right) => left.file.localeCompare(right.file)));
  };
  const before = read('binary-before.json');
  const after = read('binary-after.json');
  if (before.sha256 !== binaryBefore || after.sha256 !== binaryBefore ||
      (options['closed-loop'] && (before.native_sha256 !== nativeBefore || after.native_sha256 !== nativeBefore))) throw new Error('supplied binary does not match preparation');
  const source = normalize(read('source-before.json'));
  if (source !== normalize(read('source-after.json')) || source !== normalize(sourceBefore)) throw new Error('staged source does not match preparation');
  writeJSON('build-binding.json', { build_capture: redact(directory), binary: redact(options.binary), binary_sha256: binaryBefore,
    native_sha256: nativeBefore, source_before: path.join(directory, 'source-before.json'), source_after: path.join(directory, 'source-after.json'), status: 'passed' });
}
function readReport(result, loadCase, observed) {
  try {
    const report = JSON.parse(result.stdout);
    if (loadCase && (report.offered_load?.config?.load_case !== loadCase || report.offered_load.config.factorial !== true)) throw new Error('factorial report does not match requested case');
    if (report.environment?.gomaxprocs !== 10 || report.environment?.GOOS !== 'linux' || report.environment?.GOARCH !== 'amd64') throw new Error('report runtime must be Linux amd64 with ten active Ps');
    return { valid: true, observed, report };
  } catch (error) { return { valid: false, error: error.message }; }
}
try {
  for (let index = 2; index < process.argv.length; index++) {
    const name = process.argv[index].replace(/^--/, '');
    if (!process.argv[index].startsWith('--') || options[name] === true) throw new Error('invalid or duplicate option');
    if (switches.has(name)) options[name] = true;
    else if (values.has(name) && process.argv[index + 1] && !process.argv[index + 1].startsWith('--')) options[name] = process.argv[++index];
    else throw new Error(`unknown or missing option: ${name}`);
  }
  if (!options.capture || !path.isAbsolute(options.capture) || path.normalize(options.capture) !== options.capture) throw new Error('--capture must be a fresh clean absolute path');
  if (!/^[1-9]\d*$/.test(options.repetitions) || Number(options.repetitions) > 100) throw new Error('repetitions must be 1..100');
  if (!/^[1-9]\d*$/.test(options['trace-debug-max-buffer-mb']) || Number(options['trace-debug-max-buffer-mb']) < 16 || Number(options['trace-debug-max-buffer-mb']) > 1024) throw new Error('--trace-debug-max-buffer-mb must be an integer from 16 to 1024');
  if (!['1000', '2000', '4000'].includes(options.rate)) throw new Error('invalid offered rate');
  const cases = options.cases.split(',');
  if (new Set(cases).size !== cases.length || cases.some(value => !['none', 'cpu', 'alloc', 'both'].includes(value))) throw new Error('invalid cases');
  if (options.prepare && (options.seed || options.observe || options['closed-loop'])) throw new Error('--prepare cannot run measurements or seeds');
  if (options['closed-loop'] && options.observe) throw new Error('offered observation cannot label closed-loop runs');
  if (!fs.existsSync(path.join(root, 'integration/p07/benchmark/main.go'))) throw new Error('run from repository root');
  if (!options.prepare) {
    for (const name of ['build-capture', 'binary', 'monitors', 'fsid', 'key-file', 'pool']) if (!options[name]) throw new Error(`missing --${name}`);
    if (!/^client\.[A-Za-z0-9_.-]+$/.test(options.entity)) throw new Error('invalid client entity');
    if (!path.isAbsolute(options['build-capture']) || path.normalize(options['build-capture']) !== options['build-capture']) throw new Error('--build-capture must be a clean absolute path');
    if (!/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(options.fsid)) throw new Error('invalid cluster fsid');
    if (!/^[A-Za-z0-9_.-]+$/.test(options.pool) || options.monitors.split(',').some(value => !value.trim() || /\s/.test(value))) throw new Error('invalid pool or monitors');
    for (const name of ['binary', 'key-file']) {
      if (!path.isAbsolute(options[name])) throw new Error(`--${name} must be absolute`);
      fs.accessSync(options[name], name === 'binary' ? fs.constants.X_OK : fs.constants.R_OK);
    }
    registerPrivate(options['key-file']);
    if (options['closed-loop']) {
      for (const name of ['native-binary', 'native-conf', 'native-keyring']) {
        if (!options[name] || !path.isAbsolute(options[name])) throw new Error(`closed-loop requires absolute --${name}`);
        fs.accessSync(options[name], name === 'native-binary' ? fs.constants.X_OK : fs.constants.R_OK);
      }
      registerPrivate(options['native-keyring']); registerPrivate(options['native-conf']);
    }
  }
  fs.mkdirSync(options.capture, { mode: 0o700 });
  capture = options.capture;
  sourceBefore = snapshot(); writeJSON('source-before.json', sourceBefore);
  run('source-revision', 'git', ['rev-parse', 'HEAD']);
  run('source-status', 'git', ['status', '--short', '--untracked-files=no']);
  const uname = run('host-uname', 'uname', ['-s', '-m', '-r']);
  requireSuccess(uname, 'cannot identify host');
  requireSuccess(run('go-version', 'go', ['version']), 'Go toolchain unavailable');
  writeJSON('host.json', { platform: os.platform(), arch: os.arch(), cpus: os.cpus().map(cpu => ({ model: cpu.model, speed: cpu.speed })), available_parallelism: os.availableParallelism(), total_memory_bytes: os.totalmem(), node_version: process.version });
  if (!options.prepare) {
    if (!/^Linux\s+/.test(uname.stdout.trim()) || !/\bx86_64\b/.test(uname.stdout)) throw new Error('measurements require a Linux amd64 host');
    const processors = run('host-processors', 'getconf', ['_NPROCESSORS_ONLN']);
    requireSuccess(processors, 'cannot identify processor count');
    if (Number(processors.stdout.trim()) < 10 || !Number.isInteger(Number(processors.stdout.trim()))) throw new Error('host needs at least ten processors; verify CPU affinity and quota separately');
    for (const [name, file] of [['cpu', '/proc/cpuinfo'], ['memory', '/proc/meminfo'], ['affinity', '/proc/self/status']]) {
      if (fs.existsSync(file)) fs.writeFileSync(path.join(capture, `host-${name}.txt`), fs.readFileSync(file), { flag: 'wx', mode: 0o600 });
    }
  }
  if (options.prepare) {
    options.binary = path.join(capture, 'build_linuxamd64');
    requireSuccess(run('build-go', 'go', ['build', '-trimpath', '-o', options.binary, './integration/p07/benchmark'], { GOOS: 'linux', GOARCH: 'amd64', CGO_ENABLED: '0' }), 'Go build failed');
    if (options['build-native']) {
      if (!/^Linux\s+/.test(uname.stdout.trim()) || !/\bx86_64\b/.test(uname.stdout)) throw new Error('native build requires Linux amd64 C toolchain');
      options['native-binary'] = path.join(capture, 'native-benchmark');
      requireSuccess(run('build-native', 'cc', ['-std=c11', '-Wall', '-Wextra', '-Werror', '-O2', '-pthread', 'integration/p07/native_benchmark.c', '-ldl', '-o', options['native-binary']]), 'native build failed');
    }
  }
  binaryBefore = hash(options.binary);
  nativeBefore = options['native-binary'] ? hash(options['native-binary']) : null;
  writeJSON('binary-before.json', { sha256: binaryBefore, native_sha256: nativeBefore });
  if (!options.prepare) bindPreparation();
  requireSuccess(run('go-buildinfo', 'go', ['version', '-m', options.binary]), 'cannot capture binary build information');
  if (!options.prepare) {
    writeJSON('measurement.json', { entity: options.entity, monitors: options.monitors, fsid: options.fsid, pool: options.pool, credential: '<redacted>', transport: 'secure', label: options.observe ? 'instrumented' : options['closed-loop'] ? 'closed-loop-context-only' : 'primary', repetitions: Number(options.repetitions), cases, rate: Number(options.rate), runtime: runtimeEnv, seed_opt_in: Boolean(options.seed), limitations: 'No native offered-load parity. Separate instrumented legs are not primary latency samples. Verify host affinity/quota, dedicated pool and externally provisioned credentials.' });
    const args = ['-monitors', options.monitors, '-fsid', options.fsid, '-key-file', options['key-file'], '-pool', options.pool, '-transport', 'secure', '-entity', options.entity];
    let seedOK = true;
    if (options.seed) {
      console.error('WARNING: --seed writes ONLY p07-shared-read-0..15; use a dedicated pool and an explicitly authorized credential. No objects are deleted.');
      const result = run('seed', options.binary, args, { ...runtimeEnv, P07_SEED_ONLY: '1' });
      seedOK = result.status === 0;
      if (!seedOK) console.error('seed failed; measurements skipped, artifacts retained');
    }
    for (let repetition = 1; seedOK && repetition <= Number(options.repetitions); repetition++) {
      if (options['closed-loop']) {
        const goResult = run(`closed-go-${repetition}`, options.binary, args, { ...runtimeEnv, P07_BACKGROUND_WORKERS: '0' });
        const report = readReport(goResult, null, false);
        writeJSON(`closed-go-${repetition}.report.json`, report);
        if (!report.valid) results.push({ name: `closed-go-${repetition}-validation`, exit_code: 1, error: report.error });
        run(`closed-native-${repetition}`, options['native-binary'], [options['native-conf'], options['native-keyring'], options.pool, 'secure', options.entity], { P07_READ_DIAGNOSTIC: '1' });
        continue;
      }
      for (const loadCase of cases) {
        const name = `${options.observe ? 'instrumented' : 'primary'}-${loadCase}-${repetition}`;
        const env = { ...runtimeEnv, P07_BACKGROUND_WORKERS: '8', P07_OFFERED_LOAD: '1', P07_OFFERED_FACTORIAL: '1', P07_OFFERED_CASE: loadCase, P07_OFFERED_RATE: options.rate, P07_FIXED_ALLOC_RATE: '100' };
        const observation = path.join(capture, `${name}.observation`);
        if (options.observe) Object.assign(env, { P07_OFFERED_OBSERVATION_DIR: observation, P07_OFFERED_OBSERVATION_LABEL: 'instrumented' });
        const result = run(name, options.binary, args, env);
        const report = readReport(result, loadCase, Boolean(options.observe));
        writeJSON(`${name}.report.json`, report);
        if (!report.valid) results.push({ name: `${name}-validation`, exit_code: 1, error: report.error });
        if (options.observe) {
          try {
            const timing = JSON.parse(fs.readFileSync(path.join(observation, 'timing.json')));
            if (timing.label !== 'instrumented' || timing.schema !== 1 || !Array.isArray(timing.calls)) throw new Error('observation not integrated or invalid');
            if (!report.valid || timing.calls.length !== report.report.offered_load.attempted) throw new Error('observation count does not match attempted reads');
            const debug = path.join(observation, 'trace-debug.txt');
            const decode = run(`${name}-decode`, process.execPath, ['integration/p07/trace-stall-summary.mjs', '--decode', path.join(observation, 'trace.out'), debug], { P07_TRACE_DEBUG_MAX_BUFFER_MB: options['trace-debug-max-buffer-mb'] });
            if (decode.status === 0) run(`${name}-stalls`, process.execPath, ['integration/p07/trace-stall-summary.mjs', debug, path.join(observation, 'timing.json')]);
          } catch (error) { results.push({ name: `${name}-observation`, exit_code: 1, error: error.message }); }
        }
      }
    }
  }
} catch (error) {
  results.push({ name: 'wrapper', exit_code: 1, error: redact(error.message) });
}
if (capture) {
  try {
    const sourceAfter = snapshot(); writeJSON('source-after.json', sourceAfter);
    if (JSON.stringify(sourceBefore) !== JSON.stringify(sourceAfter)) results.push({ name: 'source-unchanged', exit_code: 1, error: 'source changed during capture' });
    if (binaryBefore) {
      const after = hash(options.binary);
      const nativeAfter = options['native-binary'] ? hash(options['native-binary']) : null;
      writeJSON('binary-after.json', { sha256: after, native_sha256: nativeAfter });
      if (after !== binaryBefore) results.push({ name: 'binary-unchanged', exit_code: 1 });
      if (nativeAfter !== nativeBefore) results.push({ name: 'native-binary-unchanged', exit_code: 1 });
    }
  } catch (error) { results.push({ name: 'finalize', exit_code: 1, error: redact(error.message) }); }
}
const failed = results.some(result => result.exit_code !== 0);
const summary = { schema: 1, mode: options.prepare ? 'preparation' : 'measurement', status: failed ? 'failed' : 'passed', capture: capture ?? null, failed, results };
if (capture) {
  writeJSON('summary.json', summary);
  const artifacts = [];
  function visitArtifacts(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) visitArtifacts(file);
      else if (entry.isFile()) artifacts.push({ file: path.relative(capture, file), sha256: hash(file) });
    }
  }
  visitArtifacts(capture);
  writeJSON('artifact-hashes.json', artifacts.sort((left, right) => left.file.localeCompare(right.file)));
}
console.log(JSON.stringify(summary));
process.exitCode = summary.failed ? 1 : 0;
NODE