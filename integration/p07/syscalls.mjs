import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {validateParityHealth, fixtureName} from './parity.mjs';
import {validatePGOCapture} from './pgo.mjs';

const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const digest = file => hash(fs.readFileSync(file));
const save = (root, name, value) => fs.writeFileSync(path.join(root, name), JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});
const dependencies = ['integration/p07/syscalls.mjs', 'integration/p07/parity.mjs', 'integration/p07/pgo.mjs', 'integration/p07/qualification.mjs'];

export function syscallPlan() {
  const plan = {version: 1, toolchain: 'go1.27.1', rounds: 2, probes: ['baseline', 'strace', 'perf'],
    cells: [{id: 'small-read', pool: 'readcache', size: 4096, concurrency: 1, workload: 'read'},
      {id: 'concurrent-read', pool: 'readcache', size: 65536, concurrency: 16, workload: 'read'},
      {id: 'concurrent-write', pool: 'test-3x', size: 1048576, concurrency: 16, workload: 'write'}],
    runtime: {CGO_ENABLED: '0', GOTOOLCHAIN: 'go1.27.1', GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'},
    affinity: '0-9', minima: {warmup_ns: 1000000000, warmup_operations: 1000, measured_ns: 8000000000, measured_operations: 10000},
    verdict: 'no-production-change-without-independent-causal-benefit',
    limitations: ['Excluded whole-process probes; never native parity or library-only CPU', 'Two mirrored repetitions are attribution, not qualification',
      'Native ordinary-message debug logging and unknown retries remain instrumented', 'Control-sized writes are not proven ACKs',
      'Interwrite gaps are not measured ready-queue density', 'No scheduler tracepoints: software context-switch samples, not runnable-state or wakeup causality',
      'No host-load/idle candidate trial or optimization adoption without a demonstrated local candidate']};
  return {...plan, plan_id: hash(JSON.stringify(plan))};
}

export function phaseWindow(capture) {
  const value = capture.measurement_window;
  assert.equal(value?.clock, 'realtime');
  for (const key of ['start_ns', 'end_ns']) assert(typeof value[key] === 'string' && /^[0-9]+$/.test(value[key]), 'lossless phase clock');
  const start = BigInt(value.start_ns), end = BigInt(value.end_ns);
  assert(start > 0n && end > start, 'positive phase window');
  const elapsed = BigInt(capture.attempt.measured.elapsed_ns);
  const difference = end - start - elapsed;
  assert(difference >= -200000000n && difference <= 200000000n, 'phase clock continuity');
  return {start, end};
}

const timestamp = text => {
  const match = /^([0-9]+)\.([0-9]{1,9})$/.exec(text);
  assert(match, 'trace timestamp');
  return BigInt(match[1]) * 1000000000n + BigInt(match[2].padEnd(9, '0'));
};

export function parseSyscalls(traces, window) {
  const events = [], unresolved = [];
  for (const [thread, text] of traces.entries()) {
    let pending, exited;
    const incomplete = [];
    for (const line of text.split('\n')) {
      const match = /^([0-9]+\.[0-9]+)\s+(.*)$/.exec(line);
      if (!match) { assert(!line || /^(\+\+\+|---|strace:)/.test(line), 'unparsed trace line'); continue; }
      const at = timestamp(match[1]), body = match[2];
      if (/^\+\+\+ exited with 0 \+\+\+$/.test(body)) { exited = at; continue; }
      if (body.includes('<unfinished ...>')) { assert(!pending, 'nested unfinished syscall'); pending = {at, body: body.replace('<unfinished ...>', '')}; continue; }
      const resumed = /^<\.\.\. ([a-z0-9_]+) resumed>(.*)$/.exec(body);
      const full = resumed ? (assert(pending && pending.body.startsWith(resumed[1] + '('), 'orphan resumed syscall'), pending.body + resumed[2]) : body;
      const start = resumed ? pending.at : at; if (resumed) pending = null;
      if (/^(\+\+\+|---)/.test(full)) continue;
      if (/^[a-z0-9_]+\(.*\)\s+=\s+\?$/.test(full)) { incomplete.push({at: start}); continue; }
      const call = /^([a-z0-9_]+)\((.*)\)\s+=\s+(-?(?:0x[0-9a-f]+|[0-9]+)|\?)(.*?)\s*<([0-9]+\.[0-9]+)>$/.exec(full);
      assert(call, 'unparsed syscall record');
      const duration = timestamp(call[5]);
      events.push({thread, at: start, end: start + duration, name: call[1], args: call[2], result: call[3], detail: call[4]});
    }
    if (pending) incomplete.push(pending);
    for (const event of incomplete) {
      assert(event.at > window.end || exited && exited > window.end, 'unfinished measured syscall without post-window exit');
      unresolved.push(event);
    }
  }
  events.sort((left, right) => left.at < right.at ? -1 : left.at > right.at ? 1 : 0);
  const sockets = new Set(), counts = {}, socketCounts = {}, lastWrite = new Map();
  let boundary = 0, controlCandidates = 0, socketBytes = 0;
  const gaps = {under_50us: 0, under_200us: 0, other: 0};
  for (const event of events) {
    const fdMatch = /^(0x[0-9a-f]+|[0-9]+)/.exec(event.args), fd = fdMatch ? Number(fdMatch[1]) : null;
    if (event.name === 'socket' && /AF_INET6?.*SOCK_STREAM/.test(event.args) && Number(event.result) >= 0) sockets.add(Number(event.result));
    const inWindow = event.at >= window.start && event.end <= window.end;
    if (!inWindow && event.end >= window.start && event.at <= window.end) boundary++;
    if (inWindow) {
      counts[event.name] = (counts[event.name] ?? 0) + 1;
      if (sockets.has(fd) && /^(read|write|sendto|recvfrom|sendmsg|recvmsg)$/.test(event.name)) {
        socketCounts[event.name] = (socketCounts[event.name] ?? 0) + 1;
        if (Number(event.result) > 0) socketBytes += Number(event.result);
        if (/^(write|sendto|sendmsg)$/.test(event.name) && Number(event.result) > 0) {
          if (Number(event.result) === 96) controlCandidates++;
          if (lastWrite.has(fd)) { const gap = event.at - lastWrite.get(fd); gaps[gap < 50000n ? 'under_50us' : gap < 200000n ? 'under_200us' : 'other']++; }
          lastWrite.set(fd, event.at);
        }
      }
    }
    if (event.name === 'close') sockets.delete(fd);
  }
  assert(Object.keys(socketCounts).length, 'socket trace coverage');
  return {counts, socket_counts: socketCounts, transferred_bytes_read_plus_write: socketBytes, control_96byte_write_candidates: controlCandidates,
    same_socket_interwrite_gaps: gaps, boundary_crossing_syscalls_excluded: boundary, unfinished_outside_measurement: unresolved.filter(event => event.at < window.start || event.at > window.end).length,
    termination_censored_syscalls_excluded: unresolved.filter(event => event.at <= window.end).length, blocked_duration_is_not_cpu: true};
}

export function parsePerf(text, window) {
  const flat = {}, contexts = [], cpu = [];
  let pending;
  const finish = () => {
    if (!pending || pending.at < window.start || pending.at > window.end) return;
    assert(pending.leaf, 'perf leaf attribution');
    if (pending.event === 'context-switches') contexts.push({thread: pending.thread, at_ns: pending.at.toString(), period: pending.period, leaf: pending.leaf});
    else { cpu.push({at_ns: pending.at.toString(), period: pending.period}); flat[pending.leaf] = (flat[pending.leaf] ?? 0) + pending.period; }
  };
  for (const line of text.split('\n')) {
    if (!line.trim()) continue;
    const frame = /^\s*[0-9a-f]+\s+(.*)$/.exec(line);
    if (frame) { assert(pending, 'orphan perf leaf'); if (!pending.leaf) pending.leaf = frame[1]; continue; }
    const match = /^\s*(.*?)\s+(\d+)\/(\d+)\s+([0-9]+\.[0-9]+):\s+(\d+)\s+(cpu-clock|context-switches)(?:\/period=1\/)?:\s*(.*)$/.exec(line);
    assert(match, 'unparsed perf sample');
    finish();
    pending = {at: timestamp(match[4]), event: match[6], period: Number(match[5]), thread: match[3], leaf: match[7].replace(/^[0-9a-f]+\s+/, '').trim()};
  }
  finish();
  assert(cpu.length && contexts.length, 'CPU and scheduler sample coverage');
  return {cpu_samples: cpu.length, cpu_sample_period_total: cpu.reduce((sum, sample) => sum + sample.period, 0),
    cpu_flat_periods: flat, context_switch_samples: contexts.length, context_switch_period_total: contexts.reduce((sum, sample) => sum + sample.period, 0),
    context_switch_trace: contexts};
}

function checkModes(value, implementation) {
  assert.equal(value.implementation, implementation); assert.equal(value.requested, 'secure');
  for (const service of ['monitor', 'osd']) assert(value.connections.some(connection => connection.service === service), 'secure connection coverage');
  for (const connection of value.connections) assert.equal(connection.actual, 'secure');
}

export function runSyscalls(root, prefix) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(prefix) && prefix.length <= 40);
  const plan = syscallPlan(), env = process.env;
  for (const key of ['CONFIG', 'KEY', 'KEYRING', 'MONITORS_FILE', 'FSID_FILE']) assert(env[`P07_PARITY_${key}`], 'private deployment paths required');
  fs.mkdirSync(root, {mode: 0o700}); save(root, 'plan.json', plan);
  const base = Object.fromEntries(['PATH', 'HOME', 'GOCACHE', 'TMPDIR'].filter(key => env[key]).map(key => [key, env[key]]));
  const execute = (name, program, args, extra = {}) => {
    const result = spawnSync(program, args, {env: {...base, ...plan.runtime, ...extra}, encoding: 'utf8', timeout: 960000, maxBuffer: 128 << 20});
    for (const stream of ['stdout', 'stderr']) fs.writeFileSync(path.join(root, `${name}.${stream}`), result[stream] ?? '', {flag: 'wx', mode: 0o600});
    save(root, `${name}.exit.json`, {program, args: args.map(arg => [env.P07_PARITY_CONFIG, env.P07_PARITY_KEY, env.P07_PARITY_KEYRING].includes(arg) ? '<private-path>' : arg), status: result.status, signal: result.signal, error: result.error?.message});
    assert.equal(result.status, 0, `${name} failed; private evidence retained`); return result.stdout;
  };
  const files = [...new Set(execute('source-files', 'git', ['ls-files', '-co', '--exclude-standard']).trim().split('\n'))].filter(file => /(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sources = () => files.map(file => ({file, sha256: digest(file)})), before = sources(); save(root, 'sources.json', before);
  const binaries = {go: path.join(root, 'go'), native: path.join(root, 'native'), checker: path.join(root, 'checker')};
  execute('build-go', 'go', ['build', '-pgo=off', '-o', binaries.go, './integration/p07/benchmark']);
  execute('build-native', 'gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', 'integration/p07/native_qualification.c', '-ldl', '-o', binaries.native]);
  execute('build-checker', 'go', ['build', '-pgo=off', '-o', binaries.checker, './tools/perf-mode-check']);
  const builds = Object.fromEntries(Object.entries(binaries).map(([key, file]) => [key, digest(file)]));
  assert.match(execute('build-info', 'go', ['version', '-m', binaries.go]), /CGO_ENABLED=0/); save(root, 'builds.json', builds);
  const library = fs.realpathSync('/lib64/librados.so.2'), libraryPin = digest(library); save(root, 'library.json', {library, sha256: libraryPin});
  const fsid = fs.readFileSync(env.P07_PARITY_FSID_FILE, 'utf8').trim(), monitors = fs.readFileSync(env.P07_PARITY_MONITORS_FILE, 'utf8').trim().split(/\s+/).map(host => `${host}:3300`).join(',');
  const ceph = (name, args) => JSON.parse(execute(name, 'ceph', ['-c', env.P07_PARITY_CONFIG, '-n', 'client.amakura', '-k', env.P07_PARITY_KEY, ...args, '--format', 'json']));
  const warnings = validateParityHealth(ceph('health-initial', ['status']), fsid), legs = [];
  let lastGoMode;
  try {
    for (const [cellIndex, cell] of plan.cells.entries()) for (const probe of plan.probes) for (let round = 1; round <= plan.rounds; round++) for (const implementation of round === 1 ? ['go', 'native'] : ['native', 'go']) {
      const name = `${cell.id}-${probe}-r${round}-${implementation}`, namespace = `${prefix}-c${cellIndex}-${probe}-r${round}-${implementation}`;
      assert.deepEqual(sources(), before); assert.equal(digest(library), libraryPin); assert.equal(digest(binaries[implementation]), builds[implementation]);
      const healthBefore = ceph(`${name}-health-before`, ['status']); assert.deepEqual(validateParityHealth(healthBefore, fsid), warnings);
      const placement = Array.from({length: cell.concurrency}, (_, worker) => ({worker, before: ceph(`${name}-w${worker}-before`, ['osd', 'map', cell.pool, fixtureName(cell, worker), namespace])}));
      const captureFile = path.join(root, `${name}.capture.json`), modeFile = path.join(root, `${name}.modes.${implementation === 'go' ? 'json' : 'log'}`);
      const extra = {P07_PGO_DIAGNOSTIC: '1', P07_PARITY_NAMESPACE: namespace, P07_MATRIX_SIZE: String(cell.size), P07_MATRIX_CONCURRENCY: String(cell.concurrency), P07_MATRIX_WORKLOAD: cell.workload,
        P07_QUALIFICATION_FILE: captureFile, P07_QUALIFICATION_ROUND: String(round), P07_QUALIFICATION_LEG: name, P07_QUALIFICATION_SEED: '1001'};
      const args = implementation === 'go' ? ['-monitors', monitors, '-fsid', fsid, '-key-file', env.P07_PARITY_KEY, '-entity', 'client.amakura', '-pool', cell.pool, '-transport', 'secure'] : [env.P07_PARITY_CONFIG, env.P07_PARITY_KEYRING, cell.pool, 'secure', 'client.amakura'];
      if (implementation === 'go') { extra.P07_READ_INTO = '1'; extra.P07_MODE_EVIDENCE_FILE = modeFile; } else extra.P07_NATIVE_MODE_LOG = modeFile;
      const target = ['taskset', '-c', plan.affinity, binaries[implementation], ...args];
      if (probe === 'strace') execute(name, 'strace', ['-ff', '-ttt', '-T', '-yy', '-e', 'trace=socket,connect,close,read,write,sendto,recvfrom,sendmsg,recvmsg,epoll_wait,epoll_pwait,epoll_ctl,futex', '-e', 'raw=read,write,sendto,recvfrom,sendmsg,recvmsg', '-o', path.join(root, `${name}.trace`), ...target], extra);
      else if (probe === 'perf') {
        execute(name, 'perf', ['record', '-q', '-F', '199', '-e', 'cpu-clock', '-e', 'context-switches/period=1/', '-g', '--clockid', 'realtime', '-o', path.join(root, `${name}.perf`), '--', ...target], extra);
        execute(`${name}-script`, 'perf', ['script', '-i', path.join(root, `${name}.perf`), '--ns', '--max-stack', '1', '-F', 'comm,pid,tid,time,period,event,ip,sym,dso']);
        for (const file of [`${name}.stderr`, `${name}-script.stderr`]) assert(!/lost|truncat|failed|error/i.test(fs.readFileSync(path.join(root, file), 'utf8')), 'perf loss or failure diagnostic');
      } else execute(name, target[0], target.slice(1), extra);
      const capture = JSON.parse(fs.readFileSync(captureFile)), metrics = validatePGOCapture(capture, cell, {round, leg: name, seed: 1001}, implementation), window = phaseWindow(capture);
      if (implementation === 'go') lastGoMode = modeFile;
      else execute(`${name}-mode-check`, binaries.checker, ['-go', lastGoMode, '-native-log', modeFile, '-requested', 'secure', '-native-out', path.join(root, `${name}.modes.json`)]);
      checkModes(JSON.parse(fs.readFileSync(path.join(root, `${name}.modes.json`))), implementation);
      const raw = fs.readdirSync(root).filter(file => file.startsWith(`${name}.trace.`) || file === `${name}-script.stdout` || file === `${name}.perf` || probe === 'perf' && [ `${name}.stderr`, `${name}-script.stderr` ].includes(file)).sort();
      const attribution = probe === 'strace' ? parseSyscalls(raw.map(file => fs.readFileSync(path.join(root, file), 'utf8')), window) : probe === 'perf' ? parsePerf(fs.readFileSync(path.join(root, `${name}-script.stdout`), 'utf8'), window) : null;
      const healthAfter = ceph(`${name}-health-after`, ['status']); assert.deepEqual(validateParityHealth(healthAfter, fsid), warnings);
      for (const value of placement) { value.after = ceph(`${name}-w${value.worker}-after`, ['osd', 'map', cell.pool, fixtureName(cell, value.worker), namespace]); assert(value.before.pgid && value.before.acting?.length); for (const key of ['pgid', 'acting', 'acting_primary']) assert.deepEqual(value.before[key], value.after[key]); }
      assert.deepEqual(sources(), before); assert.equal(digest(library), libraryPin); assert.equal(digest(binaries[implementation]), builds[implementation]);
      const reference = file => ({file, sha256: digest(path.join(root, file))});
      const leg = {name, implementation, probe, round, cell, namespace, metrics, operations: capture.attempt.measured.successful_operations,
        capture: reference(path.basename(captureFile)), modes: reference(`${name}.modes.json`), raw: raw.map(reference), attribution,
        health: {before: healthBefore, after: healthAfter}, placement, binary_sha256: builds[implementation]};
      legs.push(leg); save(root, `${name}.leg.json`, leg); console.log(`${name} verified (${leg.operations} operations)`);
    }
    save(root, 'summary.json', {status: 'syscall_attribution_complete_unqualified', plan_id: plan.plan_id, fsid, warnings, legs});
  } catch (error) { save(root, 'failure.json', {error: error.message, completed_legs: legs.length}); throw error; }
}

export function reproduceSyscalls(root, output) {
  const plan = JSON.parse(fs.readFileSync(path.join(root, 'plan.json'))), summary = JSON.parse(fs.readFileSync(path.join(root, 'summary.json'))), sources = JSON.parse(fs.readFileSync(path.join(root, 'sources.json'))), builds = JSON.parse(fs.readFileSync(path.join(root, 'builds.json')));
  assert.deepEqual(plan, syscallPlan()); assert.equal(summary.plan_id, plan.plan_id); assert.equal(summary.status, 'syscall_attribution_complete_unqualified');
  validateAttributionPopulation(plan, summary.legs);
  for (const file of dependencies) assert.equal(sources.find(value => value.file === file)?.sha256, digest(file), 'executing attribution dependency pin');
  assert.equal(summary.legs.length, plan.cells.length * plan.probes.length * plan.rounds * 2, 'complete attribution population');
  const seen = new Set(), load = reference => { assert(/^[a-z0-9.-]+$/.test(reference.file) && !['.', '..'].includes(reference.file)); const file = path.join(root, reference.file); assert(fs.lstatSync(file).isFile() && !fs.lstatSync(file).isSymbolicLink()); assert.equal(digest(file), reference.sha256); return fs.readFileSync(file, 'utf8'); };
  for (const leg of summary.legs) {
    const cell = plan.cells.find(cell => cell.id === leg.cell.id); assert.deepEqual(leg.cell, cell); assert(plan.probes.includes(leg.probe)); assert([1, 2].includes(leg.round)); assert(['go', 'native'].includes(leg.implementation));
    const key = `${cell.id}-${leg.probe}-r${leg.round}-${leg.implementation}`; assert.equal(leg.name, key); assert(!seen.has(key)); seen.add(key);
    assert.equal(leg.binary_sha256, builds[leg.implementation]); assert.equal(digest(path.join(root, leg.implementation)), leg.binary_sha256);
    const capture = JSON.parse(load(leg.capture)); assert.deepEqual(validatePGOCapture(capture, cell, {round: leg.round, leg: leg.name, seed: 1001}, leg.implementation), leg.metrics);
    assert.equal(leg.operations, capture.attempt.measured.successful_operations, 'measured population binding');
    checkModes(JSON.parse(load(leg.modes)), leg.implementation);
    assert.deepEqual(validateParityHealth(leg.health.before, summary.fsid), summary.warnings); assert.deepEqual(validateParityHealth(leg.health.after, summary.fsid), summary.warnings);
    assert.equal(leg.placement.length, cell.concurrency); for (const [worker, value] of leg.placement.entries()) { assert.equal(value.worker, worker); assert(value.before.pgid && value.before.acting?.length); for (const key of ['pgid', 'acting', 'acting_primary']) assert.deepEqual(value.before[key], value.after[key]); }
    const window = phaseWindow(capture); for (const reference of leg.raw) load(reference);
    const expected = fs.readdirSync(root).filter(file => file.startsWith(`${leg.name}.trace.`) || file === `${leg.name}-script.stdout` || file === `${leg.name}.perf` || leg.probe === 'perf' && [ `${leg.name}.stderr`, `${leg.name}-script.stderr` ].includes(file)).sort();
    assert.deepEqual(leg.raw.map(value => value.file).sort(), expected, 'complete owned probe artifact population');
    if (leg.probe === 'perf') for (const reference of leg.raw.filter(value => value.file.endsWith('.stderr'))) assert(!/lost|truncat|failed|error/i.test(load(reference)), 'perf loss or failure diagnostic');
    const actual = leg.probe === 'strace' ? parseSyscalls(leg.raw.filter(value => value.file.includes('.trace.')).map(load), window) : leg.probe === 'perf' ? parsePerf(load(leg.raw.find(value => value.file.endsWith('-script.stdout'))), window) : null;
    assert.deepEqual(actual, leg.attribution, 'recomputed attribution');
  }
  fs.writeFileSync(output, JSON.stringify(summary, null, 2) + '\n', {flag: 'wx', mode: 0o600}); return summary;
}

export function validateAttributionPopulation(plan, legs) {
  assert.equal(legs.length, plan.cells.length * plan.probes.length * plan.rounds * 2, 'complete attribution population');
  const processes = new Set(), namespaces = new Set();
  for (const leg of legs) {
    assert(plan.cells.some(cell => cell.id === leg.cell.id) && plan.probes.includes(leg.probe) && Number.isInteger(leg.round) && leg.round >= 1 && leg.round <= plan.rounds && ['go', 'native'].includes(leg.implementation), 'planned attribution leg');
    assert.equal(leg.name, `${leg.cell.id}-${leg.probe}-r${leg.round}-${leg.implementation}`, 'planned process binding');
    assert(!processes.has(leg.name), 'independent attribution process'); processes.add(leg.name);
    assert(typeof leg.namespace === 'string' && /^p07-parity-[a-z0-9-]+$/.test(leg.namespace) && !namespaces.has(leg.namespace), 'independent attribution namespace'); namespaces.add(leg.namespace);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  if (process.argv[2] === 'analyze') { assert.equal(process.argv.length, 5); reproduceSyscalls(path.resolve(process.argv[3]), path.resolve(process.argv[4])); }
  else { assert.equal(process.argv.length, 4); runSyscalls(path.resolve(process.argv[2]), process.argv[3]); }
}