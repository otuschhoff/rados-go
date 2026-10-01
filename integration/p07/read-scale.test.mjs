import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const harness = fileURLToPath(new URL('./reproduce.sh', import.meta.url));
const source = fs.readFileSync(harness, 'utf8');
const cleanEnv = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('P07_')));
const functionStart = source.indexOf('\t\tscheduler_run() {');
const loopStart = source.indexOf('\t\tfor repeat in 1 2 3 4 5;', functionStart);
const auditStart = source.indexOf('\t\tgit ls-files', loopStart);

function directory(context) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'p07-read-scale-test-'));
  context.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
}

test('invalid scale modes and shapes fail before Docker discovery', context => {
  const root = directory(context);
  const docker = path.join(root, 'docker');
  fs.writeFileSync(docker, '#!/bin/sh\nprintf "unexpected Docker invocation\\n" >&2\nexit 99\n', { mode: 0o700 });
  const valid = { P07_READ_SCALE: '1', P07_SCHEDULER_SWEEP: '1', P07_SWEEP_FIXED_PROCS: '10', P07_DIAGNOSTIC_DIR: path.join(root, 'capture') };
  const invalid = [
    { P07_SCHEDULER_SWEEP: '' }, { P07_SCHEDULER_SWEEP: '0' },
    { P07_READ_SCALE: '0' }, { P07_SWEEP_FIXED_PROCS: '' }, { P07_SWEEP_FIXED_PROCS: '8' },
    { P07_DIAGNOSTIC_DIR: '' }, { P07_DIAGNOSTIC_DIR: 'relative' }, { P07_DIAGNOSTIC_DIR: root },
    { P07_READ_PROFILE: '1' }, { P07_INVENTORY_SWEEP: '1' },
    { P07_DEFAULT_SCRATCH_CONFIRM: '1' }, { P07_READ_INTO_COMPARE: '1' },
    { P07_RESOURCE_DIAGNOSTIC_DIR: root }, { P07_MODE_DIAGNOSTIC_DIR: root },
    { P07_READ_SIZE: '65537' }, { P07_READ_CONCURRENCY: '65' },
    { P07_READ_SCALE: '', P07_SCHEDULER_SWEEP: '', P07_SWEEP_FIXED_PROCS: '', P07_DIAGNOSTIC_DIR: '', P07_READ_SIZE: '4096' },
    { P07_READ_SCALE: '', P07_SCHEDULER_SWEEP: '', P07_SWEEP_FIXED_PROCS: '', P07_DIAGNOSTIC_DIR: '', P07_READ_CONCURRENCY: '32' },
  ];
  for (const overrides of invalid) {
    const result = spawnSync('sh', [harness], { encoding: 'utf8', env: { ...cleanEnv, ...valid, ...overrides, PATH: `${root}:${cleanEnv.PATH}` } });
    assert.equal(result.status, 2, JSON.stringify(overrides) + result.stderr);
    assert.doesNotMatch(result.stderr, /unexpected Docker invocation/);
  }
  assert.equal(fs.existsSync(valid.P07_DIAGNOSTIC_DIR), false);
});

test('valid diagnostic shapes pass scale preflight without reaching Docker', context => {
  const root = directory(context);
  const preflight = source.slice(0, source.indexOf('\ncleanup() {'));
  for (const size of ['4096', '65536', '1048576', '4194304']) {
    const capture = path.join(root, size);
    const result = spawnSync('sh', ['-c', preflight, harness], { encoding: 'utf8', env: { ...cleanEnv, P07_READ_SCALE: '1', P07_SCHEDULER_SWEEP: '1', P07_SWEEP_FIXED_PROCS: '10', P07_DIAGNOSTIC_DIR: capture, P07_READ_SIZE: size, P07_READ_CONCURRENCY: '32' } });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(fs.readFileSync(path.join(capture, 'scheduler-status'), 'utf8'), 'running\n');
    assert.equal(fs.statSync(capture).mode & 0o777, 0o700);
  }
});

test('scale seeds sixty-four selected-size objects and isolates native defaults', context => {
  const root = directory(context);
  const start = source.indexOf('\tif test "$read_size" != 65536; then');
  const end = source.indexOf('\tfor leg in $initial_legs;', start);
  for (const size of ['4096', '65536', '1048576', '4194304']) {
    const log = path.join(root, `docker-${size}`);
    const script = `set -eu
read_size=${size}; read_concurrency=16; platform=unused; network=unused; temporary=unused; image=unused; fsid=unused
docker() { printf '<%s>' "$@" >>"$LOG"; printf '\\n' >>"$LOG"; printf '{"seeded":true}\\n'; }
${source.slice(start, end)}
test -z "$initial_legs"`;
    const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, LOG: log, P07_READ_SCALE: '1', P07_DIAGNOSTIC_DIR: root } });
    assert.equal(result.status, 0, result.stderr);
    const invocations = fs.readFileSync(log, 'utf8').trim().split('\n');
    assert.equal(invocations.length, size === '65536' ? 1 : 2);
    assert.match(invocations.at(-1), /<P07_READ_CONCURRENCY=64>/);
    assert.ok(invocations.at(-1).includes(`<P07_READ_SIZE=${size}>`));
    if (size !== '65536') assert.doesNotMatch(invocations[0], /P07_READ_(SIZE|CONCURRENCY)/);
  }
});

test('scale emits exactly thirty alternating Go legs, ten native context legs and no traces', context => {
  const root = directory(context);
  for (const size of ['4096', '65536', '1048576', '4194304']) {
    const script = `set -eu
read_size=${size}; read_concurrency=16
scheduler_run() { printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' "$1" "$2" "$3" "$4" "\${5:-}" "\${6:-}" "\${7:-}" "\${8:-}" "\${9:-}" "\${10:-}" "\${11:-}"; }
${source.slice(loopStart, auditStart)}`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, P07_READ_SCALE: '1', P07_DIAGNOSTIC_DIR: root } });
  assert.equal(result.status, 0, result.stderr);
  const methodology = JSON.parse(fs.readFileSync(path.join(root, 'scale-methodology.json'), 'utf8'));
  assert.equal(methodology.matched_native, false);
  assert.equal(methodology.benchmark_claim, false);
  assert.equal(methodology.go.size_bytes, Number(size));
  assert.equal(methodology.go.measured_legs, 30);
  assert.deepEqual(methodology.native_context, { size_bytes: 65536, concurrency: 16, operations: 4096, scope: 'shorter unmatched context' });
    const legs = result.stdout.trim().split('\n').map(line => line.split('\t'));
    const go = legs.filter(leg => leg[1] === 'go');
    assert.equal(go.length, 30);
    assert.equal(legs.filter(leg => leg[1] === 'native').length, 10);
    for (let repeat = 1; repeat <= 5; repeat++) {
      const sample = go.filter(leg => leg[0].startsWith(`scale-${repeat}-`));
      const order = repeat % 3 === 1 ? [16, 32, 64] : repeat % 3 === 2 ? [32, 64, 16] : [64, 16, 32];
      assert.deepEqual(sample.map(leg => Number(leg[9])), order.flatMap(concurrency => [concurrency, concurrency]));
      assert.deepEqual(sample.map(leg => Number(leg[5])), repeat % 2 ? [4, 8, 4, 8, 4, 8] : [8, 4, 8, 4, 8, 4]);
      for (const leg of sample) {
        assert.equal(leg[2], '10'); assert.equal(leg[3], 'no'); assert.equal(leg[4], '1');
        assert.equal(leg[6], leg[9]); assert.equal(leg[7], '1024'); assert.equal(leg[10], size);
      }
    }
  }
  assert.match(source, /if test "\$\{P07_READ_SCALE:-\}" = 1; then initial_legs=''; fi/);
});

test('legacy profile branch retains five baselines plus CPU, allocation and trace legs', () => {
  const script = `set -eu
read_size=65536; read_concurrency=16
scheduler_run() { printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' "$1" "$2" "$3" "$4" "\${5:-}" "\${6:-}" "\${7:-}" "\${8:-}" "\${9:-}"; }
${source.slice(loopStart, auditStart)}`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, P07_READ_PROFILE: '1' } });
  assert.equal(result.status, 0, result.stderr);
  const legs = result.stdout.trim().split('\n').map(line => line.split('\t')).filter(leg => leg[1] === 'go');
  assert.deepEqual(legs.map(leg => leg[0]), ['profile-baseline-1', 'profile-baseline-2', 'profile-baseline-3', 'profile-baseline-4', 'profile-baseline-5', 'profile-cpu', 'profile-allocs', 'profile-trace']);
  for (const leg of legs) {
    assert.equal(leg[2], '10'); assert.equal(leg[4], '1'); assert.equal(leg[5], '');
    assert.equal(leg[6], '16'); assert.equal(leg[7], '1024');
  }
  assert.equal(legs.at(-1)[3], 'yes');
  assert.equal(legs.at(-3)[8], 'cpu'); assert.equal(legs.at(-2)[8], 'allocs');
});

test('artifact checksums tolerate an absent trace glob', context => {
  const root = directory(context);
  for (const name of ['benchmark', 'native-benchmark', 'sample.json', 'source-before.sha256']) fs.writeFileSync(path.join(root, name), name);
  const start = source.indexOf('\t\t(cd "$P07_DIAGNOSTIC_DIR" && shasum');
  const end = source.indexOf('\n\t\tif test "${P07_READ_PROFILE', start);
  const result = spawnSync('sh', ['-ec', source.slice(start, end)], { encoding: 'utf8', env: { ...cleanEnv, P07_DIAGNOSTIC_DIR: root } });
  assert.equal(result.status, 0, result.stderr);
  assert.doesNotMatch(fs.readFileSync(path.join(root, 'artifacts.sha256'), 'utf8'), /\.trace/);
});

test('scheduler validates shapes, API, counters and native context without Docker', context => {
  const root = directory(context);
  const base = { implementation: 'go', transport: 'secure', environment: { gomaxprocs: 10 }, rows: [{ operations: 65536, size_bytes: 65536, concurrency: 64, workload: 'read' }], diagnostic: { warmup_operations: 512, operations_per_worker: 1024, read_api: 'read_into', scratch_slots: 8, admission_window: 64, scratch: { hits: 66000, misses: 48, bypasses: 0 } } };
  function run(report, args) {
    const script = `set -eu
P07_DIAGNOSTIC_DIR="$ROOT"; temporary="$ROOT/absent"; image=unused; platform=unused; network=unused
read_size=65536; read_concurrency=16
docker() { printf '%s\\n' "$@" >"$ROOT/docker-args"; printf '%s\\n' "$REPORT"; }
${source.slice(functionStart, loopStart)}
scheduler_run ${args}`;
    return spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, ROOT: root, REPORT: JSON.stringify(report) } });
  }
  const args = 'sample go 10 no 1 8 64 1024 "" 64 65536';
  assert.equal(run(base, args).status, 0);
  const invocation = fs.readFileSync(path.join(root, 'docker-args'), 'utf8');
  assert.doesNotMatch(invocation, /^P07_READ_(SIZE|CONCURRENCY)=/m);
  assert.match(invocation, /if test "\$implementation" = go; then\n\s*export P07_READ_CONCURRENCY="\$4" P07_READ_SIZE="\$5"/);
  for (const mutate of [report => report.rows[0].size_bytes = 4096, report => report.rows[0].concurrency = 16, report => report.rows[0].operations = 4096, report => report.diagnostic.warmup_operations = 128, report => report.diagnostic.read_api = 'read', report => report.diagnostic.scratch.hits--]) {
    const report = structuredClone(base); mutate(report);
    assert.notEqual(run(report, args).status, 0);
  }
  for (const size of [4096, 1048576, 4194304]) {
    const report = structuredClone(base);
    report.rows[0].size_bytes = size;
    report.diagnostic.scratch = { hits: 0, misses: 0, bypasses: 66048 };
    assert.equal(run(report, `sample go 10 no 1 8 64 1024 "" 64 ${size}`).status, 0);
  }
  const native = { implementation: 'native', transport: 'secure', rows: [{ operations: 4096, size_bytes: 65536, concurrency: 16, workload: 'read' }] };
  assert.equal(run(native, 'context native 10 no 1 8 64 1024 "" 64 4096').status, 0);
  native.rows[0].size_bytes = 4096;
  assert.notEqual(run(native, 'context native 10 no').status, 0);
});