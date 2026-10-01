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
const helperStart = source.indexOf('\t\t\toffered_run() {');
const helperEnd = source.indexOf('\n\t\t\tif test "${P07_OFFERED_OBSERVE:-}" = 1;', helperStart);
const loopStart = source.indexOf('\t\t\tfor repeat in 1 2 3 4 5;', helperStart);
const historicalLoopEnd = source.indexOf('\n\t\t\telse', loopStart);
const loopEnd = source.indexOf('\n\t\t\ttest "$offered_failed"', loopStart);

function directory(context) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'p07-offered-test-'));
  context.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
}

test('offered mode rejects competing modes, orphan limits and non-fixed shape before Docker', context => {
  const root = directory(context);
  fs.writeFileSync(path.join(root, 'docker'), '#!/bin/sh\nprintf "unexpected Docker invocation\\n" >&2\nexit 99\n', { mode: 0o700 });
  const valid = { P07_OFFERED_LOAD: '1', P07_SCHEDULER_SWEEP: '1', P07_SWEEP_FIXED_PROCS: '10', P07_DIAGNOSTIC_DIR: path.join(root, 'fresh') };
  const invalid = [
    { P07_OFFERED_LOAD: '0' }, { P07_SCHEDULER_SWEEP: '' }, { P07_SWEEP_FIXED_PROCS: '8' },
    { P07_DIAGNOSTIC_DIR: '' }, { P07_DIAGNOSTIC_DIR: 'relative' }, { P07_DIAGNOSTIC_DIR: root },
    ...['P07_READ_PROFILE', 'P07_READ_SCALE', 'P07_INVENTORY_SWEEP', 'P07_DEFAULT_SCRATCH_CONFIRM', 'P07_READ_INTO_COMPARE', 'P07_RESOURCE_DIAGNOSTIC_DIR', 'P07_MODE_DIAGNOSTIC_DIR', 'P07_BACKGROUND_ALLOCATIONS', 'P07_TIMING_FILE', 'P07_SCRATCH_SLOTS', 'P07_ADMISSION_WINDOW', 'P07_OPERATIONS_PER_WORKER', 'P07_REPORT'].map(name => ({ [name]: '1' })),
    { P07_BACKGROUND_WORKERS: '8' }, { P07_READ_SIZE: '4096' }, { P07_READ_CONCURRENCY: '32' },
    { P07_OFFERED_RATE: '1000' }, { P07_FIXED_ALLOC_RATE: '100' },
    { P07_OFFERED_LOAD: '', P07_OFFERED_RATE: '1000' }, { P07_OFFERED_LOAD: '', P07_FIXED_ALLOC_RATE: '100' },
    { P07_OFFERED_FACTORIAL: '0' }, { P07_OFFERED_FACTORIAL: '2' },
    { P07_OFFERED_LOAD: '', P07_OFFERED_FACTORIAL: '1' },
    { P07_OFFERED_CASE: 'none' }, { P07_OFFERED_FACTORIAL: '1', P07_OFFERED_CASE: 'both' },
    { P07_OFFERED_OBSERVATION_DIR: '/tmp/observation' }, { P07_OFFERED_OBSERVATION_LABEL: 'instrumented' },
    { P07_OFFERED_LOAD: '', P07_OFFERED_OBSERVATION_DIR: '/tmp/observation' },
    { P07_OFFERED_LOAD: '', P07_OFFERED_OBSERVATION_LABEL: 'instrumented' },
    { P07_OFFERED_OBSERVE: '0' }, { P07_OFFERED_OBSERVE: '2', P07_OFFERED_FACTORIAL: '1' },
    { P07_OFFERED_OBSERVE: '1' }, { P07_OFFERED_OBSERVE: '1', P07_OFFERED_FACTORIAL: '1', P07_OFFERED_LOAD: '' },
    { P07_OFFERED_OBSERVE: '1', P07_OFFERED_FACTORIAL: '1', P07_SWEEP_FIXED_PROCS: '8' },
    { P07_OFFERED_OBSERVE: '1', P07_OFFERED_FACTORIAL: '1', P07_OFFERED_OBSERVATION_LABEL: 'instrumented' },
    { P07_OFFERED_OBSERVE: '1', P07_OFFERED_FACTORIAL: '1', P07_OFFERED_OBSERVATION_DIR: '/tmp/observation' },
  ];
  for (const overrides of invalid) {
    const result = spawnSync('sh', [harness], { encoding: 'utf8', env: { ...cleanEnv, ...valid, ...overrides, PATH: `${root}:${cleanEnv.PATH}` } });
    assert.equal(result.status, 2, JSON.stringify(overrides) + result.stderr);
    assert.doesNotMatch(result.stderr, /unexpected Docker invocation/);
  }
  assert.equal(fs.existsSync(valid.P07_DIAGNOSTIC_DIR), false);
});

test('offered preflight requires a fresh directory and preserves production build selection', context => {
  const root = directory(context);
  const capture = path.join(root, 'fresh');
  const preflight = source.slice(0, source.indexOf('\ncleanup() {'));
  const result = spawnSync('sh', ['-c', preflight, harness], { encoding: 'utf8', env: { ...cleanEnv, P07_OFFERED_LOAD: '1', P07_SCHEDULER_SWEEP: '1', P07_SWEEP_FIXED_PROCS: '10', P07_DIAGNOSTIC_DIR: capture } });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(fs.statSync(capture).mode & 0o777, 0o700);
  assert.equal(fs.readFileSync(path.join(capture, 'scheduler-status'), 'utf8'), 'running\n');
  assert.match(source, /if test "\$\{P07_INVENTORY_SWEEP:-\}" = 1 \|\| test "\$\{P07_READ_SCALE:-\}" = 1; then\n\s*compile .*go build -tags p12diagnostics/);
  assert.match(source, /elif test "\$\{P07_OFFERED_LOAD:-\}" = 1; then\n\s*compile .*GOFLAGS=-mod=readonly GOENV=off GOEXPERIMENT='' GOWORK=off go build -trimpath/);
  assert.match(source, /if test "\$\{P07_OFFERED_LOAD:-\}" = 1; then initial_legs=''; fi/);
});

test('offered matrix contains fifteen rotated legs and no native or traced legs', () => {
  const script = `set -eu\noffered_run() { printf '%s %s\\n' "$1" "$2"; }\n${source.slice(loopStart, historicalLoopEnd)}`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: cleanEnv });
  assert.equal(result.status, 0, result.stderr);
  const lines = result.stdout.trim().split('\n');
  assert.equal(lines.length, 15);
  for (let repeat = 1; repeat <= 5; repeat++) {
    const rates = repeat % 3 === 1 ? [1000, 2000, 4000] : repeat % 3 === 2 ? [2000, 4000, 1000] : [4000, 1000, 2000];
    assert.deepEqual(lines.slice((repeat - 1) * 3, repeat * 3), rates.map(rate => `offered-${repeat}-rate-${rate} ${rate}`));
  }
});

test('factorial matrix contains sixty rotated primary legs and preserves historical branch', () => {
  const script = `set -eu\noffered_run() { printf '%s %s %s\\n' "$1" "$2" "\${3:-both}"; }\n${source.slice(helperEnd, loopEnd)}`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, P07_OFFERED_FACTORIAL: '1' } });
  assert.equal(result.status, 0, result.stderr);
  const lines = result.stdout.trim().split('\n');
  assert.equal(lines.length, 60);
  assert.equal(new Set(lines).size, 60);
  const cases = ['none', 'cpu', 'alloc', 'both'];
  const rates = [1000, 2000, 4000];
  for (let repeat = 1; repeat <= 5; repeat++) {
    const rateOrder = [...rates.slice((repeat - 1) % 3), ...rates.slice(0, (repeat - 1) % 3)];
    const caseOrder = [...cases.slice((repeat - 1) % 4), ...cases.slice(0, (repeat - 1) % 4)];
    assert.deepEqual(lines.slice((repeat - 1) * 12, repeat * 12), rateOrder.flatMap(rate => caseOrder.map(loadCase => `factorial-${repeat}-case-${loadCase}-rate-${rate} ${rate} ${loadCase}`)));
  }
  const historical = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: cleanEnv });
  assert.equal(historical.status, 0, historical.stderr);
  assert.equal(historical.stdout.trim().split('\n').length, 15);
});

test('observation matrix contains twelve rotated instrumented legs at rate 2000', () => {
  const script = `set -eu\noffered_run() { printf '%s %s %s\\n' "$1" "$2" "$3"; }\n${source.slice(helperEnd, loopEnd)}`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, P07_OFFERED_FACTORIAL: '1', P07_OFFERED_OBSERVE: '1' } });
  assert.equal(result.status, 0, result.stderr);
  const lines = result.stdout.trim().split('\n');
  assert.equal(lines.length, 12);
  assert.equal(new Set(lines).size, 12);
  const cases = ['none', 'cpu', 'alloc', 'both'];
  for (let repeat = 1; repeat <= 3; repeat++) {
    const order = [...cases.slice(repeat - 1), ...cases.slice(0, repeat - 1)];
    assert.deepEqual(lines.slice((repeat - 1) * 4, repeat * 4), order.map(loadCase => `observed-${repeat}-case-${loadCase}-rate-2000 2000 ${loadCase}`));
  }
});

function fixture(failed) {
  return {
    implementation: 'go', transport: 'secure', environment: { gomaxprocs: 10 },
    diagnostic: { read_api: 'read_into', scratch_slots: 4, warmup_operations: 128, background_cpu_workers: 8 },
    rows: [{ operations: 8000, size_bytes: 65536, concurrency: 16 }],
    offered_load: {
      config: { instrumented: false, rate_ops_per_second: 1000, issuance_window_ns: 8000000000, deadline_ns: 500000000, queue_capacity: 128, workers: 16 },
      expected: 8000, success: failed ? 7999 : 8000, timeouts: failed ? 1 : 0, overload: 0, errors: 0, canceled: 0,
      outcomes: Array.from({ length: 8000 }, (_, index) => ({ kind: failed && index === 7999 ? 'timeout' : 'success' })), failed, delivery_invalid: false, slo_failures: failed ? 1 : 0,
      background: { expected: 6400, completed: 6400, missed: 0, delivery_invalid: false },
    },
  };
}

function factorialFixture(loadCase, rate) {
  const report = fixture(true);
  const cpu = ['cpu', 'both'].includes(loadCase) ? 8 : 0;
  const alloc = ['alloc', 'both'].includes(loadCase) ? 8 : 0;
  if (cpu > 0) report.diagnostic.background_cpu_workers = cpu;
  else delete report.diagnostic.background_cpu_workers;
  if (alloc > 0) report.diagnostic.background_allocations = true;
  report.rows[0].operations = rate * 8;
  Object.assign(report.offered_load.config, { factorial: true, load_case: loadCase, cpu_workers: cpu, allocation_workers: alloc, rate_ops_per_second: rate });
  Object.assign(report.offered_load, { expected: rate * 8, success: rate * 8 - 1, outcomes: Array.from({ length: rate * 8 }, (_, index) => ({ kind: index === rate * 8 - 1 ? 'timeout' : 'success' })) });
  Object.assign(report.offered_load.background, { cpu_workers: cpu, allocation_workers: alloc, expected: alloc * 800, completed: alloc * 800, workers: Array.from({ length: cpu + alloc }, () => ({})), hash_iterations: cpu * 3, warmup_hash_iterations: cpu, window_hash_iterations: cpu, drain_hash_iterations: cpu });
  return report;
}

test('all sixty failed factorial legs retain artifacts and case-specific validation continues', context => {
  const root = directory(context);
  fs.mkdirSync(path.join(root, 'work'));
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
    for (const rate of [1000, 2000, 4000]) fs.writeFileSync(path.join(root, `fixture-${loadCase}-${rate}`), JSON.stringify(factorialFixture(loadCase, rate)));
  }
  const script = `set -eu
P07_DIAGNOSTIC_DIR="$ROOT"; temporary="$ROOT/work"; platform=unused; network=unused; image=unused; offered_failed=0
ceph_cli() { printf '{}\\n'; }
docker() {
  printf '%s\\n' "$@" >"$ROOT/$label-docker-args"
  printf 'synthetic failure\\n' >&2
  cat "$ROOT/fixture-$load_case-$rate"
  for entry in cpu.max cpu.stat cpuset.cpus.effective memory.max; do
    printf before >"$temporary/$label-$entry-before"
    printf after >"$temporary/$label-$entry-after"
  done
  printf '7\\n' >"$temporary/$label-benchmark-exit-code"
  return 7
}
${source.slice(helperStart, loopEnd)}
test "$offered_failed" -eq 60`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, ROOT: root, P07_OFFERED_FACTORIAL: '1' } });
  assert.equal(result.status, 0, result.stderr);
  for (let repeat = 1; repeat <= 5; repeat++) {
    for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
      for (const rate of [1000, 2000, 4000]) {
        const label = `factorial-${repeat}-case-${loadCase}-rate-${rate}`;
        assert.equal(JSON.parse(fs.readFileSync(path.join(root, `${label}.json`))).offered_load.config.load_case, loadCase);
        const stderr = fs.readFileSync(path.join(root, `${label}.stderr`), 'utf8');
        assert.equal(stderr, 'synthetic failure\n');
        assert.equal(fs.readFileSync(path.join(root, `${label}-exit-code`), 'utf8'), '7\n');
        assert.equal(fs.readFileSync(path.join(root, `${label}-capture-exit-code`), 'utf8'), '1\n');
        assert.equal(fs.readFileSync(path.join(root, `${label}-cpu.stat-after`), 'utf8'), 'after');
        assert.equal(fs.readFileSync(path.join(root, `${label}-benchmark-exit-code`), 'utf8'), '7\n');
        assert.ok(fs.existsSync(path.join(root, `${label}-osd-2-after.json`)));
        const args = fs.readFileSync(path.join(root, `${label}-docker-args`), 'utf8');
        assert.ok(args.includes('P07_OFFERED_FACTORIAL=1'));
        assert.ok(args.includes(`P07_OFFERED_CASE=${loadCase}`));
        assert.match(args, /P07_OFFERED_OBSERVATION_DIR=\n/);
        assert.match(args, /P07_OFFERED_OBSERVATION_LABEL=\n/);
        assert.doesNotMatch(args, /P07_(TIMING_FILE|TRACE_FILE|CPU_PROFILE)=/);
      }
    }
  }
});

for (const observe of [false, true]) test(`${observe ? 'instrumented' : 'factorial'} preflight freezes selected sources and audits snapshot without containers`, context => {
  const root = directory(context);
  const original = path.join(root, 'original'); fs.mkdirSync(original);
  const capture = path.join(root, 'capture');
  fs.writeFileSync(path.join(original, 'source.go'), 'package fixture\n');
  const prefix = source.slice(0, source.indexOf('\nfinalize() {')).replace(/root=\$\(CDPATH=.*\n/, 'root=$SOURCE_ROOT\n');
  const script = `git() { case "$1" in rev-parse) printf 'synthetic-head\\n' ;; ls-files) printf 'source.go\\0' ;; esac; }\n${prefix}
test "$root" != "$original_root"
cmp "$root/source.go" "$original_root/source.go"
test "$GOFLAGS" = -mod=readonly
test "$GOENV" = off
test "$GOWORK" = off
test -f "$P07_DIAGNOSTIC_DIR/workload-sources.tar.gz"`;
  const result = spawnSync('sh', ['-c', script, harness], { encoding: 'utf8', env: { ...cleanEnv, SOURCE_ROOT: original, P07_OFFERED_LOAD: '1', P07_OFFERED_FACTORIAL: '1', P07_OFFERED_OBSERVE: observe ? '1' : '', P07_SCHEDULER_SWEEP: '1', P07_SWEEP_FIXED_PROCS: '10', P07_DIAGNOSTIC_DIR: capture } });
  assert.equal(result.status, 0, result.stderr);
  const status = JSON.parse(fs.readFileSync(path.join(capture, 'final-status.json')));
  assert.equal(status.source_check, 'unchanged');
  assert.equal(status.measurement, observe ? 'instrumented' : 'primary');
  const methodology = fs.readFileSync(path.join(capture, 'offered-methodology.txt'), 'utf8');
  assert.match(methodology, observe ? /instrumented; 12 legs.*excluded from primary comparisons.*normal untagged binary/ : /60 primary legs.*no runtime observation/);
  assert.match(fs.readFileSync(path.join(capture, 'source-snapshot-check.txt'), 'utf8'), /source.go: OK/);
  assert.match(fs.readFileSync(path.join(capture, 'artifacts.sha256'), 'utf8'), /workload-sources.tar.gz/);
});

test('factorial helper accepts encoder omissions and rejects explicit wrong values in all four cases', context => {
  const root = directory(context);
  fs.mkdirSync(path.join(root, 'work'));
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
    const report = factorialFixture(loadCase, 1000);
    Object.assign(report.offered_load, { success: 8000, timeouts: 0, failed: false, slo_failures: 0 });
    report.offered_load.outcomes.at(-1).kind = 'success';
    fs.writeFileSync(path.join(root, `fixture-${loadCase}`), JSON.stringify(report));
    assert.equal(Object.hasOwn(report.diagnostic, 'background_cpu_workers'), ['cpu', 'both'].includes(loadCase));
    assert.equal(Object.hasOwn(report.diagnostic, 'background_allocations'), ['alloc', 'both'].includes(loadCase));
    for (const field of ['background_cpu_workers', 'background_allocations']) {
      const wrong = structuredClone(report);
      wrong.diagnostic[field] = field === 'background_cpu_workers'
        ? (report.diagnostic[field] === 8 ? 0 : 8)
        : !report.diagnostic[field];
      fs.writeFileSync(path.join(root, `fixture-${loadCase}-${field}`), JSON.stringify(wrong));
    }
  }
  const bad = factorialFixture('none', 1000);
  Object.assign(bad.offered_load, { success: 8000, timeouts: 0, failed: false, slo_failures: 0 });
  bad.offered_load.outcomes.at(-1).kind = 'success';
  bad.offered_load.config.cpu_workers = 8;
  fs.writeFileSync(path.join(root, 'fixture-bad'), JSON.stringify(bad));
  const script = `set -eu
P07_DIAGNOSTIC_DIR="$ROOT"; temporary="$ROOT/work"; platform=unused; network=unused; image=unused; offered_failed=0
ceph_cli() { printf '{}\\n'; }
docker() { cat "$ROOT/fixture-$fixture_case"; }
${source.slice(helperStart, helperEnd)}
for fixture_case in none cpu alloc both; do offered_run "success-$fixture_case" 1000 "$fixture_case"; done
test "$offered_failed" -eq 0
for expected_case in none cpu alloc both; do
  for field in background_cpu_workers background_allocations; do
    fixture_case="$expected_case-$field"
    offered_run "wrong-$fixture_case" 1000 "$expected_case"
  done
done
test "$offered_failed" -eq 8
fixture_case=bad
offered_run mismatch 1000 none
test "$offered_failed" -eq 9`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, ROOT: root, P07_OFFERED_FACTORIAL: '1' } });
  assert.equal(result.status, 0, result.stderr);
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) assert.equal(fs.readFileSync(path.join(root, `success-${loadCase}-capture-exit-code`), 'utf8'), '0\n');
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
    for (const field of ['background_cpu_workers', 'background_allocations']) {
      assert.equal(fs.readFileSync(path.join(root, `wrong-${loadCase}-${field}-capture-exit-code`), 'utf8'), '1\n');
    }
  }
  assert.equal(fs.readFileSync(path.join(root, 'mismatch-capture-exit-code'), 'utf8'), '1\n');
});

test('observed helper retains all twelve legs and rejects broken observation evidence without stopping', context => {
  const root = directory(context);
  fs.mkdirSync(path.join(root, 'work'));
  for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
    const report = factorialFixture(loadCase, 2000);
    Object.assign(report.offered_load, { attempted: 16000, success: 16000, timeouts: 0, failed: false, slo_failures: 0 });
    report.offered_load.outcomes.at(-1).kind = 'success';
    report.offered_load.config.instrumented = true;
    fs.writeFileSync(path.join(root, `fixture-${loadCase}`), JSON.stringify(report));
  }
  const mismatch = JSON.parse(fs.readFileSync(path.join(root, 'fixture-none')));
  mismatch.offered_load.config.instrumented = false;
  fs.writeFileSync(path.join(root, 'fixture-mismatch'), JSON.stringify(mismatch));
  fs.writeFileSync(path.join(root, 'timing'), JSON.stringify({ label: 'instrumented', calls: Array.from({ length: 16000 }, () => ({})) }));
  const script = `set -eu
P07_DIAGNOSTIC_DIR="$ROOT"; temporary="$ROOT/work"; platform=unused; network=unused; image=unused; offered_failed=0
ceph_cli() { printf '{}\\n'; }
docker() {
  printf '%s\\n' "$@" >"$ROOT/$label-docker-args"
  cat "$ROOT/fixture-\${fixture_case:-$load_case}"
  mkdir "$temporary/$label-observation"
  if test "$label" != missing-trace; then printf trace >"$temporary/$label-observation/trace.out"; fi
  if test "$label" != missing-timing; then cp "$ROOT/timing" "$temporary/$label-observation/timing.json"; fi
  case "$label" in
    bad-count) printf '{"label":"instrumented","calls":[]}' >"$temporary/$label-observation/timing.json" ;;
    bad-json) printf invalid >"$temporary/$label-observation/timing.json" ;;
    observed-1-case-none-rate-2000) printf 'synthetic observation failure\\n' >&2; return 7 ;;
  esac
}
${source.slice(helperStart, loopEnd)}
test "$offered_failed" -eq 1
fixture_case=none
for defect in missing-trace missing-timing bad-count bad-json; do offered_run "$defect" 2000 none; done
fixture_case=mismatch
offered_run bad-flag 2000 none
test "$offered_failed" -eq 6`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, ROOT: root, P07_OFFERED_FACTORIAL: '1', P07_OFFERED_OBSERVE: '1' } });
  assert.equal(result.status, 0, result.stderr);
  for (let repeat = 1; repeat <= 3; repeat++) {
    for (const loadCase of ['none', 'cpu', 'alloc', 'both']) {
      const label = `observed-${repeat}-case-${loadCase}-rate-2000`;
      const failed = repeat === 1 && loadCase === 'none';
      assert.equal(fs.readFileSync(path.join(root, `${label}-capture-exit-code`), 'utf8'), failed ? '1\n' : '0\n');
      assert.equal(fs.readFileSync(path.join(root, `${label}-exit-code`), 'utf8'), failed ? '7\n' : '0\n');
      assert.equal(JSON.parse(fs.readFileSync(path.join(root, `${label}.json`))).offered_load.config.instrumented, true);
      assert.equal(fs.readFileSync(path.join(root, `${label}-observation`, 'trace.out'), 'utf8'), 'trace');
      assert.equal(JSON.parse(fs.readFileSync(path.join(root, `${label}-observation`, 'timing.json'))).calls.length, 16000);
      assert.ok(fs.existsSync(path.join(root, `${label}-osd-2-after.json`)));
      const args = fs.readFileSync(path.join(root, `${label}-docker-args`), 'utf8');
      assert.ok(args.includes(`P07_OFFERED_OBSERVATION_DIR=/work/${label}-observation`));
      assert.ok(args.includes('P07_OFFERED_OBSERVATION_LABEL=instrumented'));
      assert.doesNotMatch(args, /P07_(TIMING_FILE|TRACE_FILE|CPU_PROFILE)=/);
    }
  }
  assert.match(fs.readFileSync(path.join(root, 'observed-1-case-none-rate-2000.stderr'), 'utf8'), /synthetic observation failure/);
  for (const defect of ['missing-trace', 'missing-timing', 'bad-count', 'bad-json', 'bad-flag']) {
    assert.equal(fs.readFileSync(path.join(root, `${defect}-capture-exit-code`), 'utf8'), '1\n');
    assert.ok(fs.existsSync(path.join(root, `${defect}-observation`)));
  }
});

test('failed legs retain JSON stderr exit status and after-counters, then continue', context => {
  const root = directory(context);
  const work = path.join(root, 'work'); fs.mkdirSync(work);
  const reportFile = path.join(root, 'fixture'); fs.writeFileSync(reportFile, JSON.stringify(fixture(true)));
  const script = `set -eu
P07_DIAGNOSTIC_DIR="$ROOT"; temporary="$ROOT/work"; platform=unused; network=unused; image=unused; offered_failed=0
ceph_cli() { printf '{}\\n'; }
docker() {
  printf '%s\\n' "$@" >"$ROOT/docker-args"
  printf 'synthetic failure\\n' >&2
  cat "$REPORT"
  for entry in cpu.max cpu.stat cpuset.cpus.effective memory.max; do
    printf before >"$temporary/$label-$entry-before"
    printf after >"$temporary/$label-$entry-after"
  done
  printf '7\\n' >"$temporary/$label-benchmark-exit-code"
  return 7
}
${source.slice(helperStart, helperEnd)}
offered_run first 1000
offered_run second 1000
test "$offered_failed" -eq 2`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, ROOT: root, REPORT: reportFile } });
  assert.equal(result.status, 0, result.stderr);
  for (const label of ['first', 'second']) {
    assert.equal(JSON.parse(fs.readFileSync(path.join(root, `${label}.json`))).offered_load.expected, 8000);
    assert.match(fs.readFileSync(path.join(root, `${label}.stderr`), 'utf8'), /synthetic failure/);
    assert.equal(fs.readFileSync(path.join(root, `${label}-exit-code`), 'utf8'), '7\n');
    assert.equal(fs.readFileSync(path.join(root, `${label}-capture-exit-code`), 'utf8'), '1\n');
    assert.equal(fs.readFileSync(path.join(root, `${label}-cpu.stat-after`), 'utf8'), 'after');
    assert.equal(fs.readFileSync(path.join(root, `${label}-benchmark-exit-code`), 'utf8'), '7\n');
    assert.ok(fs.existsSync(path.join(root, `${label}-osd-2-after.json`)));
  }
  const invocation = fs.readFileSync(path.join(root, 'docker-args'), 'utf8');
  assert.match(invocation, /P07_OFFERED_OBSERVATION_DIR=\n/);
  assert.match(invocation, /P07_OFFERED_OBSERVATION_LABEL=\n/);
  for (const setting of ['P07_OFFERED_LOAD=1', 'P07_READ_INTO=1', 'P07_FIXED_ALLOC_RATE=100', 'P07_BACKGROUND_WORKERS=8', 'GOMAXPROCS=10', 'GOGC=100', 'GOMEMLIMIT=off']) assert.ok(invocation.includes(setting));
  assert.match(invocation, /benchmark_exit=0[\s\S]*timeout 120[\s\S]*benchmark_exit=\$\?[\s\S]*cpu.stat[\s\S]*-after[\s\S]*exit "\$benchmark_exit"/);
  assert.doesNotMatch(invocation, /P07_(TIMING_FILE|TRACE_FILE|CPU_PROFILE|SCRATCH_SLOTS)=/);
});

test('offered finalizer audits sources, hashes artifacts and cleans up on failure', context => {
  const root = directory(context);
  const binding = path.join(root, 'source.go'); fs.writeFileSync(binding, 'package test\n');
  fs.mkdirSync(path.join(root, 'observed-observation'));
  fs.writeFileSync(path.join(root, 'observed-observation', 'trace.out'), 'trace');
  fs.writeFileSync(path.join(root, 'observed-observation', 'timing.json'), '{}');
  const start = source.indexOf('offered_finalize() {');
  const end = source.indexOf('\nif test "${P07_OFFERED_LOAD:-}" = 1; then', start);
  const script = `set -eu
P07_DIAGNOSTIC_DIR="$ROOT"; original_root="$ROOT"; offered_failed=2; started_at=now
git() { printf '%s\\0' "$ROOT/source.go"; }
cleanup() { printf cleaned >"$ROOT/cleanup-marker"; }
shasum -a 256 "$ROOT/source.go" >"$ROOT/source-before.sha256"
${source.slice(start, end)}
trap offered_finalize EXIT
exit 1`;
  const result = spawnSync('sh', ['-c', script], { encoding: 'utf8', env: { ...cleanEnv, ROOT: root } });
  assert.equal(result.status, 1, result.stderr);
  const status = JSON.parse(fs.readFileSync(path.join(root, 'final-status.json')));
  assert.equal(status.status, 'diagnostic-failed'); assert.equal(status.failed_legs, 2); assert.equal(status.source_check, 'unchanged');
  assert.equal(fs.readFileSync(path.join(root, 'scheduler-status'), 'utf8'), 'diagnostic-failed\n');
  assert.ok(fs.existsSync(path.join(root, 'cleanup-marker')));
  const hashes = fs.readFileSync(path.join(root, 'artifacts.sha256'), 'utf8');
  assert.match(hashes, /final-status.json/); assert.doesNotMatch(hashes, /\.trace/);
  assert.match(hashes, /observed-observation\/trace.out/);
  assert.match(hashes, /observed-observation\/timing.json/);
});