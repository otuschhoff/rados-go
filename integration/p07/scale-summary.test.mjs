import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { summarize } from './scale-summary.mjs';

const leg = 'scale-1-concurrency-16-slots-4';
const source = `${'a'.repeat(64)}  integration/p07/benchmark/read_shape.go\n`;
const binaryHash = 'b'.repeat(64);
const build = '/original/build/benchmark: go1.27.1\n\tpath\tgithub.com/otuschhoff/rados-go/integration/p07/benchmark\n\tbuild\t-tags=p12diagnostics\n\tbuild\tGOOS=linux\n\tbuild\tGOARCH=arm64\n';

function fixture(context) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'read-scale-test-'));
  context.after(() => fs.rmSync(root, { recursive: true, force: true }));
  function write(workload, filename, value) {
    fs.writeFileSync(path.join(root, workload, filename), typeof value === 'string' ? value : JSON.stringify(value));
  }
  function cgroup(workload, label) {
    write(workload, `${label}-cpu.stat-before`, 'nr_throttled 999999999999999999999999\nthrottled_usec 1000000000000000000000000\n');
    write(workload, `${label}-cpu.stat-after`, 'nr_throttled 999999999999999999999999\nthrottled_usec 1000000000000000000000000\n');
    write(workload, `${label}-cpu.max-before`, 'max 100000\n');
    write(workload, `${label}-cpuset.cpus.effective-before`, '0-9\n');
  }
  function report(size, concurrency, repeat, scratchSlots, native = false, loaded = false) {
    const operations = native ? 4096 : 1024 * concurrency;
    return { implementation: native ? 'native' : 'go', transport: 'secure',
      environment: native ? { library: 'librados.so.2' } : { GOOS: 'linux', GOARCH: 'arm64', go_version: 'go1.27.1', gomaxprocs: 10 },
      resources: { cpu_user_ns: 100, cpu_system_ns: 100, allocations: native ? null : 100 + repeat,
        allocated_bytes: native ? null : 1000 + repeat, max_rss_bytes: 1000,
        ...(native ? {} : { gc_cycles: 0, gc_pause_ns: 0 }) },
      rows: [{ size_bytes: size, concurrency, workload: 'read', operations, bytes: size * operations,
        elapsed_ns: 1000000, p50_ns: 10, p95_ns: 20, p99_ns: 100 + repeat - scratchSlots,
        throughput_bytes_per_second: 1000000, iops: 100 + repeat + scratchSlots }],
      diagnostic: { warmup_operations: 8 * concurrency, resource_scope: 'warmup_and_measured_reads',
        object_set: `${size === 65536 ? 'p07-shared-read-' : `p07-shared-read-size-${size}-`}0..${concurrency - 1}`,
        ...(native ? {} : { read_api: 'read_into', operations_per_worker: 1024, scratch_slots: scratchSlots,
          admission_window: concurrency, ...(loaded ? { background_cpu_workers: 8, background_allocations: true } : {}),
          scratch: { hits: size === 65536 ? operations : 0, misses: size === 65536 ? 8 * concurrency : 0,
            bypasses: size === 65536 ? 0 : operations + 8 * concurrency, shared_receive_retained_bytes: size === 65536 ? 663552 : 0 } }) } };
  }
  for (const [workload, size] of [['idle', 65536], ['alloc', 65536], ['small', 4096], ['large', 1048576]]) {
    fs.mkdirSync(path.join(root, workload));
    write(workload, 'source-before.sha256', source);
    write(workload, 'source-after.sha256', source);
    write(workload, 'source-head', `${'c'.repeat(40)}\n`);
    write(workload, 'scheduler-status', 'passed\n');
    write(workload, 'go-buildinfo.txt', build.replace('/original/build', `/original/${workload}`));
    write(workload, 'artifacts.sha256', `${binaryHash}  benchmark\n`);
    write(workload, 'scale-methodology.json', { kind: 'read-scale-diagnostic', benchmark_claim: false, matched_native: false,
      go: { size_bytes: size, concurrencies: [16, 32, 64], scratch_slots: [4, 8], operations_per_worker: 1024, measured_legs: 30 },
      native_context: { size_bytes: 65536, concurrency: 16, operations: 4096, scope: 'shorter unmatched context' } });
    for (const repeat of [1, 2, 3, 4, 5]) {
      for (const concurrency of [16, 32, 64]) for (const scratchSlots of [4, 8]) {
        const label = `scale-${repeat}-concurrency-${concurrency}-slots-${scratchSlots}`;
        write(workload, `${label}.json`, report(size, concurrency, repeat, scratchSlots, false, workload === 'alloc'));
        cgroup(workload, label);
      }
      for (const phase of ['before', 'after']) {
        const label = `sweep-${repeat}-native-${phase}`;
        write(workload, `${label}.json`, report(65536, 16, repeat, 0, true));
        cgroup(workload, label);
      }
    }
  }
  function mutate(workload, filename, change) {
    const value = JSON.parse(fs.readFileSync(path.join(root, workload, filename), 'utf8'));
    change(value);
    write(workload, filename, value);
  }
  return { root, write, mutate };
}

test('valid matrix preserves 120 samples, 40 unmatched native rows, medians and paired wins', context => {
  const { root } = fixture(context);
  const summary = summarize(root);
  assert.equal(summary.sample_count, 120);
  assert.equal(summary.native_context.sample_count, 40);
  assert.equal(summary.groups.length, 24);
  assert.equal(summary.pairs.length, 12);
  assert.equal(summary.groups[0].medians.p99_ns, 99);
  assert.equal(summary.groups[0].medians.iops, 107);
  for (const aggregate of summary.aggregates) {
    assert.equal(aggregate.paired_repeats, 15);
    assert.equal(aggregate.p99_wins_slots_8, 15);
    assert.equal(aggregate.iops_wins_slots_8, 15);
    assert.equal(aggregate.joint_wins_slots_8, 15);
  }
  assert.equal(summary.samples[0].cgroup.deltas.nr_throttled, '0');
  assert.equal(summary.throttling.go_legs_with_nonzero_delta, 0);
  assert.equal(summary.samples.find(sample => sample.workload === 'small').hit_fraction, null);
  assert.equal(summary.captures[0].binary_verification, 'manifest_identity_only_binary_absent');
  assert.equal(summary.benchmark_sha256, binaryHash);
  assert.equal(summary.source.files[0].sha256, 'a'.repeat(64));
  assert.equal(JSON.stringify(summary), JSON.stringify(summarize(root)));
});

test('missing directory argument reports usage', () => assert.throws(() => summarize(), /Usage:/));

test('nonexistent root reports usage', context => {
  const { root } = fixture(context);
  assert.throws(() => summarize(path.join(root, 'absent')), /Usage:/);
});

test('missing leg rejects incomplete capture', context => {
  const { root } = fixture(context);
  fs.unlinkSync(path.join(root, 'idle', `${leg}.json`));
  assert.throws(() => summarize(root), /missing or extra leg/);
});

test('source before and after must match', context => {
  const { root, write } = fixture(context);
  write('idle', 'source-after.sha256', source.replace('a', 'd'));
  assert.throws(() => summarize(root), /source changed/);
});

test('all four sources and heads must match', context => {
  const capture = fixture(context);
  capture.write('small', 'source-before.sha256', source.replace('a', 'd'));
  capture.write('small', 'source-after.sha256', source.replace('a', 'd'));
  assert.throws(() => summarize(capture.root), /cross-capture source/);
  capture.write('small', 'source-before.sha256', source);
  capture.write('small', 'source-after.sha256', source);
  capture.write('large', 'source-head', `${'d'.repeat(40)}\n`);
  assert.throws(() => summarize(capture.root), /cross-capture head/);
});

for (const [name, change, pattern] of [
  ['shape', report => { report.rows[0].bytes--; }, /bytes: mismatch/],
  ['API', report => { report.diagnostic.read_api = 'read'; }, /read_api: mismatch/],
  ['load', report => { report.diagnostic.background_cpu_workers = 8; }, /background_cpu_workers: mismatch/],
  ['null load', report => { report.diagnostic.background_allocations = null; }, /background_allocations: mismatch/],
  ['environment', report => { report.environment.gomaxprocs = 8; }, /gomaxprocs: mismatch/],
  ['missing counter', report => { delete report.diagnostic.scratch.misses; }, /scratch.misses/],
  ['fractional counter', report => { report.diagnostic.scratch.hits = 0.5; }, /safe integer/],
  ['nonfinite resource', report => { report.resources.allocated_bytes = null; }, /resources.allocated_bytes/],
  ['nonpositive elapsed', report => { report.rows[0].elapsed_ns = 0; }, /elapsed_ns/],
  ['eligible missing replies despite bypasses', report => { report.diagnostic.scratch.hits--; report.diagnostic.scratch.bypasses = 100000; }, /missing eligible replies/],
  ['global retained budget', report => { report.diagnostic.scratch.shared_receive_retained_bytes = 256 * 1024 * 1024 + 1; }, /global 256 MiB/],
]) test(`${name} mismatch rejects capture`, context => {
  const { root, mutate } = fixture(context);
  mutate('idle', `${leg}.json`, change);
  assert.throws(() => summarize(root), pattern);
});

test('eligible extra replies and bypasses are allowed, snapshot is not limited to 512 KiB', context => {
  const { root, mutate } = fixture(context);
  mutate('idle', `${leg}.json`, report => {
    report.diagnostic.scratch.hits += 20;
    report.diagnostic.scratch.bypasses = 40;
    report.diagnostic.scratch.shared_receive_retained_bytes = 256 * 1024 * 1024;
  });
  const sample = summarize(root).samples[0];
  assert.equal(sample.bypasses, 40);
  assert.equal(sample.retained_bytes, 256 * 1024 * 1024);
});

test('fallback hit rejected', context => {
  const { root, mutate } = fixture(context);
  mutate('small', `${leg}.json`, report => { report.diagnostic.scratch.hits = 1; });
  assert.throws(() => summarize(root), /fallback hits\/misses/);
});

test('cgroup decimals must be integers and nondecreasing', context => {
  const { root, write } = fixture(context);
  write('idle', `${leg}-cpu.stat-after`, 'nr_throttled 0.5\nthrottled_usec 1000000000000000000000000\n');
  assert.throws(() => summarize(root), /invalid decimal/);
  write('idle', `${leg}-cpu.stat-after`, 'nr_throttled 0\nthrottled_usec 1000000000000000000000000\n');
  assert.throws(() => summarize(root), /decreased cgroup counter/);
});

test('missing cgroup counter rejected', context => {
  const { root, write } = fixture(context);
  write('idle', `${leg}-cpu.stat-after`, 'nr_throttled 999999999999999999999999\n');
  assert.throws(() => summarize(root), /missing cgroup counter/);
});

test('arbitrary precision throttling retained as strings and counted, not rejected', context => {
  const { root, write } = fixture(context);
  write('idle', `${leg}-cpu.stat-after`, 'nr_throttled 1000000000000000000000000\nthrottled_usec 2000000000000000000000000\n');
  const summary = summarize(root);
  assert.deepEqual(summary.samples[0].cgroup.deltas, { nr_throttled: '1', throttled_usec: '1000000000000000000000000' });
  assert.equal(summary.throttling.go_legs_with_nonzero_delta, 1);
  assert.equal(summary.samples[0].cgroup.zero_throttling, false);
});

test('different benchmark manifest identity rejected', context => {
  const { root, write } = fixture(context);
  write('alloc', 'artifacts.sha256', `${'d'.repeat(64)}  benchmark\n`);
  assert.throws(() => summarize(root), /cross-capture benchmark_sha256/);
});

test('untagged or wrong toolchain build rejected', context => {
  const { root, write } = fixture(context);
  write('idle', 'go-buildinfo.txt', build.replace('p12diagnostics', 'other'));
  assert.throws(() => summarize(root), /missing p12diagnostics/);
  write('idle', 'go-buildinfo.txt', build.replace('go1.27.1', 'go1.27.0'));
  assert.throws(() => summarize(root), /build toolchain/);
});

test('present binaries are verified; omitted binaries retain manifest identity only', context => {
  const { root, write } = fixture(context);
  const bytes = 'fixture benchmark bytes';
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  for (const workload of ['idle', 'alloc', 'small', 'large']) write(workload, 'artifacts.sha256', `${sha256}  benchmark\n`);
  write('idle', 'benchmark', bytes);
  assert.equal(summarize(root).captures[0].binary_verification, 'verified_present_bytes');
  write('idle', 'benchmark', 'tampered');
  assert.throws(() => summarize(root), /benchmark binary hash/);
});

test('methodology and native work scope must match', context => {
  const { root, mutate } = fixture(context);
  mutate('large', 'scale-methodology.json', value => { value.go.size_bytes = 4096; });
  assert.throws(() => summarize(root), /methodology go.size_bytes/);
  mutate('large', 'scale-methodology.json', value => { value.go.size_bytes = 1048576; });
  mutate('idle', 'sweep-1-native-before.json', report => { report.rows[0].operations = 1024; });
  assert.throws(() => summarize(root), /operations: mismatch/);
});