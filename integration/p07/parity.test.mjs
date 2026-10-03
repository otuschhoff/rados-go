import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fixtureName, validateParity, validateParityHealth, validateQualificationLeg} from './parity.mjs';
import {createQualificationPlan, qualificationOrder, analyzeQualificationMetrics, analyzeQualificationCaptures, qualificationDistribution, validateQualificationCapture, freezeQualificationPlan, analyzeQualificationFiles} from './qualification.mjs';

test('predeclared paired-round matrix bootstrap passes, fails and rejects incomplete evidence', () => {
  const plan = createQualificationPlan({cells: [{id: 'read-small', pool: 'readcache', size: 4096, concurrency: 1, workload: 'read'},
    {id: 'write-small', pool: 'readcache', size: 4096, concurrency: 1, workload: 'write'}], seed: 42, bootstrapSeed: 123});
  const rounds = plan.cells.flatMap(cell => Array.from({length: plan.rounds}, (_, index) => ({cell_id: cell.id, round: index + 1, plan_id: plan.plan_id,
    namespace: `p07-parity-${cell.id}-r${index + 1}`, legs: qualificationOrder(plan, cell.id, index + 1).map((implementation, position) => ({implementation,
      process_id: `${cell.id}-r${index + 1}-l${position + 1}`, metrics: {cpu_ns_per_operation: 100, p99_ns: 1000, successful_iops: 1000, rss_incremental_peak_bytes: 8192}}))})));
  const passing = analyzeQualificationMetrics(plan, rounds);
  assert.equal(passing.status, 'statistical_gates_passed_not_qualification');
  assert.deepEqual(analyzeQualificationMetrics(plan, rounds), passing);
  for (const cell of passing.cells) for (const metric of Object.values(cell.metrics)) assert.equal(metric.point, 1);
  const failed = structuredClone(rounds);
  for (const round of failed.filter(round => round.cell_id === 'write-small')) for (const leg of round.legs) if (leg.implementation === 'go') leg.metrics.cpu_ns_per_operation = 200;
  const failing = analyzeQualificationMetrics(plan, failed);
  assert.equal(failing.status, 'statistical_gates_failed');
  assert.equal(failing.cells[0].status, 'statistical_gates_passed');
  assert.equal(failing.cells[1].metrics.cpu_ns_per_operation.passed, false);
  assert.equal(analyzeQualificationMetrics(plan, rounds.slice(1)).status, 'invalid');
  const duplicate = structuredClone(rounds); duplicate[1] = duplicate[0];
  assert.equal(analyzeQualificationMetrics(plan, duplicate).status, 'invalid');
  const brokenOrder = structuredClone(rounds); brokenOrder[0].legs.reverse(); brokenOrder[0].legs[1].implementation = brokenOrder[0].legs[0].implementation;
  assert.equal(analyzeQualificationMetrics(plan, brokenOrder).status, 'invalid');
  const reused = structuredClone(rounds); reused[1].legs[0].process_id = reused[0].legs[0].process_id;
  assert.equal(analyzeQualificationMetrics(plan, reused).status, 'invalid');
  for (const denominator of [0, -1, 1, NaN]) {
    const undefinedRSS = structuredClone(rounds);
    undefinedRSS[0].legs.find(leg => leg.implementation === 'native').metrics.rss_incremental_peak_bytes = denominator;
    const unavailable = analyzeQualificationMetrics(plan, undefinedRSS);
    assert.equal(unavailable.status, 'unknown');
    assert.equal(unavailable.cells[0].metrics.cpu_ns_per_operation.status, 'passed');
    assert.equal(unavailable.cells[0].metrics.rss_incremental_peak_bytes.upper, null);
  }
  const changedPlan = structuredClone(plan); changedPlan.gates.cpu_ns_per_operation.limit = 3;
  assert.equal(analyzeQualificationMetrics(changedPlan, rounds).status, 'invalid');
  assert.throws(() => createQualificationPlan({cells: plan.cells, rounds: 4, seed: 42, bootstrapSeed: 123}));
  assert.throws(() => createQualificationPlan({cells: plan.cells, seed: 42, bootstrapSeed: 123, bootstrapReplicates: 10}));
  const boundary = structuredClone(rounds);
  for (const round of boundary) for (const leg of round.legs) if (leg.implementation === 'go') {
    leg.metrics.cpu_ns_per_operation = 120; leg.metrics.p99_ns = 1250; leg.metrics.successful_iops = 900; leg.metrics.rss_incremental_peak_bytes = 10240;
  }
  assert.equal(analyzeQualificationMetrics(plan, boundary).status, 'statistical_gates_passed_not_qualification');
  assert.equal(analyzeQualificationCaptures(plan, rounds).status, 'invalid_evidence');
  const distribution = qualificationDistribution(plan, [{start_ns: 0, end_ns: 1000}, {start_ns: 1, end_ns: 10002}, {start_ns: 2, end_ns: 40000000002}]);
  assert.deepEqual(distribution.histogram.counts, [1, 0, 1, 0, 0, 0, 0, 0, 1]);
  assert.equal(distribution.histogram.overflow, 1); assert.equal(distribution.p99_ns, 40000000000);
  assert.equal(distribution.tails.length, 1);
  const variable = structuredClone(rounds);
  for (const round of variable) for (const leg of round.legs) {
    if (leg.implementation === 'native') leg.metrics.cpu_ns_per_operation = round.round === 1 ? 10000 : 100;
    else leg.metrics.cpu_ns_per_operation = (round.round === 1 ? 10000 : 100) * (0.5 + round.round / 10);
  }
  const varied = analyzeQualificationMetrics(plan, variable);
  const expected = Math.exp([.6, .7, .8, .9, 1].reduce((sum, ratio) => sum + Math.log(ratio), 0) / 5);
  assert(Math.abs(varied.cells[0].metrics.cpu_ns_per_operation.point - expected) < 1e-12);
  assert(varied.cells[0].metrics.cpu_ns_per_operation.lower < expected && varied.cells[0].metrics.cpu_ns_per_operation.upper > expected);
  assert.deepEqual(varied.cells[0].metrics.cpu_ns_per_operation, varied.cells[1].metrics.cpu_ns_per_operation);
  const unequal = structuredClone(rounds);
  for (const round of unequal) {
    const go = round.legs.filter(leg => leg.implementation === 'go'), native = round.legs.filter(leg => leg.implementation === 'native');
    go[0].metrics.cpu_ns_per_operation = 50; go[1].metrics.cpu_ns_per_operation = 150;
    native[0].metrics.cpu_ns_per_operation = 25; native[1].metrics.cpu_ns_per_operation = 75;
  }
  assert.equal(analyzeQualificationMetrics(plan, unequal).cells[0].metrics.cpu_ns_per_operation.point, 2);
});

test('plan freeze and file analysis are exclusive and reject incomplete inputs', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rados-go-plan-test-'));
  try {
    const planFile = path.join(root, 'plan.json'), manifestFile = path.join(root, 'manifest.json'), output = path.join(root, 'analysis.json');
    const specification = {cells: [{id: 'read-small', pool: 'readcache', size: 4096, concurrency: 1, workload: 'read'}], seed: 42, bootstrapSeed: 123};
    const plan = freezeQualificationPlan(specification, planFile);
    assert.equal(fs.statSync(planFile).mode & 0o777, 0o600);
    assert.throws(() => freezeQualificationPlan(specification, planFile), /EEXIST/);
    const dependencies = Object.fromEntries(['qualification.mjs', 'parity.mjs'].map(file => [file, crypto.createHash('sha256').update(fs.readFileSync(new URL(file, import.meta.url))).digest('hex')]));
    const manifest = {plan_id: plan.plan_id, capture_started_ms: Math.ceil(fs.statSync(planFile).mtimeMs) + 1, analyzer_dependencies: dependencies, rounds: []};
    fs.writeFileSync(manifestFile, JSON.stringify(manifest));
    assert.equal(analyzeQualificationFiles(planFile, manifestFile, output).status, 'invalid');
    assert.equal(fs.statSync(output).mode & 0o777, 0o600);
    assert.throws(() => analyzeQualificationFiles(planFile, manifestFile, output), /must be fresh/);
    for (const pins of [undefined, {...dependencies, 'parity.mjs': '0'.repeat(64)}, {'qualification.mjs': dependencies['qualification.mjs']}]) {
      fs.writeFileSync(manifestFile, JSON.stringify({...manifest, analyzer_dependencies: pins}));
      assert.throws(() => analyzeQualificationFiles(planFile, manifestFile, path.join(root, 'dependency-mismatch.json')), /executing analyzer dependency pins/);
      assert.equal(fs.existsSync(path.join(root, 'dependency-mismatch.json')), false);
    }
    const unpinned = {...manifest, rounds: [{legs: [{provenance: {pins: {before: {tool: '0'.repeat(64)}}}}]}]};
    fs.writeFileSync(manifestFile, JSON.stringify(unpinned));
    assert.throws(() => analyzeQualificationFiles(planFile, manifestFile, path.join(root, 'unpinned.json')), /executing analyzer tool pin/);
    manifest.capture_started_ms = 0;
    fs.writeFileSync(manifestFile, JSON.stringify(manifest));
    assert.throws(() => analyzeQualificationFiles(planFile, manifestFile, path.join(root, 'too-late.json')), /frozen before capture/);
  } finally { fs.rmSync(root, {recursive: true, force: true}); }
});

test('native sustained collector and driver reject invalid capture settings before connecting', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rados-go-qualification-test-'));
  try {
    for (const source of ['native_qualification', 'native_qualification_test']) {
      const result = spawnSync('gcc', ['-O2', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-Wall', '-Wextra', '-Werror', '-pthread', `integration/p07/${source}.c`, '-ldl', '-o', path.join(root, source)], {encoding: 'utf8', timeout: 30000});
      assert.equal(result.status, 0, result.stderr);
    }
    const result = spawnSync(path.join(root, 'native_qualification_test'), [], {encoding: 'utf8', timeout: 30000});
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /native qualification core:.*tests passed/);
    const environment = {P07_QUALIFICATION_FILE: path.join(root, 'capture.json'), P07_NATIVE_MODE_LOG: path.join(root, 'modes.log'),
      P07_PARITY_NAMESPACE: 'p07-parity-invalid-config-test', P07_MATRIX_SIZE: '4096', P07_MATRIX_CONCURRENCY: '1', P07_MATRIX_WORKLOAD: 'read',
      P07_QUALIFICATION_ROUND: '1', P07_QUALIFICATION_SEED: '42', P07_QUALIFICATION_LEG: 'r1-native'};
    for (const invalid of [{P07_QUALIFICATION_FILE: ''}, {P07_NATIVE_MODE_LOG: ''}, {P07_QUALIFICATION_ROUND: '0'}, {P07_QUALIFICATION_ROUND: '9007199254740992'},
      {P07_QUALIFICATION_SEED: '-1'}, {P07_QUALIFICATION_LEG: 'bad"leg'}, {P07_PARITY_NAMESPACE: 'unapproved'}, {P07_MATRIX_SIZE: '1'}, {P07_MATRIX_CONCURRENCY: '999'},
      {P07_MATRIX_WORKLOAD: 'unknown'}, {P07_OFFERED_LOAD: '1'}, {P07_MATRIX_OPERATIONS_PER_WORKER: '256'}, {P07_NATIVE_MODE_LOG: environment.P07_QUALIFICATION_FILE},
      {P07_PGO_DIAGNOSTIC: '0'}, {P07_PGO_PROFILE_FILE: 'training.pprof'}]) {
      const rejected = spawnSync(path.join(root, 'native_qualification'), ['missing-config', 'missing-key', 'unused-pool', 'secure'], {env: {...environment, ...invalid}, encoding: 'utf8', timeout: 5000});
      assert.equal(rejected.status, 2, JSON.stringify(invalid));
      assert.equal(fs.existsSync(environment.P07_QUALIFICATION_FILE), false);
      assert.equal(fs.existsSync(environment.P07_NATIVE_MODE_LOG), false);
    }
  } finally { fs.rmSync(root, {recursive: true, force: true}); }
});

test('qualification rejects short, incomplete, failed or censored leg evidence', () => {
  const cell = {size: 4096, concurrency: 1, workload: 'read', operations: 100000};
  const duration = 60000000000, latency = duration / cell.operations;
  const report = {implementation: 'go', transport: 'secure', environment: {gomaxprocs: 10}, rows: [{size_bytes: cell.size, concurrency: 1, workload: 'read', operations: cell.operations, bytes: cell.operations * cell.size,
    elapsed_ns: duration, p50_ns: latency, p95_ns: latency, p99_ns: latency, iops: cell.operations / 60,
    parity: {payload_verified: true, cleanup_verified: true, rss_before_bytes: 1, rss_after_bytes: 1, rss_after_cleanup_bytes: 1, measured_resources: {cpu_user_ns: 100, cpu_system_ns: 10}}}]};
  const evidence = {round: 1, leg: 'round1-leg1', seed: 42, warmup: {elapsed_ns: 10000000000, successful_operations: 10000, unexpected_failures: 0, censored: 0},
    records: Array.from({length: cell.operations}, (_, ordinal) => ({round: 1, leg: 'round1-leg1', operation_id: `worker0-op${ordinal}`, worker: 0, ordinal,
      object: fixtureName(cell, 0), type: 'read', start_ns: ordinal * latency, end_ns: (ordinal + 1) * latency, success: true, error: null, timeout: false, censored: false, retry_count: 0, timeout_deadline_ns: null})),
    memory: {clock: 'measurement_relative_ns', interval_ns: 1000000000, idle: {connected: true, equally_warmed: true, at_ns: -1, rss_bytes: 1024},
      samples: Array.from({length: 61}, (_, second) => ({at_ns: second * 1000000000, rss_bytes: second === 30 ? 4096 : 2048}))}};
  const validate = (row = report, capture = evidence) => validateQualificationLeg(row, 'go', cell, capture);
  assert.equal(validate().rss_incremental_peak_bytes, 3072);
  assert.equal(validate().evidence_status, 'leg_validated_not_matrix_qualification');
  const plan = createQualificationPlan({cells: [{id: 'read-small', pool: 'readcache', size: 4096, concurrency: 1, workload: 'read'}], seed: 42, bootstrapSeed: 123});
  const attempted = {cell_id: 'read-small', round: 1, namespace: 'p07-parity-synthetic-r1', plan_id: plan.plan_id};
  const measured = {elapsed_ns: duration, successful_operations: cell.operations, unexpected_failures: 0, censored: 0, operations_per_worker: [cell.operations],
    records: evidence.records.map(record => ({...record, timeout_deadline_ns: record.start_ns + 30000000000}))};
  const warmup = {elapsed_ns: 10000000000, successful_operations: 10000, unexpected_failures: 0, censored: 0, operations_per_worker: [10000],
    records: measured.records.slice(0, 10000).map((record, ordinal) => ({...record, start_ns: ordinal * 1000000, end_ns: (ordinal + 1) * 1000000, timeout_deadline_ns: ordinal * 1000000 + 30000000000}))};
  const health = {fsid: 'synthetic-fsid', health: {status: 'HEALTH_OK', checks: {}}, pgmap: {pgs_by_state: [{state_name: 'active+clean', count: 1}], num_pgs: 1}};
  const placement = {pgid: '1.0', acting: [0], acting_primary: 0};
  const pins = {source: 'a'.repeat(64), binary: 'b'.repeat(64), tool: 'c'.repeat(64)};
  const leg = {implementation: 'go', process_id: evidence.leg, capture: {status: 'sustained_go_capture_unqualified', error: null,
    identity: {round: 1, seed: 42, leg: evidence.leg}, report, attempt: {warmup, measured, payload_verified: true, cleanup_verified: true,
      memory: {...evidence.memory, interval_ns: 100000000, samples: Array.from({length: 601}, (_, interval) => ({at_ns: interval * 100000000, rss_bytes: 16384}))}}},
    modes: {implementation: 'go', requested: 'secure', connections: ['monitor', 'osd'].map(service => ({service, connection: service, actual: 'secure', source: 'go-auth-metadata'}))},
    provenance: {plan_id: plan.plan_id, namespace: attempted.namespace, pool: 'readcache', runtime: plan.runtime, resource_scope: plan.resource_scope,
      instrumentation: plan.instrumentation.go, rss_method: plan.rss_method, pins: {before: pins, after: {...pins}}, fsid: health.fsid,
      health: {before: health, after: health}, placement: [{worker: 0, before: placement, after: placement}]}};
  assert.equal(validateQualificationCapture(plan, attempted, leg).row.evidence_status, 'leg_validated_not_matrix_qualification');
  for (const changed of [{namespace: 'p07-parity-other'}, {pool: 'test-3x'}, {resource_scope: 'library_only'}, {runtime: {...plan.runtime, CGO_ENABLED: '1'}},
    {instrumentation: 'uninstrumented'}, {pins: {before: pins, after: {...pins, source: 'd'.repeat(64)}}}, {placement: []}]) {
    assert.throws(() => validateQualificationCapture(plan, attempted, {...leg, provenance: {...leg.provenance, ...changed}}));
  }
  for (const population of [{...warmup, successful_operations: 10001}, {...warmup, records: warmup.records.slice(1)},
    {...warmup, records: [{...warmup.records[0], retry_count: null}, ...warmup.records.slice(1)]}]) {
    assert.throws(() => validateQualificationCapture(plan, attempted, {...leg, capture: {...leg.capture, attempt: {...leg.capture.attempt, warmup: population}}}));
  }
  assert.throws(() => validateQualificationCapture(plan, attempted, {...leg, modes: {...leg.modes, connections: leg.modes.connections.slice(0, 1)}}));
  const rejected = analyzeQualificationCaptures(plan, [{...attempted, legs: [{...leg, provenance: undefined}, {...leg, process_id: 'other'}]}]);
  assert.equal(rejected.status, 'invalid_evidence'); assert.equal(rejected.attempted_legs.length, 2); assert.equal(rejected.findings.length, 2);
  const concurrentCell = {...cell, concurrency: 2, operations: 50000};
  const concurrentRecords = evidence.records.map((record, index) => {
    const worker = index < 40000 ? 0 : 1, ordinal = worker === 0 ? index : index - 40000;
    return {...record, worker, ordinal, object: fixtureName(concurrentCell, worker), start_ns: ordinal * latency, end_ns: (ordinal + 1) * latency};
  });
  const concurrentReport = {...report, rows: [{...report.rows[0], concurrency: 2}]};
  const concurrentEvidence = {...evidence, records: concurrentRecords, operations_per_worker: [40000, 60000]};
  assert.equal(validateQualificationLeg(concurrentReport, 'go', concurrentCell, concurrentEvidence).operations, 100000);
  for (const populations of [[50000, 50000], [40000, 60001], [100000], [0, 100000], [-1, 100001], [40000.5, 59999.5]]) {
    assert.throws(() => validateQualificationLeg(concurrentReport, 'go', concurrentCell, {...concurrentEvidence, operations_per_worker: populations}));
  }
  assert.throws(() => validateQualificationLeg(report, 'go', cell, undefined));
  const short = structuredClone(report); short.rows[0].elapsed_ns--;
  assert.throws(() => validate(short));
  for (const field of ['elapsed_ns', 'successful_operations']) {
    const capture = {...evidence, warmup: {...evidence.warmup, [field]: evidence.warmup[field] - 1}};
    assert.throws(() => validate(report, capture));
  }
  for (const change of [{success: false}, {error: 'EIO'}, {timeout: true}, {censored: true}, {retry_count: null}, {retry_count: -1}, {timeout_deadline_ns: 1}, {ordinal: 1}, {operation_id: evidence.records[1].operation_id}, {type: 'write'}]) {
    const records = [...evidence.records]; records[0] = {...records[0], ...change};
    assert.throws(() => validate(report, {...evidence, records}));
  }
  assert.throws(() => validate(report, {...evidence, records: evidence.records.slice(1)}));
  const overlapping = [...evidence.records]; overlapping[1] = {...overlapping[1], start_ns: overlapping[1].start_ns - 1};
  assert.throws(() => validate(report, {...evidence, records: overlapping}), /worker operations overlap/);
  for (const field of ['unexpected_failures', 'censored']) {
    assert.throws(() => validate(report, {...evidence, warmup: {...evidence.warmup, [field]: 1}}));
  }
  const wrongQuantile = structuredClone(report); wrongQuantile.rows[0].p99_ns++;
  assert.throws(() => validate(wrongQuantile));
  const wrongThroughput = structuredClone(report); wrongThroughput.rows[0].iops++;
  assert.throws(() => validate(wrongThroughput), /throughput agreement/);
  const negativeCPU = structuredClone(report); negativeCPU.rows[0].parity.measured_resources.cpu_system_ns = -1;
  assert.throws(() => validate(negativeCPU), /measured CPU delta/);
  for (const memory of [{...evidence.memory, samples: []}, {...evidence.memory, interval_ns: duration, samples: [evidence.memory.samples[0], evidence.memory.samples.at(-1)]}, {...evidence.memory, samples: evidence.memory.samples.filter((_, index) => index % 5 === 0)},
    {...evidence.memory, idle: {...evidence.memory.idle, at_ns: -2000000000}}, {...evidence.memory, idle: {...evidence.memory.idle, connected: false}}]) {
    assert.throws(() => validate(report, {...evidence, memory}));
  }
});

test('matched fixture naming and fail-closed report contract', () => {
  const cell = {size: 1048576, concurrency: 1, workload: 'write', operations: 1024};
  assert.equal(fixtureName(cell, 0), 'p07-parity-1048576-c1-write-w0');
  const report = {implementation: 'go', transport: 'secure', environment: {gomaxprocs: 10}, rows: [{size_bytes: cell.size, concurrency: 1, workload: 'write', operations: 1024, bytes: 1024 * cell.size, elapsed_ns: 100, p50_ns: 1, p95_ns: 2, p99_ns: 3, iops: 10, parity: {payload_verified: true, cleanup_verified: true, rss_before_bytes: 1, rss_after_bytes: 1, rss_after_cleanup_bytes: 1, measured_resources: {cpu_user_ns: 100, cpu_system_ns: 10}}}]};
  assert.equal(validateParity(report, 'go', cell).cpu_ns_per_operation, 110 / 1024);
  for (const key of ['payload_verified', 'cleanup_verified']) {
    const invalid = structuredClone(report); invalid.rows[0].parity[key] = false;
    assert.throws(() => validateParity(invalid, 'go', cell));
  }
  const invalid = structuredClone(report); invalid.rows[0].operations--;
  assert.throws(() => validateParity(invalid, 'go', cell));
});

test('all parity runners reject wrong clusters and any non-clean PG state', () => {
  const report = {fsid: 'expected-fsid', health: {status: 'HEALTH_WARN', checks: {POOL_NO_REDUNDANCY: {}}}, pgmap: {pgs_by_state: [{state_name: 'active+clean', count: 737}], num_pgs: 737}};
  assert.deepEqual(validateParityHealth(report, 'expected-fsid'), ['POOL_NO_REDUNDANCY']);
  assert.throws(() => validateParityHealth(report, 'different-fsid'));
  for (const state of ['active+recovering', 'active+clean+scrubbing', 'active+remapped', 'peering']) {
    const invalid = structuredClone(report); invalid.pgmap.pgs_by_state[0].state_name = state;
    assert.throws(() => validateParityHealth(invalid, 'expected-fsid'));
  }
  for (const mutate of [value => value.pgmap.pgs_by_state = [], value => value.pgmap.pgs_by_state[0].count = 0, value => value.pgmap.num_pgs++, value => value.health.status = 'HEALTH_ERR']) {
    const invalid = structuredClone(report); mutate(invalid); assert.throws(() => validateParityHealth(invalid, 'expected-fsid'));
  }
});