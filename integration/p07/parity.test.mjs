import {test} from 'node:test';
import assert from 'node:assert/strict';
import {fixtureName, validateParity, validateParityHealth, validateQualificationLeg} from './parity.mjs';

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