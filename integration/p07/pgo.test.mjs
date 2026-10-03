import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {pgoExperimentPlan, pgoOrder, assessPGO, validatePGOCapture, validatePGOToolPins, validatePGOPhaseBindings} from './pgo.mjs';

test('PGO composition and held-out decision retain failures and uncertainty', () => {
  const plan = pgoExperimentPlan();
  assert.deepEqual(pgoExperimentPlan(), plan);
  assert.deepEqual([...new Set(plan.training.map(cell => cell.workload))].sort(), ['mixed', 'read', 'write']);
  assert.deepEqual([...new Set(plan.training.map(cell => cell.size))].sort((left, right) => left - right), [4096, 65536, 1048576, 4194304]);
  const rounds = plan.heldout.flatMap(cell => Array.from({length: 5}, (_, index) => ({cell_id: cell.id, round: index + 1, namespace: `p07-parity-pgo-${cell.id}-r${index + 1}`, plan_id: plan.plan_id,
    legs: pgoOrder(plan, cell, index + 1).map((variant, position) => ({variant, binary_sha256: (variant === 'off' ? 'b' : variant === 'pgo' ? 'c' : 'd').repeat(64), process_id: `${cell.id}-r${index + 1}-l${position + 1}`,
      metrics: {cpu_ns_per_operation: 100, p99_ns: 1000, successful_iops: 1000, rss_incremental_peak_bytes: 8192}}))})));
  const build = {compiler_profile_used: true, profile_sha256: 'a'.repeat(64), off_sha256: 'b'.repeat(64), pgo_sha256: 'c'.repeat(64), native_sha256: 'd'.repeat(64), off_bytes: 100, pgo_bytes: 101};
  const deferred = assessPGO(plan, rounds, build);
  assert.equal(deferred.decision, 'defer'); assert.equal(deferred.candidate_vs_off.status, 'statistical_gates_passed_not_qualification');
  assert.equal(deferred.off_vs_native.status, 'descriptive_native_comparator_not_randomized');
  const failed = structuredClone(rounds);
  for (const round of failed.filter(round => round.cell_id === plan.heldout[0].id)) for (const leg of round.legs) if (leg.variant === 'pgo') leg.metrics.cpu_ns_per_operation = 200;
  assert.equal(assessPGO(plan, failed, build).decision, 'reject');
  assert.equal(assessPGO(plan, rounds.slice(1), build).status, 'invalid_assessment');
  const invalid = structuredClone(rounds); invalid[0].legs[1].variant = 'native';
  assert.throws(() => assessPGO(plan, invalid, build), /assignment/);
  assert.throws(() => assessPGO(plan, rounds, {...build, compiler_profile_used: false}));
  assert.throws(() => assessPGO(plan, rounds, {...build, pgo_sha256: build.off_sha256}));
  const reused = structuredClone(rounds); reused[1].legs[0].process_id = reused[0].legs[0].process_id;
  assert.throws(() => assessPGO(plan, reused, build), /independent/);
  const altered = structuredClone(rounds); altered[0].legs[1].binary_sha256 = 'e'.repeat(64);
  assert.throws(() => assessPGO(plan, altered, build), /build binding/);
  assert.throws(() => validatePGOCapture({status: 'sustained_go_capture_unqualified'}, plan.heldout[0], {}, 'go'), /nonqualifying/);
});

test('PGO reproduction isolates all phases and requires every training profile', () => {
  const plan = pgoExperimentPlan(), build = {off_sha256: 'a'.repeat(64), pgo_sha256: 'b'.repeat(64), native_sha256: 'c'.repeat(64)};
  const leg = (name, variant, profile = null) => ({process_id: name, variant, implementation: variant === 'native' ? 'native' : 'go', binary_sha256: build[`${variant}_sha256`], profile, provenance: {fixtureNamespace: name}});
  const summary = {build, training: plan.training.map(cell => ({cell, leg: leg(`train-${cell.id}`, 'off', {file: `${cell.id}.pprof`, sha256: 'd'.repeat(64)})})),
    probes: ['off', 'pgo'].map(variant => leg(`probe-${variant}`, variant)), rounds: [{namespace: 'heldout-round', legs: [leg('heldout-off', 'off'), leg('heldout-pgo', 'pgo')]}]};
  validatePGOPhaseBindings(plan, summary);
  for (const change of [
    value => { value.training[0].leg.profile = null; },
    value => { value.probes[0].process_id = value.training[0].leg.process_id; },
    value => { value.probes[0].provenance.fixtureNamespace = value.training[0].leg.provenance.fixtureNamespace; },
    value => { value.rounds[0].namespace = value.probes[0].provenance.fixtureNamespace; },
    value => { value.probes[0].profile = value.training[0].leg.profile; },
    value => { value.training[0].leg.binary_sha256 = build.pgo_sha256; },
    value => { value.training[0].leg.implementation = 'native'; },
    value => { value.training.pop(); },
  ]) {
    const altered = structuredClone(summary); change(altered);
    assert.throws(() => validatePGOPhaseBindings(plan, altered));
  }
});

test('PGO reproduction pins every executing analyzer dependency', () => {
  const pins = ['integration/p07/pgo.mjs', 'integration/p07/parity.mjs', 'integration/p07/qualification.mjs'].map(file => ({file, sha256: crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')}));
  validatePGOToolPins(pins);
  assert.throws(() => validatePGOToolPins(pins.slice(1)), /dependency pin/);
  assert.throws(() => validatePGOToolPins(pins.map(pin => pin.file.endsWith('/parity.mjs') ? {...pin, sha256: '0'.repeat(64)} : pin)), /dependency pin/);
});

test('PGO raw diagnostics validate sampled populations without accepting native retries', () => {
  const plan = pgoExperimentPlan(), cell = plan.heldout[0], identity = {round: 1, leg: 'synthetic-pgo', seed: 901};
  const phase = (operations, elapsed) => ({elapsed_ns: elapsed, successful_operations: operations, unexpected_failures: 0, censored: 0, operations_per_worker: [operations],
    records: Array.from({length: operations}, (_, ordinal) => ({...identity, operation_id: `op-${ordinal}`, ordinal, worker: 0, object: 'p07-parity-4096-c1-read-w0', type: 'read',
      start_ns: ordinal * elapsed / operations, end_ns: (ordinal + 1) * elapsed / operations, success: true, error: null, timeout: false, censored: false, retry_count: 0, timeout_deadline_ns: ordinal * elapsed / operations + 30000000000}))});
  const capture = {status: 'pgo_diagnostic_go_capture_unqualified', error: null, identity, attempt: {payload_verified: true, cleanup_verified: true, warmup: phase(1000, 1000000000), measured: phase(10000, 8000000000),
    memory: {clock: 'measurement_relative_ns', interval_ns: 100000000, idle: {at_ns: -1, connected: true, equally_warmed: true, rss_bytes: 4096}, samples: Array.from({length: 81}, (_, interval) => ({at_ns: interval * 100000000, rss_bytes: 16384}))}},
    report: {implementation: 'go', transport: 'secure', environment: {gomaxprocs: 10}, rows: [{size_bytes: 4096, concurrency: 1, workload: 'read', operations: 10000, bytes: 40960000, elapsed_ns: 8000000000,
      p50_ns: 800000, p95_ns: 800000, p99_ns: 800000, iops: 1250, parity: {payload_verified: true, cleanup_verified: true, rss_before_bytes: 4096, rss_after_bytes: 16384, rss_after_cleanup_bytes: 16384, measured_resources: {cpu_user_ns: 1000000000, cpu_system_ns: 100000000}}}]}};
  assert.equal(validatePGOCapture(capture, cell, identity, 'go').successful_iops, 1250);
  for (const change of [{retry_count: null}, {success: false}, {timeout: true}, {censored: true}, {timeout_deadline_ns: 1}]) {
    const altered = {...capture, attempt: {...capture.attempt, measured: {...capture.attempt.measured, records: [{...capture.attempt.measured.records[0], ...change}, ...capture.attempt.measured.records.slice(1)]}}};
    assert.throws(() => validatePGOCapture(altered, cell, identity, 'go'));
  }
  const native = structuredClone(capture); native.status = 'pgo_diagnostic_native_capture_unqualified'; native.report.implementation = 'native';
  for (const phase of [native.attempt.warmup, native.attempt.measured]) for (const record of phase.records) record.retry_count = null;
  assert.equal(validatePGOCapture(native, cell, identity, 'native').successful_iops, 1250);
  assert.equal(native.attempt.measured.records[0].retry_count, null);
});