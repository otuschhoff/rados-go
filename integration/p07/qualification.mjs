import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL, fileURLToPath} from 'node:url';
import {validateQualificationLeg, validateQualificationRecords, validateParityHealth} from './parity.mjs';

const metrics = {
  cpu_ns_per_operation: {direction: 'upper', limit: 1.20},
  p99_ns: {direction: 'upper', limit: 1.25},
  successful_iops: {direction: 'lower', limit: 0.90},
  rss_incremental_peak_bytes: {direction: 'upper', limit: 1.25}
};

const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const safeSeed = value => Number.isSafeInteger(value) && value >= 0;

export function qualificationToolPins() {
  return Object.fromEntries(['qualification.mjs', 'parity.mjs'].map(file => [file,
    crypto.createHash('sha256').update(fs.readFileSync(new URL(file, import.meta.url))).digest('hex')]));
}

export function createQualificationPlan({cells, rounds = 5, seed, bootstrapSeed, bootstrapReplicates, rssResolutionBytes = 4096}) {
  assert(Array.isArray(cells) && cells.length >= 1 && cells.length <= 256, 'declared matrix');
  const identifiers = new Set();
  const matrix = cells.map(cell => {
    assert(typeof cell.id === 'string' && /^[a-z0-9-]{1,80}$/.test(cell.id) && !identifiers.has(cell.id), 'unique cell identity');
    identifiers.add(cell.id);
    assert(['readcache', 'test-3x'].includes(cell.pool), 'declared fixture pool');
    assert([4096, 65536, 1048576, 4194304].includes(cell.size), 'declared payload size');
    assert([1, 16, 32, 64, 128, 256].includes(cell.concurrency), 'declared concurrency');
    assert(['read', 'write', 'mixed'].includes(cell.workload), 'declared workload');
    return {id: cell.id, pool: cell.pool, size: cell.size, concurrency: cell.concurrency, workload: cell.workload};
  });
  assert(Number.isSafeInteger(rounds) && rounds >= 5 && rounds <= 30, 'at least five fixed independent rounds');
  assert(safeSeed(seed) && safeSeed(bootstrapSeed), 'predeclared safe seeds');
  assert(Number.isSafeInteger(rssResolutionBytes) && rssResolutionBytes >= 4096, 'RSS measurement resolution');
  const comparisons = matrix.length * Object.keys(metrics).length;
  const tailProbability = 0.05 / (2 * comparisons);
  const minimumReplicates = Math.ceil(20 / tailProbability);
  const replicates = bootstrapReplicates ?? Math.max(10000, minimumReplicates);
  assert(Number.isSafeInteger(replicates) && replicates >= minimumReplicates && replicates <= 1000000, 'bootstrap tail resolution');
  const body = {version: 1, cells: matrix, rounds, seed, bootstrap_seed: bootstrapSeed, bootstrap_replicates: replicates,
    rss_resolution_bytes: rssResolutionBytes, confidence: 0.95, comparisons, tail_probability: tailProbability,
    schedule: 'sha256-cell-round-abba-v1', conditioning: 'fresh_process_and_fresh_fixture_namespace_per_round',
    within_round: 'ratio_of_equal_weight_two_leg_arithmetic_means', across_rounds: 'geometric_mean_of_paired_round_ratios',
    resampling_unit: 'whole_paired_round_vector', interval: 'percentile_bootstrap_bonferroni_two_sided', quantile: 'nearest_rank',
    histogram_boundaries_ns: [1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 30000000000], tail_fraction: 0.01,
    exclusions: [], gates: structuredClone(metrics), runtime: {CGO_ENABLED: '0', GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off', affinity: '0-9'},
    resource_scope: 'whole_process_including_worker_start_sampler_observation_and_recorder', rss_method: '/proc/self/status:VmRSS', rss_interval_ns: 100000000,
    instrumentation: {go: 'opt_in_request_preparation_and_messenger_dispatch_counts', native: 'debug_ms_1_1_negotiation_and_per_message_logging'},
    claim: 'statistical_tooling_not_qualification'};
  return {...body, plan_id: digest(body)};
}

export function validateQualificationPlan(plan) {
  assert(plan && typeof plan === 'object', 'missing predeclared plan');
  const expected = createQualificationPlan({cells: plan.cells, rounds: plan.rounds, seed: plan.seed, bootstrapSeed: plan.bootstrap_seed,
    bootstrapReplicates: plan.bootstrap_replicates, rssResolutionBytes: plan.rss_resolution_bytes});
  assert.deepEqual(plan, expected, 'plan changed or unsupported methodology');
  return expected;
}

export function qualificationOrder(plan, cellID, round) {
  assert(plan.cells.some(cell => cell.id === cellID), 'scheduled cell');
  assert(Number.isSafeInteger(round) && round >= 1 && round <= plan.rounds, 'scheduled round');
  const bit = crypto.createHash('sha256').update(`${plan.seed}\0${cellID}\0${round}`).digest()[0] & 1;
  return bit ? ['go', 'native', 'native', 'go'] : ['native', 'go', 'go', 'native'];
}

function bootstrapWeights(plan) {
  const weights = new Uint8Array(plan.bootstrap_replicates * plan.rounds);
  let counter = 0, buffer = Buffer.alloc(0), offset = 0;
  const limit = Math.floor(4294967296 / plan.rounds) * plan.rounds;
  for (let replicate = 0; replicate < plan.bootstrap_replicates; replicate++) {
    for (let draw = 0; draw < plan.rounds; draw++) {
      let value;
      do {
        if (offset >= buffer.length) {
          buffer = crypto.createHash('sha256').update(`${plan.bootstrap_seed}\0${counter++}`).digest();
          offset = 0;
        }
        value = buffer.readUInt32LE(offset); offset += 4;
      } while (value >= limit);
      weights[replicate * plan.rounds + value % plan.rounds]++;
    }
  }
  return weights;
}

function nearestRank(sorted, probability) {
  return sorted[Math.max(0, Math.ceil(sorted.length * probability) - 1)];
}

export function analyzeQualificationMetrics(plan, attemptedRounds) {
  const output = {status: 'invalid', claim: 'statistical_tooling_not_qualification', plan_id: plan?.plan_id ?? null, cells: [], findings: []};
  try {
    validateQualificationPlan(plan);
    assert(Array.isArray(attemptedRounds) && attemptedRounds.length === plan.cells.length * plan.rounds, 'complete attempted round population');
    const rounds = new Map(), processes = new Set();
    for (const attempted of attemptedRounds) {
      assert(plan.cells.some(cell => cell.id === attempted.cell_id), 'unexpected cell');
      assert(Number.isSafeInteger(attempted.round) && attempted.round >= 1 && attempted.round <= plan.rounds, 'unexpected round');
      const key = `${attempted.cell_id}:${attempted.round}`;
      assert(!rounds.has(key), 'duplicate round');
      assert.equal(attempted.plan_id, plan.plan_id, 'round plan binding');
      assert(typeof attempted.namespace === 'string' && /^p07-parity-[a-z0-9-]+$/.test(attempted.namespace) && attempted.namespace.length <= 64, 'round conditioning namespace');
      assert(![...rounds.values()].some(previous => previous.namespace === attempted.namespace), 'round conditioning namespace reused');
      const order = qualificationOrder(plan, attempted.cell_id, attempted.round);
      assert(Array.isArray(attempted.legs) && attempted.legs.length === 4, 'complete ABBA legs');
      for (const [position, leg] of attempted.legs.entries()) {
        assert.equal(leg.implementation, order[position], 'predeclared ABBA assignment');
        assert(typeof leg.process_id === 'string' && leg.process_id.length > 0 && !processes.has(leg.process_id), 'independent process lifecycle');
        processes.add(leg.process_id);
      }
      rounds.set(key, attempted);
    }
    const weights = bootstrapWeights(plan);
    output.bootstrap_weights_sha256 = crypto.createHash('sha256').update(weights).digest('hex');
    for (const cell of plan.cells) {
      const result = {cell, status: 'unknown', rounds: [], metrics: Object.fromEntries(Object.entries(metrics).map(([metric, gate]) => [metric, {status: 'unknown', ...gate, point: null, lower: null, upper: null, passed: false}])), findings: []};
      output.cells.push(result);
      try {
        const logs = Object.fromEntries(Object.keys(metrics).map(metric => [metric, []]));
        for (let round = 1; round <= plan.rounds; round++) {
          const attempted = rounds.get(`${cell.id}:${round}`);
          assert(attempted, 'missing paired round');
          const retained = {round, namespace: attempted.namespace, legs: attempted.legs, ratios: {}};
          result.rounds.push(retained);
          for (const metric of Object.keys(metrics)) {
            try {
            const values = {};
            for (const implementation of ['go', 'native']) {
              const pair = attempted.legs.filter(leg => leg.implementation === implementation).map(leg => leg.metrics?.[metric]);
              assert(pair.length === 2 && pair.every(value => Number.isFinite(value) && value > 0), `${metric}: missing or nonpositive estimator`);
              if (metric === 'rss_incremental_peak_bytes' && implementation === 'native') assert(pair.every(value => value >= plan.rss_resolution_bytes), 'native RSS increment below resolution');
              values[implementation] = pair[0] / 2 + pair[1] / 2;
            }
            const ratio = values.go / values.native, log = Math.log(ratio);
            assert(Number.isFinite(ratio) && ratio > 0, `${metric}: undefined ratio`);
            retained.ratios[metric] = ratio;
            logs[metric].push(log);
            } catch (error) {
              result.findings.push(`round ${round}: ${error.message}`);
              retained.ratios[metric] = null;
              logs[metric].push(NaN);
            }
          }
        }
        for (const [metric, gate] of Object.entries(metrics)) {
          const samples = new Float64Array(plan.bootstrap_replicates);
          const values = logs[metric];
          if (!values.every(Number.isFinite)) continue;
          const constant = values.every(value => value === values[0]);
          const point = constant ? result.rounds[0].ratios[metric] : Math.exp(values.reduce((sum, value) => sum + value, 0) / plan.rounds);
          for (let replicate = 0; replicate < samples.length; replicate++) {
            let total = 0;
            for (let round = 0; round < plan.rounds; round++) total += values[round] * weights[replicate * plan.rounds + round];
            samples[replicate] = constant ? point : Math.exp(total / plan.rounds);
          }
          samples.sort();
          const lower = nearestRank(samples, plan.tail_probability), upper = nearestRank(samples, 1 - plan.tail_probability);
          assert([point, lower, upper].every(value => Number.isFinite(value) && value > 0), `${metric}: undefined bootstrap bound`);
          const passed = gate.direction === 'upper' ? upper <= gate.limit : lower >= gate.limit;
          result.metrics[metric] = {status: passed ? 'passed' : 'failed', point, lower, upper, ...gate, passed};
        }
        result.status = Object.values(result.metrics).some(metric => metric.status === 'unknown') ? 'unknown' : Object.values(result.metrics).every(metric => metric.passed) ? 'statistical_gates_passed' : 'statistical_gates_failed';
      } catch (error) { result.findings.push(error.message); }
    }
    output.status = output.cells.some(cell => cell.status === 'unknown') ? 'unknown' : output.cells.every(cell => cell.status === 'statistical_gates_passed') ? 'statistical_gates_passed_not_qualification' : 'statistical_gates_failed';
  } catch (error) { output.findings.push(error.message); }
  return output;
}

function validateModes(evidence, implementation) {
  assert.equal(evidence?.implementation, implementation, 'mode implementation');
  assert.equal(evidence.requested, 'secure', 'mode policy');
  assert(Array.isArray(evidence.connections), 'actual mode population');
  const identities = new Set(), services = new Set();
  for (const connection of evidence.connections) {
    assert(['monitor', 'osd'].includes(connection.service), 'mode service');
    assert(typeof connection.connection === 'string' && connection.connection.length > 0, 'mode connection identity');
    const identity = `${connection.service}:${connection.connection}`;
    assert(!identities.has(identity), 'mode connection duplicate');
    identities.add(identity); services.add(connection.service);
    assert.equal(connection.actual, 'secure', 'actual mode mismatch');
    assert.equal(connection.source, implementation === 'go' ? 'go-auth-metadata' : 'ceph-ready-log', 'mode provenance');
  }
  assert(services.has('monitor') && services.has('osd'), 'actual MON and OSD mode evidence');
}

export function qualificationDistribution(plan, records) {
  assert(Array.isArray(records) && records.length > 0, 'raw distribution population');
  const boundaries = plan.histogram_boundaries_ns, counts = Array(boundaries.length + 1).fill(0);
  const latencies = records.map(record => {
    const latency = record.end_ns - record.start_ns;
    assert(Number.isSafeInteger(latency) && latency > 0, 'raw distribution latency');
    const bin = boundaries.findIndex(boundary => latency <= boundary);
    counts[bin < 0 ? boundaries.length : bin]++;
    return latency;
  }).sort((left, right) => left - right);
  const p99 = nearestRank(latencies, .99);
  return {operations: latencies.length, p50_ns: nearestRank(latencies, .5), p95_ns: nearestRank(latencies, .95), p99_ns: p99,
    histogram: {boundaries_ns: boundaries, convention: 'upper_inclusive_with_overflow', counts, overflow: counts.at(-1)},
    tail_threshold_ns: p99, tails: records.filter(record => record.end_ns - record.start_ns >= p99)};
}

export function validateQualificationCapture(plan, attempted, leg) {
  validateQualificationPlan(plan);
  const cell = plan.cells.find(cell => cell.id === attempted.cell_id), capture = leg.capture;
  assert(cell, 'raw cell');
  assert.equal(attempted.plan_id, plan.plan_id, 'capture plan binding');
  assert.equal(capture?.status, `sustained_${leg.implementation}_capture_unqualified`, 'complete collector capture');
  assert.equal(capture.error, null, 'collector failure');
  assert.deepEqual(capture.identity, {round: attempted.round, seed: plan.seed, leg: leg.process_id}, 'capture lifecycle identity');
  assert.equal(capture.attempt?.payload_verified, true, 'capture payload correctness');
  assert.equal(capture.attempt?.cleanup_verified, true, 'capture cleanup correctness');
  const provenance = leg.provenance;
  assert.equal(provenance?.plan_id, plan.plan_id, 'provenance plan binding');
  assert.equal(provenance.namespace, attempted.namespace, 'fixture conditioning binding');
  assert.equal(provenance.pool, cell.pool, 'pool binding');
  assert.deepEqual(provenance.runtime, plan.runtime, 'runtime binding');
  assert.equal(provenance.resource_scope, plan.resource_scope, 'resource scope binding');
  assert.equal(provenance.instrumentation, plan.instrumentation[leg.implementation], 'instrumentation scope binding');
  assert.equal(provenance.rss_method, plan.rss_method, 'RSS OS method binding');
  assert.equal(capture.attempt.memory?.interval_ns, plan.rss_interval_ns, 'RSS cadence binding');
  const pins = provenance.pins;
  for (const name of ['source', 'binary', 'tool', ...(leg.implementation === 'native' ? ['library'] : [])]) {
    assert(/^[a-f0-9]{64}$/.test(pins?.before?.[name] ?? ''), `${name} artifact pin`);
    assert.equal(pins.after?.[name], pins.before[name], `${name} continuity`);
  }
  assert(typeof provenance.fsid === 'string' && provenance.fsid.length > 0, 'cluster identity');
  const warnings = validateParityHealth(provenance.health?.before, provenance.fsid);
  assert.deepEqual(validateParityHealth(provenance.health?.after, provenance.fsid), warnings, 'health warning continuity');
  const placements = provenance.placement;
  assert(Array.isArray(placements) && placements.length === cell.concurrency, 'complete worker placement population');
  for (const [worker, placement] of placements.entries()) {
    assert.equal(placement.worker, worker, 'placement worker');
    assert(placement.before?.pgid && Array.isArray(placement.before.acting) && placement.before.acting.length > 0 && Number.isInteger(placement.before.acting_primary), 'placement topology');
    for (const name of ['pgid', 'acting', 'acting_primary']) assert.deepEqual(placement.after?.[name], placement.before[name], 'placement continuity');
  }
  validateModes(leg.modes, leg.implementation);
  for (const [phase, time, count] of [['warmup', 10000000000, 10000], ['measured', 60000000000, 100000]]) {
    const population = capture.attempt[phase];
    assert(Number.isSafeInteger(population?.elapsed_ns) && population.elapsed_ns >= time, `${phase} time minimum`);
    assert(Number.isSafeInteger(population.successful_operations) && population.successful_operations >= count, `${phase} count minimum`);
    assert.equal(population.unexpected_failures, 0, `${phase} failures`);
    assert.equal(population.censored, 0, `${phase} censoring`);
    assert.equal(population.records?.length, population.successful_operations, `${phase} counter agreement`);
    validateQualificationRecords(cell, {...capture.identity, records: population.records}, population.elapsed_ns, population.operations_per_worker);
    for (const record of population.records) assert(Number.isSafeInteger(record.timeout_deadline_ns) && record.timeout_deadline_ns >= record.end_ns && record.timeout_deadline_ns <= record.start_ns + 30000000000, 'recorded operation budget');
  }
  const measured = capture.attempt.measured;
  const row = validateQualificationLeg(capture.report, leg.implementation, cell, {...capture.identity, warmup: capture.attempt.warmup,
    records: measured.records, operations_per_worker: measured.operations_per_worker, memory: capture.attempt.memory});
  return {row, distribution: qualificationDistribution(plan, measured.records), metrics: {cpu_ns_per_operation: row.cpu_ns_per_operation,
    p99_ns: row.p99_ns, successful_iops: measured.successful_operations * 1000000000 / row.elapsed_ns, rss_incremental_peak_bytes: row.rss_incremental_peak_bytes}};
}

export function analyzeQualificationCaptures(plan, attemptedRounds) {
  const output = {status: 'invalid_evidence', claim: 'statistical_tooling_not_qualification', plan_id: plan?.plan_id ?? null, findings: [], distributions: [], attempted_legs: []};
  try {
    validateQualificationPlan(plan);
    assert(Array.isArray(attemptedRounds), 'raw attempted rounds');
    const continuity = new Map();
    const converted = attemptedRounds.map(attempted => {
      const cell = plan.cells.find(cell => cell.id === attempted.cell_id);
      assert(cell && Array.isArray(attempted.legs), 'raw cell/legs');
      return {...attempted, legs: attempted.legs.map(leg => {
        const attempt = {cell_id: cell.id, round: attempted.round, process_id: leg.process_id, implementation: leg.implementation, status: 'invalid', findings: []};
        output.attempted_legs.push(attempt);
        try {
          const distribution = qualificationDistribution(plan, leg.capture?.attempt?.measured?.records);
          const retainedDistribution = {cell_id: cell.id, round: attempted.round, process_id: leg.process_id, implementation: leg.implementation, evidence_status: 'raw_distribution_not_qualification', ...distribution};
          output.distributions.push(retainedDistribution);
          const {metrics: measuredMetrics} = validateQualificationCapture(plan, attempted, leg);
          for (const [name, pin] of Object.entries(leg.provenance.pins.before)) {
            const key = ['source', 'tool', 'library'].includes(name) ? name : `${leg.implementation}:${name}`;
            if (continuity.has(key)) assert.equal(pin, continuity.get(key), `matrix ${key} continuity`);
            else continuity.set(key, pin);
          }
          retainedDistribution.evidence_status = 'leg_validated_not_matrix_qualification';
          attempt.status = 'leg_validated_not_matrix_qualification';
          return {implementation: leg.implementation, process_id: leg.process_id, metrics: measuredMetrics};
        } catch (error) {
          attempt.findings.push(error.message);
          output.findings.push(`${cell.id}:${attempted.round}:${leg.process_id}: ${error.message}`);
          return {implementation: leg.implementation, process_id: leg.process_id};
        }
      })};
    });
    if (output.findings.length) return output;
    const analyzed = analyzeQualificationMetrics(plan, converted);
    return {...output, ...analyzed, distributions: output.distributions, attempted_legs: output.attempted_legs};
  } catch (error) { output.findings.push(error.message); }
  return output;
}

const writeExclusive = (file, value) => fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n', {flag: 'wx', mode: 0o600});

export function freezeQualificationPlan(specification, output) {
  const plan = createQualificationPlan(specification);
  writeExclusive(output, plan);
  return plan;
}

export function analyzeQualificationFiles(planFile, manifestFile, outputFile) {
  assert(!fs.existsSync(outputFile), 'analysis output must be fresh');
  const plan = JSON.parse(fs.readFileSync(planFile, 'utf8'));
  const manifest = JSON.parse(fs.readFileSync(manifestFile, 'utf8'));
  assert.equal(manifest.plan_id, plan.plan_id, 'manifest plan binding');
  assert(Number.isSafeInteger(manifest.capture_started_ms) && manifest.capture_started_ms > fs.statSync(planFile).mtimeMs, 'plan must be frozen before capture');
  assert.deepEqual(manifest.analyzer_dependencies, qualificationToolPins(), 'executing analyzer dependency pins');
  const root = path.dirname(path.resolve(manifestFile)), inputs = [];
  const analyzerSHA256 = crypto.createHash('sha256').update(fs.readFileSync(fileURLToPath(import.meta.url))).digest('hex');
  const rounds = manifest.rounds.map(round => ({...round, legs: round.legs.map(leg => {
    assert.equal(leg.provenance?.pins?.before?.tool, analyzerSHA256, 'executing analyzer tool pin');
    const load = name => {
      const reference = leg[name];
      assert(reference && /^[a-zA-Z0-9_.-]+$/.test(reference.file) && reference.file !== '.' && reference.file !== '..', 'local immutable input reference');
      const file = path.join(root, reference.file), stat = fs.lstatSync(file);
      assert(stat.isFile() && !stat.isSymbolicLink() && stat.size <= 512 * 1024 * 1024, 'bounded regular evidence file');
      const bytes = fs.readFileSync(file), sha256 = crypto.createHash('sha256').update(bytes).digest('hex');
      assert.equal(sha256, reference.sha256, 'evidence input continuity');
      inputs.push({file: reference.file, sha256});
      return JSON.parse(bytes);
    };
    return {...leg, capture: load('capture'), modes: load('modes')};
  })}));
  const result = {...analyzeQualificationCaptures(plan, rounds), attempted_rounds: manifest.rounds, inputs, analyzer_sha256: analyzerSHA256, analyzer_dependencies: manifest.analyzer_dependencies,
    plan_file_sha256: crypto.createHash('sha256').update(fs.readFileSync(planFile)).digest('hex'), manifest_sha256: crypto.createHash('sha256').update(fs.readFileSync(manifestFile)).digest('hex')};
  writeExclusive(outputFile, result);
  return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [command, ...args] = process.argv.slice(2);
  if (command === 'freeze') {
    assert.equal(args.length, 2, 'usage: qualification.mjs freeze SPECIFICATION_JSON FRESH_PLAN_JSON');
    console.log(JSON.stringify({plan_id: freezeQualificationPlan(JSON.parse(fs.readFileSync(args[0], 'utf8')), args[1]).plan_id}));
  } else {
    assert.equal(command, 'analyze', 'command must be freeze or analyze');
    assert.equal(args.length, 3, 'usage: qualification.mjs analyze PLAN_JSON MANIFEST_JSON FRESH_ANALYSIS_JSON');
    console.log(JSON.stringify({status: analyzeQualificationFiles(...args).status}));
  }
}