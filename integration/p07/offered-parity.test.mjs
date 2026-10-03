import {test} from 'node:test';
import assert from 'node:assert/strict';
import {offeredParityMetrics} from './offered-parity.mjs';

test('native offered outcomes are independently recounted and checked', () => {
  const rate = 1000;
  const outcomes = Array.from({length: rate * 8}, (_, index) => ({scheduled_ns: index * 1000000, enqueued_ns: index * 1000000, worker_start_ns: index * 1000000, read_start_ns: index * 1000000, returned_ns: index * 1000000 + 1000, delivery_delay_ns: 0, kind: 'success', admitted: true, attempted: true}));
  const report = {implementation: 'native', transport: 'secure', resources: {}, offered_load: {rate, window_ns: 8000000000, deadline_ns: 500000000, workers: 16, queue_capacity: 128, expected: rate * 8, admitted: rate * 8, attempted: rate * 8, success: rate * 8, timeouts: 0, overload: 0, errors: 0, canceled: 0, all_outcome_p99_ns: 1000, success_ops_per_issuance_second: rate, delivery_invalid: false, outcomes}};
  assert.equal(offeredParityMetrics(report, 'native', rate).within_budget_iops, rate);
  for (const mutate of [value => value.offered_load.success--, value => value.offered_load.outcomes.pop(), value => value.offered_load.all_outcome_p99_ns++, value => value.offered_load.outcomes[0].scheduled_ns++]) {
    const invalid = structuredClone(report); mutate(invalid);
    assert.throws(() => offeredParityMetrics(invalid, 'native', rate));
  }
  const go = structuredClone(report); go.implementation = 'go'; go.environment = {gomaxprocs: 10}; go.diagnostic = {read_api: 'read_into'};
  go.offered_load.config = {rate_ops_per_second: rate, issuance_window_ns: 8000000000, deadline_ns: 500000000, workers: 16, queue_capacity: 128, gomaxprocs: 10, gogc: 100, gomemlimit: 'off', load_case: 'none', cpu_workers: 0, allocation_workers: 0};
  assert.equal(offeredParityMetrics(go, 'go', rate).within_budget_iops, rate);
  go.offered_load.config.cpu_workers = 8;
  assert.throws(() => offeredParityMetrics(go, 'go', rate));
});