import test from 'node:test';
import assert from 'node:assert/strict';
import {retentionMetrics} from './retention-parity.mjs';

test('retention validates every window and separates allocator metrics', () => {
  const cell = {size: 1048576, concurrency: 1, workload: 'write', operations: 1024};
  for (const implementation of ['go', 'native']) {
    const report = {implementation, transport: 'secure', windows: Array.from({length: 16}, (_, index) => ({index, size_bytes: cell.size, concurrency: 1, workload: 'write', operations: 1024, payload_verified: true, cleanup_verified: true, rss_after_cleanup_bytes: 64 * 1048576, heap_after_gc_bytes: 32 * 1048576, glibc_allocated_bytes: 32 * 1048576}))};
    const metrics = retentionMetrics(report, implementation, cell);
    assert(metrics.rss.bounded_growth); assert(metrics.allocated.bounded_growth);
    for (const window of report.windows.slice(12)) window.rss_after_cleanup_bytes += 17 * 1048576;
    assert.equal(retentionMetrics(report, implementation, cell).rss.bounded_growth, false);
    for (const mutate of [value => value.windows.pop(), value => value.windows[0].operations--, value => value.windows[5].cleanup_verified = false, value => value.windows[2].index = 0, value => value.windows[0].rss_after_cleanup_bytes = 0]) {
      const altered = structuredClone(report); mutate(altered); assert.throws(() => retentionMetrics(altered, implementation, cell));
    }
  }
});