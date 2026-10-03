import {test} from 'node:test';
import assert from 'node:assert/strict';
import {syscallPlan, phaseWindow, parseSyscalls, parsePerf, validateAttributionPopulation} from './syscalls.mjs';

test('attribution clocks are lossless and reject missing or discontinuous windows', () => {
  const capture = {measurement_window: {clock: 'realtime', start_ns: '1791000000000000000', end_ns: '1791000008000000000'}, attempt: {measured: {elapsed_ns: 8000000000}}};
  assert.equal(phaseWindow(capture).end - phaseWindow(capture).start, 8000000000n);
  for (const change of [{start_ns: 1791000000000000000}, {clock: 'monotonic'}, {end_ns: '1791000009000000000'}]) assert.throws(() => phaseWindow({...capture, measurement_window: {...capture.measurement_window, ...change}}));
  assert.deepEqual(syscallPlan(), syscallPlan());
});

test('syscalls exclude file I/O and phase crossings and retain control ambiguity', () => {
  const window = {start: 2000000000n, end: 3000000000n};
  const parsed = parseSyscalls(['1.000000 socket(AF_INET, SOCK_STREAM, IPPROTO_TCP) = 5 <0.000001>\n2.100000 write(0x5, 0xab, 0x60) = 0x60 <0.000010>\n2.100020 write(0x5, 0xac, 0x180) = 0x180 <0.000010>\n2.200000 write(0x6, 0xac, 0x180) = 0x180 <0.000010>\n2.300000 futex(0xabc, FUTEX_WAIT, 1, NULL) = 0 <0.100000>\n2.900000 read(0x5, 0xab, 0x60) = 0x60 <0.200000>\n'], window);
  assert.equal(parsed.counts.write, 3); assert.equal(parsed.socket_counts.write, 2); assert.equal(parsed.control_96byte_write_candidates, 1);
  assert.equal(parsed.boundary_crossing_syscalls_excluded, 1); assert.equal(parsed.same_socket_interwrite_gaps.under_50us, 1);
  assert.throws(() => parseSyscalls(['2.000000 unexpected data'], window));
  assert.throws(() => parseSyscalls(['2.000000 read(0x5, <unfinished ...>'], window));
  const censored = parseSyscalls(['1.000000 socket(AF_INET, SOCK_STREAM, IPPROTO_TCP) = 5 <0.000001>\n2.100000 write(0x5, 0xab, 0x60) = 0x60 <0.000010>\n2.200000 futex(0xabc, FUTEX_WAIT, 0, NULL) = ?\n3.200000 +++ exited with 0 +++\n'], window);
  assert.equal(censored.termination_censored_syscalls_excluded, 1);
});

test('CPU periods and scheduler samples are separate and phase filtered', () => {
  const text = 'go 12/13 1.900000000: 100 cpu-clock: abc outside (go)\ngo 12/13 2.100000000: 100 cpu-clock: abc syscall (go)\n  abc parent (go)\ngo 12/13 2.200000000: 1 context-switches: abc schedule (kernel)\n';
  const parsed = parsePerf(text, {start: 2000000000n, end: 3000000000n});
  assert.equal(parsed.cpu_samples, 1); assert.equal(parsed.cpu_sample_period_total, 100); assert.equal(parsed.context_switch_period_total, 1);
  assert.throws(() => parsePerf('unknown', {start: 1n, end: 3n}));
  const split = parsePerf('go 12/13 2.100000000: 100 cpu-clock: \n abc syscall (go)\ngo 12/13 2.200000000: 1 context-switches/period=1/: \n abc schedule (kernel)\n', {start: 2000000000n, end: 3000000000n});
  assert.equal(split.cpu_flat_periods['syscall (go)'], 100);
  assert.equal(split.context_switch_trace[0].leaf, 'schedule (kernel)');
});

test('attribution requires every independent planned process and namespace', () => {
  const plan = syscallPlan(), legs = plan.cells.flatMap(cell => plan.probes.flatMap(probe => [1, 2].flatMap(round => ['go', 'native'].map(implementation => {
    const name = `${cell.id}-${probe}-r${round}-${implementation}`;
    return {cell, probe, round, implementation, name, namespace: `p07-parity-${name}`};
  }))));
  validateAttributionPopulation(plan, legs);
  assert.throws(() => validateAttributionPopulation(plan, legs.slice(1)));
  for (const change of [value => { value[1] = value[0]; }, value => { value[1].namespace = value[0].namespace; }, value => { value[0].round = 3; }]) {
    const altered = structuredClone(legs); change(altered); assert.throws(() => validateAttributionPopulation(plan, altered));
  }
});