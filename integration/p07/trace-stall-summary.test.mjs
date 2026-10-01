import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { decodeTrace, summarizeTrace, traceLines } from './trace-stall-summary.mjs';

test('incremental trace lines preserve chunk boundaries, UTF-8 and final line', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'p07-trace-lines-'));
  try {
    const file = path.join(root, 'debug.txt');
    const text = 'a'.repeat(1024 * 1024 - 1) + '\u00e9\nsecond\nlast';
    fs.writeFileSync(file, text);
    assert.deepEqual([...traceLines(file)], text.split('\n'));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('decoder buffer is bounded to 1024 MiB before spawning', () => {
  for (const limit of [0, -1, 1.5, 1025, NaN]) {
    assert.throws(() => decodeTrace('unused', 'unused', 'unused', limit), /1 to 1024 MiB/);
  }
});

test('independent writer timestamps and missing ReadFrame stages remain explicit', () => {
  const text = [
    'M=1 P=0 G=7 Log Time=1000 Task=0 Category="p07/sync" Message="0"',
    'M=1 P=0 G=7 TaskBegin Time=1010 ID=1 Type="p07/read"',
    'M=1 P=0 G=7 Log Time=1011 Task=1 Category="p07/index" Message="3"',
    'M=1 P=0 G=7 RangeBegin Time=1020 Name="stop-the-world (GC mark termination)" Scope=Goroutine(7)',
    'M=1 P=0 G=7 RangeBegin Time=1025 Name="stop-the-world (GC sweep termination)" Scope=Goroutine(7)',
    'M=1 P=0 G=7 StateTransition Time=1025 Resource=Goroutine(7) Reason="" GoID=7 Running->Runnable',
    'M=1 P=0 G=7 RangeEnd Time=1030 Name="stop-the-world (GC mark termination)" Scope=Goroutine(7)',
    'M=1 P=0 G=7 RangeEnd Time=1035 Name="stop-the-world (GC sweep termination)" Scope=Goroutine(7)',
    'M=1 P=0 G=7 StateTransition Time=1040 Resource=Goroutine(7) Reason="" GoID=7 Runnable->Running',
    'M=1 P=0 G=7 TaskEnd Time=1090 ID=1',
    'M=1 P=0 G=7 StateTransition Time=1092 Resource=Goroutine(7) Reason="" GoID=7 Running->Runnable',
    'M=1 P=0 G=7 StateTransition Time=1098 Resource=Goroutine(7) Reason="" GoID=7 Runnable->Running',
    'M=1 P=0 G=7 Log Time=1100 Task=0 Category="p07/sync" Message="1"',
  ].join('\n');
  const timing = { schema: 1, label: 'instrumented', go_version: 'go1.27.1', sync: [{ id: 0, before_ns: 0, after_ns: 0 }, { id: 1, before_ns: 100, after_ns: 100 }], calls: [{ index: 3, begin_ns: 10, end_ns: 90, events: [{ stage: 'read_enter', elapsed_ns: 10 }, { stage: 'write_end', elapsed_ns: 100 }, { stage: 'frame_received', elapsed_ns: 60 }, { stage: 'read_return', elapsed_ns: 80 }] }] };
  const result = summarizeTrace(text, timing), intervals = result.calls[0].intervals;
  assert.deepEqual(result.gc_stw_union, [{ begin: 20, end: 35 }]);
  assert.equal(intervals.find(interval => interval.name === 'read_enter->read_return').gc_stw_overlap_ns, 15);
  assert.equal(intervals.find(interval => interval.name === 'read_enter->read_return').caller_runnable_overlap_ns, 15);
  const reversed = intervals.find(interval => interval.name === 'write_end->frame_received');
  assert.equal(reversed.status, 'independent_timestamp_order');
  assert.equal(reversed.duration_ns, -40);
  assert.equal(reversed.gc_stw_overlap_ns, null);
  assert.equal(intervals.find(interval => interval.name === 'write_end->read_frame_end').status, 'missing_stages');
  assert.equal(intervals.find(interval => interval.name === 'read_enter->write_end').duration_ns, 90);
  assert.equal(intervals.find(interval => interval.name === 'read_enter->write_end').caller_runnable_overlap_ns, 15);
  assert.equal(intervals.find(interval => interval.name === 'frame_received->read_return').duration_ns, 20);
  const retry = structuredClone(timing); retry.calls[0].events.push(retry.calls[0].events[0]);
  assert.throws(() => summarizeTrace(text, retry), /duplicate stages/);
});

test('actual toolchain trace reconstructs task, GC STW, runnable delays and transport overlap', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'p07-trace-test-'));
  try {
    const capture = path.join(root, 'capture');
    const result = spawnSync('go', ['test', 'integration/p07/benchmark/offered_index.go', 'integration/p07/benchmark/offered_observation_linux.go', 'integration/p07/benchmark/offered_observation_fixture_test.go', 'integration/p07/benchmark/trace_writer.go', '-run', 'TestOfferedObservationTraceFixture'], { encoding: 'utf8', env: { ...process.env, P07_TRACE_FIXTURE_DIR: capture } });
    assert.equal(result.status, 0, result.stderr + result.stdout);
    const debug = path.join(root, 'debug.txt');
    const decoder = decodeTrace(path.join(capture, 'trace.out'), debug);
    assert.ok(['parsed', '1'].includes(decoder.mode));
    const text = fs.readFileSync(debug, 'utf8'), timing = JSON.parse(fs.readFileSync(path.join(capture, 'timing.json')));
    const summary = summarizeTrace(text, timing);
    assert.equal(summary.calls[0].index, 7);
    assert.ok(summary.gc_stw.length >= 2);
    assert.ok(summary.runnable_delays.some(delay => delay.duration_ns > 0));
    assert.ok(summary.calls[0].intervals.find(interval => interval.name === 'write_end->read_frame_end').gc_stw_overlap_ns > 0);
    assert.throws(() => summarizeTrace('wire output', timing), /unsupported trace/);
    assert.throws(() => summarizeTrace(text, { ...timing, calls: [...timing.calls, timing.calls[0]] }), /duplicate/);
    assert.throws(() => summarizeTrace(text.replaceAll('TaskEnd', 'UnsupportedEnd'), timing), /missing completed/);
    assert.throws(() => summarizeTrace(text, { ...timing, sync: [] }), /anchors/);
    const retry = structuredClone(timing); retry.calls[0].events.push(retry.calls[0].events[0]);
    assert.throws(() => summarizeTrace(text, retry), /duplicate stages/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});