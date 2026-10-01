import fs from 'node:fs';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { StringDecoder } from 'node:string_decoder';

export function* traceLines(file) {
  const descriptor = fs.openSync(file, 'r'), buffer = Buffer.alloc(1024 * 1024), decoder = new StringDecoder('utf8');
  let pending = '';
  try {
    let count;
    while ((count = fs.readSync(descriptor, buffer)) !== 0) {
      pending += decoder.write(buffer.subarray(0, count));
      let begin = 0, end;
      while ((end = pending.indexOf('\n', begin)) !== -1) {
        yield pending.slice(begin, end);
        begin = end + 1;
      }
      pending = pending.slice(begin);
    }
    pending += decoder.end();
    if (pending) yield pending;
  } finally { fs.closeSync(descriptor); }
}

function field(line, name) {
  const match = line.match(new RegExp(`(?:^| )${name}=("(?:[^"\\\\]|\\\\.)*"|[^ ]+)`));
  return match ? (match[1].startsWith('"') ? JSON.parse(match[1]) : match[1]) : undefined;
}
function integer(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`invalid ${name}`);
  return value;
}
const overlap = (begin, end, intervals) => intervals.reduce((total, interval) => total + Math.max(0, Math.min(end, interval.end) - Math.max(begin, interval.begin)), 0);
function union(intervals) {
  const result = [];
  for (const interval of [...intervals].sort((left, right) => left.begin - right.begin)) {
    if (interval.end < interval.begin) throw new Error('reversed trace range');
    const last = result.at(-1);
    if (last && interval.begin <= last.end) last.end = Math.max(last.end, interval.end);
    else result.push({ begin: interval.begin, end: interval.end });
  }
  return result;
}

export function summarizeTrace(text, timing) {
  if (timing.schema !== 1 || timing.label !== 'instrumented' || !Array.isArray(timing.calls) || !Array.isArray(timing.sync)) throw new Error('unsupported timing schema or label');
  const events = [];
  for (const line of typeof text === 'string' ? text.split('\n') : text) {
    const match = line.match(/^M=\S+ P=\S+ G=\S+ (\w+) Time=\d+/);
    if (match && ['Log', 'TaskBegin', 'TaskEnd', 'RangeBegin', 'RangeEnd', 'StateTransition'].includes(match[1])) events.push({ line, kind: match[1], time: BigInt(field(line, 'Time')) });
  }
  if (!events.length) throw new Error('unsupported trace debug format: require parsed high-level events (-d=parsed or -d=1)');
  events.sort((left, right) => left.time < right.time ? -1 : left.time > right.time ? 1 : 0);
  const sync = events.filter(event => event.kind === 'Log' && field(event.line, 'Category') === 'p07/sync');
  let lower, upper;
  for (const point of timing.sync) {
    integer(point.before_ns, 'sync before'); integer(point.after_ns, 'sync after');
    if (point.after_ns < point.before_ns) throw new Error('reversed sync bracket');
    const matches = sync.filter(event => field(event.line, 'Message') === String(point.id));
    if (matches.length !== 1) throw new Error('missing or duplicate synchronization log');
    const lo = matches[0].time - BigInt(point.after_ns);
    const hi = matches[0].time - BigInt(point.before_ns);
    lower = lower === undefined || lo > lower ? lo : lower;
    upper = upper === undefined || hi < upper ? hi : upper;
  }
  if (lower === undefined || lower > upper || timing.sync.length < 2) throw new Error('inconsistent or insufficient clock anchors');
  const offset = (lower + upper) / 2n;
  const relative = event => {
    const value = Number(event.time - offset);
    if (!Number.isSafeInteger(value)) throw new Error('trace duration exceeds safe integer range');
    return value;
  };
  const tasks = new Map(), indices = new Map(), runnable = new Map(), delays = [], gc = [], ranges = new Map();
  for (const event of events) {
    const { line, kind } = event;
    const time = relative(event);
    if (kind === 'TaskBegin' && field(line, 'Type') === 'p07/read') {
      const id = field(line, 'ID');
      if (tasks.has(id)) throw new Error('duplicate task');
      tasks.set(id, { begin: time, goroutine: field(line, 'G') });
    } else if (kind === 'TaskEnd' && tasks.has(field(line, 'ID'))) {
      tasks.get(field(line, 'ID')).end = time;
    } else if (kind === 'Log' && field(line, 'Category') === 'p07/index') {
      const index = Number(field(line, 'Message')), task = tasks.get(field(line, 'Task'));
      integer(index, 'arrival index');
      if (!task || indices.has(index)) throw new Error('missing task or duplicate index');
      task.index = index; indices.set(index, task);
    } else if ((kind === 'RangeBegin' || kind === 'RangeEnd') && /^stop-the-world \(GC /.test(field(line, 'Name') ?? '')) {
      const key = `${field(line, 'Name')}:${field(line, 'Scope')}`;
      if (kind === 'RangeBegin') {
        if (ranges.has(key)) throw new Error('duplicate GC range');
        ranges.set(key, time);
      } else {
        if (!ranges.has(key)) throw new Error('unmatched GC range');
        gc.push({ begin: ranges.get(key), end: time }); ranges.delete(key);
      }
    } else if (kind === 'StateTransition') {
      const transition = line.match(/(?:^| )([A-Za-z]+)->([A-Za-z]+)(?: |$)/);
      const goroutine = field(line, 'GoID');
      if (goroutine && transition) {
        if (transition[2] === 'Runnable' && transition[1] !== 'Undetermined') runnable.set(goroutine, time);
        else if (transition[1] === 'Runnable') {
          if (runnable.has(goroutine) && transition[2] === 'Running') delays.push({ goroutine, begin: runnable.get(goroutine), end: time });
          runnable.delete(goroutine);
        }
      }
    }
  }
  if (ranges.size) throw new Error('truncated GC ranges');
  const gcUnion = union(gc), callerDelays = new Map();
  for (const delay of delays) {
    if (!callerDelays.has(delay.goroutine)) callerDelays.set(delay.goroutine, []);
    callerDelays.get(delay.goroutine).push(delay);
  }
  for (const [goroutine, intervals] of callerDelays) callerDelays.set(goroutine, union(intervals));
  const seen = new Set();
  const calls = timing.calls.map(call => {
    integer(call.index, 'call index'); integer(call.begin_ns, 'call begin'); integer(call.end_ns, 'call end');
    if (seen.has(call.index) || call.end_ns < call.begin_ns) throw new Error('duplicate or reversed call');
    seen.add(call.index);
    const task = indices.get(call.index);
    if (!task || task.end === undefined) throw new Error(`missing completed trace task for index ${call.index}`);
    const intervals = [];
    for (const [beginStage, endStage] of [['read_enter', 'read_return'], ['read_enter', 'submit_enter'], ['submit_enter', 'admitted'], ['admitted', 'write_begin'], ['write_begin', 'write_end'], ['read_enter', 'write_end'], ['write_end', 'read_frame_end'], ['read_frame_begin', 'read_frame_end'], ['write_end', 'frame_received'], ['frame_received', 'reply_delivered'], ['reply_delivered', 'submit_return'], ['submit_return', 'read_return'], ['frame_received', 'read_return']]) {
      const begins = call.events.filter(event => event.stage === beginStage), ends = call.events.filter(event => event.stage === endStage);
      const name = `${beginStage}->${endStage}`;
      if (!begins.length || !ends.length) {
        intervals.push({ name, status: 'missing_stages', missing_stages: [beginStage, endStage].filter(stage => !call.events.some(event => event.stage === stage)) });
        continue;
      }
      if (begins.length !== 1 || ends.length !== 1) throw new Error('retry/duplicate stages require attempt-specific analysis');
      const begin = integer(begins[0].elapsed_ns, 'stage begin'), end = integer(ends[0].elapsed_ns, 'stage end');
      if (end < begin) {
        if (beginStage !== 'write_end' || !['frame_received', 'read_frame_end'].includes(endStage)) throw new Error('reversed transport interval');
        intervals.push({ name, status: 'independent_timestamp_order', begin_ns: begin, end_ns: end, duration_ns: end - begin, gc_stw_overlap_ns: null, caller_runnable_overlap_ns: null });
        continue;
      }
      const callerBegin = Math.max(begin, task.begin), callerEnd = Math.min(end, task.end);
      const gcOverlap = overlap(begin, end, gcUnion), runnableOverlap = callerEnd > callerBegin ? overlap(callerBegin, callerEnd, callerDelays.get(task.goroutine) ?? []) : 0;
      if (gcOverlap > end - begin || runnableOverlap > end - begin) throw new Error('overlap exceeds interval duration');
      intervals.push({ name, status: 'measured', begin_ns: begin, end_ns: end, duration_ns: end - begin, gc_stw_overlap_ns: gcOverlap, caller_runnable_overlap_ns: runnableOverlap });
    }
    return { index: call.index, error: call.error ?? null, begin_ns: call.begin_ns, end_ns: call.end_ns, caller_goroutine: task.goroutine, task_begin_ns: task.begin, task_end_ns: task.end, task_duration_ns: task.end - task.begin, intervals };
  });
  if (indices.size !== seen.size || tasks.size !== seen.size) throw new Error('unmatched trace tasks');
  return { schema: 1, label: 'instrumented', go_version: timing.go_version, alignment_uncertainty_ns: Number(upper - lower), alignment_offset_bounds_ns: { lower: lower.toString(), upper: upper.toString(), midpoint: offset.toString() }, gc_stw: gc.map(interval => ({ ...interval, duration_ns: interval.end - interval.begin })), gc_stw_union: gcUnion, runnable_delays: delays.map(interval => ({ ...interval, duration_ns: interval.end - interval.begin })), calls, limitations: 'Temporal overlap is not CPU attribution or proof of causation. Runnable durations apply to the tagged caller only, not transport goroutines. Missing transport stages remain missing; retries are rejected without excluding calls. Independent writer/receiver timestamp reversals are retained as signed gaps, not positive intervals. GC overlap uses the union of ranges. GC and runnable overlap can overlap each other and must not be added. Clock alignment uses bounded log emission samples; primary runs must be separate.' };
}

export function decodeTrace(traceFile, debugFile, go = 'go', maxBufferMiB = 256) {
  if (!Number.isInteger(maxBufferMiB) || maxBufferMiB < 1 || maxBufferMiB > 1024) throw new Error('trace debug buffer must be an integer from 1 to 1024 MiB');
  const failures = [];
  for (const mode of ['parsed', '1']) {
    const result = spawnSync(go, ['tool', 'trace', `-d=${mode}`, traceFile], { maxBuffer: maxBufferMiB * 1024 * 1024 });
    if (!result.error && result.status === 0 && /^M=\S+ P=\S+ G=\S+ \w+ Time=\d+/m.test(result.stdout.subarray(0, 65536).toString('utf8'))) {
      fs.writeFileSync(debugFile, result.stdout, { flag: 'wx', mode: 0o600 });
      return { mode, stderr: result.stderr.toString('utf8') };
    }
    failures.push({ mode, status: result.status, error: result.error?.message, stderr: result.stderr?.toString('utf8') });
  }
  throw new Error(`no supported high-level trace decoder: ${JSON.stringify(failures)}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv[2] === '--decode' && process.argv.length === 5) console.log(JSON.stringify(decodeTrace(process.argv[3], process.argv[4], 'go', Number(process.env.P07_TRACE_DEBUG_MAX_BUFFER_MB ?? 256))));
    else if (process.argv.length === 4) console.log(JSON.stringify(summarizeTrace(traceLines(process.argv[2]), JSON.parse(fs.readFileSync(process.argv[3], 'utf8'))), null, 2));
    else throw new Error('usage: trace-stall-summary.mjs TRACE_DEBUG TIMING_JSON | --decode TRACE_OUT FRESH_DEBUG_FILE');
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}