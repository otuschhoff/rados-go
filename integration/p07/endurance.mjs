import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {fixtureName, validateParityHealth} from './parity.mjs';
import {validatePGOCapture} from './pgo.mjs';

const digest = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const save = (file, data) => fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n', {flag: 'wx', mode: 0o600});
export function endurancePlan() {
  return {version: 3, windows: 15, workloads: ['read', 'write', 'mixed'], pool: 'test-3x', size: 1048576, concurrency: 16, rounds: 2,
    runtime: {CGO_ENABLED: '0', GOTOOLCHAIN: 'go1.27.1', GOMAXPROCS: '10', GOGC: '100', GOMEMLIMIT: 'off'}, affinity: '0-9',
    measured_minimum_ns: 120000000000, measured_minimum_operations: 150000,
    growth: {bytes: 16 << 20, fraction: .10, baseline: [7, 11], final: [11, 15]}, post_close_samples: 32,
    pressure: {baseline_source:'memory.peak_after_full_cycle',delay_windows: 3, measured_minimum_ns: 96000000000, measured_minimum_operations: 120000, helper_bytes: 256 << 20, high_increment: 248 << 20, max_increment: 512 << 20, initial_max: 512 << 20, swap_bytes: 0},
    latency_max_ns: 50000000, pressure_throughput_minimum_ratio: .90, gc_pause_max_ns: 500000000,
    limitation: 'Resource/endurance diagnostics, not parity or native retry proof; lifecycle/fault/overload cases are deterministic tests, not live cluster fault injection'};
}

const median = values => { const sorted = [...values].sort((left, right) => left - right); return (sorted[Math.floor((sorted.length-1)/2)] + sorted[Math.floor(sorted.length/2)]) / 2; };
export function enduranceAssessment(observation, captures, implementation) {
  const plan = endurancePlan(); assert.equal(observation.implementation, implementation); assert.equal(observation.windows.length, plan.windows);
  assert.equal(captures.length, plan.windows); assert.equal(observation.post_close_idle.length, 32);
  const values = [], allocated = [], p99 = [], iops = [];
  assert(['go','native'].includes(implementation));
  let elapsed = 0, operations = 0, maxGCPause = 0, pressureElapsed = 0, pressureOperations = 0;
  for (const [index, capture] of captures.entries()) {
    const cell = {size: plan.size, concurrency: 16, workload: plan.workloads[index%3]};
    const metrics = validatePGOCapture(capture, cell, {round: 1, leg: `endurance-w${index}`, seed: 1101}, implementation);
    elapsed += capture.attempt.measured.elapsed_ns; operations += capture.attempt.measured.successful_operations;
    if(index>=plan.pressure.delay_windows){pressureElapsed+=capture.attempt.measured.elapsed_ns;pressureOperations+=capture.attempt.measured.successful_operations;}
    const value = observation.windows[index]; assert.equal(value.index, index);
    assert(Number.isSafeInteger(value.rss_bytes) && value.rss_bytes > 0); values.push(value.rss_bytes);
    const allocation = value[implementation === 'go' ? 'heap_after_gc_bytes' : 'glibc_allocated_bytes']; assert(Number.isSafeInteger(allocation) && allocation > 0); allocated.push(allocation);
    p99.push(metrics.p99_ns); iops.push(metrics.successful_iops);
    if (implementation === 'go') { assert(Number.isSafeInteger(value.gc_cycles) && value.gc_cycles >= 0); assert(Number.isSafeInteger(value.gc_pause_total_ns) && value.gc_pause_total_ns >= 0); if (index) { assert(value.gc_cycles >= observation.windows[index-1].gc_cycles); const delta=value.gc_pause_total_ns-observation.windows[index-1].gc_pause_total_ns;assert(delta>=0);maxGCPause=Math.max(maxGCPause,delta); } }
  }
  assert(elapsed >= plan.measured_minimum_ns && operations >= plan.measured_minimum_operations, 'sustained duration AND population');
  assert(pressureElapsed>=plan.pressure.measured_minimum_ns&&pressureOperations>=plan.pressure.measured_minimum_operations,'post-conditioning duration AND population');
  let previous = -1; const idle = observation.post_close_idle.map(value => { assert(Number.isSafeInteger(value.at_ns) && value.at_ns > previous && value.at_ns-previous <= 250000000); assert(Number.isSafeInteger(value.rss_bytes) && value.rss_bytes > 0); previous = value.at_ns; return value.rss_bytes; });
  assert(previous >= 3000000000, 'post-Close observation duration');
  const trend = series => { const baseline = median(series.slice(...plan.growth.baseline)), final = median(series.slice(...plan.growth.final)), budget = Math.max(plan.growth.bytes, baseline*plan.growth.fraction); return {baseline, final, budget, growth: final-baseline, passed: final-baseline <= budget}; };
  const rss = trend(values), allocation = trend(allocated), postClose = median(idle.slice(-10));
  return {operations, elapsed_ns: elapsed, post_conditioning_elapsed_ns:pressureElapsed, post_conditioning_operations:pressureOperations, rss, allocation, post_close_rss: postClose, post_close_passed: postClose <= Math.max(...values) + (16<<20),
    p99_ns: p99, successful_iops: iops, max_interwindow_gc_pause_ns:maxGCPause, passed: rss.passed && allocation.passed && postClose <= Math.max(...values)+(16<<20) && p99.every(value=>value<=plan.latency_max_ns) && maxGCPause<=plan.gc_pause_max_ns};
}

export function validateEnduranceMatrix(summary) {
  const plan=endurancePlan();assert.deepEqual(summary.plan,plan);assert.equal(summary.status,'endurance_diagnostic_complete_not_parity');assert.equal(summary.legs.length,8);
  const names=new Set(), namespaces=new Set();
  for(const leg of summary.legs){assert([1,2].includes(leg.round));assert(['plain','pressure'].includes(leg.condition));assert(['go','native'].includes(leg.implementation));assert.equal(leg.name,`r${leg.round}-${leg.condition}-${leg.implementation}`);assert(!names.has(leg.name));names.add(leg.name);assert(/^p07-parity-[a-z0-9-]+$/.test(leg.namespace)&&leg.namespace.length<=64&&!namespaces.has(leg.namespace));namespaces.add(leg.namespace);
    for(const boundary of ['before','after'])assert.deepEqual(validateParityHealth(leg.health[boundary],summary.fsid),summary.warnings);
    assert.equal(leg.placement.length,48);const placements=new Set();for(const value of leg.placement){assert(plan.workloads.includes(value.workload)&&Number.isInteger(value.worker)&&value.worker>=0&&value.worker<16);const id=`${value.workload}-${value.worker}`;assert(!placements.has(id));placements.add(id);assert(value.before.pgid&&value.before.acting?.length&&Number.isInteger(value.before.acting_primary));for(const key of ['pgid','acting','acting_primary'])assert.deepEqual(value.before[key],value.after[key]);}
    if(leg.condition==='pressure'){assert(Number.isSafeInteger(leg.events?.high)&&leg.events.high>0);for(const key of ['oom','oom_kill','max'])assert.equal(leg.events[key],0);validatePressureMetadata(leg.pressure);}else {assert.equal(leg.events,null);assert.equal(leg.pressure,null);}
  }
  const throughput=[];for(const implementation of ['go','native'])for(let round=1;round<=2;round++){const plain=summary.legs.find(value=>value.implementation===implementation&&value.round===round&&value.condition==='plain'),pressure=summary.legs.find(value=>value.implementation===implementation&&value.round===round&&value.condition==='pressure');const ratio=median(pressure.assessment.successful_iops.slice(plan.pressure.delay_windows))/median(plain.assessment.successful_iops.slice(plan.pressure.delay_windows));throughput.push({implementation,round,ratio,passed:ratio>=plan.pressure_throughput_minimum_ratio});}
  return {...summary,throughput,passed:summary.legs.every(value=>value.assessment.passed)&&throughput.every(value=>value.passed)};
}

export function validatePressureMetadata(value) {
  const policy=endurancePlan().pressure;assert.equal(value?.baseline_source,policy.baseline_source);assert.equal(value.helper_bytes,policy.helper_bytes);assert(Number.isSafeInteger(value.initial_group_bytes)&&value.initial_group_bytes>0&&value.initial_group_bytes<=policy.initial_max);assert(Number.isSafeInteger(value.initial_group_current_bytes)&&value.initial_group_current_bytes>0&&value.initial_group_current_bytes<=value.initial_group_bytes);assert.equal(value.high_bytes,value.initial_group_bytes+policy.high_increment);assert.equal(value.max_bytes,value.initial_group_bytes+policy.max_increment);
}

export function captureEndurance(root, prefix) {
  assert(/^p07-parity-[a-z0-9-]+$/.test(prefix) && prefix.length <= 40);
  const plan = endurancePlan(), env = process.env;
  for (const key of ['CONFIG','KEY','KEYRING','MONITORS_FILE','FSID_FILE']) assert(env[`P07_PARITY_${key}`]);
  fs.mkdirSync(root, {mode: 0o700}); save(path.join(root,'plan.json'),plan);
  const base = Object.fromEntries(['PATH','HOME','GOCACHE','TMPDIR'].filter(key=>env[key]).map(key=>[key,env[key]]));
  const execute = (name, program, args, extra={}) => {
    const value = spawnSync(program,args,{env:{...base,...plan.runtime,...extra},encoding:'utf8',timeout:960000,maxBuffer:128<<20});
    for(const stream of ['stdout','stderr']) fs.writeFileSync(path.join(root,`${name}.${stream}`),value[stream]??'',{flag:'wx',mode:0o600});
    save(path.join(root,`${name}.exit.json`),{program,args:args.map(arg=>[env.P07_PARITY_CONFIG,env.P07_PARITY_KEY,env.P07_PARITY_KEYRING].includes(arg)?'<private-path>':arg),status:value.status,signal:value.signal});
    assert.equal(value.status,0,`${name} failed; retained evidence`); return value.stdout;
  };
  const files=[...new Set(execute('sources','git',['ls-files','-co','--exclude-standard']).trim().split('\n'))].filter(file=>/(?:\.go|\.c|\.h|\.mjs|go\.mod)$/.test(file)).sort();
  const sources=()=>files.map(file=>({file,sha256:digest(file)})), frozen=sources();save(path.join(root,'sources.json'),frozen);
  const binaries={go:path.join(root,'go'),native:path.join(root,'native'),pressure:path.join(root,'pressure'),checker:path.join(root,'checker')};
  execute('build-go','go',['build','-pgo=off','-o',binaries.go,'./integration/p07/benchmark']);
  const buildInfo=execute('go-build-info','go',['version','-m',binaries.go]);assert(buildInfo.includes('go1.27.1')&&buildInfo.includes('CGO_ENABLED=0'));
  execute('build-native','gcc',['-O2','-std=c11','-D_POSIX_C_SOURCE=200809L','-Wall','-Wextra','-Werror','-pthread','integration/p07/native_qualification.c','-ldl','-o',binaries.native]);
  execute('build-pressure','gcc',['-O2','-std=c11','-Wall','-Wextra','-Werror','integration/p07/endurance_pressure.c','-o',binaries.pressure]);
  execute('build-checker','go',['build','-pgo=off','-o',binaries.checker,'./tools/perf-mode-check']);
  const pins=Object.fromEntries(Object.entries(binaries).map(([key,file])=>[key,digest(file)]));save(path.join(root,'binaries.json'),pins);
  const library=fs.realpathSync('/lib64/librados.so.2'), libraryPin=digest(library);save(path.join(root,'library.json'),{library,sha256:libraryPin});
  const fsid=fs.readFileSync(env.P07_PARITY_FSID_FILE,'utf8').trim(), monitors=fs.readFileSync(env.P07_PARITY_MONITORS_FILE,'utf8').trim().split(/\s+/).map(value=>`${value}:3300`).join(',');
  const ceph=(name,args)=>JSON.parse(execute(name,'ceph',['-c',env.P07_PARITY_CONFIG,'-n','client.amakura','-k',env.P07_PARITY_KEY,...args,'--format','json']));
  const warnings=validateParityHealth(ceph('health-before',['status']),fsid), legs=[];let goMode;
  try {
    for(let round=1;round<=2;round++) for(const condition of round===1?['plain','pressure']:['pressure','plain']) for(const implementation of round===1?['go','native']:['native','go']) {
      const name=`r${round}-${condition}-${implementation}`, output=path.join(root,name), namespace=`${prefix}-${name}`;fs.mkdirSync(output,{mode:0o700});
      assert.deepEqual(sources(),frozen);assert.equal(digest(library),libraryPin);
      const healthBefore=ceph(`${name}-health-before`,['status']);assert.deepEqual(validateParityHealth(healthBefore,fsid),warnings);
      const placement=plan.workloads.flatMap(workload=>Array.from({length:16},(_,worker)=>({workload,worker,before:ceph(`${name}-${workload}-w${worker}-before`,['osd','map',plan.pool,fixtureName({size:plan.size,concurrency:16,workload},worker),namespace])})));
      const mode=path.join(output,implementation==='go'?'modes.json':'modes.log');
      const extra={P11_ENDURANCE_ROOT:output,P07_PGO_DIAGNOSTIC:'1',P07_QUALIFICATION_FILE:path.join(output,'window0.capture.json'),P07_QUALIFICATION_ROUND:'1',P07_QUALIFICATION_SEED:'1101',P07_QUALIFICATION_LEG:'endurance-w0',P07_PARITY_NAMESPACE:namespace,P07_MATRIX_SIZE:String(plan.size),P07_MATRIX_CONCURRENCY:'16',P07_MATRIX_WORKLOAD:'read'};
      if(implementation==='go'){extra.P07_READ_INTO='1';extra.P07_MODE_EVIDENCE_FILE=mode;}else extra.P07_NATIVE_MODE_LOG=mode;
      const args=implementation==='go'?['-monitors',monitors,'-fsid',fsid,'-key-file',env.P07_PARITY_KEY,'-entity','client.amakura','-pool',plan.pool,'-transport','secure']:[env.P07_PARITY_CONFIG,env.P07_PARITY_KEYRING,plan.pool,'secure','client.amakura'];
      const target=['taskset','-c','0-9',binaries[implementation],...args];let events=null,pressure=null;
      if(condition==='pressure') {
        const group=`/sys/fs/cgroup/p11-${prefix}-${name}`;fs.mkdirSync(group);fs.writeFileSync(path.join(group,'memory.swap.max'),'0');fs.writeFileSync(path.join(group,'memory.max'),String(1<<30));
        try { execute(name,binaries.pressure,[group,output,...target],extra);events=Object.fromEntries(fs.readFileSync(path.join(group,'memory.events'),'utf8').trim().split('\n').map(line=>{const [key,value]=line.split(' ');return[key,Number(value)];}));assert(events.high>0,'actual pressure required');for(const key of ['oom','oom_kill','max'])assert.equal(events[key],0);const helperLine=fs.readFileSync(path.join(root,`${name}.stderr`),'utf8').split('\n').find(line=>line.startsWith('{"helper_bytes":'));pressure=JSON.parse(helperLine);validatePressureMetadata(pressure); }
        finally { const retained=Object.fromEntries(fs.readFileSync(path.join(group,'memory.events'),'utf8').trim().split('\n').map(line=>{const [key,value]=line.split(' ');return[key,Number(value)];}));save(path.join(output,'pressure-events.json'),retained);assert.equal(fs.readFileSync(path.join(group,'cgroup.procs'),'utf8').trim(),'','private pressure scope still active');fs.rmdirSync(group); }
      } else execute(name,target[0],target.slice(1),extra);
      if(implementation==='go')goMode=mode;else execute(`${name}-mode-check`,binaries.checker,['-go',goMode,'-native-log',mode,'-requested','secure','-native-out',path.join(output,'modes.json')]);
      const modes=JSON.parse(fs.readFileSync(path.join(output,'modes.json')));assert.equal(modes.implementation,implementation);assert.equal(modes.requested,'secure');for(const service of ['monitor','osd'])assert(modes.connections.some(value=>value.service===service&&value.actual==='secure'));for(const value of modes.connections)assert.equal(value.actual,'secure');
      const captures=Array.from({length:plan.windows},(_,index)=>JSON.parse(fs.readFileSync(path.join(output,`window${index}.capture.json`))));
      const observation=JSON.parse(fs.readFileSync(path.join(output,'endurance.json'))), assessment=enduranceAssessment(observation,captures,implementation);
      const healthAfter=ceph(`${name}-health-after`,['status']);assert.deepEqual(validateParityHealth(healthAfter,fsid),warnings);
      for(const value of placement){value.after=ceph(`${name}-${value.workload}-w${value.worker}-after`,['osd','map',plan.pool,fixtureName({size:plan.size,concurrency:16,workload:value.workload},value.worker),namespace]);assert(value.before.pgid&&value.before.acting?.length);for(const key of ['pgid','acting','acting_primary'])assert.deepEqual(value.before[key],value.after[key]);}
      assert.deepEqual(sources(),frozen);assert.equal(digest(library),libraryPin);for(const [key,file]of Object.entries(binaries))assert.equal(digest(file),pins[key]);
      const references=fs.readdirSync(output).filter(file=>file.endsWith('.json')).sort().map(file=>({file,sha256:digest(path.join(output,file))}));
      const leg={name,round,condition,implementation,namespace,assessment,events,pressure,references,health:{before:healthBefore,after:healthAfter},placement};legs.push(leg);save(path.join(root,`${name}.leg.json`),leg);console.log(`${name}: ${assessment.operations} operations, ${assessment.elapsed_ns/1e9}s measured, pass=${assessment.passed}`);
    }
    save(path.join(root,'summary.json'),validateEnduranceMatrix({status:'endurance_diagnostic_complete_not_parity',plan,fsid,warnings,legs}));
    save(path.join(root,'raw-inputs.json'),fs.readdirSync(root).filter(file=>fs.statSync(path.join(root,file)).isFile()).sort().map(file=>({file,sha256:digest(path.join(root,file))})));
  }catch(error){save(path.join(root,'failure.json'),{error:error.message,completed_legs:legs.length});const health=ceph('health-after-failure',['status']);save(path.join(root,'failed-health-check.json'),{warnings:validateParityHealth(health,fsid)});throw error;}
}

export function reproduceEndurance(root, output) {
  const summary=JSON.parse(fs.readFileSync(path.join(root,'summary.json')));assert.deepEqual(validateEnduranceMatrix(summary),summary);
  for(const pin of JSON.parse(fs.readFileSync(path.join(root,'raw-inputs.json')))){assert(/^[a-z0-9.-]+$/.test(pin.file));assert.equal(digest(path.join(root,pin.file)),pin.sha256,'retained input pin');}
  const sources=JSON.parse(fs.readFileSync(path.join(root,'sources.json')));for(const file of ['integration/p07/endurance.mjs','integration/p07/pgo.mjs','integration/p07/parity.mjs','integration/p07/qualification.mjs'])assert.equal(sources.find(value=>value.file===file)?.sha256,digest(file),'executing dependency pin');
  for(const leg of summary.legs){const dir=path.join(root,leg.name);const required=['endurance.json','modes.json',...Array.from({length:summary.plan.windows},(_,index)=>`window${index}.capture.json`),...(leg.condition==='pressure'?['pressure-events.json']:[])];for(const file of required)assert.equal(leg.references.filter(value=>value.file===file).length,1,'complete raw population');for(const reference of leg.references){assert(/^[a-z0-9.-]+$/.test(reference.file));assert.equal(digest(path.join(dir,reference.file)),reference.sha256);}const modes=JSON.parse(fs.readFileSync(path.join(dir,'modes.json')));assert.equal(modes.implementation,leg.implementation);assert.equal(modes.requested,'secure');for(const service of ['monitor','osd'])assert(modes.connections.some(value=>value.service===service&&value.actual==='secure'));for(const value of modes.connections)assert.equal(value.actual,'secure');if(leg.condition==='pressure')assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dir,'pressure-events.json'))),leg.events);const captures=Array.from({length:summary.plan.windows},(_,index)=>JSON.parse(fs.readFileSync(path.join(dir,`window${index}.capture.json`))));assert.deepEqual(enduranceAssessment(JSON.parse(fs.readFileSync(path.join(dir,'endurance.json'))),captures,leg.implementation),leg.assessment);assert.deepEqual(JSON.parse(fs.readFileSync(path.join(root,`${leg.name}.leg.json`))),leg);}
  save(output,summary);return summary;
}

if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){if(process.argv[2]==='analyze'){assert.equal(process.argv.length,5);reproduceEndurance(path.resolve(process.argv[3]),path.resolve(process.argv[4]));}else{assert.equal(process.argv.length,4);captureEndurance(path.resolve(process.argv[2]),process.argv[3]);}}