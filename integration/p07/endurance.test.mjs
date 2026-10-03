import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {endurancePlan, enduranceAssessment, validateEnduranceMatrix,validatePressureMetadata} from './endurance.mjs';
import {fixtureName} from './parity.mjs';

function captures(implementation) {
  return Array.from({length:endurancePlan().windows},(_,index)=>{
    const cell={size:1048576,concurrency:16,workload:['read','write','mixed'][index%3]},identity={round:1,leg:`endurance-w${index}`,seed:1101};
    const phase=(population,elapsed)=>({elapsed_ns:elapsed,successful_operations:population*16,unexpected_failures:0,censored:0,operations_per_worker:Array(16).fill(population),records:Array.from({length:16},(_,worker)=>Array.from({length:population},(_,ordinal)=>{
      const start=Math.floor(ordinal*elapsed/population);return{...identity,operation_id:`${identity.leg}-w${worker}-op${ordinal}`,ordinal,worker,object:fixtureName(cell,worker),type:cell.workload==='mixed'?(ordinal%2?'write':'read'):cell.workload,start_ns:start,end_ns:start+80000,success:true,error:null,timeout:false,censored:false,retry_count:implementation==='native'?null:0,timeout_deadline_ns:start+30000000000};
    })).flat()});
    return{status:`pgo_diagnostic_${implementation}_capture_unqualified`,error:null,identity,attempt:{payload_verified:true,cleanup_verified:true,warmup:phase(63,1000000000),measured:phase(625,8000000000),memory:{clock:'measurement_relative_ns',interval_ns:100000000,idle:{at_ns:-1,connected:true,equally_warmed:true,rss_bytes:4096},samples:Array.from({length:81},(_,sample)=>({at_ns:sample*100000000,rss_bytes:16384}))}},report:{implementation,transport:'secure',environment:{gomaxprocs:10},rows:[{size_bytes:cell.size,concurrency:16,workload:cell.workload,operations:10000,bytes:10000*cell.size,elapsed_ns:8000000000,p50_ns:80000,p95_ns:80000,p99_ns:80000,iops:1250,parity:{payload_verified:true,cleanup_verified:true,rss_before_bytes:4096,rss_after_bytes:16384,rss_after_cleanup_bytes:16384,measured_resources:{cpu_user_ns:1000000000,cpu_system_ns:100000000}}}]}};
  });
}

const observation=implementation=>({implementation,windows:Array.from({length:endurancePlan().windows},(_,index)=>({index,rss_bytes:32<<20,...(implementation==='go'?{heap_after_gc_bytes:2<<20,gc_cycles:index+1,gc_pause_total_ns:index*1000000}:{glibc_allocated_bytes:2<<20})})),post_close_idle:Array.from({length:32},(_,index)=>({at_ns:index*100000000,rss_bytes:24<<20}))});

test('endurance raw populations, duration, memory and post-Close gates fail closed',()=>{
  for(const implementation of ['go','native']) {
    const raw=captures(implementation),value=observation(implementation),assessment=enduranceAssessment(value,raw,implementation);assert.equal(assessment.passed,true);assert.equal(assessment.operations,150000);assert.equal(assessment.elapsed_ns,120000000000);assert.equal(assessment.post_conditioning_operations,120000);assert.equal(assessment.post_conditioning_elapsed_ns,96000000000);
    for(const change of [candidate=>candidate.windows.pop(),candidate=>candidate.windows[1].index=0,candidate=>candidate.windows[0].rss_bytes=0,candidate=>candidate.post_close_idle.pop(),candidate=>candidate.post_close_idle[10].at_ns=1,candidate=>candidate.post_close_idle[31].at_ns=2900000000]){const altered=structuredClone(value);change(altered);assert.throws(()=>enduranceAssessment(altered,raw,implementation));}
    const grown=structuredClone(value);for(const window of grown.windows.slice(11))window.rss_bytes+=32<<20;assert.equal(enduranceAssessment(grown,raw,implementation).passed,false);
    const idle=structuredClone(value);for(const sample of idle.post_close_idle)sample.rss_bytes=64<<20;assert.equal(enduranceAssessment(idle,raw,implementation).passed,false);
    assert.throws(()=>enduranceAssessment(value,raw.slice(1),implementation));
    const failed=[{...raw[0],error:'failed'},...raw.slice(1)];assert.throws(()=>enduranceAssessment(value,failed,implementation));
    if(implementation==='go'){const pause=structuredClone(value);for(const window of pause.windows.slice(8))window.gc_pause_total_ns+=1000000000;assert.equal(enduranceAssessment(pause,raw,implementation).passed,false);}
  }
});

test('endurance matrix rejects reused, missing, unsafe and remapped legs; pressure ratio is recomputed',()=>{
  const health={fsid:'unit-fsid',health:{status:'HEALTH_OK',checks:{}},pgmap:{num_pgs:64,pgs_by_state:[{state_name:'active+clean',count:64}]}},placement=['read','write','mixed'].flatMap(workload=>Array.from({length:16},(_,worker)=>({workload,worker,before:{pgid:'1.0',acting:[1,2,3],acting_primary:1},after:{pgid:'1.0',acting:[1,2,3],acting_primary:1}})));
  const pressure={baseline_source:'memory.peak_after_full_cycle',helper_bytes:256<<20,initial_group_bytes:128<<20,initial_group_current_bytes:96<<20,high_bytes:376<<20,max_bytes:640<<20};validatePressureMetadata(pressure);for(const change of [value=>value.high_bytes++,value=>value.baseline_source='current',value=>value.initial_group_bytes=513<<20,value=>value.initial_group_current_bytes=129<<20]){const altered=structuredClone(pressure);change(altered);assert.throws(()=>validatePressureMetadata(altered));}
  const legs=[1,2].flatMap(round=>['plain','pressure'].flatMap(condition=>['go','native'].map(implementation=>({round,condition,implementation,name:`r${round}-${condition}-${implementation}`,namespace:`p07-parity-unit-r${round}-${condition}-${implementation}`,assessment:{passed:true,successful_iops:Array(15).fill(1250)},health:{before:health,after:health},placement,events:condition==='pressure'?{high:10,oom:0,oom_kill:0,max:0}:null,pressure:condition==='pressure'?pressure:null}))));
  const summary={status:'endurance_diagnostic_complete_not_parity',plan:endurancePlan(),fsid:'unit-fsid',warnings:[],legs};assert.equal(validateEnduranceMatrix(summary).passed,true);
  for(const change of [value=>value.legs.pop(),value=>value.legs[1]=value.legs[0],value=>value.legs[1].namespace=value.legs[0].namespace,value=>value.legs[0].placement.pop(),value=>value.legs[0].placement[0].after.acting=[4],value=>value.legs[2].events.high=0,value=>value.legs[2].events.oom_kill=1,value=>value.legs[0].health.after.pgmap.pgs_by_state[0].state_name='active+degraded',value=>value.plan.growth.bytes++]){const altered=structuredClone(summary);change(altered);assert.throws(()=>validateEnduranceMatrix(altered));}
  const slow=structuredClone(summary);slow.legs[2].assessment.successful_iops.fill(1000);assert.equal(validateEnduranceMatrix(slow).passed,false);
});

test('strict pressure helper and native endurance config reject unsafe input before connection',()=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'p11-endurance-test-'));
  try{
    const helper=path.join(root,'helper'),native=path.join(root,'native');
    for(const [source,binary,extra]of [['integration/p07/endurance_pressure.c',helper,[]],['integration/p07/native_qualification.c',native,['-D_POSIX_C_SOURCE=200809L','-pthread','-ldl']]]){const built=spawnSync('gcc',['-O2','-std=c11','-Wall','-Wextra','-Werror',source,...extra,'-o',binary],{encoding:'utf8'});assert.equal(built.status,0,built.stderr);}
    assert.equal(spawnSync(helper,['/sys/fs/cgroup/not-private',root,'/bin/true','unused']).status,2);
    const valid={PATH:process.env.PATH,P07_PARITY_NAMESPACE:'p07-parity-unit',P07_PGO_DIAGNOSTIC:'1',P07_MATRIX_SIZE:'1048576',P07_MATRIX_CONCURRENCY:'16',P07_MATRIX_WORKLOAD:'read',P07_QUALIFICATION_FILE:`${root}/window0.capture.json`,P07_NATIVE_MODE_LOG:`${root}/modes.log`,P07_QUALIFICATION_ROUND:'1',P07_QUALIFICATION_LEG:'endurance-w0',P07_QUALIFICATION_SEED:'1101',P11_ENDURANCE_ROOT:root};
    for(const [key,value]of [['P11_ENDURANCE_ROOT','relative'],['P07_QUALIFICATION_FILE',`${root}/wrong.json`],['P07_PGO_DIAGNOSTIC',''],['P07_MATRIX_WORKLOAD','write'],['P07_QUALIFICATION_LEG','wrong'],['P07_QUALIFICATION_SEED','1102']])assert.equal(spawnSync(native,['/absent','/absent','test-3x','secure'],{env:{...valid,[key]:value},timeout:10000}).status,2);
    assert(!fs.existsSync(valid.P07_QUALIFICATION_FILE));
  }finally{fs.rmSync(root,{recursive:true,force:true});}
});