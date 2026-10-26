import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { OverGraph } from '../index.js';
import { fileURLToPath } from 'node:url';

const configuredEntry = process.env.OVERGRAPH_TEST_ENTRY ?? new URL('../index.js', import.meta.url).href;
const packageEntry = configuredEntry.startsWith('file:') ? fileURLToPath(configuredEntry) : configuredEntry;

// Ordinary packages select production; set 0 only for an explicit legacy build.
const candidate = process.env.OVERGRAPH_TEST_CANDIDATE !== '0';
const hooks = process.env.OVERGRAPH_TEST_HOOKS === '1';
const wait = p => { const until=Date.now()+15000; while(!existsSync(p)){if(Date.now()>until)throw Error(`missing native signal ${p}`);Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,1);} };
const drain = db => {const until=Date.now()+15000;while(db.reclamationStats().closeState!=='drained'){assert.ok(Date.now()<until,'native drain');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,1);}};
function fixture(){const root=mkdtempSync(join(tmpdir(),'eg-c3-'));return {root,path:join(root,'db')};}

test('concurrency options reject before creating a database',()=>{
  const {root,path}=fixture();
  try {
    for(const concurrency of [{unknown:1},{maxBatchOperations:true},{maxBatchBytes:-1},{generationWalBytes:1.5},{admissionTimeoutMs:Infinity},{maxBatchOperations:0},{generationOperationLimit:2**53},{maxBatchOperations:2,generationOperationLimit:1},{maxBatchBytes:NaN}]){
      assert.throws(()=>OverGraph.open(path,{concurrency}));assert.equal(existsSync(path),false);
    }
    if(!candidate){assert.throws(()=>OverGraph.open(path,{concurrency:{}}),/production-concurrency/);assert.equal(existsSync(path),false);}
  } finally {rmSync(root,{recursive:true,force:true});}
});

test('candidate structured sync/async/transaction errors and post-close stats',{skip:!candidate},async()=>{
 const {root,path}=fixture();let db;
 try{
  db=OverGraph.open(path,{concurrency:{maxBatchOperations:2,maxBatchBytes:1024*1024,generationOperationLimit:100,generationWalBytes:1024*1024,admissionTimeoutMs:1000}});
  const partial=OverGraph.open(join(root,'partial'),{concurrency:{admissionTimeoutMs:100}});partial.close();drain(partial);
  const boundary=OverGraph.open(join(root,'safe-boundary'),{concurrency:{maxBatchOperations:Number.MAX_SAFE_INTEGER,generationOperationLimit:Number.MAX_SAFE_INTEGER}});boundary.close();drain(boundary);
  const inputs=Array.from({length:3},(_,i)=>({labels:['Item'],key:`k${i}`}));
  const check=e=>{assert.ok(e instanceof Error);assert.equal(e.code,'resource_limit');assert.equal(e.resource,'max_batch_operations');assert.equal(e.requested,3);assert.equal(e.limit,2);assert.equal(e.retryable,false);return true;};
  assert.throws(()=>db.batchUpsertNodes(inputs),check);
  await assert.rejects(db.batchUpsertNodesAsync(inputs),check);
  assert.equal(db.getNodeByKey('Item','k0'),null);
  for(const asynchronous of [false,true]) {
    const txn=db.beginWriteTxn();for(let i=0;i<3;i++)txn.upsertNode('Item',`txn-${asynchronous}-${i}`);
    if(asynchronous)await assert.rejects(txn.commitAsync(),check);else assert.throws(()=>txn.commit(),check);
  }

  const stats=db.reclamationStats();assert.equal(Object.keys(stats).length,17);assert.equal(stats.closeState,'open');assert.ok(stats.registeredObjects>0);assert.equal(stats.workerCpuUs,null);
  for(const h of [stats.releaseToEligibilityUs,stats.eligibilityToDropUs]){assert.equal(h.buckets.length,64);assert.ok(h.maxUs===null||typeof h.maxUs==='number'||typeof h.maxUs==='bigint');}
  const id=await db.upsertNodeAsync('Item','owned',{props:{value:7}});const value=await db.getNodeAsync(id);
  const first=db.closeAsync();assert.strictEqual(db.closeAsync({force:true}),first);db.close();await first;await db.closeAsync();drain(db);
  assert.equal(value.props.value,7);
 }finally{db?.close({force:true});if(db)drain(db);rmSync(root,{recursive:true,force:true});}
});

test('installed native handoff completes while JS delivery is blocked',{skip:!hooks},async()=>{
 const {root,path}=fixture();let db;const hold=join(root,'handoff');
 try{
  db=OverGraph.open(path);const id=db.upsertNode('Item','snapshot',{props:{value:'original'}});
  db.bindingTestHold('handoff',hold);const result=db.getNodeAsync(id);wait(hold+'.ready');
  db.upsertNode('Item','snapshot',{props:{value:'newer'}});
  const closing=db.closeAsync();assert.strictEqual(db.closeAsync({force:true}),closing);
  db.close();drain(db); // JS is blocked; neither Promise delivery nor libuv handoff may be needed.
  writeFileSync(hold+'.release','go');
  assert.equal((await result).props.value,'original');await closing;
 }finally{writeFileSync(hold+'.release','go');db?.close({force:true});if(db)drain(db);rmSync(root,{recursive:true,force:true});}
});

test('installed real accepted outcome codes and exact u64 conversion',{skip:!hooks},async()=>{
 for(const [kind,code]of [['committed','commit_committed'],['indeterminate','commit_indeterminate']]){
  for(const async of [false,true]){
   const {root,path}=fixture();let db;
   try{db=OverGraph.open(path);db.bindingTestFault(kind);if(async)await assert.rejects(db.upsertNodeAsync('Item','accepted'),e=>e.code===code);else assert.throws(()=>db.upsertNode('Item','accepted'),e=>e.code===code);}
   finally{try{db?.close({force:true});}catch{}if(db)drain(db);rmSync(root,{recursive:true,force:true});}
  }
 }
 const {root,path}=fixture();const db=OverGraph.open(path);
 try{for(const n of [0n,9007199254740991n,9007199254740992n,18446744073709551615n]){const value=db.bindingTestAmount(n);assert.equal(BigInt(value),n);assert.equal(typeof value,n<=9007199254740991n?'number':'bigint');}}
 finally{db.close();drain(db);rmSync(root,{recursive:true,force:true});}
});

test('cancelled native transaction ticket cannot leave an ordering hole',{skip:!hooks},async()=>{
 const {root,path}=fixture();let db;
 try{db=OverGraph.open(path);const txn=db.beginWriteTxn();txn.bindingTestCancelTicket();const ref=await txn.upsertNodeAsync('Item','ordered');await assert.rejects(txn.removeNodeLabelAsync(ref,'Item'),/last node label/);txn.bindingTestCancelTicket();await txn.commitAsync();assert.ok(db.getNodeByKey('Item','ordered'));}
 finally{db?.close({force:true});if(db)drain(db);rmSync(root,{recursive:true,force:true});}
});

test('candidate close requests progress with saturated libuv pools',{skip:!hooks},async()=>{
 const {spawnSync}=await import('node:child_process');
 const entry=packageEntry;
 for(const size of [1,4]){
  const script=`
  import {createRequire} from 'node:module';import {mkdtempSync,existsSync,writeFileSync,rmSync}from'node:fs';import{join}from'node:path';import{tmpdir}from'node:os';import{spawn}from'node:child_process';import assert from'node:assert/strict';
  const require=createRequire(import.meta.url);const{OverGraph}=require(${JSON.stringify(entry)});const root=mkdtempSync(join(tmpdir(),'eg-pool-'));const db=OverGraph.open(join(root,'db'));const id=db.upsertNode('Item','admitted');
  const wait=p=>{const end=Date.now()+15000;while(!existsSync(p)){assert(Date.now()<end);Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,1);}};
  const holds=[];const active=[];
  // Each native read reaches actual admission before the next is submitted.
  for(let i=0;i<${size};i++){const p=join(root,'held'+i);holds.push(p);db.bindingTestHold('read',p);active.push(db.getNodeAsync(id));wait(p+'.ready');}
  const queued=db.getNodeAsync(id).then(()=>({ok:true}),error=>({error}));
  const closing=db.closeAsync();assert.strictEqual(db.closeAsync({force:true}),closing);assert.throws(()=>db.getNode(id),/closed/);
  const helper=spawn(process.execPath,['-e',\`const fs=require('fs');const p=JSON.parse(process.argv[1]);for(const x of p)fs.writeFileSync(x+'.release','go');\`,JSON.stringify(holds)],{stdio:'inherit'});
  db.close();await closing;for(const result of await Promise.all(active))assert.equal(result.key,'admitted');assert.match((await queued).error.message,/closed/);
  while(db.reclamationStats().closeState!=='drained')await new Promise(r=>setTimeout(r,1));rmSync(root,{recursive:true,force:true});
  `;
  const child=spawnSync(process.execPath,['--input-type=module','-e',script],{env:{...process.env,UV_THREADPOOL_SIZE:String(size)},encoding:'utf8',timeout:25000});
  assert.equal(child.status,0,child.stderr+child.stdout);
 }
});

test('returned open transaction keeps only its legitimate client custody',{skip:!hooks},async()=>{
 const {root,path}=fixture();let db;const hold=join(root,'returned');let txn;
 try{db=OverGraph.open(path);db.bindingTestHold('handoff',hold);const task=db.beginWriteTxnAsync();wait(hold+'.ready');db.close();assert.equal(db.reclamationStats().closeState,'draining');writeFileSync(hold+'.release','go');txn=await task;txn.rollback();txn=null;drain(db);}
 finally{writeFileSync(hold+'.release','go');if(txn)txn.rollback();db?.close({force:true});if(db)drain(db);rmSync(root,{recursive:true,force:true});}
});

test('sync progress callback refused close leaves wrapper usable',{skip:!candidate},()=>{
 const {root,path}=fixture();let db;let invoked=false;
 try{
  db=OverGraph.open(path,{compactAfterNFlushes:0});
  for(let part=0;part<2;part++){db.batchUpsertNodes(Array.from({length:100},(_,i)=>({labels:['Item'],key:`${part}-${i}`})));db.flush();}
  db.compactWithProgress(()=>{invoked=true;assert.throws(()=>db.close(),/self-dependent/);assert.throws(()=>db.closeAsync(),/self-dependent/);return true;});
  assert.equal(invoked,true);assert.ok(db.getNodeByKey('Item','0-0'));
 }finally{db?.close({force:true});if(db)drain(db);rmSync(root,{recursive:true,force:true});}
});

test('exact mutation RETURN survives later native commits before delivery',{skip:!hooks},async()=>{
 const {root,path}=fixture();let db;const hold=join(root,'return');
 try{
  db=OverGraph.open(path);db.bindingTestHold('handoff',hold);
  const result=db.executeGqlAsync("CREATE (n:Item {elementKey: 'return', value: 'original'}) RETURN n.value AS value");wait(hold+'.ready');
  db.upsertNode('Item','return',{props:{value:'later'}});
  db.close();drain(db);writeFileSync(hold+'.release','go');
  assert.deepEqual((await result).rows,[{value:'original'}]);
 }finally{writeFileSync(hold+'.release','go');db?.close({force:true});if(db)drain(db);rmSync(root,{recursive:true,force:true});}
});

test('Worker teardown discards pending close delivery without joining shutdown',{skip:!hooks},async()=>{
 const {Worker}=await import('node:worker_threads');const {once}=await import('node:events');
 const {root,path}=fixture();const shutdown=join(root,'shutdown');const completed=join(root,'completed');
 const entry=packageEntry;
 assert.ok(entry,'installed entry identity');
 const worker=new Worker(`const{parentPort}=require('node:worker_threads');const{OverGraph}=require(${JSON.stringify(entry)});const db=OverGraph.open(${JSON.stringify(path)});db.upsertNode('Item','survives');db.bindingTestHold('shutdown',${JSON.stringify(shutdown)});db.bindingTestHold('completed',${JSON.stringify(completed)});db.closeAsync();parentPort.postMessage('ready');setInterval(()=>{},1000);`,{eval:true});
 try{
  await once(worker,'message');wait(shutdown+'.ready');
  let timer;try{await Promise.race([worker.terminate(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Worker teardown waited for shutdown')),5000);})]);}finally{clearTimeout(timer);}
  writeFileSync(shutdown+'.release','go');writeFileSync(completed+'.release','go');wait(completed+'.ready');
  let db;const end=Date.now()+15000;
  while(!db){try{db=OverGraph.open(path);}catch(error){assert.ok(Date.now()<end,error.message);await new Promise(r=>setTimeout(r,1));}}
  try{assert.ok(db.getNodeByKey('Item','survives'));db.close();drain(db);}finally{db.close({force:true});}
 }finally{writeFileSync(shutdown+'.release','go');writeFileSync(completed+'.release','go');await worker.terminate();rmSync(root,{recursive:true,force:true});}
});

test('native close releases its Promise reference on delivery',{skip:!hooks},async()=>{
 const {root,path}=fixture();const db=OverGraph.open(path);
 try{const closing=db.closeAsync();assert.equal(db.bindingTestCloseReferenceHeld(),true);await closing;assert.equal(db.bindingTestCloseReferenceHeld(),false);drain(db);}
 finally{db.close({force:true});drain(db);rmSync(root,{recursive:true,force:true});}
});

test('an unreachable closed wrapper and attached Promise can be collected',{skip:!candidate},async()=>{
 const {spawnSync}=await import('node:child_process');const entry=packageEntry;assert.ok(entry);
 const script=`const{OverGraph}=require(${JSON.stringify(entry)});const fs=require('fs');const path=require('path');const os=require('os');const assert=require('assert/strict');
 (async()=>{const root=fs.mkdtempSync(path.join(os.tmpdir(),'eg-close-gc-'));try{let db=OverGraph.open(path.join(root,'db'));let promise=db.closeAsync();await promise;while(db.reclamationStats().closeState!=='drained')await new Promise(setImmediate);promise.owner=db;const weak=new WeakRef(db);db=null;promise=null;let collected=false;
 for(let i=0;i<100;i++){await new Promise(setImmediate);global.gc();await new Promise(setImmediate);if(!weak.deref()){collected=true;break;}}assert.equal(collected,true,'native Promise reference retained closed wrapper cycle');}finally{fs.rmSync(root,{recursive:true,force:true});}})().catch(error=>{console.error(error);process.exitCode=1;});`;
 const child=spawnSync(process.execPath,['--expose-gc','-e',script],{encoding:'utf8',timeout:20000});assert.equal(child.status,0,child.stderr+child.stdout);
});

for (const [key, value] of Object.entries({
  maxBatchOperations: 65536n,
  maxBatchBytes: 16777216n,
  generationOperationLimit: 1048576n,
  generationWalBytes: 536870912n,
  admissionTimeoutMs: 1000n,
})) {
  test(`small BigInt concurrency.${key} rejects before database creation`, {skip:!candidate}, () => {
    const {root,path}=fixture();let opened;
    try {
      assert.throws(() => { opened=OverGraph.open(path,{concurrency:{[key]:value}}); }, /Number/);
      assert.equal(existsSync(path),false);
    } finally {
      if(opened){opened.close({force:true});drain(opened);}
      rmSync(root,{recursive:true,force:true});
    }
  });
}

test('concurrency options require primitive safe integer Numbers', {skip:!candidate}, () => {
  const {root,path}=fixture();
  try {
    for (const key of ['maxBatchOperations','maxBatchBytes','generationOperationLimit','generationWalBytes','admissionTimeoutMs']) {
      for (const value of [true,'1000',null,undefined,Object(1000),Object(1000n),-1,0.5,NaN,Infinity,2**53]) {
        assert.throws(() => OverGraph.open(path,{concurrency:{[key]:value}}), /Number/);
        assert.equal(existsSync(path),false);
      }
    }
    for (const concurrency of [[],[1000],true,'1000',1000,1000n]) {
      assert.throws(() => OverGraph.open(path,{concurrency}));
      assert.equal(existsSync(path),false);
    }
  } finally {rmSync(root,{recursive:true,force:true});}
});
