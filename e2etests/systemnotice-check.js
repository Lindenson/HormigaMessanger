// Deterministic check for Strategy C — must-arrive system notices + dead-letter (ADR-014).
// Karate can't capture WS frames on this JDK (GraalVM polyglot), so we use a raw `ws` client.
// Run against a running prod app (boot with -Dprocessing.deadletter.cleanup-every=3s for a fast sweep):
//   NODE_PATH=/opt/node/lib/node_modules/wscat/node_modules node e2etests/systemnotice-check.js <onlineId> <offlineId>
// Then verify the dead_letter table (the durable record) via psql:
//   online  → 0 rows  (delivered + SYSTEM_ACK → cleanup sweep retracted the DRAFT)
//   offline → 1 DRAFT (never delivered/acked → kept = the dead letter), payload intact
//   SELECT count(*) FROM dead_letter WHERE recipient_id='<onlineId>';                       -- expect 0
//   SELECT count(*) FROM dead_letter WHERE recipient_id='<offlineId>' AND status='DRAFT';    -- expect 1
const WS = require('ws');
const http = require('http');
const BASE='localhost', PORT=8080;
const onlineId = process.argv[2] || ('on-'+Date.now()), offlineId = process.argv[3] || ('off-'+Date.now());
const hdr=(id,role)=>({'X-User-Id':id,'X-User':role,'X-Role':role.toUpperCase(),'X-User-Email':id+'@t.eu'});
const post=(p,b,h)=>new Promise((res,rej)=>{const d=JSON.stringify(b);const r=http.request({host:BASE,port:PORT,path:p,method:'POST',headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(d),...h}},rp=>{let s='';rp.on('data',x=>s+=x);rp.on('end',()=>res(rp.statusCode));});r.on('error',rej);r.write(d);r.end();});
const connect=h=>new Promise(res=>{const w=new WS(`ws://${BASE}:${PORT}/ws`,{headers:h});const frames=[];w.on('message',m=>frames.push(''+m));w.on('open',()=>res({w,frames}));w.on('error',e=>console.log('WS err',e.message));});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{
  console.log('online='+onlineId+'  offline='+offlineId);
  console.log('offline notify HTTP', await post('/api/system/notify',{recipientId:offlineId,kind:'event',body:'overload-off'},hdr('admin1','ADMIN')));
  const c = await connect(hdr(onlineId,'CLIENT'));
  await sleep(1200);
  console.log('online notify HTTP', await post('/api/system/notify',{recipientId:onlineId,kind:'event',body:'overload-on'},hdr('admin1','ADMIN')));
  let sysout=null;
  for(let i=0;i<10 && !sysout;i++){await sleep(700);sysout=c.frames.map(j=>{try{return JSON.parse(j)}catch(e){return{}}}).find(m=>m.type==='SYSTEM_OUT');}
  console.log('online SYSTEM_OUT received:', sysout? 'YES':'NO');
  if(sysout){
    c.w.send(JSON.stringify({type:'SYSTEM_ACK',senderId:onlineId,recipientId:'server',conversationId:sysout.conversationId,messageId:'ack-'+Date.now(),correlationId:sysout.messageId,senderTimestamp:Date.now(),senderTimezone:'UTC'}));
    console.log('sent SYSTEM_ACK → draft will be retracted by the cleanup sweep');
  }
  await sleep(1500); c.w.close();
  console.log(sysout? 'PASS (delivered+acked); now verify dead_letter via psql per header':'FAIL (not delivered)');
  process.exit(sysout?0:1);
})();
