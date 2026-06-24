// Deterministic check for outbox-poller live redelivery (the fix where MessageOutboundRouter marks
// the outbound context persisted, so DeliveryStage no longer skips poller (re)delivery).
// Karate cannot capture WS frames on this JDK (GraalVM polyglot getSourceLocation()==null), so we
// use a raw `ws` client. Run against a running prod app:
//   NODE_PATH=/opt/node/lib/node_modules/wscat/node_modules node e2etests/poller-redelivery-check.js [host] [port]
// Asserts: (S1) an ONLINE recipient gets exactly ONE CHAT_OUT (idempotency cache blocks poller
// re-push → no double-delivery); (S2) a recipient OFFLINE at send time gets the held message
// re-pushed live by the poller after it reconnects (was 0 before the fix).
const WS = require('ws');
const http = require('http');
const BASE = process.argv[2] || 'localhost', PORT = +(process.argv[3] || 8080);
const hdr = (id, role) => ({ 'X-User-Id': id, 'X-User': role, 'X-Role': role.toUpperCase(), 'X-User-Email': id + '@t.eu' });
const post = (p, b, h) => new Promise((res, rej) => { const d = JSON.stringify(b);
  const r = http.request({ host: BASE, port: PORT, path: p, method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(d), ...h } },
    rp => { let s = ''; rp.on('data', x => s += x); rp.on('end', () => res(JSON.parse(s))); }); r.on('error', rej); r.write(d); r.end(); });
const connect = h => new Promise(res => { const w = new WS(`ws://${BASE}:${PORT}/ws`, { headers: h }); const frames = [];
  w.on('message', m => frames.push('' + m)); w.on('open', () => res({ w, frames })); w.on('error', e => console.log('WS err', e.message)); });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const count = (frames, type) => frames.map(j => { try { return JSON.parse(j).type } catch (e) { return '' } }).filter(t => t === type).length;
const chatIn = (mId, cId, conv, body) => JSON.stringify({ type: 'CHAT_IN', senderId: mId, recipientId: cId, conversationId: conv, messageId: 'k' + Math.random().toString(36).slice(2), senderTimestamp: Date.now(), senderTimezone: 'UTC', payload: { kind: 'text', body } });

(async () => {
  let ok = true;
  { // S1 — online: exactly one CHAT_OUT
    const u = 'on' + Date.now(), cId = 'c-' + u, mId = 'm-' + u;
    const conv = (await post('/api/chats', { clientId: cId, masterId: mId, metadata: {} }, hdr(mId, 'master'))).id;
    const m = await connect(hdr(mId, 'master')), c = await connect(hdr(cId, 'client'));
    await sleep(1500); m.w.send(chatIn(mId, cId, conv, 'hi')); await sleep(5000);
    const n = count(c.frames, 'CHAT_OUT'); ok = ok && n === 1;
    console.log(`S1 online: CHAT_OUT=${n} → ${n === 1 ? 'PASS (no double-delivery)' : 'FAIL'}`); m.w.close(); c.w.close();
  }
  { // S2 — offline at send, reconnect, poller redelivers
    const u = 'off' + Date.now(), cId = 'c-' + u, mId = 'm-' + u;
    const conv = (await post('/api/chats', { clientId: cId, masterId: mId, metadata: {} }, hdr(mId, 'master'))).id;
    const m = await connect(hdr(mId, 'master'));
    await sleep(800); m.w.send(chatIn(mId, cId, conv, 'held')); await sleep(2500);
    const c = await connect(hdr(cId, 'client')); await sleep(6000);
    const n = count(c.frames, 'CHAT_OUT'); ok = ok && n >= 1;
    console.log(`S2 offline→reconnect: CHAT_OUT=${n} → ${n >= 1 ? 'PASS (poller redelivered live)' : 'FAIL'}`); m.w.close(); c.w.close();
  }
  console.log(ok ? 'ALL PASS' : 'FAILED'); process.exit(ok ? 0 : 1);
})();
