// Deterministic WS delivery check (raw `ws` client) — used to verify live frame delivery that
// Karate 1.4.1 + GraalVM cannot capture on this JDK (its WS message handler returns null).
// Run against a running prod app (ws module comes from the global wscat install):
//   NODE_PATH=/opt/node/lib/node_modules/wscat/node_modules node e2etests/wsdelivery-check.js [host] [port]
// Asserts: CHAT_IN -> recipient gets CHAT_OUT + sender gets CHAT_ACK; SIGNAL_IN -> recipient gets SIGNAL_OUT.
const WS = require('ws');
const http = require('http');
const BASE = process.argv[2] || 'localhost', PORT = +(process.argv[3] || 8080);
const uid = 'n' + Date.now(), cId = 'client-' + uid, mId = 'master-' + uid;
const hdr = (id, role) => ({ 'X-User-Id': id, 'X-User': role, 'X-Role': role.toUpperCase(), 'X-User-Email': id + '@t.eu' });
const post = (path, body, h) => new Promise((res, rej) => { const d = JSON.stringify(body);
  const r = http.request({ host: BASE, port: PORT, path, method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(d), ...h } },
    rp => { let b = ''; rp.on('data', x => b += x); rp.on('end', () => res({ status: rp.statusCode, body: b })); }); r.on('error', rej); r.write(d); r.end(); });
const connect = h => new Promise(res => { const w = new WS(`ws://${BASE}:${PORT}/ws`, { headers: h }); const frames = [];
  w.on('message', m => frames.push('' + m)); w.on('open', () => res({ w, frames })); w.on('error', e => console.log('WS err', e.message)); });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const types = a => a.map(j => { try { return JSON.parse(j).type; } catch (e) { return '?'; } });
(async () => {
  const conv = await post('/api/chats', { clientId: cId, masterId: mId, metadata: {} }, hdr(mId, 'master'));
  const convId = JSON.parse(conv.body).id;
  const m = await connect(hdr(mId, 'master')), c = await connect(hdr(cId, 'client'));
  await sleep(1500);
  m.w.send(JSON.stringify({ type: 'CHAT_IN', senderId: mId, recipientId: cId, conversationId: convId, messageId: 'chat-' + uid, senderTimestamp: Date.now(), senderTimezone: 'UTC', payload: { kind: 'text', body: 'hi' } }));
  await sleep(2000);
  m.w.send(JSON.stringify({ type: 'SIGNAL_IN', senderId: mId, recipientId: cId, conversationId: convId, messageId: 'sig-' + uid, senderTimestamp: Date.now(), senderTimezone: 'UTC', payload: { kind: 'custom', body: 'sdp' } }));
  await sleep(2500);
  const ct = types(c.frames), mt = types(m.frames);
  console.log('CLIENT:', ct.join(','), '\nMASTER:', mt.join(','));
  const ok = ct.includes('CHAT_OUT') && ct.includes('SIGNAL_OUT') && mt.includes('CHAT_ACK');
  console.log(ok ? 'PASS: CHAT_OUT + SIGNAL_OUT + CHAT_ACK all delivered' : 'FAIL');
  process.exit(ok ? 0 : 1);
})();
