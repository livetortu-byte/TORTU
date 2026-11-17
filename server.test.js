import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { once } from 'node:events';
import { test } from 'node:test';
import { WebSocket } from 'ws';

const root = path.dirname(fileURLToPath(import.meta.url));
const configFile = path.join(root, '.tortugas-config.json');

async function availablePort() {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}

async function post(base, route, body) {
  return fetch(base + route, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body)
  });
}

test('TikTok events are logged and forwarded to the overlay with config applied', { timeout: 30000 }, async () => {
  let savedConfig;
  try { savedConfig = await readFile(configFile); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const port = await availablePort();
  const base = `http://127.0.0.1:${port}`;
  const giftCounterFile = path.join(tmpdir(), `tortugas-gift-counter-${randomUUID()}.json`);
  await writeFile(giftCounterFile, JSON.stringify({ count: 7, roomId: 'test-room' }));
  const child = spawn(process.execPath, ['server.js'], {
    cwd: root,
    env: { ...process.env, PORT: String(port), AUTOCONNECT_TIKTOK: '0', GIFT_COUNTER_PATH: giftCounterFile },
    stdio: 'ignore'
  });
  let socket;
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        const response = await fetch(base + '/api/status');
        if (response.ok) { ready = true; break; }
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(ready, true, 'server should start');
    assert.equal((await fetch(base + '/api/status')).status, 200);
    for (const page of ['/', '/dashboard.html', '/events.html', '/manifest.webmanifest', '/sw.js']) {
      assert.equal((await fetch(base + page)).status, 200, `${page} should be served`);
    }
    const startupLogs = await (await fetch(base + '/api/connector-logs')).json();
    assert.ok(startupLogs.some(log => log.message.includes('Servidor iniciado')));
    await post(base, '/api/disconnect', {});
    const connectorLogs = await (await fetch(base + '/api/connector-logs')).json();
    assert.ok(connectorLogs.some(log => log.message.toLowerCase().includes('desconectado')));
    assert.equal((await post(base, '/api/connector-logs/clear', {})).status, 200);
    assert.deepEqual(await (await fetch(base + '/api/connector-logs')).json(), []);
    assert.equal((await fetch(base + '/api/connect', {
      method:'POST', headers:{'content-type':'application/json'},
      body:JSON.stringify({username:'https://tiktok.com/@bad'})
    })).status, 400);
    const config = await post(base, '/api/config', { meters: { comment: 8 }, gifts: ['rose'] });
    assert.equal(config.status, 200);
    assert.equal((await config.json()).meters.comment, 8);

    socket = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    const initialGiftCounter = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Initial gift counter timed out')), 3000);
      const onMessage = message => {
        const data = JSON.parse(message.toString());
        if (data.type !== 'giftCounter') return;
        clearTimeout(timeout);
        socket.off('message', onMessage);
        resolve(data.count);
      };
      socket.on('message', onMessage);
    });
    await once(socket, 'open');
    assert.equal(await initialGiftCounter, 7);
    const streamedConnectorLog = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Connector log timed out')), 3000);
      const onMessage = message => {
        const data = JSON.parse(message.toString());
        if (data.type !== 'connectorLog') return;
        clearTimeout(timeout);
        socket.off('message', onMessage);
        resolve(data.log);
      };
      socket.on('message', onMessage);
    });
    await post(base, '/api/disconnect', {});
    assert.match((await streamedConnectorLog).message, /desconectado/i);
    async function sendAndCheck(input, expectedTeam, expectedType) {
      let messages;
      const received = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('WebSocket event timed out')), 3000);
        const onMessage = message => {
          const data = JSON.parse(message.toString());
          if (data.event?.type !== expectedType) return;
          messages ||= [];
          messages.push(data.type);
          if (messages.includes('eventLog') && messages.includes('raceEvent')) {
            clearTimeout(timeout);
            socket.off('message', onMessage);
            resolve(data.event);
          }
        };
        socket.on('message', onMessage);
      });
      const response = await post(base, '/api/test-event', input);
      assert.equal(response.status, 201);
      const event = await received;
      assert.ok(messages.indexOf('eventLog') < messages.indexOf('raceEvent'), 'event should be logged before overlay delivery');
      assert.equal(event.team, expectedTeam);
      assert.equal(event.test, true);
      return event;
    }

    const first = await sendAndCheck({
      type: 'comment', team: 'auto', user: 'test_viewer', text: '¡Vamos cerrista!'
    }, 'cerro', 'comment');
    const second = await sendAndCheck({
      type: 'comment', team: 'auto', user: 'test_viewer', text: 'Ahora Olimpia gana'
    }, 'oli', 'comment');
    const gift = await sendAndCheck({
      type: 'gift', team: 'cerro', user: 'test_gifter'
    }, 'cerro', 'gift');
    const actions = [];
    for (const type of ['like', 'follow', 'share', 'join']) {
      actions.push(await sendAndCheck({ type, team: 'cerro', user: `test_${type}` }, 'cerro', type));
    }
    assert.equal(gift.gift, 'rose');
    assert.equal((await post(base, '/api/config', { gifts: [] })).status, 200);
    assert.equal((await post(base, '/api/test-event', { type: 'gift', team: 'cerro' })).status, 400);
    const logs = await (await fetch(base + '/api/events?limit=10')).json();
    assert.ok(logs.some(event => event.id === first.id));
    assert.ok(logs.some(event => event.id === second.id));
    assert.ok(logs.some(event => event.id === gift.id));
    for (const event of actions) assert.ok(logs.some(log => log.id === event.id), `${event.type} should be logged`);
  } finally {
    if (socket) socket.close();
    child.kill('SIGTERM');
    await once(child, 'exit');
    if (savedConfig) await writeFile(configFile, savedConfig);
    else await rm(configFile, { force: true });
    await rm(giftCounterFile, { force: true });
  }
});
