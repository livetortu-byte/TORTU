import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFile, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

const root = path.dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.PORT) || 3000;
const versionFiles = ['server.js', 'index.html', 'dashboard.html', 'events.html', 'manifest.webmanifest', 'sw.js', 'version-check.js'];
const appVersion = createHash('sha256')
  .update(Buffer.concat(await Promise.all(versionFiles.map(file => readFile(path.join(root, file))))))
  .digest('hex').slice(0, 16);
const configPath = process.env.CONFIG_PATH || path.join(root, '.tortugas-config.json');
const giftCounterPath = process.env.GIFT_COUNTER_PATH || path.join(path.dirname(configPath), '.tortugas-gift-counter.json');
const RETRY_MS = 5000;
const RETRY_WINDOW_MS = 5 * 60 * 1000;
const MAX_EVENTS = 500;
const MAX_CONNECTOR_LOGS = 500;
const defaultConfig = {
  on: true, show: 5, alertFall: 8, alertFallEnabled: false, gifts: ['rose', 'donut', 'heart', 'confetti', 'corgi', 'lion', 'galaxy', 'ice', 'rocket', 'diamond', 'crown', 'car', 'fireworks', 'music', 'star'],
  lanes: { cerro: true, oli: true },
  meters: { like: 5, comment: 15, share: 20, follow: 20, join: 2 },
  autoGift: true, autoGiftInterval: 60,
  sound: true, voice: true, vol: 70, bot: false, rate: 30
};
let config = structuredClone(defaultConfig);
try {
  const stored = JSON.parse(await readFile(configPath, 'utf8'));
  config = {
    ...config, ...stored,
    lanes: { ...config.lanes, ...(stored.lanes || {}) },
    meters: { ...config.meters, ...(stored.meters || {}) }
  };
} catch (error) {
  if (error.code !== 'ENOENT') console.warn('No se pudo cargar la configuración guardada:', error.message);
}
let giftCounter = 0;
let giftRoomId = '';
try {
  const saved = JSON.parse(await readFile(giftCounterPath, 'utf8'));
  if (Number.isSafeInteger(saved.count) && saved.count >= 0 && typeof saved.roomId === 'string') {
    giftCounter = saved.count;
    giftRoomId = saved.roomId.slice(0, 100);
  } else console.warn('El contador de regalos guardado no tiene un formato válido.');
} catch (error) {
  if (error.code !== 'ENOENT') console.warn('No se pudo cargar el contador de regalos:', error.message);
}
let status = { state: 'disconnected', username: 'tortulive', retryCount: 0, message: 'Desconectado' };
let connection = null;
let connectionGeneration = 0;
let retryTimer = null;
let retryStartedAt = 0;
let assignmentIndex = 0;
const userTeams = new Map();
const eventLog = [];
const connectorLogs = [];
const sockets = new Set();
let giftCounterWrite = Promise.resolve();

function broadcast(payload) {
  const text = JSON.stringify(payload);
  for (const socket of sockets) {
    if (socket.readyState === 1) socket.send(text);
  }
}

function setStatus(next) {
  status = { ...status, ...next };
  const level = status.state === 'failed' ? 'error' : status.state === 'retrying' ? 'warn' : 'info';
  const username = status.username ? ` @${status.username}` : '';
  const attempt = status.message === 'Conectando a TikTok…' ? ` (intento ${status.retryCount + 1})` : '';
  const retryAt = status.nextRetryAt ? `; próximo intento ${new Date(status.nextRetryAt).toISOString()}` : '';
  recordConnectorLog(level, `${status.state}${username}${attempt} — ${status.message || ''}${retryAt}`);
  broadcast({ type: 'connection', status });
}

function safeText(value, max = 500) {
  return String(value ?? '').replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, max);
}

function safeConnectorText(value, max = 500) {
  const redacted = String(value ?? '')
    .replace(/https?:\/\/[^\s"'<>]+/gi, '[URL omitida]')
    .replace(/\b(msToken|signature|sig|sessionid|session_id|token|key)=([^&\s]+)/gi, '$1=[oculto]');
  return safeText(redacted, max);
}

function recordConnectorLog(level, message) {
  const log = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: new Date().toISOString(),
    level,
    message: safeConnectorText(message)
  };
  connectorLogs.unshift(log);
  if (connectorLogs.length > MAX_CONNECTOR_LOGS) connectorLogs.length = MAX_CONNECTOR_LOGS;
  broadcast({ type: 'connectorLog', log });
}

function persistGiftCounter() {
  const snapshot = JSON.stringify({ count: giftCounter, roomId: giftRoomId });
  giftCounterWrite = giftCounterWrite.catch(() => {}).then(() => writeFile(giftCounterPath, snapshot, 'utf8'));
  giftCounterWrite.catch(error => console.error('No se pudo guardar el contador de regalos:', error.message));
  return giftCounterWrite;
}

function connectionError(error) {
  const exception = error?.exception;
  const statusCode = exception?.response?.status || exception?.status;
  const errorName = exception?.name || error?.name;
  const prefix = errorName && errorName !== 'Error' ? `${errorName}: ` : '';
  if (statusCode) return safeConnectorText(`${prefix}TikTok respondió con HTTP ${statusCode}`, 180);
  return safeConnectorText(`${prefix}${exception?.message || error?.info || error?.message || 'Error de conexión'}`, 180);
}

function normalizeTeam(text) {
  const normalized = String(text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const words = normalized.match(/[a-z0-9]+/g) || [];
  const variants = words.filter(word => word.length >= 5);
  const near = (word, target) => {
    if (word.includes(target)) return true;
    if (Math.abs(word.length - target.length) > 1) return false;
    let edits = 0, i = 0, j = 0;
    while (i < word.length && j < target.length) {
      if (word[i] === target[j]) { i++; j++; continue; }
      if (++edits > 1) return false;
      if (word.length > target.length) i++;
      else if (target.length > word.length) j++;
      else { i++; j++; }
    }
    return edits + (i < word.length || j < target.length ? 1 : 0) <= 1;
  };
  const cerro = variants.some(word => near(word, 'cerro') || near(word, 'cerrista'));
  const olimpia = variants.some(word => near(word, 'olimpia') || near(word, 'olimpista'));
  if (cerro !== olimpia) return cerro ? 'cerro' : 'oli';
  return null;
}

function teamFor(userId, comment) {
  const mentioned = comment ? normalizeTeam(comment) : null;
  if (mentioned) {
    userTeams.set(userId, mentioned);
    return mentioned;
  }
  if (userTeams.has(userId)) return userTeams.get(userId);
  const team = assignmentIndex++ % 2 === 0 ? 'cerro' : 'oli';
  userTeams.set(userId, team);
  return team;
}

function giftKey(name) {
  const key = String(name || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const aliases = [
    ['rose', ['rose', 'rosa']], ['donut', ['donut', 'dona', 'donut']], ['heart', ['heart', 'corazon']],
    ['confetti', ['confetti', 'confeti']], ['corgi', ['corgi']], ['lion', ['lion', 'leon']],
    ['galaxy', ['galaxy', 'galaxia']], ['ice', ['ice cream', 'helado']], ['rocket', ['rocket', 'cohete']],
    ['diamond', ['diamond', 'diamante']], ['crown', ['crown', 'corona']], ['car', ['race car', 'car', 'auto']],
    ['fireworks', ['fireworks', 'fuegos']], ['music', ['music', 'musica']], ['star', ['star', 'estrella']]
  ];
  return aliases.find(([, words]) => words.some(word => key.includes(word)))?.[0] || 'unknown';
}

function publishEvent({ type, user, nickname, avatar, text, gift, count = 1, team, test = false }) {
  const safeType = ['like', 'comment', 'follow', 'share', 'join', 'gift'].includes(type) ? type : null;
  if (!safeType) return null;
  const event = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: new Date().toISOString(),
    type: safeType,
    team: team || teamFor(user, safeType === 'comment' ? text : ''),
    user: safeText(user || 'espectador', 80),
    nickname: safeText(nickname || user || 'Espectador', 80),
    avatar: safeText(avatar, 1000),
    text: safeText(text, 300),
    gift: safeText(gift || '', 80),
    count: Math.max(1, Math.min(1000, Math.floor(Number(count) || 1))),
    test
  };
  event.content = safeType === 'comment' ? event.text : safeType === 'gift' ? `${event.count} × ${event.gift || 'Regalo'}` : `${event.count} × ${safeType}`;
  eventLog.unshift(event);
  if (eventLog.length > MAX_EVENTS) eventLog.length = MAX_EVENTS;
  if (safeType === 'gift' && !test) {
    giftCounter++;
    persistGiftCounter();
    broadcast({ type: 'giftCounter', count: giftCounter });
  }
  broadcast({ type: 'eventLog', event });
  broadcast({ type: 'raceEvent', event });
  return event;
}

function identity(data) {
  const user = data?.user || {};
  const imageUrl = value => typeof value === 'string'
    ? value
    : value?.urlList?.[0] || value?.url_list?.[0] || value?.url?.[0] || '';
  return {
    user: safeText(user.uniqueId || user.userId || user.displayId || 'espectador', 80),
    nickname: safeText(user.nickname || user.uniqueId || 'Espectador', 80),
    avatar: imageUrl(user.profilePictureUrl) || imageUrl(user.profilePicture) ||
      imageUrl(user.profilePictureMedium) || imageUrl(user.profilePictureLarge) || imageUrl(user.avatarThumb)
  };
}

function attachTikTokEvents(conn) {
  conn.on(WebcastEvent.CHAT, data => {
    const person = identity(data);
    publishEvent({ ...person, type: 'comment', text: data.comment });
  });
  conn.on(WebcastEvent.LIKE, data => {
    const person = identity(data);
    publishEvent({ ...person, type: 'like', count: data.likeCount || 1 });
  });
  conn.on(WebcastEvent.FOLLOW, data => publishEvent({ ...identity(data), type: 'follow' }));
  conn.on(WebcastEvent.SHARE, data => publishEvent({ ...identity(data), type: 'share' }));
  conn.on(WebcastEvent.MEMBER, data => publishEvent({ ...identity(data), type: 'join' }));
  conn.on(WebcastEvent.GIFT, data => {
    if (data.giftDetails?.giftType === 1 && data.repeatEnd === false) return;
    const person = identity(data);
    const name = data.giftDetails?.giftName || 'Regalo';
    publishEvent({ ...person, type: 'gift', gift: giftKey(name), text: name, count: data.repeatCount || 1 });
  });
  conn.on('disconnected', () => {
    if (conn === connection) scheduleRetry('La conexión con TikTok se interrumpió');
  });
  conn.on('error', error => {
    if (conn === connection) scheduleRetry(connectionError(error));
  });
}

function scheduleRetry(message) {
  if (!status.username || retryTimer || status.state === 'failed') return;
  if (!retryStartedAt) retryStartedAt = Date.now();
  const generation = connectionGeneration;
  if (connection && typeof connection.disconnect === 'function') {
    const old = connection;
    connection = null;
    old.disconnect().catch(() => {});
  }
  if (Date.now() - retryStartedAt >= RETRY_WINDOW_MS) {
    setStatus({ state: 'failed', message, retryCount: status.retryCount });
    return;
  }
  setStatus({ state: 'retrying', message, retryCount: status.retryCount + 1, nextRetryAt: Date.now() + RETRY_MS });
  retryTimer = setTimeout(() => {
    retryTimer = null;
    if (generation === connectionGeneration) connectAttempt(generation);
  }, RETRY_MS);
}

async function connectAttempt(generation) {
  if (generation !== connectionGeneration || !status.username) return;
  setStatus({ state: status.retryCount ? 'retrying' : 'connecting', message: 'Conectando a TikTok…' });
  const attemptStartedAt = Date.now();
  let conn;
  try {
    conn = new TikTokLiveConnection(status.username, {});
    connection = conn;
    attachTikTokEvents(conn);
    const connectionState = await conn.connect();
    if (generation !== connectionGeneration) {
      await conn.disconnect();
      return;
    }
    const roomId = String(connectionState?.roomId || connectionState?.roomInfo?.roomId || '');
    if (roomId && roomId !== giftRoomId) {
      giftRoomId = roomId;
      giftCounter = 0;
      persistGiftCounter();
      broadcast({ type: 'giftCounter', count: giftCounter });
      recordConnectorLog('info', 'Nueva transmisión detectada; contador de regalos reiniciado.');
    }
    recordConnectorLog('info', `LIVE conectado en ${Date.now() - attemptStartedAt} ms; sala ${roomId || 'ID no informado'}.`);
    retryStartedAt = 0;
    setStatus({ state: 'connected', message: 'Conectado', retryCount: 0, nextRetryAt: null });
  } catch (error) {
    if (generation === connectionGeneration) {
      const message = connectionError(error);
      recordConnectorLog('error', `Intento ${status.retryCount + 1} falló tras ${Date.now() - attemptStartedAt} ms: ${message}`);
      scheduleRetry(message);
    }
  }
}

async function disconnectTikTok() {
  connectionGeneration++;
  clearTimeout(retryTimer);
  retryTimer = null;
  const old = connection;
  connection = null;
  if (old) await old.disconnect().catch(() => {});
  setStatus({ state: 'disconnected', username: '', retryCount: 0, nextRetryAt: null, message: 'Desconectado' });
}

function json(response, code, payload) {
  response.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  response.end(JSON.stringify(payload));
}

async function bodyJson(request) {
  let raw = '';
  for await (const chunk of request) {
    raw += chunk;
    if (raw.length > 32_000) throw new Error('Solicitud demasiado grande');
  }
  return raw ? JSON.parse(raw) : {};
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  if (url.pathname === '/api/status' && request.method === 'GET') return json(response, 200, status);
  if (url.pathname === '/api/version' && request.method === 'GET') return json(response, 200, { version: appVersion });
  if (url.pathname === '/api/config' && request.method === 'GET') return json(response, 200, config);
  if (url.pathname === '/api/gift-counter/reset' && request.method === 'POST') {
    const previousCount = giftCounter;
    giftCounter = 0;
    try { await persistGiftCounter(); }
    catch (error) {
      giftCounter = previousCount;
      return json(response, 500, { error: 'No se pudo guardar el reinicio del contador de regalos.' });
    }
    broadcast({ type: 'giftCounter', count: giftCounter });
    return json(response, 200, { count: giftCounter });
  }
  if (url.pathname === '/api/connector-logs' && request.method === 'GET') return json(response, 200, connectorLogs);
  if (url.pathname === '/api/connector-logs/clear' && request.method === 'POST') {
    connectorLogs.length = 0;
    broadcast({ type: 'connectorLogsCleared' });
    return json(response, 200, { cleared: true });
  }
  if (url.pathname === '/api/events' && request.method === 'GET') {
    const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit')) || 100));
    return json(response, 200, eventLog.slice(0, limit));
  }
  if (url.pathname === '/api/config' && request.method === 'POST') {
    try {
      const incoming = await bodyJson(request);
      if (incoming.gifts !== undefined && (!Array.isArray(incoming.gifts) || incoming.gifts.some(gift => typeof gift !== 'string'))) {
        return json(response, 400, { error: 'La lista de regalos no es válida.' });
      };
      if (incoming.alertFall !== undefined && (!Number.isFinite(Number(incoming.alertFall)) || Number(incoming.alertFall) < 3 || Number(incoming.alertFall) > 20)) {
        return json(response, 400, { error: 'La duración de caída debe estar entre 3 y 20 segundos.' });
      }
      if (incoming.alertFallEnabled !== undefined && typeof incoming.alertFallEnabled !== 'boolean') {
        return json(response, 400, { error: 'La opción de caída debe ser booleana.' });
      }
      if (incoming.autoGift !== undefined && typeof incoming.autoGift !== 'boolean') {
        return json(response, 400, { error: 'La opción de regalo automático debe ser booleana.' });
      }
      if (incoming.autoGiftInterval !== undefined && (!Number.isFinite(Number(incoming.autoGiftInterval)) || Number(incoming.autoGiftInterval) < 10 || Number(incoming.autoGiftInterval) > 600)) {
        return json(response, 400, { error: 'El intervalo de regalo automático debe estar entre 10 y 600 segundos.' });
      }
      const nextConfig = {
        ...config, ...incoming,
        lanes: { ...config.lanes, ...(incoming.lanes || {}) },
        meters: { ...config.meters, ...(incoming.meters || {}) }
      };
      await writeFile(configPath, JSON.stringify(nextConfig, null, 2), 'utf8');
      config = nextConfig;
      broadcast({ type: 'config', config });
      return json(response, 200, config);
    } catch (error) { return json(response, 400, { error: safeText(error.message) }); }
  }
  if (url.pathname === '/api/connect' && request.method === 'POST') {
    try {
      const incoming = await bodyJson(request);
      const username = safeText(incoming.username, 40).replace(/^@/, '').toLowerCase();
      if (!/^[\w.]{2,40}$/.test(username)) return json(response, 400, { error: 'Ingresa un usuario de TikTok válido, sin URL.' });
      await disconnectTikTok();
      retryStartedAt = 0;
      setStatus({ state: 'connecting', username, retryCount: 0, message: 'Conectando a TikTok…' });
      connectAttempt(connectionGeneration);
      return json(response, 202, status);
    } catch (error) { return json(response, 400, { error: safeText(error.message) }); }
  }
  if (url.pathname === '/api/disconnect' && request.method === 'POST') {
    await disconnectTikTok();
    return json(response, 200, status);
  }
  if (url.pathname === '/api/test-event' && request.method === 'POST') {
    try {
      const data = await bodyJson(request);
      if (!['like', 'comment', 'follow', 'share', 'join', 'gift'].includes(data.type) ||
          !['cerro', 'oli'].includes(data.team) && !(data.type === 'comment' && data.team === 'auto')) {
        return json(response, 400, { error: 'Equipo o tipo de evento inválido.' });
      }
      if (data.type === 'gift' && config.gifts.length === 0) return json(response, 400, { error: 'Selecciona al menos un regalo en el panel para probarlo.' });
      const gift = data.type === 'gift' ? (data.gift && config.gifts.includes(data.gift) ? data.gift : config.gifts[0]) : '';
      const event = publishEvent({
        type: data.type, team: data.team === 'auto' ? undefined : data.team, user: safeText(data.user || 'usuario_prueba', 80),
        nickname: safeText(data.nickname || 'Usuario de prueba', 80), avatar: safeText(data.avatar, 1000),
        text: data.type === 'comment' ? safeText(data.text || '¡Vamos equipo!', 300) : '',
        gift, count: 1, test: true
      });
      return json(response, 201, event);
    } catch (error) { return json(response, 400, { error: safeText(error.message) }); }
  }
  if (url.pathname.startsWith('/api/')) return json(response, 404, { error: 'No encontrado' });

  const requested = url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname.slice(1));
  if (['.tortugas-config.json', '.tortugas-gift-counter.json'].includes(requested)) return json(response, 404, { error: 'No encontrado' });
  const file = path.resolve(root, requested);
  if (!file.startsWith(`${root}${path.sep}`) && file !== root) return json(response, 403, { error: 'Prohibido' });
  try {
    const info = await stat(file);
    if (!info.isFile()) return json(response, 404, { error: 'No encontrado' });
    let content = await readFile(file);
    if (path.extname(file) === '.html') content = content.toString('utf8').replaceAll('__APP_VERSION__', appVersion);
    const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.webmanifest': 'application/manifest+json', '.json': 'application/json' };
    response.writeHead(200, { 'content-type': types[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-cache', 'x-content-type-options': 'nosniff' });
    response.end(content);
  } catch { json(response, 404, { error: 'No encontrado' }); }
});

const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', socket => {
  sockets.add(socket);
  socket.send(JSON.stringify({ type: 'config', config }));
  socket.send(JSON.stringify({ type: 'connection', status }));
  socket.send(JSON.stringify({ type: 'connectorLogs', logs: connectorLogs }));
  socket.send(JSON.stringify({ type: 'giftCounter', count: giftCounter }));
  socket.on('close', () => sockets.delete(socket));
  socket.on('error', () => sockets.delete(socket));
});

server.listen(port, '0.0.0.0', () => {
  console.log(`Carrera de Tortugas disponible en http://localhost:${port}`);
  recordConnectorLog('info', `Servidor iniciado; Node ${process.version}, plataforma ${process.platform}.`);
  if (process.env.AUTOCONNECT_TIKTOK !== '0') connectAttempt(connectionGeneration);
});
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, async () => {
    await disconnectTikTok();
    await giftCounterWrite.catch(error => console.error('No se pudo completar el guardado del contador:', error.message));
    server.close(() => process.exit(0));
  });
}
