const CACHE_NAME = 'tortulive-shell-v20';
const APP_SHELL = [
  '/',
  '/dashboard.html',
  '/pwa-runtime.js',
  '/api-config.js',
  '/scene-picker.js',
  '/flag-picker.js',
  '/layers/flags.js',
  '/layers/game-core.js',
  '/layers/progress-controller.js',
  '/layers/turtle-controller.js',
  '/layers/event-sounds.js',
  '/overlay-coordinator.js?v=18',
  '/manifest.webmanifest',
  '/assets/icon.svg',
  '/assets/icon-192.png',
  '/assets/icon-512.png',
  '/assets/icon-maskable-512.png',
  '/layers/fondo.html',
  '/layers/pista.html',
  '/layers/ruta.html',
  '/layers/meta.html',
  '/layers/estadisticas-equipos.html',
  '/layers/progreso-1.html',
  '/layers/progreso-2.html',
  '/layers/bandera-1.html',
  '/layers/bandera-2.html',
  '/layers/tortuga-1.html',
  '/layers/tortuga-2.html',
  '/layers/eventos-ruta.html',
  '/layers/don-tortu.html',
  '/layers/alertas.html',
  '/layers/agradecimiento.html',
  '/layers/contadores.html',
  '/layers/ganador.html',
  '/layers/iconos.html',
  '/layers/regalos.html'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      // Tolerante: si un recurso falla, el resto se cachea igualmente.
      .then(cache => Promise.allSettled(APP_SHELL.map(url => cache.add(url))))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

// Red primero (siempre la versión más nueva) y caché solo como respaldo sin conexión.
self.addEventListener('fetch', event => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin || url.pathname.startsWith('/api/')) return;

  event.respondWith(
    fetch(request)
      .then(response => {
        if (response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, copy));
        }
        return response;
      })
      .catch(() =>
        caches.match(request).then(cached => {
          if (cached) return cached;
          if (request.mode === 'navigate') return caches.match('/');
          return Response.error();
        })
      )
  );
});
