/* ════════════════════════════════════════════════════════════════
   OneDrill 811 — Service Worker (PWA)

   Estratégia:
   - Assets estáticos (HTML/JS/CSS/PNG): cache-first com revalidação
   - API Supabase: NUNCA cacheado (sempre dinâmico)
   - Mapas externos (CDN): network-first com fallback cache

   Pra forçar atualização: bump da CACHE_VERSION abaixo (ou novo deploy
   troca os hashes nos query strings ?v=... que invalidam o cache).
   ════════════════════════════════════════════════════════════════ */

const CACHE_VERSION = 'onedrill-v7';
const STATIC_ASSETS = [
  './',
  './index.html',
  './app.js',
  './styles.css',
  './logo.png',
  './logo.svg',
  './icon-192.png',
  './icon-512.png',
  './apple-touch-icon.png',
  './manifest.json'
];

// ── INSTALL: pre-cache dos assets essenciais ───────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_VERSION)
      .then(cache => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting()) // ativa imediatamente, sem esperar reload
  );
});

// ── ACTIVATE: limpa caches antigos ─────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim()) // controla abas existentes imediatamente
  );
});

// ── FETCH: estratégia por tipo de request ──────────────────────
self.addEventListener('fetch', event => {
  const req = event.request;
  const url = new URL(req.url);

  // Só GET — POST/PUT/DELETE sempre vão direto pra rede
  if (req.method !== 'GET') return;

  // NUNCA cachear chamadas pra Supabase, geocoding, etc.
  if (url.hostname.includes('supabase')
   || url.hostname.includes('nominatim')
   || url.hostname.includes('openstreetmap')
   || url.hostname.includes('googleapis')
   || url.hostname.includes('arcgisonline')
   || url.hostname.includes('tile.')) {
    return; // browser handle direto
  }

  // CDNs (Leaflet, Supabase JS, SheetJS, fonts): network-first com fallback cache
  if (url.hostname.includes('cdnjs.cloudflare.com')
   || url.hostname.includes('jsdelivr.net')
   || url.hostname.includes('fonts.googleapis.com')
   || url.hostname.includes('fonts.gstatic.com')) {
    event.respondWith(
      fetch(req)
        .then(res => {
          if (res.ok) {
            const clone = res.clone();
            caches.open(CACHE_VERSION).then(c => c.put(req, clone));
          }
          return res;
        })
        .catch(() => caches.match(req))
    );
    return;
  }

  // Assets do próprio site: cache-first com revalidação em background
  event.respondWith(
    caches.match(req).then(cached => {
      const fetchPromise = fetch(req).then(res => {
        if (res.ok) {
          const clone = res.clone();
          caches.open(CACHE_VERSION).then(c => c.put(req, clone));
        }
        return res;
      }).catch(() => cached); // se off-line, usa cache

      return cached || fetchPromise;
    })
  );
});
