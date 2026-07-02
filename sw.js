/* SW v13 - 2026-05-22
 * v12: NUCLEAR — zero cache de assets, sempre rede.
 * v13: + cache offline pra Supabase REST (shared view funciona sem internet).
 *      Assets do site continuam network-first, CDN cache com revalidação.
 */

const CACHE_VERSION = 'onedrill-v13';
const API_CACHE = 'onedrill-api-v1';

self.addEventListener('install', event => {
  event.waitUntil((async () => {
    // Apaga TUDO de cache (qualquer versao antiga)
    const keys = await caches.keys();
    await Promise.all(keys.map(k => caches.delete(k)));
    console.log('[SW] todos caches antigos apagados');
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== CACHE_VERSION && k !== API_CACHE).map(k => caches.delete(k)));
    await self.clients.claim();
    // Notifica clients pra recarregar
    const clients = await self.clients.matchAll();
    clients.forEach(c => c.postMessage({type: 'sw-activated'}));
  })());
});

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);

  // Mapas e geocoding: SEMPRE rede, sem cache
  if (url.hostname.includes('nominatim') || url.hostname.includes('openstreetmap')
   || url.hostname.includes('googleapis') || url.hostname.includes('arcgisonline')
   || url.hostname.includes('tile.')) {
    return;
  }

  // Supabase REST API: network-first, cache pra offline (shared view)
  if (url.hostname.includes('supabase') && url.pathname.includes('/rest/')) {
    event.respondWith((async () => {
      try {
        const res = await fetch(req);
        if (res.ok) {
          const clone = res.clone();
          caches.open(API_CACHE).then(c => c.put(req, clone));
        }
        return res;
      } catch (e) {
        const cached = await caches.match(req);
        if (cached) {
          console.log('[SW] Supabase offline — servindo do cache:', url.pathname);
          return cached;
        }
        return new Response(JSON.stringify({message:'Offline — sem dados em cache'}),
          {status:503, headers:{'Content-Type':'application/json'}});
      }
    })());
    return;
  }

  // Supabase Auth/outros endpoints: SEMPRE rede
  if (url.hostname.includes('supabase')) {
    return;
  }

  // Mesmo domain (nossos assets index.html, app.js, styles.css, etc):
  // NETWORK-FIRST com no-store. Fallback a cache so se offline.
  if (url.origin === self.location.origin) {
    event.respondWith((async () => {
      try {
        const res = await fetch(req, {cache: 'no-store'});
        // Atualiza cache em background pra fallback offline
        if (res.ok) {
          const clone = res.clone();
          caches.open(CACHE_VERSION).then(c => c.put(req, clone));
        }
        return res;
      } catch (e) {
        // Offline - serve do cache
        const cached = await caches.match(req);
        if (cached) return cached;
        throw e;
      }
    })());
    return;
  }

  // CDNs externos: cache com revalidacao
  event.respondWith((async () => {
    try {
      const res = await fetch(req);
      if (res.ok) {
        const clone = res.clone();
        caches.open(CACHE_VERSION).then(c => c.put(req, clone));
      }
      return res;
    } catch (e) {
      const cached = await caches.match(req);
      if (cached) return cached;
      throw e;
    }
  })());
});
