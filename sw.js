/* SW NUCLEAR v11 - 2026-05-13
 * Eric reportou cache do Safari iOS nao atualiza.
 * Estrategia: ZERO cache de assets do site - sempre fetch da rede.
 * Cache so pra recursos externos (Leaflet, etc) e quando offline.
 */

const CACHE_VERSION = 'onedrill-v11';

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
    await Promise.all(keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k)));
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

  // API e mapas externos: SEMPRE rede, sem cache
  if (url.hostname.includes('supabase') || url.hostname.includes('nominatim')
   || url.hostname.includes('openstreetmap') || url.hostname.includes('googleapis')
   || url.hostname.includes('arcgisonline') || url.hostname.includes('tile.')) {
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
