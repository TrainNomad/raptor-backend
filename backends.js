/**
 * backends.js — Gestion multi-instances Render avec failover automatique
 *
 * Ajoute simplement une URL dans BACKENDS pour enregistrer une nouvelle instance.
 * Le round-robin bascule automatiquement si un backend est suspendu ou surchargé.
 */

const BACKENDS = [
  'https://raptor-backend-00p1.onrender.com',   // instance principale
  'https://raptor-backend-2vdj.onrender.com',   // instance de secours
  // 'https://raptor-backend-XXXX.onrender.com', // ← ajoutez d'autres ici
];

const HEALTH_TIMEOUT_MS  = 4000;   // délai max pour /health
const COOLDOWN_MS        = 60_000; // 1 min avant de réessayer un backend en échec
const REQUEST_TIMEOUT_MS = 15_000; // timeout requête normale

// ─── État interne ─────────────────────────────────────────────────────────────
const state = BACKENDS.map(url => ({
  url,
  failedAt:    null,   // timestamp du dernier échec
  healthy:     true,   // considéré sain jusqu'à preuve du contraire
  requestCount: 0,     // compteur pour logs
}));

let currentIndex = 0;

// ─── Helpers ─────────────────────────────────────────────────────────────────

function markFailed(entry) {
  entry.healthy  = false;
  entry.failedAt = Date.now();
  console.warn(`[backends] ⚠️  Backend suspendu/indisponible : ${entry.url}`);
}

function isAvailable(entry) {
  if (entry.healthy) return true;
  // Cooldown écoulé → on retente
  if (Date.now() - entry.failedAt > COOLDOWN_MS) {
    entry.healthy = true;
    console.log(`[backends] 🔄 Retry backend : ${entry.url}`);
    return true;
  }
  return false;
}

/**
 * Retourne le prochain backend disponible (round-robin).
 * Lance une exception si tous les backends sont en échec.
 */
function nextBackend() {
  for (let i = 0; i < state.length; i++) {
    const idx   = (currentIndex + i) % state.length;
    const entry = state[idx];
    if (isAvailable(entry)) {
      currentIndex = (idx + 1) % state.length; // avance le curseur
      return entry;
    }
  }
  // Tous en échec → forcer le premier (mieux vaut essayer que planter)
  console.error('[backends] ❌ Tous les backends sont en échec — fallback sur le premier');
  currentIndex = 1 % state.length;
  state[0].healthy = true;
  return state[0];
}

// ─── Proxy fetch avec failover ────────────────────────────────────────────────

const http  = require('http');
const https = require('https');

/**
 * Effectue une requête HTTP vers le backend sélectionné.
 * En cas d'erreur réseau ou HTTP 5xx/429, bascule automatiquement.
 *
 * @param {string} path        — ex: '/api/search?from=...'
 * @param {object} [options]   — { method, headers, body }
 * @returns {Promise<{status, headers, body}>}
 */
async function proxyRequest(path, options = {}) {
  const maxAttempts = state.length;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const backend = nextBackend();
    const fullUrl = backend.url + path;

    try {
      backend.requestCount++;
      const result = await fetchWithTimeout(fullUrl, options, REQUEST_TIMEOUT_MS);

      // 429 = rate-limit Render → backend surchargé
      if (result.status === 429 || result.status === 503) {
        console.warn(`[backends] 🚫 ${result.status} sur ${backend.url} — bascule`);
        markFailed(backend);
        continue;
      }

      return result;

    } catch (err) {
      console.warn(`[backends] ❌ Erreur réseau sur ${backend.url} : ${err.message}`);
      markFailed(backend);
    }
  }

  throw new Error('Tous les backends ont échoué pour : ' + path);
}

/**
 * Fetch bas niveau avec timeout (sans dépendances externes).
 */
function fetchWithTimeout(url, options = {}, timeoutMs = 10_000) {
  return new Promise((resolve, reject) => {
    const parsed  = new URL(url);
    const lib     = parsed.protocol === 'https:' ? https : http;
    const body    = options.body || null;

    const reqOptions = {
      hostname: parsed.hostname,
      port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path:     parsed.pathname + parsed.search,
      method:   options.method || 'GET',
      headers:  {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
        ...(body ? { 'Content-Length': Buffer.byteLength(body) } : {}),
      },
    };

    const timer = setTimeout(() => {
      req.destroy();
      reject(new Error('Timeout après ' + timeoutMs + 'ms'));
    }, timeoutMs);

    const req = lib.request(reqOptions, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        clearTimeout(timer);
        resolve({ status: res.statusCode, headers: res.headers, body: data });
      });
    });

    req.on('error', err => { clearTimeout(timer); reject(err); });
    if (body) req.write(body);
    req.end();
  });
}

// ─── Health check de tous les backends ───────────────────────────────────────

async function checkAllHealth() {
  const results = await Promise.all(state.map(async entry => {
    try {
      const r = await fetchWithTimeout(entry.url + '/health', {}, HEALTH_TIMEOUT_MS);
      const ok = r.status === 200;
      entry.healthy = ok;
      if (!ok && !entry.failedAt) entry.failedAt = Date.now();
      return { url: entry.url, ok, status: r.status, body: r.body.slice(0, 120) };
    } catch (err) {
      markFailed(entry);
      return { url: entry.url, ok: false, error: err.message };
    }
  }));
  return results;
}

/**
 * Retourne l'état résumé de tous les backends (pour /api/backends-status).
 */
function getStatus() {
  return state.map(e => ({
    url:          e.url,
    healthy:      e.healthy,
    requestCount: e.requestCount,
    failedAt:     e.failedAt ? new Date(e.failedAt).toISOString() : null,
    cooldownLeft: e.failedAt && !e.healthy
      ? Math.max(0, Math.round((COOLDOWN_MS - (Date.now() - e.failedAt)) / 1000))
      : 0,
  }));
}

module.exports = { proxyRequest, checkAllHealth, getStatus, BACKENDS };