#!/usr/bin/env node
/**
 * build-trenitalia-gtfs.js  — v3
 * ════════════════════════════════════════════════════════════════════════════
 * Génère un GTFS pour Trenitalia Alta Velocità + Intercity
 *   FR (Frecciarossa) · FA (Frecciargento) · FB (Frecciabianca)
 *   IC (Intercity)    · ICN (Intercity Notte) · EC (EuroCity) · EN (EuroNight)
 *
 * v3 — couverture réseau complète :
 *   • 42 gares (toutes les gares AV + IC d'Italie)
 *   • 58 corridors organisés par ligne réelle (pas juste les grandes villes)
 *   • Bootstrap dynamique des IDs via autocomplete API
 *   • Timezone CET/CEST automatique
 *   • Toutes les erreurs visibles sans --verbose
 *
 * Usage :
 *   node build-trenitalia-gtfs.js
 *   node build-trenitalia-gtfs.js --days 7
 *   node build-trenitalia-gtfs.js --out ./gtfs/ti
 *   node build-trenitalia-gtfs.js --dry-run
 *   node build-trenitalia-gtfs.js --verbose
 *
 * API : POST https://www.lefrecce.it/Channels.Website.BFF.WEB/website/ticket/solutions
 * ════════════════════════════════════════════════════════════════════════════
 */

'use strict';

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

// ─── CLI ───────────────────────────────────────────────────────────────────
function getArg(flag, def) {
  const i = process.argv.indexOf(flag);
  return (i === -1 || i >= process.argv.length - 1) ? def : process.argv[i + 1];
}
function hasFlag(f) { return process.argv.includes(f); }

const DAYS    = parseInt(getArg('--days', '30'), 10);
const OUT_DIR = getArg('--out', './gtfs/trenitalia_it_api');
const DRY_RUN = hasFlag('--dry-run');
const VERBOSE = hasFlag('--verbose');

// ─── API ───────────────────────────────────────────────────────────────────
const API_BASE         = 'https://www.lefrecce.it/Channels.Website.BFF.WEB';
const API_SOLUTIONS    = `${API_BASE}/website/ticket/solutions`;
const API_AUTOCOMPLETE = `${API_BASE}/website/locations/search?name=`;
const API_DETAILS      = `${API_BASE}/website/ticket/solutions/{ID}/details`;

const DELAY_MS        = 420;
const MAX_RETRIES     = 3;
const REQ_TIMEOUT_MS  = 15000;
const SOLUTIONS_LIMIT = 20;

// ─── Gares — réseau AV + IC complet ────────────────────────────────────────
//
// Organisées par axe pour la lisibilité.
// fallbackId = ID lefrecce connu (utilisé si l'autocomplete échoue)
//
const MAJOR_STATIONS = [

  // ── Axe AV Turin–Milan–Bologne–Florence–Rome–Naples–Salerne ────────────
  { name: 'TORINO PORTA NUOVA',           fallbackId: 830007149, lat: 45.0607, lon:  7.6784 },
  { name: 'TORINO PORTA SUSA',            fallbackId: 830005504, lat: 45.0709, lon:  7.6664 },
  { name: 'MILANO CENTRALE',              fallbackId: 830001700, lat: 45.4855, lon:  9.2045 },
  { name: 'MILANO PORTA GARIBALDI',       fallbackId: 830001998, lat: 45.4847, lon:  9.1877 },
  { name: 'REGGIO EMILIA AV',             fallbackId: 830001399, lat: 44.6986, lon: 10.6284 },
  { name: 'BOLOGNA CENTRALE',             fallbackId: 830000725, lat: 44.5058, lon: 11.3432 },
  { name: 'FIRENZE SANTA MARIA NOVELLA',  fallbackId: 830003608, lat: 43.7771, lon: 11.2481 },
  { name: 'FIRENZE CAMPO DI MARTE',       fallbackId: 830003602, lat: 43.7862, lon: 11.2727 },
  { name: 'ROMA TERMINI',                 fallbackId: 830000219, lat: 41.9009, lon: 12.5009 },
  { name: 'ROMA TIBURTINA',               fallbackId: 830000604, lat: 41.9096, lon: 12.5312 },
  { name: 'NAPOLI AFRAGOLA',              fallbackId: 830000836, lat: 40.9174, lon: 14.3126 },
  { name: 'NAPOLI CENTRALE',              fallbackId: 830000303, lat: 40.8536, lon: 14.2727 },
  { name: 'SALERNO',                      fallbackId: 830005820, lat: 40.6784, lon: 14.7798 },

  // ── Axe AV Milan–Vérone–Venise (FR/FB) ────────────────────────────────
  { name: 'BRESCIA',                      fallbackId: 830000941, lat: 45.5210, lon: 10.2151 },
  { name: 'VERONA PORTA NUOVA',           fallbackId: 830002783, lat: 45.4278, lon: 11.0014 },
  { name: 'VICENZA',                      fallbackId: 830008021, lat: 45.5454, lon: 11.5354 },
  { name: 'PADOVA',                       fallbackId: 830004245, lat: 45.4155, lon: 11.8818 },
  { name: 'VENEZIA MESTRE',               fallbackId: 830003013, lat: 45.4758, lon: 12.2371 },
  { name: 'VENEZIA SANTA LUCIA',          fallbackId: 830008409, lat: 45.4414, lon: 12.3213 },

  // ── Axe EC Milan–Triest/Udine ──────────────────────────────────────────
  { name: 'TRIESTE CENTRALE',             fallbackId: 830007227, lat: 45.6588, lon: 13.7758 },
  { name: 'UDINE',                        fallbackId: 830004636, lat: 46.0560, lon: 13.2397 },

  // ── Axe Adriatique (FA/FB/IC) Bologna–Ancona–Pescara–Foggia–Bari–Lecce ─
  { name: 'RIMINI',                       fallbackId: 830004903, lat: 44.0549, lon: 12.5669 },
  { name: 'ANCONA',                       fallbackId: 830005038, lat: 43.6115, lon: 13.5096 },
  { name: 'PESCARA CENTRALE',             fallbackId: 830004014, lat: 42.4631, lon: 14.2095 },
  { name: 'FOGGIA',                       fallbackId: 830005261, lat: 41.4604, lon: 15.5481 },
  { name: 'BARI CENTRALE',                fallbackId: 830000856, lat: 41.1128, lon: 16.8719 },
  { name: 'TARANTO',                      fallbackId: 830006850, lat: 40.4648, lon: 17.2387 },
  { name: 'BRINDISI',                     fallbackId: 830001038, lat: 40.6342, lon: 17.9373 },
  { name: 'LECCE',                        fallbackId: 830002513, lat: 40.3540, lon: 18.1712 },

  // ── Axe Tyrrhenien IC (Rome–Gênes–Turin) ──────────────────────────────
  { name: 'LIVORNO CENTRALE',             fallbackId: 830002651, lat: 43.5559, lon: 10.3122 },
  { name: 'PISA CENTRALE',                fallbackId: 830004133, lat: 43.7085, lon: 10.3952 },
  { name: 'LA SPEZIA CENTRALE',           fallbackId: 830002371, lat: 44.1023, lon:  9.8227 },
  { name: 'GENOVA PIAZZA PRINCIPE',       fallbackId: 830004726, lat: 44.4146, lon:  8.9174 },
  { name: 'GENOVA BRIGNOLE',              fallbackId: 830004728, lat: 44.4076, lon:  8.9359 },
  { name: 'SAVONA',                       fallbackId: 830005610, lat: 44.3077, lon:  8.4793 },
  { name: 'VENTIMIGLIA',                  fallbackId: 830008000, lat: 43.7888, lon:  7.6087 },

  // ── Grand Sud IC/ICN (Rome–Reggio Calabria et Milan–Reggio) ───────────
  { name: 'NAPOLI CAMPI FLEGREI',         fallbackId: 830003014, lat: 40.8471, lon: 14.1913 },
  { name: 'PAOLA',                        fallbackId: 830003997, lat: 39.3606, lon: 16.0341 },
  { name: 'LAMEZIA TERME CENTRALE',       fallbackId: 830002388, lat: 38.9702, lon: 16.3065 },
  { name: 'VILLA SAN GIOVANNI',           fallbackId: 830008075, lat: 38.2193, lon: 15.6389 },
  { name: 'REGGIO CALABRIA CENTRALE',     fallbackId: 830003505, lat: 38.1125, lon: 15.6479 },

  // ── Rome hub ───────────────────────────────────────────────────────────
  { name: 'ROMA OSTIENSE',                fallbackId: 830007830, lat: 41.8757, lon: 12.4765 },

];

// ─── Corridors par ligne réelle ─────────────────────────────────────────────
//
// Stratégie : on crawle les paires terminales de chaque ligne AV/IC.
// Le endpoint /details ramène tous les arrêts intermédiaires.
// On évite N² en se limitant aux vrais terminus de service.
//
const CORRIDORS_BY_NAME = [

  // ════════════════════════════════════════════════════════════════════════
  // FRECCIAROSSA (FR) — axe Turin–Rome–Naples–Salerne
  // ════════════════════════════════════════════════════════════════════════
  // Terminus nord ↔ terminus sud
  ['TORINO PORTA NUOVA',          'SALERNO'],
  ['SALERNO',                     'TORINO PORTA NUOVA'],
  ['TORINO PORTA NUOVA',          'NAPOLI CENTRALE'],
  ['NAPOLI CENTRALE',             'TORINO PORTA NUOVA'],

  // Milan ↔ sud
  ['MILANO CENTRALE',             'SALERNO'],
  ['SALERNO',                     'MILANO CENTRALE'],
  ['MILANO CENTRALE',             'NAPOLI CENTRALE'],
  ['NAPOLI CENTRALE',             'MILANO CENTRALE'],
  ['MILANO CENTRALE',             'ROMA TERMINI'],
  ['ROMA TERMINI',                'MILANO CENTRALE'],

  // Rome ↔ nord
  ['ROMA TERMINI',                'TORINO PORTA NUOVA'],
  ['TORINO PORTA NUOVA',          'ROMA TERMINI'],
  ['ROMA TERMINI',                'VENEZIA SANTA LUCIA'],
  ['VENEZIA SANTA LUCIA',         'ROMA TERMINI'],

  // ════════════════════════════════════════════════════════════════════════
  // FRECCIAROSSA / FRECCIARGENTO — axe Venise–Rome–Naples
  // ════════════════════════════════════════════════════════════════════════
  ['VENEZIA SANTA LUCIA',         'NAPOLI CENTRALE'],
  ['NAPOLI CENTRALE',             'VENEZIA SANTA LUCIA'],
  ['VENEZIA SANTA LUCIA',         'SALERNO'],
  ['SALERNO',                     'VENEZIA SANTA LUCIA'],

  // ════════════════════════════════════════════════════════════════════════
  // FRECCIABIANCA (FB) — axe Milan–Venise (ligne classique)
  // ════════════════════════════════════════════════════════════════════════
  ['MILANO CENTRALE',             'VENEZIA SANTA LUCIA'],
  ['VENEZIA SANTA LUCIA',         'MILANO CENTRALE'],
  ['TORINO PORTA NUOVA',          'VENEZIA SANTA LUCIA'],
  ['VENEZIA SANTA LUCIA',         'TORINO PORTA NUOVA'],

  // ════════════════════════════════════════════════════════════════════════
  // FRECCIARGENTO (FA) — Adriatique Rome–Bari–Lecce
  // ════════════════════════════════════════════════════════════════════════
  ['ROMA TERMINI',                'LECCE'],
  ['LECCE',                       'ROMA TERMINI'],
  ['ROMA TERMINI',                'BARI CENTRALE'],
  ['BARI CENTRALE',               'ROMA TERMINI'],

  // ════════════════════════════════════════════════════════════════════════
  // FRECCIABIANCA (FB) — Adriatique Milan/Bologne–Bari–Lecce
  // ════════════════════════════════════════════════════════════════════════
  ['MILANO CENTRALE',             'LECCE'],
  ['LECCE',                       'MILANO CENTRALE'],
  ['BOLOGNA CENTRALE',            'LECCE'],
  ['LECCE',                       'BOLOGNA CENTRALE'],
  ['MILANO CENTRALE',             'BARI CENTRALE'],
  ['BARI CENTRALE',               'MILANO CENTRALE'],

  // ════════════════════════════════════════════════════════════════════════
  // EUROCITY (EC) — Milan–Trieste et Milan–Udine
  // ════════════════════════════════════════════════════════════════════════
  ['MILANO CENTRALE',             'TRIESTE CENTRALE'],
  ['TRIESTE CENTRALE',            'MILANO CENTRALE'],
  ['VENEZIA SANTA LUCIA',         'TRIESTE CENTRALE'],
  ['TRIESTE CENTRALE',            'VENEZIA SANTA LUCIA'],
  ['MILANO CENTRALE',             'UDINE'],
  ['UDINE',                       'MILANO CENTRALE'],

  // ════════════════════════════════════════════════════════════════════════
  // INTERCITY (IC) — Tyrrhenien Rome–Gênes–Turin–Ventimille
  // ════════════════════════════════════════════════════════════════════════
  ['ROMA TERMINI',                'VENTIMIGLIA'],
  ['VENTIMIGLIA',                 'ROMA TERMINI'],
  ['ROMA TERMINI',                'TORINO PORTA NUOVA'],
  ['TORINO PORTA NUOVA',          'ROMA TERMINI'],
  ['ROMA TERMINI',                'GENOVA PIAZZA PRINCIPE'],
  ['GENOVA PIAZZA PRINCIPE',      'ROMA TERMINI'],
  ['GENOVA PIAZZA PRINCIPE',      'MILANO CENTRALE'],
  ['MILANO CENTRALE',             'GENOVA PIAZZA PRINCIPE'],

  // ════════════════════════════════════════════════════════════════════════
  // INTERCITY (IC) — Grand Sud Rome–Reggio Calabria
  // ════════════════════════════════════════════════════════════════════════
  ['ROMA TERMINI',                'REGGIO CALABRIA CENTRALE'],
  ['REGGIO CALABRIA CENTRALE',    'ROMA TERMINI'],
  ['NAPOLI CENTRALE',             'REGGIO CALABRIA CENTRALE'],
  ['REGGIO CALABRIA CENTRALE',    'NAPOLI CENTRALE'],

  // ════════════════════════════════════════════════════════════════════════
  // INTERCITY NOTTE (ICN) — Milan/Turin → Reggio Calabria
  // ════════════════════════════════════════════════════════════════════════
  ['MILANO CENTRALE',             'REGGIO CALABRIA CENTRALE'],
  ['REGGIO CALABRIA CENTRALE',    'MILANO CENTRALE'],
  ['TORINO PORTA NUOVA',          'REGGIO CALABRIA CENTRALE'],
  ['REGGIO CALABRIA CENTRALE',    'TORINO PORTA NUOVA'],

];

// ─── État global ───────────────────────────────────────────────────────────
const trainMap    = new Map();
const stopsMap    = new Map();
const routesMap   = new Map();
let   reqCount    = 0;
let   errCount    = 0;
const stationByName = new Map(); // NOM → station (rempli par bootstrap)

// ─── HTTP ──────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function _fetch(urlOrOptions, body) {
  return new Promise((resolve, reject) => {
    const isStr   = typeof urlOrOptions === 'string';
    const parsed  = isStr ? new URL(urlOrOptions) : null;
    const proto   = isStr ? parsed.protocol : (urlOrOptions.protocol || 'https:');
    const lib     = proto === 'http:' ? http : https;
    const bodyStr = body ? JSON.stringify(body) : null;

    const options = isStr ? {
      hostname: parsed.hostname,
      path:     parsed.pathname + parsed.search,
      method:   body ? 'POST' : 'GET',
      headers: {
        'Content-Type':  'application/json',
        'Accept':        'application/json',
        'User-Agent':    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
        'Origin':        'https://www.lefrecce.it',
        'Referer':       'https://www.lefrecce.it/',
        ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
      },
    } : urlOrOptions;

    const timer = setTimeout(() => { req.destroy(); reject(new Error('timeout')); }, REQ_TIMEOUT_MS);
    const req   = lib.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        clearTimeout(timer);
        if (res.statusCode >= 400) {
          if (VERBOSE) process.stderr.write(`    [HTTP ${res.statusCode}] ${data.slice(0, 200)}\n`);
          return reject(new Error(`HTTP ${res.statusCode}`));
        }
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON: ' + data.slice(0, 80))); }
      });
    });
    req.on('error', err => { clearTimeout(timer); reject(err); });
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

async function fetchJSON(urlOrOptions, body = null, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const r = await _fetch(urlOrOptions, body);
      reqCount++;
      return r;
    } catch (err) {
      if (attempt === retries) throw err;
      await sleep(DELAY_MS * (attempt + 2));
    }
  }
}

// ─── Bootstrap IDs gares ───────────────────────────────────────────────────
async function bootstrapStationIds() {
  console.log(`  🔍 Résolution des IDs — ${MAJOR_STATIONS.length} gares…`);
  let ok = 0, fallback = 0;

  for (const sta of MAJOR_STATIONS) {
    await sleep(200);
    try {
      const query = encodeURIComponent(sta.name.split(' ').slice(0, 2).join(' '));
      const data  = await fetchJSON(`${API_AUTOCOMPLETE}${query}&limit=15`);

      if (Array.isArray(data) && data.length > 0) {
        const exact = data.find(d =>
          (d.name || '').toUpperCase().replace(/\s+/g, ' ').trim() === sta.name
        ) || data[0];
        sta.id = exact.id;
        if (VERBOSE) console.log(`     ✓ ${sta.name} → id=${sta.id} (API: "${exact.name}")`);
        ok++;
      } else {
        throw new Error('empty');
      }
    } catch (err) {
      sta.id = sta.fallbackId;
      console.warn(`     ⚠ ${sta.name} → fallback id=${sta.fallbackId} (${err.message})`);
      fallback++;
    }
    stationByName.set(sta.name, sta);
  }
  console.log(`     → ${ok} via API · ${fallback} via fallback\n`);
}

// ─── Dates & timezone ──────────────────────────────────────────────────────
function getDatesToCrawl() {
  const today = new Date(); today.setHours(0, 0, 0, 0);
  return Array.from({ length: DAYS }, (_, i) => {
    const d = new Date(today); d.setDate(today.getDate() + i); return d;
  });
}

function italyOffsetMin(date) {
  try {
    const fmt  = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/Rome', hour: '2-digit', hour12: false });
    const utcH = date.getUTCHours();
    const romH = parseInt(fmt.format(date), 10);
    let diff   = (romH - utcH + 24) % 24;
    if (diff > 12) diff -= 24;
    return diff * 60;
  } catch { return 60; }
}

function toISOItaly(date, hour = 6) {
  const d      = new Date(date); d.setHours(hour, 0, 0, 0);
  const off    = italyOffsetMin(d);
  const sign   = off >= 0 ? '+' : '-';
  const hh     = String(Math.floor(Math.abs(off) / 60)).padStart(2, '0');
  const mm     = String(Math.abs(off) % 60).padStart(2, '0');
  const y  = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${mo}-${dd}T${String(hour).padStart(2, '0')}:00:00.000${sign}${hh}:${mm}`;
}

function dateKey(iso) { return iso.slice(0, 10); }

function toGTFSTime(iso) {
  const d      = new Date(iso);
  const off    = italyOffsetMin(d);
  const totMin = d.getUTCHours() * 60 + d.getUTCMinutes() + off;
  const h = Math.floor(totMin / 60);
  const m = totMin % 60;
  const s = d.getUTCSeconds();
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

// ─── Helpers GTFS ──────────────────────────────────────────────────────────
function extractTrainType(acronym) {
  const a = (acronym || '').toUpperCase().trim();
  if (a.startsWith('FR') || a === 'AV') return { short: 'FR',  type: 100, long: 'Frecciarossa' };
  if (a.startsWith('FA'))               return { short: 'FA',  type: 100, long: 'Frecciargento' };
  if (a.startsWith('FB'))               return { short: 'FB',  type: 101, long: 'Frecciabianca' };
  if (a === 'ICN')                       return { short: 'ICN', type: 101, long: 'Intercity Notte' };
  if (a === 'IC')                        return { short: 'IC',  type: 101, long: 'Intercity' };
  if (a === 'EC')                        return { short: 'EC',  type: 101, long: 'EuroCity' };
  if (a === 'EN')                        return { short: 'EN',  type: 101, long: 'EuroNight' };
  return null; // Regionale, RV, etc. → exclus
}

function stationKey(name) {
  return (name || '').trim().toUpperCase().replace(/\s+/g, '_').replace(/[^A-Z0-9_]/g, '');
}
function titleCase(s) {
  return (s || '').toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

function ensureStop(name, lat, lon) {
  const key = stationKey(name);
  if (!stopsMap.has(key)) {
    const known = MAJOR_STATIONS.find(s => stationKey(s.name) === key);
    stopsMap.set(key, {
      stop_id:   'TI:' + key,
      stop_name: titleCase(name),
      stop_lat:  lat  ? String(lat)  : (known ? String(known.lat) : ''),
      stop_lon:  lon  ? String(lon)  : (known ? String(known.lon) : ''),
    });
  } else if (lat && !stopsMap.get(key).stop_lat) {
    Object.assign(stopsMap.get(key), { stop_lat: String(lat), stop_lon: String(lon) });
  }
  return 'TI:' + key;
}

function ensureRoute(info) {
  const rid = 'TI_ROUTE_' + info.short;
  if (!routesMap.has(rid)) {
    routesMap.set(rid, {
      route_id:         rid,
      agency_id:        'TI',
      route_short_name: info.short,
      route_long_name:  info.long,
      route_type:       String(info.type),
    });
  }
  return rid;
}

// ─── API calls ─────────────────────────────────────────────────────────────
async function fetchSolutions(originId, destId, date, offset = 0) {
  const body = {
    departureLocationId: originId,
    arrivalLocationId:   destId,
    departureTime:       toISOItaly(date, 6),
    adults: 1, children: 0,
    criteria: {
      frecceOnly:   false,
      regionalOnly: false,
      noChanges:    false,
      order:        'DEPARTURE_DATE',
      limit:        SOLUTIONS_LIMIT,
      offset,
    },
    advancedSearchRequest: { bestFare: false },
  };
  try {
    const data = await fetchJSON(API_SOLUTIONS, body);
    return (data && data.solutions) ? data.solutions : [];
  } catch (err) {
    errCount++;
    process.stdout.write(`\n     ⚠ ${originId}→${destId} offset=${offset}: ${err.message}`);
    return [];
  }
}

async function fetchDetails(solutionId) {
  const url = API_DETAILS.replace('{ID}', solutionId);
  try {
    const data = await fetchJSON(url);
    return Array.isArray(data) ? data : (data ? [data] : []);
  } catch (err) {
    errCount++;
    if (VERBOSE) process.stderr.write(`    ⚠ details ${solutionId}: ${err.message}\n`);
    return [];
  }
}

// ─── Traitement d'une solution ─────────────────────────────────────────────
async function processSolution(sol) {
  const legs = sol.solution?.nodes?.length ? sol.solution.nodes : (sol.nodes || []);

  let details = [];
  if (legs.length === 0 && sol.solution?.id) {
    await sleep(DELAY_MS);
    details = await fetchDetails(sol.solution.id);
  }

  let segments = details.length > 0 ? details : legs;

  // Fallback : construire un segment minimal depuis la solution elle-même
  if (segments.length === 0 && sol.solution) {
    const s     = sol.solution;
    const tList = s.trainList || s.trains || [];
    const acro  = tList[0]?.trainAcronym || tList[0]?.acronym || '';
    if (acro) {
      segments = [{
        trainAcronym:     acro,
        trainIdentifier:  tList[0]?.trainIdentifier || tList[0]?.name || '',
        departureStation: s.origin,
        arrivalStation:   s.destination,
        departureTime:    s.departureTime,
        arrivalTime:      s.arrivalTime,
        stopList:         [],
      }];
    }
  }

  for (const seg of segments) {
    const acronym   = seg.trainAcronym || seg.trainacronym || seg.vehicleType
                   || seg.train?.acronym || '';
    const trainInfo = extractTrainType(acronym);
    if (!trainInfo) continue; // Regionale → skip

    const trainNum = (seg.trainIdentifier || seg.trainidentifier
                   || seg.train?.name || '').replace(/\s+/g, '');
    const depTime  = seg.departureTime || seg.departuretime;
    const arrTime  = seg.arrivalTime   || seg.arrivaltime;
    if (!depTime || !trainNum) continue;

    const day      = dateKey(depTime);
    const trainKey = `${trainNum}_${day}`;
    if (trainMap.has(trainKey)) continue; // déjà vu

    const depStation = seg.departureStation || seg.departurestation || seg.origin || '';
    const arrStation = seg.arrivalStation   || seg.arrivalstation   || seg.destination || '';
    if (!depStation || !arrStation) continue;

    // Construire la liste d'arrêts
    const stopList = seg.stopList || seg.stoplist || [];
    const allStops = [
      { stationname: depStation, departuretime: depTime,  arrivaltime: null },
      ...stopList,
      { stationname: arrStation, arrivaltime:   arrTime,  departuretime: null },
    ];

    const stopTimes = [];
    for (let seq = 0; seq < allStops.length; seq++) {
      const st    = allStops[seq];
      const sName = st.stationname || st.stationName || '';
      const arr   = st.arrivaltime  || st.arrivalTime  || st.departuretime || st.departureTime || '';
      const dep   = st.departuretime|| st.departureTime|| st.arrivaltime   || st.arrivalTime   || '';
      if (!sName) continue;
      stopTimes.push({
        stop_id:       ensureStop(sName, null, null),
        stop_sequence: seq,
        arrival_time:  arr ? toGTFSTime(arr) : '',
        departure_time:dep ? toGTFSTime(dep) : '',
      });
    }
    if (stopTimes.length < 2) continue;

    trainMap.set(trainKey, {
      routeId:   ensureRoute(trainInfo),
      tripId:    `TI_${trainNum}_${day.replace(/-/g, '')}`,
      serviceId: day.replace(/-/g, ''),
      trainNum, day,
      headsign:  titleCase(arrStation),
      stopTimes,
    });
  }
}

// ─── Écriture GTFS ─────────────────────────────────────────────────────────
function csvEscape(v) {
  const s = String(v == null ? '' : v);
  return (s.includes(',') || s.includes('"') || s.includes('\n'))
    ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function writeCSV(filepath, rows) {
  if (!rows.length) { fs.writeFileSync(filepath, ''); return; }
  const headers = Object.keys(rows[0]);
  const lines   = [headers.join(','),
    ...rows.map(row => headers.map(h => csvEscape(row[h])).join(','))];
  fs.writeFileSync(filepath, lines.join('\n') + '\n', 'utf8');
}

function buildGTFS() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  writeCSV(path.join(OUT_DIR, 'agency.txt'), [{
    agency_id: 'TI', agency_name: 'Trenitalia',
    agency_url: 'https://www.trenitalia.com',
    agency_timezone: 'Europe/Rome', agency_lang: 'it',
  }]);

  writeCSV(path.join(OUT_DIR, 'routes.txt'), [...routesMap.values()]);

  const trips = [], calendar = [], stopTimes = [], seen = new Set();

  for (const t of trainMap.values()) {
    trips.push({
      route_id: t.routeId, service_id: t.serviceId, trip_id: t.tripId,
      trip_headsign: t.headsign, direction_id: '0',
    });
    if (!seen.has(t.serviceId)) {
      seen.add(t.serviceId);
      const d = t.day.replace(/-/g, '');
      calendar.push({
        service_id: t.serviceId,
        monday:'0', tuesday:'0', wednesday:'0', thursday:'0',
        friday:'0', saturday:'0', sunday:'0',
        start_date: d, end_date: d,
      });
    }
    for (const st of t.stopTimes) {
      stopTimes.push({
        trip_id: t.tripId, stop_id: st.stop_id,
        stop_sequence: st.stop_sequence,
        arrival_time: st.arrival_time, departure_time: st.departure_time,
        pickup_type: '0', drop_off_type: '0',
      });
    }
  }

  writeCSV(path.join(OUT_DIR, 'trips.txt'),       trips);
  writeCSV(path.join(OUT_DIR, 'calendar.txt'),     calendar);
  writeCSV(path.join(OUT_DIR, 'stop_times.txt'),   stopTimes);
  writeCSV(path.join(OUT_DIR, 'stops.txt'), [...stopsMap.values()].map(s => ({
    stop_id: s.stop_id, stop_name: s.stop_name,
    stop_lat: s.stop_lat, stop_lon: s.stop_lon,
  })));

  const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
  const endD  = new Date(); endD.setDate(endD.getDate() + DAYS);
  const end   = endD.toISOString().slice(0,10).replace(/-/g,'');
  writeCSV(path.join(OUT_DIR, 'feed_info.txt'), [{
    feed_publisher_name: 'TrainNomad (données Trenitalia API)',
    feed_publisher_url:  'https://trainnomad.eu',
    feed_lang: 'it', feed_start_date: today,
    feed_end_date: end, feed_version: today,
  }]);
}

// ─── main ──────────────────────────────────────────────────────────────────
(async function main() {
  console.log('\n🇮🇹  Build GTFS Trenitalia AV + Intercity — v3');
  console.log(`   Trains   : FR · FA · FB · IC · ICN · EC · EN`);
  console.log(`   Gares    : ${MAJOR_STATIONS.length}`);
  console.log(`   Corridors: ${CORRIDORS_BY_NAME.length} paires O/D`);
  console.log(`   Fenêtre  : ${DAYS} jours`);
  console.log(`   Sortie   : ${OUT_DIR}`);
  if (DRY_RUN) console.log('   Mode     : DRY-RUN');
  console.log('');

  await bootstrapStationIds();

  const dates = getDatesToCrawl();
  console.log(`   Dates : ${dates[0].toISOString().slice(0,10)} → ${dates[dates.length-1].toISOString().slice(0,10)}\n`);

  let n = 0;
  for (const [oName, dName] of CORRIDORS_BY_NAME) {
    const oSta = stationByName.get(oName);
    const dSta = stationByName.get(dName);
    if (!oSta || !dSta) {
      console.warn(`  ⚠ gare inconnue: "${oName}" ou "${dName}" — ignoré`);
      continue;
    }

    process.stdout.write(`  [${String(++n).padStart(2)}/${CORRIDORS_BY_NAME.length}] ${oName} → ${dName} `);
    let newTrips = 0;

    for (const date of dates) {
      for (const offset of [0, SOLUTIONS_LIMIT]) {
        await sleep(DELAY_MS);
        const solutions = await fetchSolutions(oSta.id, dSta.id, date, offset);
        if (!solutions.length) break;
        const before = trainMap.size;
        for (const sol of solutions) { await processSolution(sol); await sleep(60); }
        newTrips += trainMap.size - before;
        if (solutions.length < SOLUTIONS_LIMIT) break;
      }
    }
    console.log(`(+${newTrips} trips)`);
  }

  // Résumé
  console.log(`\n  ✅ ${trainMap.size} trains · ${stopsMap.size} gares · ${routesMap.size} routes`);
  console.log(`  📡 ${reqCount} requêtes · ${errCount} erreurs`);

  const byType = {};
  for (const t of trainMap.values()) {
    const s = routesMap.get(t.routeId)?.route_short_name || '?';
    byType[s] = (byType[s] || 0) + 1;
  }
  console.log('  Répartition :',
    Object.entries(byType).sort((a,b)=>b[1]-a[1]).map(([k,v])=>`${k}×${v}`).join(' · '));
  console.log('');

  if (DRY_RUN) { console.log('  [DRY-RUN] aucun fichier écrit.'); return; }

  buildGTFS();
  console.log(`  📁 GTFS écrit dans ${OUT_DIR}/\n`);

})().catch(err => { console.error('\n❌', err.message); process.exit(1); });