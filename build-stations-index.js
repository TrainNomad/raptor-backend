/**
 * build-stations-index.js
 *
 * Génère stations.json à partir de stations.csv (Trainline open data) :
 * ce fichier contient les liens UIC8-SNCF ↔ trenitalia_id pour toutes les gares.
 * C'est la source la plus fiable — plus besoin de heuristiques de nom ou GPS.
 *
 * Usage :
 *   node build-stations-index.js [engine_data_dir] [stations_csv] [out_file]
 */

const fs   = require('fs');
const path = require('path');

const DATA_DIR   = process.argv[2] || './engine_data';
const CSV_FILE   = process.argv[3] || path.join(__dirname, 'stations.csv');
const OUT_FILE   = process.argv[4] || path.join(__dirname, 'stations.json');
const STOPS_FILE = path.join(DATA_DIR, 'stops.json');
const XFER_FILE  = path.join(DATA_DIR, 'transfer_index.json');

function extractOperator(sid) {
  const m = (sid||'').match(/^([A-Z]+):/);
  return m ? m[1] : 'SNCF';
}

function parseCsv(filePath) {
  const lines = fs.readFileSync(filePath, 'utf8').split('\n');
  const headers = lines[0].split(';');
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const vals = lines[i].split(';');
    const obj = {};
    for (let j = 0; j < headers.length; j++) obj[headers[j]] = vals[j] || '';
    rows.push(obj);
  }
  return rows;
}

// ── Extraction de la ville depuis le nom de la gare ───────────────────────────
// Utilisé pour grouper les gares d'une même ville dans l'autocomplétion.
// La liste couvre les villes multi-gares connues ; les autres gares reçoivent
// leur propre nom comme ville (cas des gares uniques).
const CITY_PREFIXES = [
  // France
  'Aix-en-Provence', 'Angers', 'Avignon', 'Bordeaux', 'Brest', 'Caen',
  'Clermont-Ferrand', 'Dijon', 'Grenoble', 'Le Havre', 'Le Mans', 'Lille',
  'Limoges', 'Lyon', 'Marseille', 'Metz', 'Montpellier', 'Nancy', 'Nantes',
  'Nice', 'Nimes', 'Orleans', 'Paris', 'Perpignan', 'Poitiers', 'Reims',
  'Rennes', 'Rouen', 'Saint-Etienne', 'Strasbourg', 'Toulon', 'Toulouse',
  'Tours',
  // Italie
  'Milano', 'Torino', 'Roma', 'Firenze', 'Venezia', 'Genova', 'Napoli', 'Bologna',
  // Benelux
  'Amsterdam', 'Rotterdam', 'Bruxelles', 'Antwerpen', 'Liege',
  // Allemagne
  'Koln', 'Dusseldorf', 'Dortmund', 'Duisburg', 'Essen', 'Aachen', 'Frankfurt',
  // UK
  'London', 'Londres',
  // Espagne — villes multi-gares
  'Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Zaragoza', 'Bilbao',
  'Malaga', 'Alicante', 'Cordoba', 'Valladolid', 'San Sebastian',
  'Donostia', 'Vitoria', 'Pamplona', 'Murcia', 'Palma', 'Las Palmas',
  'Granada', 'Toledo', 'Salamanca', 'Cadiz', 'Burgos', 'Leon',
  'Santander', 'Oviedo', 'Gijon', 'Vigo', 'Santiago', 'A Coruna',
  'Tarragona', 'Lleida', 'Girona', 'Albacete', 'Cuenca', 'Ciudad Real',
  // Portugal
  'Lisboa', 'Porto', 'Coimbra', 'Braga', 'Faro', 'Setubal', 'Aveiro',
  'Evora', 'Guimaraes', 'Viseu', 'Leiria', 'Santarem', 'Viana do Castelo',
  'Vila Nova de Gaia', 'Funchal',
];

function extractCity(name) {
  // Normalise accents pour comparaison (Köln → Koln, etc.)
  const normalized = name.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  for (const prefix of CITY_PREFIXES) {
    const normPrefix = prefix.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    if (normalized === normPrefix || normalized.startsWith(normPrefix + ' ') || normalized.startsWith(normPrefix + '-')) {
      // Retourner le vrai préfixe (avec accents) extrait du nom original
      return name.slice(0, prefix.length);
    }
  }
  // Pas de préfixe connu : la ville = le nom complet de la gare
  return name;
}


// ── Table de correspondance explicite slug CSV → slug ES ──────────────────────
// Nécessaire quand les slugs ES (Eurostar GTFS) sont abrégés par rapport aux
// slugs du CSV Trainline. Format clé : slug CSV tel quel (avec tirets).
// Format valeur : slug de base ES (avec underscores, sans numéro de quai).
//
// Pour trouver les slugs ES disponibles, lancer :
//   node build-stations-index.js puis chercher dans le diagnostic.
const CSV_SLUG_TO_ES_SLUG = {
  'paris-gare-du-nord':          'paris_nord',
  // Si votre GTFS ES contient 'paris_est', décommentez :
  // 'paris-gare-de-lest':       'paris_est',
  'london-st-pancras':           'st_pancras_international',
  'st-pancras-international':    'st_pancras_international',
  // Ajouter d'autres cas si besoin
  // Note: bruxelles-midi, lille-europe, etc. sont gérés via la whitelist transfer_index
};

// ── Pays des gares ES-only (non couvertes par le CSV is_suggestable) ──────────
// Gares Eurostar sans entrée CSV suggestable : on les crée manuellement avec
// le bon pays afin qu'elles apparaissent dans l'autocomplétion.
const ES_SLUG_COUNTRY = {
  'paris_nord':                  'FR',
  'st_pancras_international':    'GB',
  'amsterdam_centraal':          'NL',
  'rotterdam_centraal':          'NL',
  'schiphol_airport':            'NL',
  'bruxelles_midi':              'BE',
  'antwerpen_centraal':          'BE',
  'liege_guillemins':            'BE',
  'koln_hbf':                    'DE',
  'dusseldorf_hbf':              'DE',
  'duisburg_hbf':                'DE',
  'essen_hbf':                   'DE',
  'dortmund_hbf':                'DE',
  'aachen_hbf':                  'DE',
  'moutiers_salins_brides_les_bai': 'FR',
  'albertville':                 'FR',
};

// ── Ponts ibériques ES ↔ PT ───────────────────────────────────────────────────
// Relie les stop_ids RENFE et CP qui désignent la même gare physique
// (ou le point de correspondance frontalier le plus proche).
//
// Format : 'OPERATEUR:stop_id' → 'OPERATEUR:stop_id_jumeau'
//
// CORRIDOR NORD — Tren Celta (Vigo ↔ Porto)
//   Renfe opère avec ses propres IDs dans les gares portugaises.
//   GPS identiques → même quai physique.
//   Renfe 22402  ↔  CP 94_7005   : Valença / Valença do Minho
//   Renfe 94033  ↔  CP 94_18002  : Viana do Castelo
//   Renfe 96122  ↔  CP 94_6122   : Barcelos
//   Renfe 94021  ↔  CP 94_6007   : Nine
//   Renfe 94346  ↔  CP 94_2006   : Porto Campanha
//
// CORRIDOR SUD — Lusitânia (Lisboa ↔ Madrid via Badajoz)
//   Elvas (dernière gare CP) et Badajoz (première gare Renfe) sont à 13 km.
//   Pont interCity : correspondance en bus/taxi ou attente sur place.
//   CP 94_57497  ↔  RENFE 37606  : Elvas ↔ Badajoz  [interCity]
//
const IBERIAN_BRIDGES = [
  // ── Tren Celta : gares partagées Renfe / CP (même quai) ──
  { a: 'RENFE:22402',   b: 'CP:94_7005',   name: 'Valença',          interCity: false },
  { a: 'RENFE:94033',   b: 'CP:94_18002',  name: 'Viana do Castelo', interCity: false },
  { a: 'RENFE:96122',   b: 'CP:94_6122',   name: 'Barcelos',         interCity: false },
  { a: 'RENFE:94021',   b: 'CP:94_6007',   name: 'Nine',             interCity: false },
  { a: 'RENFE:94346',   b: 'CP:94_2006',   name: 'Porto Campanha',   interCity: false },

  // ── Corridor Badajoz : correspondance frontalière Elvas ↔ Badajoz ──
  { a: 'CP:94_57497',   b: 'RENFE:37606',  name: 'Elvas ↔ Badajoz', interCity: true  },
];

console.log('\n🔨 Construction stations.json depuis stations.csv...\n');

if (!fs.existsSync(STOPS_FILE)) {
  console.error('❌ ' + STOPS_FILE + ' introuvable. Lance d\'abord : node gtfs-ingest.js');
  process.exit(1);
}
if (!fs.existsSync(CSV_FILE)) {
  console.error('❌ ' + CSV_FILE + ' introuvable.');
  process.exit(1);
}

const stops = JSON.parse(fs.readFileSync(STOPS_FILE, 'utf8'));
const xfer  = fs.existsSync(XFER_FILE) ? JSON.parse(fs.readFileSync(XFER_FILE, 'utf8')) : {};

console.log('  stops.json    : ' + Object.keys(stops).length + ' stops');

// ── Index UIC → stop_ids ──────────────────────────────────────────────────────
const uic8ToStops = {};

for (const [sid, stop] of Object.entries(stops)) {
  const op = stop.operator || extractOperator(sid);

  if (op === 'SNCF') {
    let m = sid.match(/-(\d{7,8})$/);
    if (!m) m = sid.match(/OCE(\d{7,8})$/);
    if (m) {
      const uic = m[1];
      if (!uic8ToStops[uic]) uic8ToStops[uic] = [];
      uic8ToStops[uic].push(sid);
    }
  } else if (op === 'TI') {
    const m = sid.match(/^TI:(\d+)$/);
    if (m) {
      const uic = m[1];
      if (!uic8ToStops[uic]) uic8ToStops[uic] = [];
      uic8ToStops[uic].push(sid);
    }
  }
}

console.log('  Index UIC     : ' + Object.keys(uic8ToStops).length + ' codes uniques');

// ── Index ES slug → stop_ids ──────────────────────────────────────────────────
// Extrait le slug de base des stop_ids Eurostar :
//   ES:paris_nord_3         → paris_nord
//   ES:paris_nord_10a       → paris_nord
//   ES:paris_nord_station_area → paris_nord
const slugToEsStops = {};

for (const sid of Object.keys(stops)) {
  if (!sid.startsWith('ES:')) continue;
  const raw  = sid.slice(3);
  const base = raw
    .replace(/_station_area$/, '')
    .replace(/_\d+[ab]?$/, '');
  if (!slugToEsStops[base]) slugToEsStops[base] = [];
  slugToEsStops[base].push(sid);
}

console.log('  Index ES slug : ' + Object.keys(slugToEsStops).length + ' slugs Eurostar');
console.log('  Slugs ES dispo: ' + Object.keys(slugToEsStops).sort().join(', ') + '\n');

// ── Index CP stop_id → stop_ids (pour fusion avec RENFE) ─────────────────────
// Les stop IDs CP ont la forme "CP:94_XXXX".
// On construit un index rawId → [CP:94_XXXX] pour les gares frontières.
const cpRawToStops = {};
for (const sid of Object.keys(stops)) {
  if (!sid.startsWith('CP:')) continue;
  const raw = sid.slice(3); // "94_XXXX"
  if (!cpRawToStops[raw]) cpRawToStops[raw] = [];
  cpRawToStops[raw].push(sid);
}
console.log('  Index CP      : ' + Object.keys(cpRawToStops).length + ' stops CP\n');

// ── Ponts ibériques manuels CP ↔ RENFE ───────────────────────────────────────
// Résultat d'une analyse GPS exhaustive des GTFS CP et Renfe.
// Chaque paire [stopA, stopB] force la fusion dans la même entrée stations.json.
//
// Corridors validés :
//   NORD  : Vigo ↔ Porto (train Celta — Renfe circule sur infrastructure CP)
//     - Valença/Valença do Minho    CP:94_7005  ↔ RENFE:22402    (28m)
//     - Viana do Castelo            CP:94_18002 ↔ RENFE:94033    (6m)
//     - Porto Campanha              CP:94_2006  ↔ RENFE:94346    (54m)
//   CENTRE: Vilar Formoso (CP terminus, pas d'équivalent Renfe actif)
//   SUD   : Elvas ↔ Badajoz  CP:94_57497 ↔ RENFE:37606  (14km — 2 villes distinctes)
//           → lien interCity uniquement (correspondance bus ~15 min)
//
// Format : [stopId_A, stopId_B, type]
//   "merge"     → même quai physique, fusionner dans une seule station
//   "interCity" → villes différentes, lien de correspondance avec délai majoré
// const IBERIAN_BRIDGES = [
//   // Corridor Nord : Vigo ↔ Porto (Renfe utilise l'infra CP)
//   ['CP:94_7005',  'RENFE:22402', 'merge'],     // Valença / Valença do Minho — 28m
//   ['CP:94_18002', 'RENFE:94033', 'merge'],     // Viana do Castelo — 6m
//   ['CP:94_2006',  'RENFE:94346', 'merge'],     // Porto Campanha — 54m

//   // Corridor Sud : Elvas (PT) ↔ Badajoz (ES) — 2 villes, 14km
//   // Correspondance interCity (bus ou taxi ~15 min de trajet + attente)
//   ['CP:94_57497', 'RENFE:37606', 'interCity'], // Elvas ↔ Badajoz
// ];

// Index rapide stopId → [stopIds à fusionner avec lui]
const iberianMergeIndex = {};   // pour type "merge"
const iberianInterCity  = [];   // pour type "interCity"

for (const [a, b, type] of IBERIAN_BRIDGES) {
  if (!stops[a] || !stops[b]) continue; // l'un des deux n'est pas dans le GTFS chargé
  if (type === 'merge') {
    if (!iberianMergeIndex[a]) iberianMergeIndex[a] = [];
    if (!iberianMergeIndex[b]) iberianMergeIndex[b] = [];
    iberianMergeIndex[a].push(b);
    iberianMergeIndex[b].push(a);
  } else if (type === 'interCity') {
    iberianInterCity.push([a, b]);
  }
}
console.log('  Ponts ibériques : ' + Object.keys(iberianMergeIndex).length/2 + ' fusions, ' + iberianInterCity.length + ' liens interCity\n');

// ── Blacklist des liens SNCF→ES erronés dans le transfer_index ────────────────
// Format : 'uic8:es_slug_base' — ces liens existent dans transfer_index
// mais sont géographiquement incorrects (erreur de données source).
const ES_TRANSFER_BLACKLIST = new Set([
  '87113001:paris_nord',  // Gare de l'Est → paris_nord (devrait être Gare du Nord)
]);

// ── Whitelist des liens SNCF→ES valides via transfer_index ────────────────────
// Construite automatiquement depuis le transfer_index en excluant la blacklist.
// Permet à Bruxelles, Lille-Europe, etc. de récupérer leurs stops ES
// via le transfer_index plutôt que via la table de slugs manuelle.
const validEsTransfers = {};  // uic8 → Set<es_slug_base>

for (const [key, vals] of Object.entries(xfer)) {
  if (!key.startsWith('SNCF:StopArea:')) continue;
  const esVals = vals.filter(v => v.startsWith('ES:'));
  if (!esVals.length) continue;
  const uicMatch = key.match(/(\d{7,9})$/);
  if (!uicMatch) continue;
  const uic = uicMatch[1];
  for (const esId of esVals) {
    const base = esId.slice(3).replace(/_(\d+[ab]?|station_area)$/, '');
    if (ES_TRANSFER_BLACKLIST.has(uic + ':' + base)) continue;
    if (!validEsTransfers[uic]) validEsTransfers[uic] = new Set();
    validEsTransfers[uic].add(base);
  }
}
console.log('  Liens ES valides: ' + Object.keys(validEsTransfers).length + ' gares SNCF avec stops ES légitimes\n');

// ── Lecture du CSV ────────────────────────────────────────────────────────────
const csvRows = parseCsv(CSV_FILE);
console.log('  stations.csv  : ' + csvRows.length + ' lignes\n');

const stations      = [];
const assignedStops = new Set();
const assignedEsSlugs = new Set(); // slugs ES déjà rattachés à une gare CSV
let nbFusionsTI     = 0;

for (const row of csvRows) {
  if (row.is_suggestable !== 't') continue;
  if (!row.name?.trim()) continue;

  const uic8Sncf  = row.uic8_sncf?.trim();
  const uicIntl   = row.uic?.trim();
  const tiId      = row.trenitalia_id?.trim();
  const country   = row.country?.trim() || 'FR';
  const lat       = parseFloat(row.latitude)  || 0;
  const lon       = parseFloat(row.longitude) || 0;
  const isTiEn    = row.trenitalia_is_enabled === 't';
  const csvSlug   = (row.slug || '').trim(); // ex: "paris-gare-du-nord"

  const allStopIds = new Set();
  const operators  = new Set();

  // (1) Stops SNCF via UIC8 SNCF
  if (uic8Sncf) {
    for (const sid of (uic8ToStops[uic8Sncf] || [])) {
      allStopIds.add(sid);
      operators.add(extractOperator(sid));
    }
  }

  // (2) Stops via UIC international
  if (uicIntl && uicIntl !== uic8Sncf) {
    for (const sid of (uic8ToStops[uicIntl] || [])) {
      if (!assignedStops.has(sid)) {
        allStopIds.add(sid);
        operators.add(extractOperator(sid));
      }
    }
    if (isTiEn) {
      for (const sid of (uic8ToStops[uicIntl] || [])) {
        if (extractOperator(sid) === 'TI') {
          allStopIds.add(sid);
          operators.add('TI');
        }
      }
    }
  }

  // (3) Stops TI via trenitalia_id
  if (tiId && isTiEn) {
    for (const sid of (uic8ToStops[tiId] || [])) {
      if (!assignedStops.has(sid)) {
        allStopIds.add(sid);
        operators.add('TI');
        nbFusionsTI++;
      }
    }
  }

  // (4) Stops Eurostar via table de correspondance explicite (slug CSV → slug ES)
  //     puis fallback : slug CSV avec tirets → underscores (cas simples)
  const esSlugExplicit = CSV_SLUG_TO_ES_SLUG[csvSlug];
  const esSlugAuto     = csvSlug.replace(/-/g, '_');

  for (const esSlug of [esSlugExplicit, esSlugAuto].filter(Boolean)) {
    if (esSlug === esSlugAuto && esSlugExplicit) continue; // ne pas doubler si explicit déjà traité
    for (const sid of (slugToEsStops[esSlug] || [])) {
      if (!assignedStops.has(sid)) {
        allStopIds.add(sid);
        operators.add('ES');
      }
    }
    if ((slugToEsStops[esSlug] || []).length > 0) {
      assignedEsSlugs.add(esSlug);
    }
  }
  // Marquer le slug ES explicite comme assigné même si déjà dans assignedStops
  if (esSlugExplicit) assignedEsSlugs.add(esSlugExplicit);

  // (4b) Stops ES via validEsTransfers (whitelist extraite du transfer_index)
  // Couvre Bruxelles-Midi, Marne-la-Vallée, Moûtiers, Albertville, etc.
  // On cherche via uic8Sncf (CSV) ET via les UIC extraits des stopIds déjà collectés
  // car certaines gares (ex: Bruxelles uic8=88140010) ont uic8_sncf=null dans le CSV
  // mais leurs stopIds contiennent l'UIC qui est dans validEsTransfers.
  const uicsToCheck = new Set();
  if (uic8Sncf) uicsToCheck.add(uic8Sncf);
  // Extraire les UIC depuis les stopIds SNCF déjà collectés
  for (const sid of allStopIds) {
    const m = sid.match(/-(\d{7,9})$/) || sid.match(/OCE(\d{7,9})$/);
    if (m) uicsToCheck.add(m[1]);
  }
  for (const uic of uicsToCheck) {
    if (!validEsTransfers[uic]) continue;
    for (const esBase of validEsTransfers[uic]) {
      for (const sid of (slugToEsStops[esBase] || [])) {
        if (!assignedStops.has(sid)) {
          allStopIds.add(sid);
          operators.add('ES');
          assignedEsSlugs.add(esBase);
        }
      }
    }
  }

  // (5) Propagation via transfer_index (stops SNCF/TI uniquement, ES bloqués)
  // Les ES: sont gérés exclusivement via les étapes (4) et (4b) pour éviter
  // les faux liens par proximité GPS (ex: Est → paris_nord).
  for (const sid of [...allStopIds]) {
    for (const sister of (xfer[sid] || [])) {
      if (assignedStops.has(sister)) continue;
      if (sister.startsWith('ES:')) continue;  // ES uniquement via whitelist
      allStopIds.add(sister);
      operators.add(extractOperator(sister));
    }
  }

  // (6) Ponts ibériques CP ↔ RENFE (fusions "merge" uniquement)
  // Force l'ajout des stops de l'opérateur partenaire pour les gares frontières
  // où les deux réseaux partagent physiquement la même infrastructure.
  for (const sid of [...allStopIds]) {
    for (const partner of (iberianMergeIndex[sid] || [])) {
      if (assignedStops.has(partner)) continue;
      allStopIds.add(partner);
      operators.add(extractOperator(partner));
    }
  }

  if (!allStopIds.size) continue;

  stations.push({
    name:      row.name.trim(),
    city:      extractCity(row.name.trim()),
    slug:      csvSlug,
    country,
    lat,
    lon,
    stopIds:   [...allStopIds],
    operators: [...operators].sort(),
    sncf_id:   row.sncf_id?.trim()  || null,
    ti_id:     tiId || null,
    uic8:      uic8Sncf || null,
  });

  for (const sid of allStopIds) assignedStops.add(sid);
}

// ── Gares ES non rattachées au CSV ────────────────────────────────────────────
// Certaines gares Eurostar (Amsterdam, Bruxelles, St-Pancras…) n'ont pas
// d'entrée is_suggestable=t dans le CSV Trainline. On les crée ici depuis
// l'index ES, groupées par slug de base, avec le bon pays.
// On fusionne aussi les stops SNCF orphelins qui correspondent à la même gare
// (même nom normalisé ou distance GPS < 300m).
const esOnlyAdded = [];

function distMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 +
            Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) * Math.sin(dLon/2)**2;
  return R * 2 * Math.asin(Math.sqrt(a));
}

function normalizeForMerge(n) {
  return n.toLowerCase().trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[-_\s]+/g, ' ');
}

// Pré-collecter les stops SNCF orphelins (non encore assignés, non ES)
// pour pouvoir les fusionner avec les gares ES-only
const sncfOrphansByName = new Map();
for (const [sid, stop] of Object.entries(stops)) {
  if (assignedStops.has(sid) || sid.startsWith('ES:')) continue;
  const key = normalizeForMerge(stop.name || sid);
  if (!sncfOrphansByName.has(key)) sncfOrphansByName.set(key, []);
  sncfOrphansByName.get(key).push({ sid, stop });
}

for (const [esBase, esStopIds] of Object.entries(slugToEsStops)) {
  if (assignedEsSlugs.has(esBase)) continue; // déjà rattaché à une gare CSV
  if (esStopIds.every(sid => assignedStops.has(sid))) continue; // déjà assignés

  // Récupérer nom et coords depuis le stop "station_area" ou le premier stop
  const areaSid  = esStopIds.find(s => s.endsWith('_station_area')) || esStopIds[0];
  const areaStop = stops[areaSid] || stops[esStopIds[0]] || {};
  const name     = areaStop.name  || esBase.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  const lat      = areaStop.lat   || 0;
  const lon      = areaStop.lon   || 0;
  const country  = ES_SLUG_COUNTRY[esBase] || 'EU';

  const allStopIds = new Set(esStopIds);
  const operators  = new Set(['ES']);

  // Fusionner les stops SNCF orphelins ayant le même nom normalisé
  const nameKey = normalizeForMerge(name);
  for (const { sid, stop: orphStop } of (sncfOrphansByName.get(nameKey) || [])) {
    if (assignedStops.has(sid)) continue;
    allStopIds.add(sid);
    operators.add(extractOperator(sid));
    assignedStops.add(sid);
  }

  // Fusionner aussi les stops SNCF orphelins très proches géographiquement (< 300m)
  // mais seulement si leurs coordonnées sont connues
  if (lat && lon) {
    for (const [key, orphans] of sncfOrphansByName.entries()) {
      for (const { sid, stop: orphStop } of orphans) {
        if (assignedStops.has(sid)) continue;
        if (!orphStop.lat || !orphStop.lon) continue;
        if (distMeters(lat, lon, orphStop.lat, orphStop.lon) < 300) {
          allStopIds.add(sid);
          operators.add(extractOperator(sid));
          assignedStops.add(sid);
        }
      }
    }
  }

  stations.push({
    name,
    city:      extractCity(name),
    slug:      esBase.replace(/_/g, '-'),
    country,
    lat,
    lon,
    stopIds:   [...allStopIds],
    operators: [...operators].sort(),
    sncf_id:   null,
    ti_id:     null,
    uic8:      null,
  });

  for (const sid of esStopIds) assignedStops.add(sid);
  esOnlyAdded.push(name);
}

// ── Stops orphelins SNCF/TI (non couverts par le CSV) ────────────────────────
function normalizeStationName(n) {
  return n.toLowerCase().trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[-_\s]+/g, ' ');
}

// Détecte le pays d'une gare orpheline depuis son stop_id ou son UIC
// Les UIC commencent par le code pays : 87/86 = FR, 88 = BE, 80 = DE, 83 = IT, etc.
function countryFromStopId(sid) {
  // ✅ Opérateurs espagnols — stop IDs courts (5 chiffres), pas de préfixe UIC
  if (sid.startsWith('RENFE:') || sid.startsWith('OUIGO_ES:')) return 'ES';
  // ✅ CP Portugal — stop IDs préfixés 94_
  if (sid.startsWith('CP:')) return 'PT';

  const m = sid.match(/(\d{7,9})$/);
  if (!m) return 'FR';
  const uic = m[1];
  const prefix = uic.slice(0, 2);
  const map = { '87':'FR','86':'FR','88':'BE','80':'DE','81':'DE','82':'AT',
                '83':'IT','84':'ES','85':'PT','71':'ES','70':'GB','74':'CH',  // ✅ 71=Espagne, 70=GB
                '79':'NL','78':'NL','55':'PL','54':'CZ','53':'SK' };
  return map[prefix] || 'FR';
}

// Index stopId → index dans stations[] pour absorption des orphelins
const stopIdToStation = new Map();
for (let i = 0; i < stations.length; i++) {
  for (const sid of stations[i].stopIds) stopIdToStation.set(sid, i);
}

const orphanGroups = new Map();
for (const [sid, stop] of Object.entries(stops)) {
  if (assignedStops.has(sid)) continue;
  if (sid.startsWith('ES:')) continue;

  // Si ce StopPoint a un StopArea parent déjà assigné à une gare,
  // l'absorber dans cette gare plutôt que d'en créer une orpheline
  // (ex: SNCF:StopPoint:OCETGV INOUI-88140010 → parent OCE88140010 → Bruxelles-Midi)
  const parentArea = (xfer[sid] || []).find(v => v.startsWith('SNCF:StopArea:'));
  if (parentArea && stopIdToStation.has(parentArea)) {
    const parentStation = stations[stopIdToStation.get(parentArea)];
    if (!parentStation.stopIds.includes(sid)) {
      parentStation.stopIds.push(sid);
    }
    assignedStops.add(sid);
    continue;
  }

  const op   = stop.operator || extractOperator(sid);
  const name = stop.name || sid;
  const key  = normalizeStationName(name);
  if (!orphanGroups.has(key)) {
    orphanGroups.set(key, { name, country: op === 'TI' ? 'IT' : op === 'CP' ? 'PT' : countryFromStopId(sid),  // ✅ RENFE/OUIGO_ES → 'ES', CP → 'PT'
      lat: stop.lat||0, lon: stop.lon||0, stopIds: [sid], operators: new Set([op]) });
  } else {
    const e = orphanGroups.get(key);
    e.stopIds.push(sid);
    e.operators.add(op);
    if (op === 'SNCF' && !e.operators.has('SNCF')) e.name = name;
  }
}
for (const e of orphanGroups.values()) {
  stations.push({ ...e, city: extractCity(e.name), slug: '', operators: [...e.operators].sort(), sncf_id:null, ti_id:null, uic8:null });
}

// ── Application des ponts ibériques dans stations.json ───────────────────────
// Fusionne les stopIds RENFE et CP pour les gares partagées ou frontalières
// définies dans IBERIAN_BRIDGES (déclaré plus haut dans ce fichier).
//
// • Ponts sans interCity (Tren Celta) : les deux stops sont ajoutés à la même
//   station → l'algorithme RAPTOR les traite comme un seul quai.
// • Pont interCity (Elvas ↔ Badajoz) : les deux stations restent distinctes mais
//   le transfer_index de gtfs-ingest.js injecte un lien { id, interCity:true }
//   pour que server.js applique un temps de correspondance majoré.
{
  // Reconstruire l'index stopId → indice station après tous les orphelins
  const sidToIdx = new Map();
  for (let i = 0; i < stations.length; i++) {
    for (const sid of stations[i].stopIds) sidToIdx.set(sid, i);
  }

  let mergedCount = 0;
  for (const bridge of IBERIAN_BRIDGES) {
    const { a, b, interCity, name: bridgeName } = bridge;
    const iA = sidToIdx.get(a);
    const iB = sidToIdx.get(b);

    if (iA === undefined && iB === undefined) {
      console.warn(`  ⚠  Pont ibérique ignoré (aucun des deux stops trouvé) : ${a} ↔ ${b}`);
      continue;
    }

    if (interCity) {
      // Elvas ↔ Badajoz : gares séparées, correspondance inter-city
      // Le lien sera injecté par gtfs-ingest.js dans le transfer_index.
      // Ici on s'assure juste que les deux stations existent dans stations.json.
      const stA = iA !== undefined ? stations[iA] : null;
      const stB = iB !== undefined ? stations[iB] : null;
      if (stA && stB) {
        console.log(`  🌉 Pont interCity : ${stA.name} (PT) ↔ ${stB.name} (ES) — ${bridgeName}`);
      } else {
        console.warn(`  ⚠  Pont interCity incomplet : ${a}=${stA?.name} / ${b}=${stB?.name}`);
      }
    } else {
      // Gares Tren Celta : fusion des stopIds dans la station dominante
      // On garde la station qui a le plus d'opérateurs (ou A par défaut)
      if (iA !== undefined && iB !== undefined && iA !== iB) {
        // Fusionner B dans A
        const stA = stations[iA];
        const stB = stations[iB];
        const allIds = new Set([...stA.stopIds, ...stB.stopIds]);
        stA.stopIds = [...allIds];
        for (const op of stB.operators) {
          if (!stA.operators.includes(op)) stA.operators.push(op);
        }
        stA.operators.sort();
        // Mettre à jour l'index pour tous les stopIds de B
        for (const sid of stB.stopIds) sidToIdx.set(sid, iA);
        // Marquer B comme supprimée
        stations[iB] = null;
        mergedCount++;
        console.log(`  🔗 Fusion Tren Celta : ${stB.name} (${b}) → ${stA.name} (${a})`);
      } else if (iA !== undefined && iB === undefined) {
        // B n'a pas encore de station : créer un stop orphelin dans la station A
        const stA = stations[iA];
        if (!stA.stopIds.includes(b)) {
          stA.stopIds.push(b);
          const opB = b.split(':')[0];
          if (!stA.operators.includes(opB)) { stA.operators.push(opB); stA.operators.sort(); }
          sidToIdx.set(b, iA);
          console.log(`  🔗 Stop ajouté : ${b} → station ${stA.name}`);
        }
      } else if (iA === undefined && iB !== undefined) {
        // A n'a pas encore de station : créer un stop orphelin dans la station B
        const stB = stations[iB];
        if (!stB.stopIds.includes(a)) {
          stB.stopIds.push(a);
          const opA = a.split(':')[0];
          if (!stB.operators.includes(opA)) { stB.operators.push(opA); stB.operators.sort(); }
          sidToIdx.set(a, iB);
          console.log(`  🔗 Stop ajouté : ${a} → station ${stB.name}`);
        }
      }
    }
  }

  // Supprimer les entrées nulles (stations fusionnées)
  const before = stations.length;
  stations.splice(0, stations.length, ...stations.filter(Boolean));
  if (mergedCount > 0) {
    console.log(`  ✅ ${mergedCount} station(s) fusionnée(s) (Tren Celta) — ${before - stations.length} entrée(s) supprimée(s)`);
  }
}

// Certaines gares créées via CSV ou orphelins SNCF ont un uic8 dans validEsTransfers
// (ex: Bruxelles-Midi uic8=88140010) mais leurs stops ES ont été traités séparément.
// On fusionne ici les deux entrées en ajoutant les stops ES à la gare SNCF
// et en supprimant l'entrée ES-only redondante.

const esOnlySlugs = new Set(esOnlyAdded.map(n => {
  // Retrouver le esBase depuis le nom
  for (const [base, ids] of Object.entries(slugToEsStops)) {
    const areaSid = ids.find(s => s.endsWith('_station_area')) || ids[0];
    const areaStop = stops[areaSid] || {};
    if ((areaStop.name || '').toLowerCase() === n.toLowerCase()) return base;
    if (base.replace(/_/g, '-') === n.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '')) return base;
  }
  return null;
}).filter(Boolean));

// Index stopId -> index dans stations[]
const stopIdToStationIdx = {};
for (let i = 0; i < stations.length; i++) {
  for (const sid of stations[i].stopIds) {
    stopIdToStationIdx[sid] = i;
  }
}

const toRemoveIdxs = new Set();

for (const [uic, esBases] of Object.entries(validEsTransfers)) {
  // Trouver la gare SNCF ayant ce uic8 — deux méthodes :
  // 1. Via le champ uic8 (gares françaises avec uic8_sncf dans le CSV)
  // 2. Via les stopIds SNCF contenant l'UIC (gares étrangères comme Bruxelles
  //    qui ont uic8=null mais dont les stopIds contiennent l'UIC ex: OCE88140010)
  let sncfStation = stations.find(s => s.uic8 === uic);
  if (!sncfStation) {
    sncfStation = stations.find(s =>
      s.stopIds.some(sid => {
        const m = sid.match(/(\d{7,9})$/);
        return m && m[1] === uic;
      })
    );
  }
  if (!sncfStation) continue;

  for (const esBase of esBases) {
    const esStopIds = slugToEsStops[esBase] || [];
    if (!esStopIds.length) continue;

    // Vérifier si ces stops ES sont dans une gare ES-only séparée
    const firstEs = esStopIds[0];
    const esStationIdx = stopIdToStationIdx[firstEs];
    const esStation = esStationIdx !== undefined ? stations[esStationIdx] : null;

    if (esStation && esStation !== sncfStation) {
      // Fusion : ajouter tous les stops ES à la gare SNCF
      const allIds = new Set(sncfStation.stopIds);
      for (const sid of esStation.stopIds) allIds.add(sid);
      sncfStation.stopIds = [...allIds];
      if (!sncfStation.operators.includes('ES')) {
        sncfStation.operators.push('ES');
        sncfStation.operators.sort();
      }
      // Marquer la gare ES-only pour suppression
      toRemoveIdxs.add(esStationIdx);
      console.log('  Fusion ES: ' + esBase + ' -> ' + sncfStation.name);
    } else if (!esStation) {
      // Les stops ES ne sont dans aucune gare : les ajouter directement
      const allIds = new Set(sncfStation.stopIds);
      for (const sid of esStopIds) allIds.add(sid);
      sncfStation.stopIds = [...allIds];
      if (!sncfStation.operators.includes('ES')) {
        sncfStation.operators.push('ES');
        sncfStation.operators.sort();
      }
    }
    assignedEsSlugs.add(esBase);
  }
}

// Supprimer les gares ES-only qui ont été fusionnées
const stationsFiltered = stations.filter((_, i) => !toRemoveIdxs.has(i));
stations.length = 0;
stations.push(...stationsFiltered);

if (toRemoveIdxs.size > 0) {
  console.log('  ' + toRemoveIdxs.size + ' gare(s) ES-only fusionnée(s) supprimée(s)');
}

// ── Tri ───────────────────────────────────────────────────────────────────────
stations.sort((a, b) => {
  const score = s =>
    (s.operators.includes('SNCF')     ? 8 : 0) +
    (s.operators.includes('ES')       ? 4 : 0) +
    (s.operators.includes('TI')       ? 2 : 0) +
    (s.operators.includes('RENFE')    ? 6 : 0) +
    (s.operators.includes('OUIGO_ES') ? 5 : 0) +
    (s.operators.includes('CP')       ? 5 : 0);
  if (score(b) !== score(a)) return score(b) - score(a);
  return a.name.localeCompare(b.name, 'fr');
});

fs.writeFileSync(OUT_FILE, JSON.stringify(stations, null, 2), 'utf8');

// ── Injection des ponts ibériques interCity dans transfer_index.json ──────────
// Après finalisation de stations.json, on enrichit le transfer_index.json
// avec les liens interCity Elvas↔Badajoz pour que server.js puisse calculer
// les correspondances cross-border avec le bon délai (MIN_TRANSFER_CITY).
if (global._iberianInterCityPairs && global._iberianInterCityPairs.length > 0) {
  const xferPath = path.join(DATA_DIR, 'transfer_index.json');
  if (fs.existsSync(xferPath)) {
    const xferData = JSON.parse(fs.readFileSync(xferPath, 'utf8'));
    let injected = 0;
    for (const [idsA, idsB] of global._iberianInterCityPairs) {
      for (const idA of idsA) {
        if (!xferData[idA]) xferData[idA] = [];
        for (const idB of idsB) {
          if (!xferData[idA].some(x => (x.id || x) === idB)) {
            xferData[idA].push({ id: idB, interCity: true });
            injected++;
          }
        }
      }
      for (const idB of idsB) {
        if (!xferData[idB]) xferData[idB] = [];
        for (const idA of idsA) {
          if (!xferData[idB].some(x => (x.id || x) === idA)) {
            xferData[idB].push({ id: idA, interCity: true });
            injected++;
          }
        }
      }
    }
    fs.writeFileSync(xferPath, JSON.stringify(xferData));
    console.log(`  🌉 ${injected} liens interCity ibériques injectés dans transfer_index.json`);
  }
}

const sizeKb = Math.round(fs.statSync(OUT_FILE).size / 1024);
console.log('✅ stations.json : ' + stations.length + ' gares — ' + sizeKb + ' KB');
console.log('  Fusions TI réussies  : ' + nbFusionsTI);
console.log('  Gares ES créées      : ' + esOnlyAdded.join(', '));
console.log('  Stops orphelins SNCF : ' + [...orphanGroups.values()].reduce((s,e)=>s+e.stopIds.length,0));

// ── Diagnostic ────────────────────────────────────────────────────────────────
console.log('\n── Diagnostic gares clés ─────────────────────────────────────────');
const CHECK = [
  'Paris Gare de Lyon', 'Paris Gare du Nord', "Paris Gare de l'Est",
  'Lyon Part-Dieu', 'Marseille St-Charles',
  'Milano Centrale', 'Milano Porta Garibaldi',
  'Torino Porta Susa', 'Torino Porta Nuova', 'Ventimiglia',
  'Amsterdam-Centraal', 'Bruxelles Midi', 'St-Pancras-International',
  // Espagne
  'Madrid Pta.Atocha - Almudena Grandes', 'Madrid-Chamartin-Clara Campoamor',
  'Barcelona Sants', 'Valencia Joaquin Sorolla', 'Sevilla Santa Justa',
  'Zaragoza-Delicias', 'Malaga-Maria Zambrano',
  // Portugal (CP)
  'Lisboa Santa Apolonia', 'Lisboa Oriente', 'Porto Campanha', 'Porto Sao Bento',
  'Coimbra B', 'Braga', 'Faro', 'Aveiro',
  // Gares frontières ES↔PT
  'Valença', 'Viana do Castelo', 'Elvas', 'Badajoz', 'Vilar Formoso',
];
for (const nom of CHECK) {
  const normName = str => str.toLowerCase().replace(/’/g, "'");
  const f = stations.find(s => normName(s.name) === normName(nom));
  if (f) {
    const es   = f.stopIds.filter(id => id.startsWith('ES:'));
    const ti   = f.stopIds.filter(id => id.startsWith('TI:'));
    const sncf = f.stopIds.filter(id => !id.startsWith('ES:') && !id.startsWith('TI:'));
    const warn = (!es.length && ['Amsterdam-Centraal','Bruxelles Midi','St-Pancras-International','Paris Gare du Nord'].includes(nom))
      ? ' ⚠ pas de stop ES' : '';
    console.log(`  ✅ ${nom.padEnd(30)} ${f.stopIds.length} stops [${f.operators.join('+')}]${warn}`);
    if (es.length)   console.log(`       ES  : ${es[0]}${es.length > 1 ? ` … +${es.length-1}` : ''}`);
    if (sncf.length) console.log(`       SNCF: ${sncf[0]}${sncf.length > 1 ? ` … +${sncf.length-1}` : ''}`);
  } else {
    console.log(`  ❌ ${nom} — introuvable dans stations.json`);
  }
}
console.log('\n→ Relancez : node server.js');