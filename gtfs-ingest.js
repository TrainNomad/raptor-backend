/**
 * GTFS Multi-Opérateurs → RAPTOR
 *
 * Usage :
 *   node gtfs-ingest.js                            ← lit operators.json
 *   node gtfs-ingest.js ./operators.json ./engine_data
 *
 * operators.json :
 * [
 *   { "id": "SNCF",  "name": "SNCF",             "gtfs_dir": "./gtfs/sncf" },
 *   { "id": "TI",    "name": "Trenitalia France", "gtfs_dir": "./gtfs/trenitalia" },
 *   { "id": "DB",    "name": "Deutsche Bahn",     "gtfs_dir": "./gtfs/db" },
 *   { "id": "SNCB",  "name": "SNCB Belgique",     "gtfs_dir": "./gtfs/sncb" },
 *   { "id": "RENFE", "name": "Renfe",              "gtfs_dir": "./gtfs/renfe" },
 *   { "id": "ESTA",  "name": "Eurostar",           "gtfs_dir": "./gtfs/eurostar" }
 * ]
 *
 * Tous les IDs sont préfixés : stop_id → "SNCF:StopPoint:OCE..."
 * Les correspondances inter-opérateurs sont créées par proximité géographique (< 300m)
 * ou par UIC partagé.
 */

const fs      = require('fs');
const path    = require('path');
const readline = require('readline');

const OPS_FILE = process.argv[2] || './operators.json';
const OUT_DIR  = process.argv[3] || './engine_data';

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

// ─── Utilitaires ─────────────────────────────────────────────────────────────

function timeToSeconds(t) {
  if (!t || !t.includes(':')) return null;
  const [h, m, s] = t.trim().split(':').map(Number);
  if (isNaN(h) || isNaN(m)) return null;
  return h * 3600 + m * 60 + (s || 0); // GTFS permet h > 23 pour après minuit
}

function parseGTFSDate(d) {
  const s = String(d).trim();
  const date = new Date(parseInt(s.slice(0,4)), parseInt(s.slice(4,6)) - 1, parseInt(s.slice(6,8)));
  return { date, dow: date.getDay() };
}

const DOW_KEYS = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'];

async function readCSV(filePath) {
  return new Promise((resolve) => {
    if (!fs.existsSync(filePath)) {
      console.warn('    ⚠  Manquant : ' + path.basename(filePath));
      return resolve([]);
    }
    const rows = []; let headers = null;
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath, { encoding: 'utf8' }),
      crlfDelay: Infinity,
    });
    rl.on('line', (line) => {
      // Gestion des champs entre guillemets contenant des virgules
      const clean = line.replace(/^\uFEFF/, '').trim();
      if (!clean) return;
      const cols = parseCSVLine(clean);
      if (!headers) { headers = cols; return; }
      const obj = {};
      headers.forEach((h, i) => { obj[h] = (cols[i] !== undefined ? cols[i] : '').trim(); });
      rows.push(obj);
    });
    rl.on('close', () => resolve(rows));
    rl.on('error', () => resolve([]));
  });
}

function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { inQ = !inQ; }
    else if (c === ',' && !inQ) { result.push(cur); cur = ''; }
    else { cur += c; }
  }
  result.push(cur);
  return result;
}

// ─── Calendrier ───────────────────────────────────────────────────────────────

function computeActiveServices(calendarRows, calendarDatesRows, gtfsDate) {
  const { date, dow } = parseGTFSDate(gtfsDate);
  const dowKey = DOW_KEYS[dow];
  const active = new Set();
  for (const row of calendarRows) {
    const start = parseGTFSDate(row.start_date).date;
    const end   = parseGTFSDate(row.end_date).date;
    if (date >= start && date <= end && row[dowKey] === '1') active.add(row.service_id);
  }
  for (const row of calendarDatesRows) {
    if (row.date.trim() === gtfsDate) {
      if (row.exception_type === '1')      active.add(row.service_id);
      else if (row.exception_type === '2') active.delete(row.service_id);
    }
  }
  return active;
}

function buildCalendarIndex(calendarRows, calendarDatesRows, prefix) {
  const allDates = new Set();
  for (const row of calendarRows) {
    const start = parseGTFSDate(row.start_date).date;
    const end   = parseGTFSDate(row.end_date).date;
    const cur   = new Date(start);
    while (cur <= end) {
      const y = cur.getFullYear(), m = String(cur.getMonth()+1).padStart(2,'0'), d = String(cur.getDate()).padStart(2,'0');
      allDates.add(y+''+m+''+d);
      cur.setDate(cur.getDate()+1);
    }
  }
  for (const row of calendarDatesRows) allDates.add(row.date.trim());

  const index = {};
  for (const gtfsDate of allDates) {
    const services = computeActiveServices(calendarRows, calendarDatesRows, gtfsDate);
    const iso = gtfsDate.slice(0,4)+'-'+gtfsDate.slice(4,6)+'-'+gtfsDate.slice(6,8);
    // On préfixe les service_ids pour éviter les collisions
    index[iso] = [...services].map(s => prefix + ':' + s);
  }
  return index;
}

// ─── Détection du type de train par opérateur ─────────────────────────────────

/**
 * Retourne le train_type à partir de l'opérateur, du stop_id et du trip_id.
 * Centralisé ici pour que l'ingestion et le serveur partagent la même logique.
 *
 * Convention de stockage dans route_trips : chaque trip a un champ `operator`
 * égal au prefix opérateur (ex: "SNCF", "TI", "DB"...)
 */
function detectTrainType(operatorId, stopId, tripId, routeType) {
  const sid = (stopId  || '').toUpperCase();
  const tid = (tripId  || '').toUpperCase();

  switch (operatorId) {
    case 'SNCF': {
      // SNCF : lecture du quai dans le stop_id
      const m = stopId.match(/StopPoint:OCE(.+)-\d{8}$/);
      const quai = m ? m[1].trim() : '';
      if (quai === 'OUIGO' || tid.includes('OUIGO')) {
        const numM = tripId.match(/^OCESN([47]\d{3})/);
        const num  = numM ? parseInt(numM[1]) : null;
        if (num !== null) return num >= 7000 ? 'OUIGO' : 'OUIGO_CLASSIQUE';
        return 'OUIGO';
      }
      if (quai === 'TGV INOUI'           || tid.includes('INOUI'))      return 'INOUI';
      if (quai === 'INTERCITES de nuit')                                 return 'IC_NUIT';
      if (quai === 'INTERCITES'          || tid.includes('INTERCITES'))  return 'IC';
      if (quai === 'Lyria'               || tid.includes('LYRIA'))       return 'LYRIA';
      if (quai === 'ICE')                                                return 'ICE';
      if (quai === 'TramTrain')                                          return 'TRAMTRAIN';
      if (quai === 'Car TER')                                            return 'CAR';
      if (quai === 'Train TER')                                          return 'TER';
      if (quai === 'Navette')                                            return 'NAVETTE';
      return 'TRAIN';
    }

    case 'TI': {
      // Identification claire de Trenitalia France
      if (tid.includes('FRECCIAROSSA') || tid.includes('FR')) return 'FRECCIAROSSA';
      return 'FRECCIAROSSA'; // Par défaut pour TI en France
    }

    case 'ES': {
      // Identification claire d'Eurostar
      return 'EUROSTAR';
    }

    case 'RENFE': {
      // Identification claire pour l'Espagne
      if (tid.includes('AVE')) return 'AVE';
      if (tid.includes('ALVIA')) return 'ALVIA';
      return 'RENFE';
    }

    default:
      return 'TRAIN';
  }
}
// ─── Correspondances inter-opérateurs (par UIC ou proximité géo) ─────────────

/**
 * Extrait le code UIC 8 chiffres depuis un stop_id de n'importe quel opérateur.
 * SNCF   : "SNCF:StopPoint:OCE...-87391003"  → 87391003
 * Trenitalia : souvent "TI:S00020" (codes italiens) → pas de UIC direct
 * DB/SNCB : parfois "8300010" (format UIC partiel)
 */
function extractUIC(stopId) {
  const m = (stopId || '').match(/(\d{8})(?:[^0-9]|$)/);
  return m ? m[1] : null;
}

/**
 * Distance en mètres entre deux points GPS (formule haversine simplifiée).
 */
function distanceMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

/**
 * Noms TI connus pour être mal nommés dans le GTFS → nom réel normalisé cible.
 * Permet de forcer la correspondance même si les coords GPS sont trop éloignées.
 *
 * Clé   : normName(stop.name) du stop TI
 * Valeur : normName(stop.name) du/des stops SNCF cible(s)
 *
 * Ex : TI "Lyon-Perrache-Voyageurs" → SNCF "Lyon Part-Dieu"
 * (Le Frecciarossa s'arrête à Part-Dieu ; TI l'appelle Perrache par erreur)
 */
const FORCE_MERGE_NAMES = {
  'LYON PERRACHE VOYAGEURS': ['LYON PART DIEU', 'LYON PERRACHE'],
  'LYON-PERRACHE-VOYAGEURS': ['LYON PART DIEU', 'LYON PERRACHE'],
  'MARSEILLE ST CHARLES':    ['MARSEILLE SAINT CHARLES', 'MARSEILLE ST CHARLES'],
  'MARSEILLE-ST-CHARLES':    ['MARSEILLE SAINT CHARLES', 'MARSEILLE ST CHARLES'],
};

function normNameForMerge(name) {
  return (name||'').toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[-_]/g,' ').replace(/\s+/g,' ').trim();
}

/**
 * Construit l'index de correspondances inter-opérateurs.
 * Deux arrêts de deux opérateurs différents sont "la même gare physique" si :
 *   a) Ils partagent un code UIC à 8 chiffres identique, OU
 *   b) Ils sont à moins de MAX_DIST mètres l'un de l'autre, OU
 *   c) Ils sont dans FORCE_MERGE_NAMES (nommage GTFS incohérent)
 */
/**
 * Construit l'index des correspondances (transferts).
 * Utilise la proximité GPS ET le fichier stations.json pour fusionner les réseaux.
 */
function buildTransferIndex(stopsDict) {
  console.log('\n🔗 Construction de l\'index de transfert...');
  const transferIndex = {};
  const ids = Object.keys(stopsDict);

  // --- 1. Logique de proximité GPS (300m) ---
  // On garde cette logique pour les gares non répertoriées dans stations.json
  for (let i = 0; i < ids.length; i++) {
    const s1 = stopsDict[ids[i]];
    for (let j = i + 1; j < ids.length; j++) {
      const s2 = stopsDict[ids[j]];
      const dist = haversine(s1.lat, s1.lon, s2.lat, s2.lon);
      
      if (dist < 300) {
        if (!transferIndex[ids[i]]) transferIndex[ids[i]] = [];
        if (!transferIndex[ids[j]]) transferIndex[ids[j]] = [];
        if (!transferIndex[ids[i]].includes(ids[j])) transferIndex[ids[i]].push(ids[j]);
        if (!transferIndex[ids[j]].includes(ids[i])) transferIndex[ids[j]].push(ids[i]);
      }
    }
  }

  // --- 2. Injection forcée via stations.json (Crucial pour SNCF <-> Eurostar) ---
  // On utilise ici votre fichier stations.json comme "vérité" pour lier les IDs
  try {
    // Note: On remonte d'un dossier si stations.json est à la racine et le script ailleurs
    const stationsPath = path.join(__dirname, 'stations.json');
    
    if (fs.existsSync(stationsPath)) {
      console.log('📖 Enrichissement des transferts via stations.json...');
      const stations = JSON.parse(fs.readFileSync(stationsPath, 'utf8'));
      let manualLinks = 0;

      stations.forEach(station => {
        if (station.stopIds && station.stopIds.length > 1) {
          // Pour chaque identifiant de la gare (SNCF, ES, TI...)
          station.stopIds.forEach(idA => {
            if (!stopsDict[idA]) return; // On ignore si l'ID n'est pas dans le GTFS actuel

            if (!transferIndex[idA]) transferIndex[idA] = [];

            station.stopIds.forEach(idB => {
              if (idA !== idB && stopsDict[idB]) {
                if (!transferIndex[idA].includes(idB)) {
                  transferIndex[idA].push(idB);
                  manualLinks++;
                }
              }
            });
          });
        }
      });
      console.log(`✅ ${manualLinks} liaisons inter-opérateurs ajoutées depuis stations.json`);
    }
  } catch (err) {
    console.error('⚠ Erreur lors de la lecture de stations.json:', err.message);
  }

  return transferIndex;
}

/**
 * Calcul de distance Haversine (en mètres)
 */
function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371000; 
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// ─── Ingestion d'un opérateur ─────────────────────────────────────────────────

async function ingestOperator(op) {
  const { id: operatorId, name, gtfs_dir } = op;
  const P = (rawId) => operatorId + ':' + rawId; // préfixe tous les IDs

  console.log(`\n  📂 ${name} (${operatorId}) — ${gtfs_dir}`);

  if (!fs.existsSync(gtfs_dir)) {
    console.warn(`    ❌ Dossier introuvable : ${gtfs_dir}`);
    return null;
  }

  const [stopTimesRaw, tripsRaw, stopsRaw, routesRaw, calendarRaw, calendarDatesRaw] = await Promise.all([
    readCSV(path.join(gtfs_dir, 'stop_times.txt')),
    readCSV(path.join(gtfs_dir, 'trips.txt')),
    readCSV(path.join(gtfs_dir, 'stops.txt')),
    readCSV(path.join(gtfs_dir, 'routes.txt')),
    readCSV(path.join(gtfs_dir, 'calendar.txt')),
    readCSV(path.join(gtfs_dir, 'calendar_dates.txt')),
  ]);

  console.log(`    stop_times     : ${stopTimesRaw.length.toLocaleString()}`);
  console.log(`    trips          : ${tripsRaw.length.toLocaleString()}`);
  console.log(`    stops          : ${stopsRaw.length.toLocaleString()}`);
  console.log(`    routes         : ${routesRaw.length.toLocaleString()}`);

  // Calendrier
  const calendarIndex = buildCalendarIndex(calendarRaw, calendarDatesRaw, operatorId);
  console.log(`    dates GTFS     : ${Object.keys(calendarIndex).length}`);

  // Routes
  const routeInfo = {};
  const routeTypeMap = {};
  for (const r of routesRaw) {
    routeInfo[P(r.route_id)] = {
      short:    r.route_short_name || '',
      long:     r.route_long_name  || '',
      type:     parseInt(r.route_type) || 0,
      operator: operatorId,
    };
    routeTypeMap[r.route_id] = parseInt(r.route_type) || 0;
  }

  // Trips
  const tripToService  = {};
  const tripToRoute    = {};
  const tripToHeadsign = {};
  for (const t of tripsRaw) {
    tripToService[t.trip_id]  = P(t.service_id);
    tripToRoute[t.trip_id]    = P(t.route_id);
    tripToHeadsign[t.trip_id] = t.trip_headsign || '';
  }

  // Arrêts
  const stopsDict = {};
  for (const s of stopsRaw) {
    stopsDict[P(s.stop_id)] = {
      name:     s.stop_name || s.stop_id,
      lat:      parseFloat(s.stop_lat)  || 0,
      lon:      parseFloat(s.stop_lon)  || 0,
      operator: operatorId,
    };
  }

  // Stop times → tripStops
  // FIX TI : certains trip_id GTFS fusionnent deux sens de marche dans le même trip
  // (ex: seq 5=Turin, 24=Milano, 38=Milano, 39=Paris -> le temps RECULE entre seq 38 et 39)
  // On détecte ces ruptures temporelles et on ne garde que le segment le plus long.
  const validTripIds = new Set(tripsRaw.map(t => t.trip_id));
  const tripStops = {};
  for (const st of stopTimesRaw) {
    if (!validTripIds.has(st.trip_id)) continue;
    if (!tripStops[st.trip_id]) tripStops[st.trip_id] = [];
    tripStops[st.trip_id].push({
      seq:      parseInt(st.stop_sequence) || 0,
      stop_id:  P(st.stop_id),
      dep_time: timeToSeconds(st.departure_time),
      arr_time: timeToSeconds(st.arrival_time),
    });
  }

  // FIX TI : les stop_sequence encodent parfois un trajet circulaire (rotation de rame).
  // Ex: seq 5=Turin(11:36), 24=Milano(12:22), 38=Milano(13:11), 39=Paris(06:30), 90=Lyon(08:31)...
  // Le temps recule entre seq 38 (13h) et seq 39 (06h30) : c est une rupture de rotation.
  // Solution : trier par temps chronologique plutôt que par stop_sequence,
  // puis éliminer les stops dont le temps est incohérent avec le sens de marche principal.
  for (const [trip_id, stops] of Object.entries(tripStops)) {
    stops.sort((a, b) => a.seq - b.seq);

    // Détecter les ruptures temporelles (le temps recule fortement)
    const segments = [];
    let segStart = 0;
    for (let i = 1; i < stops.length; i++) {
      const prevTime = stops[i-1].dep_time ?? stops[i-1].arr_time ?? -1;
      const currTime = stops[i].arr_time   ?? stops[i].dep_time   ?? prevTime + 1;
      if (prevTime >= 0 && currTime < prevTime - 600) {
        segments.push({ start: segStart, end: i, stops: stops.slice(segStart, i) });
        segStart = i;
      }
    }
    segments.push({ start: segStart, end: stops.length, stops: stops.slice(segStart) });

    if (segments.length > 1) {
      // Recoller les segments dans l ordre chronologique (par heure du premier stop)
      segments.sort((a, b) => {
        const ta = a.stops[0].dep_time ?? a.stops[0].arr_time ?? 0;
        const tb = b.stops[0].dep_time ?? b.stops[0].arr_time ?? 0;
        return ta - tb;
      });

      // Verifier la coherence : si le dernier stop du segment A arrive avant le premier
      // stop du segment B, ils font bien partie du meme trajet -> les concatener.
      // Sinon (deux trajets independants), garder le plus long.
      const merged = [segments[0].stops];
      for (let k = 1; k < segments.length; k++) {
        const lastStop  = merged[merged.length - 1].slice(-1)[0];
        const firstStop = segments[k].stops[0];
        const lastTime  = lastStop.arr_time  ?? lastStop.dep_time  ?? -1;
        const firstTime = firstStop.dep_time ?? firstStop.arr_time ?? lastTime + 1;
        if (firstTime >= lastTime - 600) {
          // Coherent : meme trajet, on concatene
          merged[merged.length - 1] = merged[merged.length - 1].concat(segments[k].stops);
        } else {
          // Deux trajets distincts : commencer un nouveau groupe
          merged.push(segments[k].stops);
        }
      }

      // Garder le groupe le plus long
      merged.sort((a, b) => b.length - a.length);
      // RE-TRIER par temps chronologique car apres recollement les seq sont dans le mauvais ordre
      // (ex: 39,90,124,129,146,5,24,38) -> doit devenir 39,90,124,129,146,5,24,38 trié par heure
      merged[0].sort((a, b) => (a.dep_time ?? a.arr_time ?? 0) - (b.dep_time ?? b.arr_time ?? 0));
      tripStops[trip_id] = merged[0];
    } else {
      // Meme pour les trips sans rupture : trier par temps pour etre sur
      stops.sort((a, b) => (a.dep_time ?? a.arr_time ?? 0) - (b.dep_time ?? b.arr_time ?? 0));
    }
  }

  // Connexions
  // NOTE: Ne pas re-trier par seq ici — l'ordre chronologique a déjà été établi
  // dans la boucle de pré-traitement (fix backtracking TI). Un re-sort par seq
  // annulerait le réordonnancement et remettrait le trajet retour en tête.
  const connections = [];
  for (const [trip_id, stops] of Object.entries(tripStops)) {
    const route_id   = tripToRoute[trip_id]   || P('unknown');
    const service_id = tripToService[trip_id] || '';
    for (let i = 0; i < stops.length - 1; i++) {
      const from = stops[i], to = stops[i+1];
      if (from.dep_time === null || to.arr_time === null) continue;
      connections.push({
        dep_stop: from.stop_id, arr_stop: to.stop_id,
        dep_time: from.dep_time, arr_time: to.arr_time,
        trip_id: P(trip_id), route_id, service_id,
        headsign: tripToHeadsign[trip_id] || '',
        operator: operatorId,
      });
    }
  }

  // RAPTOR structures
  const routesByStop = {};
  const routeStops   = {};
  const routeTrips   = {};

  for (const [trip_id, stops] of Object.entries(tripStops)) {
    const route_id   = tripToRoute[trip_id]   || P('unknown');
    const service_id = tripToService[trip_id] || '';
    const routeType  = routeTypeMap[tripToRoute[trip_id]?.replace(operatorId+':','') || ''] || 0;
    // NOTE: Ne pas re-trier par seq — l'ordre chronologique a été établi dans le pré-traitement.

    // FIX 1 : routeStops = variante avec le plus d arrêts (la plus complète)
    if (!routeStops[route_id] || stops.length > routeStops[route_id].length) {
      routeStops[route_id] = stops.map(s => s.stop_id);
    }
    if (!routeTrips[route_id]) routeTrips[route_id] = [];

    // Train type détecté à l'ingestion et stocké dans le trip
    const trainType = detectTrainType(
      operatorId,
      stops[0]?.stop_id || '',
      trip_id,
      routeType
    );

    // FIX 2 : dep_time_first = premier dep_time non-null après tri par seq
    const firstDep = stops.find(s => s.dep_time !== null)?.dep_time ?? Infinity;

    routeTrips[route_id].push({
      trip_id: P(trip_id),
      service_id,
      dep_time_first: firstDep,
      train_type: trainType,
      operator:   operatorId,
      stop_times: stops,
    });

    for (const s of stops) {
      if (!routesByStop[s.stop_id]) routesByStop[s.stop_id] = new Set();
      routesByStop[s.stop_id].add(route_id);
    }
  }

  for (const rid of Object.keys(routeTrips)) {
    routeTrips[rid].sort((a, b) => a.dep_time_first - b.dep_time_first);
  }

  const routesByStopSerial = {};
  for (const [stop, routes] of Object.entries(routesByStop)) {
    routesByStopSerial[stop] = [...routes];
  }

  console.log(`    connexions     : ${connections.length.toLocaleString()}`);
  console.log(`    routes         : ${Object.keys(routeInfo).length.toLocaleString()}`);
  console.log(`    arrêts         : ${Object.keys(stopsDict).length.toLocaleString()}`);

  return { connections, stopsDict, routeInfo, routesByStopSerial, routeStops, routeTrips, calendarIndex };
}

// ─── Fusion multi-opérateurs ──────────────────────────────────────────────────

function mergeResults(results) {
  const merged = {
    connections:   [],
    stopsDict:     {},
    routeInfo:     {},
    routesByStop:  {},   // stop_id → Set
    routeStops:    {},
    routeTrips:    {},
    calendarIndex: {},
  };

  for (const r of results) {
    if (!r) continue;

    // ⚠ NE PAS utiliser push(...bigArray) → stack overflow sur >100k éléments
    // concat alloue un nouveau tableau sans passer par la pile d'appels
    merged.connections = merged.connections.concat(r.connections);

    Object.assign(merged.stopsDict,  r.stopsDict);
    Object.assign(merged.routeInfo,  r.routeInfo);
    Object.assign(merged.routeStops, r.routeStops);
    Object.assign(merged.routeTrips, r.routeTrips);

    // routesByStop : union des sets
    for (const [stop, routes] of Object.entries(r.routesByStopSerial)) {
      if (!merged.routesByStop[stop]) merged.routesByStop[stop] = new Set();
      for (const rid of routes) merged.routesByStop[stop].add(rid);
    }

    // calendarIndex : union par date (même problème avec push(...services))
    for (const [date, services] of Object.entries(r.calendarIndex)) {
      if (!merged.calendarIndex[date]) {
        merged.calendarIndex[date] = services.slice(); // copie directe
      } else {
        for (const s of services) merged.calendarIndex[date].push(s);
      }
    }
  }

  merged.connections.sort((a, b) => a.dep_time - b.dep_time);

  // Sérialiser routesByStop (Set → Array)
  const routesByStopSerial = {};
  for (const [stop, routes] of Object.entries(merged.routesByStop)) {
    routesByStopSerial[stop] = [...routes];
  }
  merged.routesByStop = routesByStopSerial;

  return merged;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║  GTFS Multi-Opérateurs → RAPTOR Ingestion            ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  console.time('Total');

  // Charger la configuration opérateurs
  if (!fs.existsSync(OPS_FILE)) {
    // Créer un operators.json d'exemple si inexistant
    const example = [
      { "id": "SNCF",  "name": "SNCF",             "gtfs_dir": "./gtfs/sncf" },
      { "id": "TI",    "name": "Trenitalia France", "gtfs_dir": "./gtfs/trenitalia" }
    ];
    fs.writeFileSync(OPS_FILE, JSON.stringify(example, null, 2));
    console.log(`\n⚠  operators.json créé avec un exemple. Editez-le puis relancez.`);
    console.log(`   Chemin : ${path.resolve(OPS_FILE)}`);
    process.exit(0);
  }

  const operators = JSON.parse(fs.readFileSync(OPS_FILE, 'utf8'));
  console.log(`\n${operators.length} opérateur(s) configuré(s) : ${operators.map(o => o.id).join(', ')}`);

  // Ingestion opérateur par opérateur
  console.log('\n── Ingestion ─────────────────────────────────────────');
  const results = [];
  for (const op of operators) {
    const r = await ingestOperator(op);
    results.push(r);
  }

  // Fusion
  console.log('\n── Fusion ────────────────────────────────────────────');
  const merged = mergeResults(results.filter(Boolean));

  // Index de correspondances inter-opérateurs
  const transferIndex = buildTransferIndex(merged.stopsDict);

  // Écriture
  console.log('\n── Écriture ──────────────────────────────────────────');
  const writeJSON = (filename, data) => {
    const p = path.join(OUT_DIR, filename);
    fs.writeFileSync(p, JSON.stringify(data));
    const size = (fs.statSync(p).size / 1024 / 1024).toFixed(2);
    console.log(`  ✓ ${filename.padEnd(26)} ${size} MB`);
  };

  writeJSON('connections.json',    merged.connections);
  writeJSON('stops.json',          merged.stopsDict);
  writeJSON('routes_info.json',    merged.routeInfo);
  writeJSON('routes_by_stop.json', merged.routesByStop);
  writeJSON('route_stops.json',    merged.routeStops);
  writeJSON('route_trips.json',    merged.routeTrips);
  writeJSON('calendar_index.json', merged.calendarIndex);
  writeJSON('transfer_index.json', transferIndex);

  const sortedDates = Object.keys(merged.calendarIndex).sort();
  const meta = {
    generated_at:      new Date().toISOString(),
    operators:         operators.map(o => o.id),
    total_connections: merged.connections.length,
    total_stops:       Object.keys(merged.stopsDict).length,
    total_routes:      Object.keys(merged.routeInfo).length,
    total_trips:       Object.values(merged.routeTrips).reduce((s, t) => s + t.length, 0),
    total_transfers:   Object.keys(transferIndex).length,
    date_range: {
      first: sortedDates[0] || null,
      last:  sortedDates[sortedDates.length-1] || null,
      count: sortedDates.length,
    },
  };
  writeJSON('meta.json', meta);

  console.log('\n══ Résumé ════════════════════════════════════════════');
  console.log(`  Opérateurs         : ${meta.operators.join(', ')}`);
  console.log(`  Connexions totales : ${meta.total_connections.toLocaleString()}`);
  console.log(`  Arrêts             : ${meta.total_stops.toLocaleString()}`);
  console.log(`  Dates              : ${meta.date_range.first} → ${meta.date_range.last}`);
  console.log(`  Correspondances    : ${meta.total_transfers.toLocaleString()} arrêts inter-op`);
  console.timeEnd('Total');
}

main().catch(err => { console.error('Erreur :', err); process.exit(1); });