/**
 * GTFS Multi-Opérateurs → RAPTOR
 *
 * Usage :
 *   node gtfs-ingest.js                            ← lit operators.json
 *   node gtfs-ingest.js ./operators.json ./engine_data
 *
 * Filtres appliqués par opérateur (trains longue distance uniquement) :
 *   SNCF    : exclut CAR, NAVETTE, TRAMTRAIN et route_type 3 (bus)
 *   SNCB    : garde uniquement IC, EC, NJ, OTC
 *   TI      : tout (déjà uniquement Frecciarossa)
 *   ES      : tout (Eurostar)
 *   RENFE   : exclut PROXIMDAD, FEVE et bus (TRENCELTA gardé — liaison ES↔PT)
 *   OUIGO_ES: tout sauf bus
 *   CP      : garde uniquement AP (Alfa Pendular), IC (Intercidades), IR (Inter-regional)
 */

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

const OPS_FILE = process.argv[2] || './operators.json';
const OUT_DIR  = process.argv[3] || './engine_data';

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

// ─── Utilitaires ──────────────────────────────────────────────────────────────

function timeToSeconds(t) {
  if (!t || !t.includes(':')) return null;
  const [h, m, s] = t.trim().split(':').map(Number);
  if (isNaN(h) || isNaN(m)) return null;
  return h * 3600 + m * 60 + (s || 0);
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

// ─── Filtres par opérateur ────────────────────────────────────────────────────

const SNCF_EXCLUDE_SHORT = new Set(['CAR', 'NAVETTE', 'TRAMTRAIN']);
const SNCB_KEEP_SHORT    = new Set(['IC', 'EC', 'NJ', 'OTC']);

function shouldKeepRoute(operatorId, r) {
  const short = (r.route_short_name || '').trim();
  const rtype = parseInt(r.route_type) || 0;

  switch (operatorId) {
    case 'SNCF':
      if (rtype === 3) return false;
      if (SNCF_EXCLUDE_SHORT.has(short)) return false;
      return true;

    case 'SNCB':
      return SNCB_KEEP_SHORT.has(short);

    case 'RENFE': {
      // ✅ Exclure banlieue/commuter Renfe
      const s = (r.route_short_name || '').trim().toUpperCase();
      // TRENCELTA gardé : seule liaison ferroviaire directe Vigo ↔ Porto (ES↔PT)
      const RENFE_EXCLUDE = new Set(['PROXIMDAD', 'FEVE']);
      if (RENFE_EXCLUDE.has(s)) return false;
      return rtype !== 3;
    }

    case 'OUIGO_ES':
      // Tout garder pour OUIGO España
      return rtype !== 3;

    case 'CP': {
      // CP Portugal — garder uniquement longue distance
      // AP = Alfa Pendular (grande vitesse), IC = Intercidades, IR = Inter-regional
      // Exclure : Linha de... (banlieue), R (Regional), U (Urbain)
      const s = (r.route_short_name || '').trim().toUpperCase();
      const CP_KEEP = new Set(['AP', 'IC', 'IR']);
      return CP_KEEP.has(s);
    }

    default:
      // TI, ES, DB : garder tout le ferroviaire
      return rtype !== 3;
  }
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
    index[iso] = [...services].map(s => prefix + ':' + s);
  }
  return index;
}

// ─── Détection du type de train ───────────────────────────────────────────────

function detectTrainType(operatorId, stopId, tripId, routeShort) {
  const tid = (tripId || '').toUpperCase();

  switch (operatorId) {
    case 'SNCF': {
      const m    = (stopId || '').match(/StopPoint:OCE(.+)-\d{8}$/);
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
      if (quai === 'Train TER')                                          return 'TER';
      return 'TRAIN';
    }

    case 'TI':
      return 'FRECCIAROSSA';

    case 'ES':
      return 'EUROSTAR';

    case 'SNCB': {
      const s = (routeShort || '').toUpperCase();
      if (s === 'NJ')  return 'NIGHTJET';
      if (s === 'EC')  return 'EC';
      if (s === 'OTC') return 'THALYS_CORRIDOR';
      if (s === 'IC')  return 'IC_SNCB';
      return 'TRAIN_SNCB';
    }

    case 'DB': {
      if (tid.includes('ICE'))                      return 'ICE';
      if (tid.includes('IC'))                       return 'IC_DB';
      if (tid.includes('EC'))                       return 'EC';
      if (tid.includes('NJ') || tid.includes('NIGHT')) return 'NIGHTJET';
      return 'TRAIN_DB';
    }

    case 'RENFE': {
      // ✅ Les trip_id Renfe sont numériques → on utilise route_short_name
      const rs = (routeShort || '').trim().toUpperCase();
      if (rs === 'AVE INT')   return 'AVE_INT';
      if (rs === 'AVE')       return 'AVE';
      if (rs === 'AVLO')      return 'AVLO';
      if (rs === 'ALVIA')     return 'ALVIA';
      if (rs === 'AVANT EXP') return 'AVANT';
      if (rs === 'AVANT')     return 'AVANT';
      if (rs === 'EUROMED')   return 'EUROMED';
      if (rs === 'INTERCITY') return 'INTERCITY_ES';
      if (rs === 'MD')        return 'MD';
      if (rs === 'REG.EXP.')  return 'REG_EXP';
      if (rs === 'REGIONAL')  return 'REGIONAL_ES';
      if (rs === 'TRENCELTA') return 'REGIONAL_ES';
      if (rs === 'PROXIMDAD') return 'MD';
      return 'RENFE'; // fallback générique
    }

    case 'OUIGO_ES': {
      // OUIGO España — toujours un seul type
      return 'OUIGO_ES';
    }

    case 'CP': {
      // CP Portugal — Alfa Pendular, Intercidades, Inter-regional
      const rs = (routeShort || '').trim().toUpperCase();
      if (rs === 'AP') return 'ALFA_PENDULAR';
      if (rs === 'IC') return 'IC_CP';
      if (rs === 'IR') return 'IR_CP';
      return 'CP';
    }

    default:
      return 'TRAIN';
  }
}

// ─── Haversine ────────────────────────────────────────────────────────────────

function haversine(lat1, lon1, lat2, lon2) {
  const R    = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a    = Math.sin(dLat/2)**2 +
               Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// ─── Index de transfert inter-opérateurs ─────────────────────────────────────

function buildTransferIndex(stopsDict) {
  console.log('\n🔗 Construction de l\'index de transfert...');
  const transferIndex = {};
  const ids = Object.keys(stopsDict);

  // 0. Liens parent_station → tous les quais enfants sont frères entre eux.
  //    Plus fiable et O(n) vs la proximité GPS O(n²).
  //    Couvre Bruxelles-Midi (8814001 / 8814001_9 / 8814001_10 …), Bruges, etc.
  {
    const parentToChildren = new Map(); // parentId → [childId, ...]
    for (const id of ids) {
      const parent = stopsDict[id].parent_station;
      if (!parent) continue;
      if (!parentToChildren.has(parent)) parentToChildren.set(parent, []);
      parentToChildren.get(parent).push(id);
    }
    let parentLinks = 0;
    for (const [, children] of parentToChildren) {
      for (let ci = 0; ci < children.length; ci++) {
        for (let cj = ci + 1; cj < children.length; cj++) {
          const a = children[ci], b = children[cj];
          if (!transferIndex[a]) transferIndex[a] = [];
          if (!transferIndex[b]) transferIndex[b] = [];
          if (!transferIndex[a].includes(b)) { transferIndex[a].push(b); parentLinks++; }
          if (!transferIndex[b].includes(a)) { transferIndex[b].push(a); parentLinks++; }
        }
      }
    }
    console.log(`  parent_station  : ${parentLinks} liens quai<->quai (${parentToChildren.size} gares)`);
  }

  // 1. Proximité GPS < 300m
  for (let i = 0; i < ids.length; i++) {
    const s1 = stopsDict[ids[i]];
    for (let j = i + 1; j < ids.length; j++) {
      const s2   = stopsDict[ids[j]];
      const dist = haversine(s1.lat, s1.lon, s2.lat, s2.lon);
      if (dist < 300) {
        if (!transferIndex[ids[i]]) transferIndex[ids[i]] = [];
        if (!transferIndex[ids[j]]) transferIndex[ids[j]] = [];
        if (!transferIndex[ids[i]].includes(ids[j])) transferIndex[ids[i]].push(ids[j]);
        if (!transferIndex[ids[j]].includes(ids[i])) transferIndex[ids[j]].push(ids[i]);
      }
    }
  }

  // 2. Liaisons manuelles depuis stations.json
  const stationsPath = path.join(__dirname, 'stations.json');
  if (fs.existsSync(stationsPath)) {
    console.log('  📖 Enrichissement via stations.json...');
    const stations = JSON.parse(fs.readFileSync(stationsPath, 'utf8'));
    let manualLinks = 0;
    for (const station of stations) {
      if (!station.stopIds || station.stopIds.length < 2) continue;
      for (const idA of station.stopIds) {
        if (!stopsDict[idA]) continue;
        if (!transferIndex[idA]) transferIndex[idA] = [];
        for (const idB of station.stopIds) {
          if (idA !== idB && stopsDict[idB] && !transferIndex[idA].includes(idB)) {
            transferIndex[idA].push(idB);
            manualLinks++;
          }
        }
      }
    }
    console.log(`  ✅ ${manualLinks} liaisons inter-opérateurs depuis stations.json`);
  }

  // 3. Correspondances inter-gares dans la même ville (via stations.json + champ city)
  //    Ex : Paris Montparnasse ↔ Paris Gare du Nord — même ville, gares différentes
  //    Ces liens sont marqués { id, interCity: true } pour que server.js applique
  //    un temps de correspondance spécifique (MIN_TRANSFER_CITY, ex : 45 min).
  if (fs.existsSync(stationsPath)) {
    const stations = JSON.parse(fs.readFileSync(stationsPath, 'utf8'));

    // Regrouper les stations par ville + pays
    const cityGroups = new Map();
    for (const station of stations) {
      const city    = station.city || station.name;
      const country = station.country || 'FR';
      const key     = city.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
                      + ':' + country;
      if (!cityGroups.has(key)) cityGroups.set(key, []);
      cityGroups.get(key).push(station);
    }

    let cityLinks = 0;
    for (const [, group] of cityGroups) {
      if (group.length < 2) continue;

      for (let gi = 0; gi < group.length; gi++) {
        for (let gj = gi + 1; gj < group.length; gj++) {
          for (const idA of (group[gi].stopIds || [])) {
            if (!stopsDict[idA]) continue;
            for (const idB of (group[gj].stopIds || [])) {
              if (!stopsDict[idB]) continue;
              if (!transferIndex[idA]) transferIndex[idA] = [];
              if (!transferIndex[idB]) transferIndex[idB] = [];
              // On stocke un objet { id, interCity:true } pour distinguer ces liens
              // des correspondances quai-à-quai normales
              if (!transferIndex[idA].some(x => (x.id || x) === idB)) {
                transferIndex[idA].push({ id: idB, interCity: true });
                cityLinks++;
              }
              if (!transferIndex[idB].some(x => (x.id || x) === idA)) {
                transferIndex[idB].push({ id: idA, interCity: true });
                cityLinks++;
              }
            }
          }
        }
      }
    }
    console.log(`  🏙  ${cityLinks} liens inter-gares (même ville, gares différentes)`);
  }

  // 4. Ponts ibériques ES ↔ PT : liaisons manuelles Renfe ↔ CP
  //    Couvre le Tren Celta (Vigo ↔ Porto) et le corridor Badajoz (Lisboa ↔ Madrid).
  //    Source : analyse GPS + routes GTFS croisées (voir IBERIAN_BRIDGES dans build-stations-index.js)
  {
    const IBERIAN_BRIDGES = [
      // Tren Celta — gares physiquement identiques (GPS < 5m)
      { a: 'RENFE:22402',   b: 'CP:94_7005',   interCity: false }, // Valença
      { a: 'RENFE:94033',   b: 'CP:94_18002',  interCity: false }, // Viana do Castelo
      { a: 'RENFE:96122',   b: 'CP:94_6122',   interCity: false }, // Barcelos
      { a: 'RENFE:94021',   b: 'CP:94_6007',   interCity: false }, // Nine
      { a: 'RENFE:94346',   b: 'CP:94_2006',   interCity: false }, // Porto Campanha
      // Corridor Badajoz — correspondance frontalière Elvas ↔ Badajoz (13 km)
      { a: 'CP:94_57497',   b: 'RENFE:37606',  interCity: true  }, // Elvas ↔ Badajoz
    ];

    let iberianLinks = 0;
    for (const bridge of IBERIAN_BRIDGES) {
      const { a, b, interCity } = bridge;
      if (!stopsDict[a] || !stopsDict[b]) continue;
      if (!transferIndex[a]) transferIndex[a] = [];
      if (!transferIndex[b]) transferIndex[b] = [];
      const linkAB = interCity ? { id: b, interCity: true } : b;
      const linkBA = interCity ? { id: a, interCity: true } : a;
      if (!transferIndex[a].some(x => (x.id || x) === b)) { transferIndex[a].push(linkAB); iberianLinks++; }
      if (!transferIndex[b].some(x => (x.id || x) === a)) { transferIndex[b].push(linkBA); iberianLinks++; }
    }
    console.log(`  🇵🇹🇪🇸 ${iberianLinks} ponts ibériques Renfe ↔ CP (Tren Celta + Badajoz)`);
  }

  console.log(`  Total : ${Object.keys(transferIndex).length} arrêts avec correspondances`);
  return transferIndex;
}

// ─── Ingestion d'un opérateur ─────────────────────────────────────────────────

async function ingestOperator(op) {
  const { id: operatorId, name, gtfs_dir } = op;
  const P = (rawId) => operatorId + ':' + rawId;

  console.log(`\n  📂 ${name} (${operatorId}) — ${gtfs_dir}`);

  if (!fs.existsSync(gtfs_dir)) {
    console.warn(`    ❌ Dossier introuvable : ${gtfs_dir}`);
    return null;
  }

  const [stopTimesRaw, tripsRaw, stopsRaw, routesRawAll, calendarRaw, calendarDatesRaw] = await Promise.all([
    readCSV(path.join(gtfs_dir, 'stop_times.txt')),
    readCSV(path.join(gtfs_dir, 'trips.txt')),
    readCSV(path.join(gtfs_dir, 'stops.txt')),
    readCSV(path.join(gtfs_dir, 'routes.txt')),
    readCSV(path.join(gtfs_dir, 'calendar.txt')),
    readCSV(path.join(gtfs_dir, 'calendar_dates.txt')),
  ]);

  console.log(`    stop_times brut : ${stopTimesRaw.length.toLocaleString()}`);
  console.log(`    trips brut      : ${tripsRaw.length.toLocaleString()}`);
  console.log(`    routes brut     : ${routesRawAll.length.toLocaleString()}`);

  // ── Filtre routes : longue distance uniquement ──
  const routesRaw = routesRawAll.filter(r => shouldKeepRoute(operatorId, r));
  console.log(`    routes gardées  : ${routesRaw.length.toLocaleString()} (filtre longue distance)`);

  const keptRouteIds = new Set(routesRaw.map(r => r.route_id));

  // ── Calendrier ──
  const calendarIndex = buildCalendarIndex(calendarRaw, calendarDatesRaw, operatorId);
  console.log(`    dates GTFS      : ${Object.keys(calendarIndex).length}`);

  // ── Routes ──
  const routeInfo    = {};
  const routeTypeMap = {};
  for (const r of routesRaw) {
    routeInfo[P(r.route_id)] = {
      short:    r.route_short_name || '',
      long:     r.route_long_name  || '',
      type:     parseInt(r.route_type) || 0,
      operator: operatorId,
    };
    routeTypeMap[r.route_id] = r.route_short_name || '';
  }

  // ── Trips filtrés ──
  const tripToService  = {};
  const tripToRoute    = {};
  const tripToHeadsign = {};
  for (const t of tripsRaw) {
    if (!keptRouteIds.has(t.route_id)) continue;
    tripToService[t.trip_id]  = P(t.service_id);
    tripToRoute[t.trip_id]    = P(t.route_id);
    tripToHeadsign[t.trip_id] = t.trip_headsign || '';
  }
  const validTripIds = new Set(Object.keys(tripToRoute));
  console.log(`    trips gardés    : ${validTripIds.size.toLocaleString()}`);

  // ── Stops : uniquement ceux utilisés ──
  const usedStopIds = new Set();
  for (const st of stopTimesRaw) {
    if (validTripIds.has(st.trip_id)) usedStopIds.add(st.stop_id);
  }
  const stopsDict = {};
  for (const s of stopsRaw) {
    if (!usedStopIds.has(s.stop_id)) continue;
    stopsDict[P(s.stop_id)] = {
      name:          s.stop_name || s.stop_id,
      lat:           parseFloat(s.stop_lat)  || 0,
      lon:           parseFloat(s.stop_lon)  || 0,
      operator:      operatorId,
      // Conserver le parent_station (avec préfixe opérateur) pour lier les quais entre eux
      parent_station: s.parent_station ? P(s.parent_station) : null,
    };
  }
  console.log(`    stops gardés    : ${Object.keys(stopsDict).length.toLocaleString()}`);

  // ── Stop times → tripStops ──
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

  // ── FIX : correction des trips circulaires (TI) ──
  for (const [trip_id, stops] of Object.entries(tripStops)) {
    stops.sort((a, b) => a.seq - b.seq);

    const segments = [];
    let segStart = 0;
    for (let i = 1; i < stops.length; i++) {
      const prevTime = stops[i-1].dep_time ?? stops[i-1].arr_time ?? -1;
      const currTime = stops[i].arr_time   ?? stops[i].dep_time   ?? prevTime + 1;
      if (prevTime >= 0 && currTime < prevTime - 600) {
        segments.push({ stops: stops.slice(segStart, i) });
        segStart = i;
      }
    }
    segments.push({ stops: stops.slice(segStart) });

    if (segments.length > 1) {
      segments.sort((a, b) => {
        const ta = a.stops[0].dep_time ?? a.stops[0].arr_time ?? 0;
        const tb = b.stops[0].dep_time ?? b.stops[0].arr_time ?? 0;
        return ta - tb;
      });
      const mergedSegs = [segments[0].stops];
      for (let k = 1; k < segments.length; k++) {
        const lastStop  = mergedSegs[mergedSegs.length-1].slice(-1)[0];
        const firstStop = segments[k].stops[0];
        const lastTime  = lastStop.arr_time  ?? lastStop.dep_time  ?? -1;
        const firstTime = firstStop.dep_time ?? firstStop.arr_time ?? lastTime + 1;
        if (firstTime >= lastTime - 600) {
          mergedSegs[mergedSegs.length-1] = mergedSegs[mergedSegs.length-1].concat(segments[k].stops);
        } else {
          mergedSegs.push(segments[k].stops);
        }
      }
      mergedSegs.sort((a, b) => b.length - a.length);
      mergedSegs[0].sort((a, b) => (a.dep_time ?? a.arr_time ?? 0) - (b.dep_time ?? b.arr_time ?? 0));
      tripStops[trip_id] = mergedSegs[0];
    } else {
      stops.sort((a, b) => (a.dep_time ?? a.arr_time ?? 0) - (b.dep_time ?? b.arr_time ?? 0));
    }
  }

  // ── RAPTOR structures ──
  const routesByStop = {};
  const routeStops   = {};
  const routeTrips   = {};

  for (const [trip_id, stops] of Object.entries(tripStops)) {
    const route_id   = tripToRoute[trip_id]   || P('unknown');
    const service_id = tripToService[trip_id] || '';
    const rawRouteId = route_id.replace(operatorId + ':', '');
    const routeShort = routeTypeMap[rawRouteId] || '';

    if (!routeStops[route_id] || stops.length > routeStops[route_id].length) {
      routeStops[route_id] = stops.map(s => s.stop_id);
    }
    if (!routeTrips[route_id]) routeTrips[route_id] = [];

    const trainType = detectTrainType(operatorId, stops[0]?.stop_id || '', trip_id, routeShort);
    const firstDep  = stops.find(s => s.dep_time !== null)?.dep_time ?? Infinity;

    routeTrips[route_id].push({
      trip_id:        P(trip_id),
      service_id,
      dep_time_first: firstDep,
      train_type:     trainType,
      operator:       operatorId,
      stop_times:     stops,
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

  const totalTrips = Object.values(routeTrips).reduce((s, t) => s + t.length, 0);
  console.log(`    trips RAPTOR    : ${totalTrips.toLocaleString()}`);
  console.log(`    routes RAPTOR   : ${Object.keys(routeInfo).length.toLocaleString()}`);

  return { stopsDict, routeInfo, routesByStopSerial, routeStops, routeTrips, calendarIndex };
}

// ─── Fusion multi-opérateurs ──────────────────────────────────────────────────

function mergeResults(results) {
  const merged = {
    stopsDict:     {},
    routeInfo:     {},
    routesByStop:  {},
    routeStops:    {},
    routeTrips:    {},
    calendarIndex: {},
  };

  for (const r of results) {
    if (!r) continue;
    Object.assign(merged.stopsDict,  r.stopsDict);
    Object.assign(merged.routeInfo,  r.routeInfo);
    Object.assign(merged.routeStops, r.routeStops);
    Object.assign(merged.routeTrips, r.routeTrips);

    for (const [stop, routes] of Object.entries(r.routesByStopSerial)) {
      if (!merged.routesByStop[stop]) merged.routesByStop[stop] = new Set();
      for (const rid of routes) merged.routesByStop[stop].add(rid);
    }

    for (const [date, services] of Object.entries(r.calendarIndex)) {
      if (!merged.calendarIndex[date]) {
        merged.calendarIndex[date] = services.slice();
      } else {
        for (const s of services) merged.calendarIndex[date].push(s);
      }
    }
  }

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

  if (!fs.existsSync(OPS_FILE)) {
    const example = [
      { "id": "SNCF",     "name": "SNCF",             "gtfs_dir": "./gtfs/sncf" },
      { "id": "TI",       "name": "Trenitalia France", "gtfs_dir": "./gtfs/trenitalia" },
      { "id": "ES",       "name": "Eurostar",          "gtfs_dir": "./gtfs/eurostar" },
      { "id": "SNCB",     "name": "SNCB Belgique",     "gtfs_dir": "./gtfs/sncb" },
      { "id": "RENFE",    "name": "Renfe Espagne",      "gtfs_dir": "./gtfs/renfe" },
      { "id": "OUIGO_ES", "name": "OUIGO España",       "gtfs_dir": "./gtfs/ouigo_es" },
      { "id": "CP",       "name": "CP Portugal",        "gtfs_dir": "./gtfs/cp" },
    ];
    fs.writeFileSync(OPS_FILE, JSON.stringify(example, null, 2));
    console.log(`\n⚠  operators.json créé. Editez-le puis relancez.`);
    process.exit(0);
  }

  const operators = JSON.parse(fs.readFileSync(OPS_FILE, 'utf8'));
  console.log(`\n${operators.length} opérateur(s) : ${operators.map(o => o.id).join(', ')}`);

  console.log('\n── Ingestion ─────────────────────────────────────────');
  const results = [];
  for (const op of operators) {
    const r = await ingestOperator(op);
    results.push(r);
  }

  console.log('\n── Fusion ────────────────────────────────────────────');
  const merged = mergeResults(results.filter(Boolean));

  console.log('\n── Transferts ────────────────────────────────────────');
  const transferIndex = buildTransferIndex(merged.stopsDict);

  console.log('\n── Écriture ──────────────────────────────────────────');
  const writeJSON = (filename, data) => {
    const p = path.join(OUT_DIR, filename);
    fs.writeFileSync(p, JSON.stringify(data));
    const size = (fs.statSync(p).size / 1024 / 1024).toFixed(2);
    console.log(`  ✓ ${filename.padEnd(26)} ${size} MB`);
  };

  writeJSON('stops.json',          merged.stopsDict);
  writeJSON('routes_info.json',    merged.routeInfo);
  writeJSON('routes_by_stop.json', merged.routesByStop);
  writeJSON('route_stops.json',    merged.routeStops);
  writeJSON('route_trips.json',    merged.routeTrips);
  writeJSON('calendar_index.json', merged.calendarIndex);
  writeJSON('transfer_index.json', transferIndex);

  const sortedDates = Object.keys(merged.calendarIndex).sort();
  const meta = {
    generated_at:    new Date().toISOString(),
    operators:       operators.map(o => o.id),
    total_stops:     Object.keys(merged.stopsDict).length,
    total_routes:    Object.keys(merged.routeInfo).length,
    total_trips:     Object.values(merged.routeTrips).reduce((s, t) => s + t.length, 0),
    total_transfers: Object.keys(transferIndex).length,
    date_range: {
      first: sortedDates[0] || null,
      last:  sortedDates[sortedDates.length-1] || null,
      count: sortedDates.length,
    },
  };
  writeJSON('meta.json', meta);

  console.log('\n══ Résumé ════════════════════════════════════════════');
  console.log(`  Opérateurs    : ${meta.operators.join(', ')}`);
  console.log(`  Arrêts        : ${meta.total_stops.toLocaleString()}`);
  console.log(`  Routes        : ${meta.total_routes.toLocaleString()}`);
  console.log(`  Trips         : ${meta.total_trips.toLocaleString()}`);
  console.log(`  Transferts    : ${meta.total_transfers.toLocaleString()} arrêts`);
  console.log(`  Dates         : ${meta.date_range.first} → ${meta.date_range.last}`);
  console.timeEnd('Total');
}

main().catch(err => { console.error('Erreur :', err); process.exit(1); });