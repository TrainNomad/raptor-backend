/**
 * filter_uk.js
 * Extrait les opérateurs longue distance UK depuis le GTFS complet UK Rail.
 * Remplace filter_avanti.js (qui ne gardait que VT = Avanti).
 *
 * Agences conservées (national & intercités) :
 *
 *   — Intercités grande distance —
 *   VT  Avanti West Coast     Euston → Glasgow, Manchester, Birmingham, Liverpool
 *   GR  LNER                  Kings Cross → Edinburgh, Newcastle, Leeds, York
 *   CS  Caledonian Sleeper    Euston → Inverness, Aberdeen, Fort William (nuit)
 *   XC  CrossCountry          Birmingham → Edinburgh/Glasgow/Bristol/Plymouth
 *   TP  TransPennine Express  Manchester/Liverpool → Edinburgh, Glasgow, Newcastle
 *   EM  East Midlands Railway St Pancras → Nottingham, Sheffield, Leeds, Derby
 *   GW  Great Western Railway Paddington → Bristol, Cardiff, Plymouth, Cornwall
 *   SW  South Western Railway Waterloo → Southampton, Bournemouth, Exeter
 *   HT  Hull Trains           Kings Cross → Hull (open access)
 *   GC  Grand Central         Kings Cross → Sunderland, Bradford (open access)
 *   LD  Lumo                  Kings Cross → Edinburgh (low-cost open access)
 *
 *   — Réseaux nationaux régionaux —
 *   SR  ScotRail              Réseau national écossais (Glasgow, Edinburgh, Inverness…)
 *   NT  Northern Trains       Nord Angleterre (Manchester, Leeds, Newcastle, Liverpool…)
 *   AW  Transport for Wales   Réseau national gallois (Cardiff, Swansea, Holyhead…)
 *
 *   — Exclus (urbain/banlieue) —
 *   LT London Underground · XR Elizabeth line · LO London Overground (métro londonien)
 *   TL Thameslink · GN Great Northern · CC c2c · SN Southern · SE Southeastern (banlieue)
 *   ME Merseyrail (métro Liverpool) · HX Heathrow Express · GX Gatwick Express · IL Island Line
 *
 * Source : GTFS UK Rail complet (transit.land f-uk~rail)
 * Sortie  : ./gtfs/UK_Intercity/
 */

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

const SOURCE_DIR = './gtfs/UK_Rail';
const TARGET_DIR = './gtfs/UK_National';

// Agences nationales conservées (intercités + réseaux nationaux régionaux)
const TARGET_AGENCIES = new Set([
  // ── Intercités grande distance ──────────────────────────────────────────
  'VT',  // Avanti West Coast     — Euston → Glasgow, Manchester, Birmingham, Liverpool
  'GR',  // LNER                  — Kings Cross → Edinburgh, Newcastle, Leeds, York
  'CS',  // Caledonian Sleeper    — Euston → Inverness, Aberdeen, Fort William (nuit)
  'XC',  // CrossCountry          — Birmingham → Edinburgh/Glasgow/Bristol/Plymouth
  'TP',  // TransPennine Express  — Manchester/Liverpool → Edinburgh, Glasgow, Newcastle
  'EM',  // East Midlands Railway — St Pancras → Nottingham, Sheffield, Leeds, Derby
  'GW',  // Great Western Railway — Paddington → Bristol, Cardiff, Plymouth, Cornwall
  'SW',  // South Western Railway — Waterloo → Southampton, Bournemouth, Exeter
  'HT',  // Hull Trains           — Kings Cross → Hull (open access)
  'GC',  // Grand Central         — Kings Cross → Sunderland, Bradford (open access)
  'LD',  // Lumo                  — Kings Cross → Edinburgh (low-cost open access)
  // ── Réseaux nationaux régionaux ─────────────────────────────────────────
  'SR',  // ScotRail              — Réseau national écossais (Glasgow, Edinburgh, Inverness, Aberdeen…)
  'NT',  // Northern Trains       — Nord Angleterre (Manchester, Leeds, Newcastle, Liverpool, Hull…)
  'AW',  // Transport for Wales   — Réseau national gallois (Cardiff, Swansea, Holyhead…)
]);

if (!fs.existsSync(TARGET_DIR)) fs.mkdirSync(TARGET_DIR, { recursive: true });

// ── Lecture CSV streaming ──────────────────────────────────────────────────────
function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (const c of line) {
    if (c === '"')              { inQ = !inQ; }
    else if (c === ',' && !inQ) { result.push(cur); cur = ''; }
    else                        { cur += c; }
  }
  result.push(cur);
  return result;
}

function processCSV(srcFile, dstFile, onRow) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(srcFile)) {
      console.warn(`  ⚠  Absent : ${path.basename(srcFile)}`);
      return resolve({ count: 0 });
    }
    const input  = fs.createReadStream(srcFile, { encoding: 'utf8' });
    const output = fs.createWriteStream(dstFile, { encoding: 'utf8' });
    const rl     = readline.createInterface({ input, crlfDelay: Infinity });
    let headers  = null;
    let count    = 0;
    rl.on('line', (raw) => {
      const line = raw.replace(/^\uFEFF/, '').trim();
      if (!line) return;
      if (!headers) { headers = parseCSVLine(line); output.write(line + '\n'); return; }
      const cols = parseCSVLine(line);
      const row  = {};
      headers.forEach((h, i) => { row[h] = (cols[i] ?? '').trim(); });
      if (onRow(row)) { output.write(line + '\n'); count++; }
    });
    rl.on('close', () => { output.end(() => resolve({ count })); });
    rl.on('error', reject);
    output.on('error', reject);
  });
}

// ── Pipeline ──────────────────────────────────────────────────────────────────
async function filterUK() {
  console.log(`\n⚙️  Filtrage UK national & intercités...`);
  console.log(`   Agences : ${[...TARGET_AGENCIES].join(', ')}`);
  console.log(`   Source  : ${SOURCE_DIR}`);
  console.log(`   Cible   : ${TARGET_DIR}\n`);

  // 1. agency.txt
  const { count: agencyCount } = await processCSV(
    `${SOURCE_DIR}/agency.txt`, `${TARGET_DIR}/agency.txt`,
    (row) => TARGET_AGENCIES.has(row.agency_id)
  );
  console.log(`  agency.txt       : ${agencyCount} agence(s)`);

  // 2. routes.txt → mémoriser route_ids
  const routeIds = new Set();
  const { count: routeCount } = await processCSV(
    `${SOURCE_DIR}/routes.txt`, `${TARGET_DIR}/routes.txt`,
    (row) => {
      if (!TARGET_AGENCIES.has(row.agency_id)) return false;
      routeIds.add(row.route_id);
      return true;
    }
  );
  console.log(`  routes.txt       : ${routeCount} route(s)`);

  // 3. trips.txt → mémoriser trip_ids + service_ids
  const tripIds    = new Set();
  const serviceIds = new Set();
  const { count: tripCount } = await processCSV(
    `${SOURCE_DIR}/trips.txt`, `${TARGET_DIR}/trips.txt`,
    (row) => {
      if (!routeIds.has(row.route_id)) return false;
      tripIds.add(row.trip_id);
      serviceIds.add(row.service_id);
      return true;
    }
  );
  console.log(`  trips.txt        : ${tripCount} trip(s)`);

  // 4. stop_times.txt → mémoriser stop_ids utilisés
  const stopIds = new Set();
  const { count: stCount } = await processCSV(
    `${SOURCE_DIR}/stop_times.txt`, `${TARGET_DIR}/stop_times.txt`,
    (row) => {
      if (!tripIds.has(row.trip_id)) return false;
      stopIds.add(row.stop_id);
      return true;
    }
  );
  console.log(`  stop_times.txt   : ${stCount} ligne(s)`);

  // 5. stops.txt — arrêts utilisés + stations parentes
  const { count: stopCount } = await processCSV(
    `${SOURCE_DIR}/stops.txt`, `${TARGET_DIR}/stops.txt`,
    (row) => stopIds.has(row.stop_id) || row.location_type === '1'
  );
  console.log(`  stops.txt        : ${stopCount} arrêt(s)`);

  // 6. calendar.txt
  if (fs.existsSync(`${SOURCE_DIR}/calendar.txt`)) {
    const { count: calCount } = await processCSV(
      `${SOURCE_DIR}/calendar.txt`, `${TARGET_DIR}/calendar.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar.txt     : ${calCount} service(s)`);
  }

  // 7. calendar_dates.txt
  if (fs.existsSync(`${SOURCE_DIR}/calendar_dates.txt`)) {
    const { count: cdCount } = await processCSV(
      `${SOURCE_DIR}/calendar_dates.txt`, `${TARGET_DIR}/calendar_dates.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar_dates   : ${cdCount} exception(s)`);
  }

  // 8. feed_info.txt
  if (fs.existsSync(`${SOURCE_DIR}/feed_info.txt`)) {
    fs.copyFileSync(`${SOURCE_DIR}/feed_info.txt`, `${TARGET_DIR}/feed_info.txt`);
    console.log(`  feed_info.txt    : copié`);
  }

  console.log(`\n✅ UK national filtré → ${TARGET_DIR}`);
  console.log(`   ${routeIds.size} routes · ${tripIds.size} trips · ${stopIds.size} arrêts\n`);
}

filterUK().catch(err => {
  console.error('❌ Erreur filtrage UK :', err);
  process.exit(1);
});