/**
 * filter_avanti.js
 * Extrait uniquement les données Avanti West Coast (agence VT)
 * depuis le GTFS complet UK Rail → dossier Avanti_Only/
 *
 * Équivalent JS de filter_avanti.py
 */

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

const SOURCE_DIR      = './gtfs/UK_Rail';
const TARGET_DIR      = './gtfs/Avanti_Only';
const TARGET_AGENCY   = 'VT';

if (!fs.existsSync(TARGET_DIR)) fs.mkdirSync(TARGET_DIR, { recursive: true });

// ── Lecture CSV streaming ──────────────────────────────────────────────────────

function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (const c of line) {
    if (c === '"')           { inQ = !inQ; }
    else if (c === ',' && !inQ) { result.push(cur); cur = ''; }
    else                        { cur += c; }
  }
  result.push(cur);
  return result;
}

/**
 * Lit un fichier CSV ligne par ligne.
 * Appelle onRow(row, writer) pour chaque ligne de données.
 * Retourne une Promise<{ headers, count }>.
 */
function processCSV(srcFile, dstFile, onRow) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(srcFile)) {
      console.warn(`  ⚠  Ignoré (absent) : ${path.basename(srcFile)}`);
      return resolve({ headers: [], count: 0 });
    }

    const input  = fs.createReadStream(srcFile, { encoding: 'utf8' });
    const output = fs.createWriteStream(dstFile, { encoding: 'utf8' });
    const rl     = readline.createInterface({ input, crlfDelay: Infinity });

    let headers = null;
    let count   = 0;
    let first   = true;

    rl.on('line', (raw) => {
      const line = raw.replace(/^\uFEFF/, '').trim();
      if (!line) return;

      if (!headers) {
        headers = parseCSVLine(line);
        output.write(line + '\n');   // écrire l'entête tel quel
        return;
      }

      const cols = parseCSVLine(line);
      const row  = {};
      headers.forEach((h, i) => { row[h] = (cols[i] ?? '').trim(); });

      if (onRow(row)) {
        output.write(line + '\n');
        count++;
      }
    });

    rl.on('close', () => { output.end(() => resolve({ headers, count })); });
    rl.on('error', reject);
    output.on('error', reject);
  });
}

// ── Pipeline de filtrage ──────────────────────────────────────────────────────

async function filterAvanti() {
  console.log(`\n⚙️  Filtrage Avanti (agence ${TARGET_AGENCY})...`);
  console.log(`   Source  : ${SOURCE_DIR}`);
  console.log(`   Cible   : ${TARGET_DIR}\n`);

  // 1. agency.txt — garder uniquement VT
  const { count: agencyCount } = await processCSV(
    `${SOURCE_DIR}/agency.txt`,
    `${TARGET_DIR}/agency.txt`,
    (row) => row.agency_id === TARGET_AGENCY
  );
  console.log(`  agency.txt       : ${agencyCount} agence(s) gardée(s)`);

  // 2. routes.txt — garder routes VT, mémoriser route_ids
  const routeIds = new Set();
  const { count: routeCount } = await processCSV(
    `${SOURCE_DIR}/routes.txt`,
    `${TARGET_DIR}/routes.txt`,
    (row) => {
      if (row.agency_id !== TARGET_AGENCY) return false;
      routeIds.add(row.route_id);
      return true;
    }
  );
  console.log(`  routes.txt       : ${routeCount} route(s)`);

  // 3. trips.txt — garder trips des routes VT, mémoriser trip_ids + service_ids
  const tripIds    = new Set();
  const serviceIds = new Set();
  const { count: tripCount } = await processCSV(
    `${SOURCE_DIR}/trips.txt`,
    `${TARGET_DIR}/trips.txt`,
    (row) => {
      if (!routeIds.has(row.route_id)) return false;
      tripIds.add(row.trip_id);
      serviceIds.add(row.service_id);
      return true;
    }
  );
  console.log(`  trips.txt        : ${tripCount} trip(s)`);

  // 4. stop_times.txt — fichier le plus lourd, mémoriser stop_ids utilisés
  const stopIds = new Set();
  const { count: stCount } = await processCSV(
    `${SOURCE_DIR}/stop_times.txt`,
    `${TARGET_DIR}/stop_times.txt`,
    (row) => {
      if (!tripIds.has(row.trip_id)) return false;
      stopIds.add(row.stop_id);
      return true;
    }
  );
  console.log(`  stop_times.txt   : ${stCount} ligne(s) (peut être long...)`);

  // 5. stops.txt — garder les arrêts utilisés + les stations parentes
  const { count: stopCount } = await processCSV(
    `${SOURCE_DIR}/stops.txt`,
    `${TARGET_DIR}/stops.txt`,
    (row) => stopIds.has(row.stop_id) || row.location_type === '1'
  );
  console.log(`  stops.txt        : ${stopCount} arrêt(s)`);

  // 6. calendar.txt (optionnel)
  if (fs.existsSync(`${SOURCE_DIR}/calendar.txt`)) {
    const { count: calCount } = await processCSV(
      `${SOURCE_DIR}/calendar.txt`,
      `${TARGET_DIR}/calendar.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar.txt     : ${calCount} service(s)`);
  }

  // 7. calendar_dates.txt (optionnel)
  if (fs.existsSync(`${SOURCE_DIR}/calendar_dates.txt`)) {
    const { count: cdCount } = await processCSV(
      `${SOURCE_DIR}/calendar_dates.txt`,
      `${TARGET_DIR}/calendar_dates.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar_dates   : ${cdCount} exception(s)`);
  }

  // 8. feed_info.txt — copie brute si présent
  if (fs.existsSync(`${SOURCE_DIR}/feed_info.txt`)) {
    fs.copyFileSync(`${SOURCE_DIR}/feed_info.txt`, `${TARGET_DIR}/feed_info.txt`);
    console.log(`  feed_info.txt    : copié`);
  }

  console.log(`\n✅ Avanti filtré → ${TARGET_DIR}`);
  console.log(`   ${routeIds.size} routes · ${tripIds.size} trips · ${stopIds.size} arrêts\n`);
}

filterAvanti().catch(err => { console.error('❌ Erreur filtrage Avanti :', err); process.exit(1); });