/**
 * filter_netherlands.js
 * Filtre le feed GTFS Pays-Bas (gtfs-openov-nl) :
 *   — Conserve uniquement les trains (inter-city, intercités, internationaux, régionaux)
 *   — Exclut bus, tram, métro, ferry, banlieue
 *   — Dédoublonne les agences : IFF:* est la source canonique pour le ferroviaire
 *     (ARR, BRAVO:ARR, BRAVO:CXX, RRREIS, etc. sont des doublons bus/multi-modes)
 *
 * ─── Agences ferroviaires conservées ─────────────────────────────────────────
 *   IFF:NS          NS (Nederlandse Spoorwegen) — réseau national intercity
 *   IFF:NS_INT      NS International            — Thalys, ICE, Eurostar côté NL
 *   IFF:ARRIVA      Arriva NL                   — trains régionaux (Friesland, Groningen…)
 *   IFF:BLAUWNET_A  Blauwnet (Arriva)           — trains régionaux (Overijssel)
 *   IFF:BLAUWNET_K  Blauwnet (Keolis)           — trains régionaux (Overijssel)
 *   IFF:EUROBAHN    Eurobahn (DE↔NL)            — liaisons transfrontalières
 *   IFF:VIAS        VIAS (DE↔NL)                — liaison Frankfurt–Amsterdam
 *   IFF:GV          GoVolta                     — trains régionaux (Valleilijn)
 *   IFF:R-NET_NS    R-net NS                    — lignes R-net opérées par NS
 *
 * ─── Agences exclues (doublons ou non-ferroviaires) ──────────────────────────
 *   ARR            Arriva générique (bus + trains — doublonné par IFF:ARRIVA)
 *   BRAVO          Bravo générique
 *   BRAVO:ARR      Bravo (Arriva) — bus
 *   BRAVO:CXX      Bravo (Connexxion) — bus
 *   BRENG          Breng — bus
 *   CXX            Connexxion — bus
 *   DELIJN         De Lijn — bus BE
 *   DOEKSEN        Rederij Doeksen — ferry
 *   EBS            EBS — bus
 *   GVB            GVB — métro/tram Amsterdam
 *   HERMES         Hermes — bus
 *   HTM            HTM — tram La Haye
 *   MEERPLUS       MeerPlus — bus
 *   QBUZZ          Qbuzz — bus
 *   QBUZZ:*        WaterShuttle — ferry
 *   RET            RET — métro/tram Rotterdam
 *   RRREIS         RRReis générique — bus (doublonné par IFF:RRREIS_*)
 *   IFF:RRREIS_A   RRReis Arriva — bus régional
 *   IFF:RRREIS_K   RRReis Keolis — bus régional
 *   IFF:R-NET_QB   R-net Qbuzz — bus
 *   TESO           TESO — ferry (Texel)
 *   TRANSDEV       Transdev — bus
 *   UOV            U-OV — bus Utrecht
 *   WATERBUS       Waterbus — ferry
 *   WATERSHUTTLE   WaterShuttle — ferry
 *   ALLGO          allGo — bus
 *   WPD            Wagenborg Passagiersdiensten — ferry îles
 *   WSF            Westerschelde Ferry — ferry
 *   ZTM:*          ZTM — bus
 *
 * ─── Types de routes conservés ───────────────────────────────────────────────
 *   2    Rail (GTFS standard)         fallback générique ferroviaire
 *   100  High-Speed Rail              Thalys, ICE côté NL
 *   101  Long Distance Rail           IC, Intercity-Direct
 *   102  Inter Regional Rail          semi-rapide transfrontalier
 *   103  Regional Rail                trains régionaux (Arriva, Blauwnet…)
 *   106  Regional Rail                variante régionale
 *
 * ─── Types EXCLUS ─────────────────────────────────────────────────────────────
 *   3    Bus (standard)
 *   109  Suburban Railway             Sprinter NS (banlieue — déjà dans IFF:NS mais filtré)
 *   200  Coach                        autocars
 *   400  Urban Railway / Metro
 *   401  Metro
 *   700  Bus (étendu)
 *   900  Tram
 *   1000 Water transport / Ferry
 *   1300 Aerial lift / Téléphérique
 *
 * Source  : https://gtfs.ovapi.nl/nl/gtfs-openov-nl.zip  (ovapi.nl / openOV)
 * Sortie  : ./gtfs/nl_rail_filtered/
 */

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

// ─── Configuration ─────────────────────────────────────────────────────────────

const SOURCE_DIR = './gtfs/nl_full';
const TARGET_DIR = './gtfs/nl_rail_filtered';

// Agences ferroviaires IFF:* à conserver (source canonique GTFS NL)
const RAIL_AGENCIES = new Set([
  'IFF:NS',
  'IFF:NS_INT',
  'IFF:ARRIVA',
  'IFF:BLAUWNET_A',
  'IFF:BLAUWNET_K',
  'IFF:EUROBAHN',
  'IFF:VIAS',
  'IFF:GV',
  'IFF:R-NET_NS',
]);

// Types de routes ferroviaires acceptés
const RAIL_ROUTE_TYPES = new Set([2, 100, 101, 102, 103, 106]);

// Types explicitement exclus (bus, tram, métro, ferry, S-Bahn banlieue)
const EXCLUDED_ROUTE_TYPES = new Set([3, 109, 200, 400, 401, 700, 900, 1000, 1300]);

// ─── Utilitaires CSV ──────────────────────────────────────────────────────────

function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (const c of line) {
    if      (c === '"')             { inQ = !inQ; }
    else if (c === ',' && !inQ)     { result.push(cur); cur = ''; }
    else                            { cur += c; }
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

// ─── Pipeline de filtrage ──────────────────────────────────────────────────────

async function filterNL() {
  console.log('\n🇳🇱  Filtrage GTFS Pays-Bas — trains uniquement');
  console.log(`   Agences conservées : ${[...RAIL_AGENCIES].join(', ')}`);
  console.log(`   Source  : ${SOURCE_DIR}`);
  console.log(`   Cible   : ${TARGET_DIR}\n`);

  if (!fs.existsSync(SOURCE_DIR)) {
    console.error(`  ❌  Dossier source absent : ${SOURCE_DIR}`);
    console.error(`  →   Lancer update-gtfs.sh d'abord pour télécharger le feed NL.`);
    process.exit(1);
  }

  fs.mkdirSync(TARGET_DIR, { recursive: true });

  // 1. agency.txt — conserver uniquement les agences ferroviaires IFF:*
  const { count: agencyCount } = await processCSV(
    `${SOURCE_DIR}/agency.txt`,
    `${TARGET_DIR}/agency.txt`,
    (row) => {
      const keep = RAIL_AGENCIES.has(row.agency_id);
      if (!keep && row.agency_id.startsWith('IFF:')) {
        // Agences IFF non listées — logguer pour info (ex: nouveaux opérateurs)
        console.log(`  ~ IFF non-ferroviaire ignoré : ${row.agency_id} (${row.agency_name})`);
      }
      return keep;
    }
  );
  console.log(`  agency.txt       : ${agencyCount} agence(s) ferroviaire(s)`);

  // 2. routes.txt — filtrer par agence ET route_type
  const routeIds = new Set();
  const { count: routeCount } = await processCSV(
    `${SOURCE_DIR}/routes.txt`,
    `${TARGET_DIR}/routes.txt`,
    (row) => {
      if (!RAIL_AGENCIES.has(row.agency_id)) return false;
      const rtype = parseInt(row.route_type, 10);
      // Exclure explicitement les types non-ferroviaires
      if (EXCLUDED_ROUTE_TYPES.has(rtype)) return false;
      // Garder les types ferroviaires connus + fallback type 2 générique
      if (!RAIL_ROUTE_TYPES.has(rtype) && rtype !== 2) {
        console.log(`  ~ route_type inconnu ignoré : ${rtype} (route ${row.route_id})`);
        return false;
      }
      routeIds.add(row.route_id);
      return true;
    }
  );
  console.log(`  routes.txt       : ${routeCount} route(s) ferroviaire(s)`);

  // 3. trips.txt
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

  // 4. stop_times.txt — collecter les stop_ids utilisés
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
  console.log(`  stop_times.txt   : ${stCount} ligne(s)`);

  // 5. stops.txt — arrêts utilisés + stations parentes (location_type=1)
  const { count: stopCount } = await processCSV(
    `${SOURCE_DIR}/stops.txt`,
    `${TARGET_DIR}/stops.txt`,
    (row) => stopIds.has(row.stop_id) || row.location_type === '1'
  );
  console.log(`  stops.txt        : ${stopCount} arrêt(s)`);

  // 6. calendar.txt
  if (fs.existsSync(`${SOURCE_DIR}/calendar.txt`)) {
    const { count: calCount } = await processCSV(
      `${SOURCE_DIR}/calendar.txt`,
      `${TARGET_DIR}/calendar.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar.txt     : ${calCount} service(s)`);
  }

  // 7. calendar_dates.txt
  if (fs.existsSync(`${SOURCE_DIR}/calendar_dates.txt`)) {
    const { count: cdCount } = await processCSV(
      `${SOURCE_DIR}/calendar_dates.txt`,
      `${TARGET_DIR}/calendar_dates.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar_dates   : ${cdCount} exception(s)`);
  }

  // 8. feed_info.txt
  if (fs.existsSync(`${SOURCE_DIR}/feed_info.txt`)) {
    fs.copyFileSync(`${SOURCE_DIR}/feed_info.txt`, `${TARGET_DIR}/feed_info.txt`);
    console.log(`  feed_info.txt    : copié`);
  }

  // 9. transfers.txt (liaisons de correspondance entre quais — utile pour NS)
  if (fs.existsSync(`${SOURCE_DIR}/transfers.txt`)) {
    const { count: trCount } = await processCSV(
      `${SOURCE_DIR}/transfers.txt`,
      `${TARGET_DIR}/transfers.txt`,
      (row) => stopIds.has(row.from_stop_id) || stopIds.has(row.to_stop_id)
    );
    console.log(`  transfers.txt    : ${trCount} correspondance(s)`);
  }

  console.log(`\n✅ NL filtré → ${TARGET_DIR}`);
  console.log(`   ${routeIds.size} routes · ${tripIds.size} trips · ${stopIds.size} arrêts\n`);
}

filterNL().catch(err => {
  console.error('❌ Erreur filtrage NL :', err);
  process.exit(1);
});