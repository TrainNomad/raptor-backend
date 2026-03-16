/**
 * filter_germany.js
 * Filtre les feeds GTFS allemands (gtfs.de / DELFI) :
 *   - DB_FV  (Fernverkehr)   : conserve tout — ICE, IC, EC, NJ, Flixtrain
 *   - DB_RV  (Regionalverkehr) : conserve RE, RB, IRE — exclut S-Bahn urbains
 *
 * ─── Types de routes conservés (spec DELFI / VDV 452 Extended) ────────────────
 *   100  High-Speed Rail        ICE, TGV
 *   101  Long Distance Rail     IC, EC
 *   102  Inter Regional Rail    IRE (semi-rapide)
 *   103  Regional Rail          RE  (RegionalExpress)
 *   105  Tourist Railway        trains touristiques (optionnel, voir KEEP_TOURIST)
 *   106  Regional Rail          RB  (RegionalBahn, omnibus régional)
 *     2  Rail (GTFS standard)   fallback générique
 *
 * ─── Types EXCLUS ─────────────────────────────────────────────────────────────
 *   109  Suburban Railway       S-Bahn urbains (Berlin, München, Hamburg, Frankfurt…)
 *   400  Urban Railway          U-Bahn / métro
 *   401  Metro                  variante métro
 *   700  Bus                    (ne devrait pas apparaître dans ces feeds)
 *   900  Tram                   (idem)
 *
 * ─── Agences S-Bahn urbaines explicitement exclues ───────────────────────────
 *   Filet de sécurité : si une agence S-Bahn utilise un type 2 (rail générique)
 *   au lieu de 109, elle sera quand même exclue via son nom.
 *
 * Source : https://download.gtfs.de/germany/fv_free/latest.zip
 *          https://download.gtfs.de/germany/rv_free/latest.zip
 * Sorties : ./gtfs/db_fv_filtered/  ./gtfs/db_rv_filtered/
 */

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

// ─── Configuration ─────────────────────────────────────────────────────────────

const FEEDS = [
  {
    id:         'DB_FV',
    label:      'Fernverkehr (ICE · IC · EC · NJ)',
    source_dir: './gtfs/db_fv',
    target_dir: './gtfs/db_fv_filtered',
    excluded_types: new Set([400, 401, 700, 900]),
    exclude_sbahn_agencies: false,
  },
];

// Conserver les trains touristiques ? (non pertinent pour FV, par sécurité)
const KEEP_TOURIST = false;
if (!KEEP_TOURIST) {
  FEEDS.forEach(f => f.excluded_types.add(105));
}

// Types ferroviaires acceptés (whitelist de sécurité)
const RAIL_TYPES = new Set([2, 100, 101, 102, 103, 104, 106, 107]);
//                                                  ↑104 = High speed rail (variante)
//                                                              ↑107 = Tourist (si KEEP_TOURIST)

// Noms d'agences S-Bahn urbains à exclure même si route_type = 2
// (filet de sécurité pour les feeds sans extended route_types)
const SBAHN_AGENCY_PATTERNS = [
  /s-bahn\s+berlin/i,
  /s-bahn\s+m[üu]nchen/i,
  /s-bahn\s+hamburg/i,
  /s-bahn\s+rhein.?main/i,      // Frankfurt
  /s-bahn\s+rhein.?ruhr/i,      // Ruhrgebiet
  /s-bahn\s+stuttgart/i,
  /s-bahn\s+hannover/i,
  /s-bahn\s+k[öo]ln/i,
  /s-bahn\s+mitteldeutschland/i, // Leipzig / Halle
  /s-bahn\s+dresden/i,
  /s-bahn\s+n[üu]rnberg/i,
];

function isSBahnAgency(name) {
  return SBAHN_AGENCY_PATTERNS.some(re => re.test(name));
}

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

async function filterFeed(feed) {
  console.log(`\n⚙️  Filtrage ${feed.id} — ${feed.label}`);
  console.log(`   Source : ${feed.source_dir}`);
  console.log(`   Cible  : ${feed.target_dir}`);

  if (!fs.existsSync(feed.source_dir)) {
    console.warn(`  ⚠  Dossier source absent — skip (lancer update-gtfs.sh d'abord)`);
    return;
  }
  fs.mkdirSync(feed.target_dir, { recursive: true });

  // 1. agency.txt ─ collecter les agences exclues (S-Bahn urbains)
  const excludedAgencyIds = new Set();
  const { count: agencyCount } = await processCSV(
    `${feed.source_dir}/agency.txt`,
    `${feed.target_dir}/agency.txt`,
    (row) => {
      if (feed.exclude_sbahn_agencies && isSBahnAgency(row.agency_name)) {
        excludedAgencyIds.add(row.agency_id);
        console.log(`  ✗ S-Bahn exclu : ${row.agency_name} (${row.agency_id})`);
        return false;
      }
      return true;
    }
  );
  console.log(`  agency.txt       : ${agencyCount} agence(s) conservée(s)`);
  if (excludedAgencyIds.size > 0) {
    console.log(`  S-Bahn exclus   : ${excludedAgencyIds.size} agence(s) urbaine(s)`);
  }

  // 2. routes.txt ─ filtrer par route_type ET agence
  const routeIds = new Set();
  const { count: routeCount } = await processCSV(
    `${feed.source_dir}/routes.txt`,
    `${feed.target_dir}/routes.txt`,
    (row) => {
      const rtype = parseInt(row.route_type, 10);
      // Exclure si agence S-Bahn urbaine
      if (excludedAgencyIds.has(row.agency_id)) return false;
      // Exclure les types non désirés
      if (feed.excluded_types.has(rtype)) return false;
      // Sur les feeds avec types étendus, seuls les types rail sont pertinents
      // On garde tout ce qui n'est pas explicitement exclu (certains opérateurs
      // privés peuvent utiliser le type 2 générique)
      routeIds.add(row.route_id);
      return true;
    }
  );
  console.log(`  routes.txt       : ${routeCount} route(s)`);

  // 3. trips.txt
  const tripIds    = new Set();
  const serviceIds = new Set();
  const { count: tripCount } = await processCSV(
    `${feed.source_dir}/trips.txt`,
    `${feed.target_dir}/trips.txt`,
    (row) => {
      if (!routeIds.has(row.route_id)) return false;
      tripIds.add(row.trip_id);
      serviceIds.add(row.service_id);
      return true;
    }
  );
  console.log(`  trips.txt        : ${tripCount} trip(s)`);

  // 4. stop_times.txt
  const stopIds = new Set();
  const { count: stCount } = await processCSV(
    `${feed.source_dir}/stop_times.txt`,
    `${feed.target_dir}/stop_times.txt`,
    (row) => {
      if (!tripIds.has(row.trip_id)) return false;
      stopIds.add(row.stop_id);
      return true;
    }
  );
  console.log(`  stop_times.txt   : ${stCount} ligne(s)`);

  // 5. stops.txt
  const { count: stopCount } = await processCSV(
    `${feed.source_dir}/stops.txt`,
    `${feed.target_dir}/stops.txt`,
    (row) => stopIds.has(row.stop_id) || row.location_type === '1'
  );
  console.log(`  stops.txt        : ${stopCount} arrêt(s)`);

  // 6. calendar.txt
  if (fs.existsSync(`${feed.source_dir}/calendar.txt`)) {
    const { count: calCount } = await processCSV(
      `${feed.source_dir}/calendar.txt`,
      `${feed.target_dir}/calendar.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar.txt     : ${calCount} service(s)`);
  }

  // 7. calendar_dates.txt
  if (fs.existsSync(`${feed.source_dir}/calendar_dates.txt`)) {
    const { count: cdCount } = await processCSV(
      `${feed.source_dir}/calendar_dates.txt`,
      `${feed.target_dir}/calendar_dates.txt`,
      (row) => serviceIds.has(row.service_id)
    );
    console.log(`  calendar_dates   : ${cdCount} exception(s)`);
  }

  // 8. feed_info.txt (copie directe)
  if (fs.existsSync(`${feed.source_dir}/feed_info.txt`)) {
    fs.copyFileSync(`${feed.source_dir}/feed_info.txt`, `${feed.target_dir}/feed_info.txt`);
    console.log(`  feed_info.txt    : copié`);
  }

  console.log(`\n✅ ${feed.id} filtré → ${feed.target_dir}`);
  console.log(`   ${routeIds.size} routes · ${tripIds.size} trips · ${stopIds.size} arrêts\n`);
}

// ─── Point d'entrée ────────────────────────────────────────────────────────────

(async function main() {
  console.log('\n🇩🇪  Filtrage GTFS Allemagne — Fernverkehr uniquement');
  console.log('   Types conservés  : ICE · IC · EC · NJ · Flixtrain');
  console.log('   Types exclus     : non-ferroviaire (bus, tram, métro)');
  if (!KEEP_TOURIST) console.log('   Touristique      : exclus');

  for (const feed of FEEDS) {
    await filterFeed(feed);
  }
})().catch(err => {
  console.error('❌ Erreur filtrage Germany :', err);
  process.exit(1);
});