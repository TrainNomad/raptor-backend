/**
 * filter_trenitalia.js
 * Filtre le feed GTFS Trenitalia Italia (portail opendata FS) :
 *   — Conserve uniquement les trains longue distance et grande vitesse
 *   — Exclut les trains régionaux (Regionale, Regionale Veloce) et urbains
 *
 * ─── Trains conservés ─────────────────────────────────────────────────────────
 *   FR   Frecciarossa       grande vitesse nationale (Roma ↔ Milano, Torino, Napoli, Venezia…)
 *   FA   Frecciargento      grande vitesse inclinée  (Roma ↔ Venezia, Ancona, Pescara…)
 *   FB   Frecciabianca      rapide classique         (Milano ↔ Venezia, Roma ↔ Reggio Calabria…)
 *   IC   Intercity          longue distance non-GV   (liaisons interrégionales)
 *   ICN  Intercity Notte    trains de nuit           (Roma ↔ Palermo, Reggio Calabria, Torino…)
 *   EC   EuroCity           liaisons internationales (Milano ↔ Genève, Zurich, München, Wien…)
 *   EN   EuroNight          trains de nuit intl.     (Roma ↔ Vienne, Paris…)
 *   AV   Alta Velocità      grande vitesse générique (fallback si short_name = 'AV')
 *
 * ─── Trains exclus ────────────────────────────────────────────────────────────
 *   R    Regionale           omnibus régional (tous les arrêts, inutile pour RAPTOR longue distance)
 *   RV   Regionale Veloce    régional semi-rapide (même raison)
 *   SFM  Servizio Ferroviario Metropolitano  réseau suburbain (Turin, Milan, Rome…)
 *   MD   Minuetto/Diretto    ancien diesel régional (quasi disparu)
 *
 * ─── Types de routes (GTFS standard + extended) ───────────────────────────────
 *   2    Rail (standard)     fallback générique ferroviaire
 *   100  High-Speed Rail     Frecciarossa, Frecciargento
 *   101  Long Distance Rail  Intercity, EuroCity
 *   102  Inter Regional Rail liaisons semi-rapides transfrontalières
 *   106  Regional Rail       Regionale Veloce (EXCLU)
 *   109  Suburban Railway    SFM (EXCLU)
 *
 * ─── Agences incluses ─────────────────────────────────────────────────────────
 *   Trenitalia S.p.A.        opérateur national principal
 *   (NTV/Italo : feed séparé — voir opérateur NTV dans operators.json)
 *
 * Source  : https://www.dati.gov.it/view-dataset/dataset?id=trenitalia-gtfs
 *           (ou mirror : https://transitfeeds.com/p/trenitalia)
 * Sorties : ./gtfs/trenitalia_it_filtered/
 */

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

// ─── Configuration ─────────────────────────────────────────────────────────────

const SOURCE_DIR = './gtfs/trenitalia_it';
const TARGET_DIR = './gtfs/trenitalia_it_filtered';

// route_short_name à conserver (whitelist — plus fiable que route_type seul
// car Trenitalia utilise souvent le type générique 2 pour tout son réseau)
const KEEP_SHORT = new Set([
  'FR',   // Frecciarossa
  'FA',   // Frecciargento
  'FB',   // Frecciabianca
  'IC',   // Intercity
  'ICN',  // Intercity Notte
  'EC',   // EuroCity
  'EN',   // EuroNight
  'AV',   // Alta Velocità (fallback)
  'NJ',   // Nightjet (ÖBB codeshare via Trenitalia)
]);

// route_short_name à exclure explicitement (blacklist de sécurité)
const EXCLUDE_SHORT = new Set([
  'R',    // Regionale
  'RV',   // Regionale Veloce
  'SFM',  // Servizio Ferroviario Metropolitano
  'MD',   // Minuetto/Diretto
  'RE',   // Regional Express (variante)
]);

// route_type à exclure (types non-ferroviaires + suburbains)
const EXCLUDED_TYPES = new Set([
  3,    // Bus
  109,  // Suburban Railway (SFM, réseau banlieue)
  400,  // Urban Railway / Métro
  401,  // Metro variante
  700,  // Bus (extended)
  900,  // Tram
  1000, // Ferry
]);

// Types ferroviaires longue distance (whitelist)
const LONGDIST_TYPES = new Set([
  100,  // High-Speed Rail
  101,  // Long Distance Rail
  102,  // Inter Regional Rail
]);

// ─── Logique de filtrage par route ────────────────────────────────────────────

function shouldKeepRoute(row) {
  const short = (row.route_short_name || '').trim().toUpperCase();
  const rtype = parseInt(row.route_type, 10) || 0;

  // Toujours exclure les types non-ferroviaires
  if (EXCLUDED_TYPES.has(rtype)) return false;

  // Toujours exclure les trains régionaux/suburbains par nom
  if (EXCLUDE_SHORT.has(short)) return false;

  // Whitelist route_short_name — si connu et explicitement listé → garder
  if (KEEP_SHORT.has(short)) return true;

  // Types extended longue distance → garder (FR, FA, EC utilisent souvent 100/101)
  if (LONGDIST_TYPES.has(rtype)) return true;

  // route_type 2 (rail générique) : seuls les trains avec un short_name connu
  // sont conservés. Les Regionali sans short_name tombent ici → exclus.
  // Cas limite : si short_name est vide, on ne peut pas savoir → on exclut
  // par sécurité (évite d'ingérer des milliers de trips régionaux).
  if (rtype === 2) {
    if (!short) return false;
    // Préfixes connus des trains longue distance Trenitalia
    if (short.startsWith('FR') || short.startsWith('FA') || short.startsWith('FB')) return true;
    if (short.startsWith('IC') || short.startsWith('EC') || short.startsWith('EN')) return true;
    if (short.startsWith('AV') || short.startsWith('NJ')) return true;
    return false;
  }

  // Tout autre type ferroviaire étendu non listé : exclure par sécurité
  return false;
}

// ─── Utilitaires CSV (identiques aux autres filtres) ─────────────────────────

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

// ─── Pipeline de filtrage ─────────────────────────────────────────────────────

async function filterTrenitalia() {
  console.log('\n⚙️  Filtrage TI_IT — Trenitalia longue distance');
  console.log(`   Source : ${SOURCE_DIR}`);
  console.log(`   Cible  : ${TARGET_DIR}`);

  if (!fs.existsSync(SOURCE_DIR)) {
    console.warn('  ⚠  Dossier source absent — skip (lancer update-gtfs.sh d\'abord)');
    return;
  }
  fs.mkdirSync(TARGET_DIR, { recursive: true });

  // 1. agency.txt — copie intégrale (Trenitalia n'a qu'une agence dans son feed)
  const { count: agencyCount } = await processCSV(
    `${SOURCE_DIR}/agency.txt`,
    `${TARGET_DIR}/agency.txt`,
    () => true
  );
  console.log(`  agency.txt       : ${agencyCount} agence(s)`);

  // 2. routes.txt — filtrage par short_name + route_type
  const routeIds    = new Set();
  const keptShorts  = {};
  const { count: routeCount } = await processCSV(
    `${SOURCE_DIR}/routes.txt`,
    `${TARGET_DIR}/routes.txt`,
    (row) => {
      if (!shouldKeepRoute(row)) return false;
      routeIds.add(row.route_id);
      const s = (row.route_short_name || 'UNKNOWN').toUpperCase();
      keptShorts[s] = (keptShorts[s] || 0) + 1;
      return true;
    }
  );
  console.log(`  routes.txt       : ${routeCount} route(s) conservée(s)`);
  console.log(`  Répartition      : ${Object.entries(keptShorts).map(([k,v]) => `${k}×${v}`).join(' · ')}`);

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

  // 4. stop_times.txt
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

  // 5. stops.txt
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

  // 8. feed_info.txt (copie directe si présent)
  if (fs.existsSync(`${SOURCE_DIR}/feed_info.txt`)) {
    fs.copyFileSync(`${SOURCE_DIR}/feed_info.txt`, `${TARGET_DIR}/feed_info.txt`);
    console.log(`  feed_info.txt    : copié`);
  }

  console.log(`\n✅ TI_IT filtré → ${TARGET_DIR}`);
  console.log(`   ${routeIds.size} routes · ${tripIds.size} trips · ${stopIds.size} arrêts\n`);
}

// ─── Point d'entrée ────────────────────────────────────────────────────────────

(async function main() {
  console.log('\n🇮🇹  Filtrage GTFS Trenitalia Italia — longue distance uniquement');
  console.log('   Conserve : FR · FA · FB · IC · ICN · EC · EN');
  console.log('   Exclut   : Regionale (R) · Regionale Veloce (RV) · SFM (banlieue)');
  await filterTrenitalia();
})().catch(err => {
  console.error('❌ Erreur filtrage Trenitalia :', err);
  process.exit(1);
});