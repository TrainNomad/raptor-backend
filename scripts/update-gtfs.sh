#!/bin/bash
set -e

# =============================================================================
#  update-gtfs.sh
#  Télécharge, dézippe et filtre tous les GTFS, puis lance l'ingestion.
#
#  Dépendances (même dossier) :
#    filter_avanti.js        — filtre les données UK Rail → UK National
#    filter_germany.js       — filtre Allemagne FV (ICE · IC · EC · NJ)
#    filter_trenitalia.js    — filtre Trenitalia Italia (FR · FA · FB · IC · ICN · EC)
#    gtfs-ingest.js          — ingestion RAPTOR multi-opérateurs
#    build-stations-index.js — index des stations
#    operators.json          — liste des opérateurs
# =============================================================================

TRANSITLAND_API_KEY="${TRANSITLAND_API_KEY:-iSQvk8H4v8dTBm5rACmwsV6gLqks8laM}"

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 1 — UK Rail (transit.land) + filtrage Avanti
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement UK Rail (transit.land)..."

mkdir -p ./gtfs/UK_Rail
mkdir -p ./gtfs/Avanti_Only

curl -k -L \
  "https://transit.land/api/v2/rest/feeds/f-uk~rail/download_latest_feed_version?apikey=${TRANSITLAND_API_KEY}" \
  -o /tmp/gtfs_uk_full.zip

unzip -o /tmp/gtfs_uk_full.zip -d ./gtfs/UK_Rail > /dev/null

echo "⚙️  Filtrage Avanti West Coast (VT)..."
node filter_avanti.js

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 2 — Autres opérateurs (SNCF, Eurostar, Renfe…) via Node
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement des autres GTFS..."

node << 'ENDNODE'
const https        = require('https');
const fs           = require('fs');
const { execSync } = require('child_process');

const ops         = require('./operators.json');
const NAP_API_KEY = process.env.NAP_API_KEY || '5c51e865-2f81-4215-a1f0-3b73985a31fa';

// ─── Téléchargement via URL directe (curl) ────────────────────────────────────
function downloadDirect(op) {
  const dir = op.gtfs_dir;
  fs.mkdirSync(dir, { recursive: true });
  const tmp = '/tmp/gtfs_' + op.id + '.zip';
  console.log('  -> ' + op.id + ' (direct) : ' + op.gtfs_url);
  execSync('curl -L -s -o ' + tmp + ' "' + op.gtfs_url + '"');
  execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
  console.log('  OK ' + op.id + ' extrait dans ' + dir);
}

// ─── Téléchargement via NAP espagnol (clé API requise) ───────────────────────
function downloadNAP(op) {
  return new Promise((resolve, reject) => {
    const dir = op.gtfs_dir;
    fs.mkdirSync(dir, { recursive: true });
    const tmp  = '/tmp/gtfs_' + op.id + '.zip';
    console.log('  -> ' + op.id + ' (NAP id=' + op.gtfs_nap_id + ')');

    const file    = fs.createWriteStream(tmp);
    const options = {
      hostname: 'nap.transportes.gob.es',
      path:     '/api/Fichero/download/' + op.gtfs_nap_id,
      method:   'GET',
      headers:  { 'ApiKey': NAP_API_KEY, 'accept': 'application/octet-stream' },
    };

    function get(opts) {
      https.get(opts, function(res) {
        if (res.statusCode === 301 || res.statusCode === 302) {
          console.log('     -> Redirection : ' + res.headers.location);
          return get(res.headers.location);
        }
        if (res.statusCode !== 200) return reject(new Error('NAP HTTP ' + res.statusCode));
        res.pipe(file);
        file.on('finish', function() {
          file.close();
          try {
            execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
            console.log('  OK ' + op.id + ' extrait dans ' + dir);
            resolve();
          } catch(e) { reject(e); }
        });
        file.on('error', reject);
      }).on('error', reject);
    }

    get(options);
  });
}

// ─── Boucle principale ────────────────────────────────────────────────────────
(async function() {
  // Ignorer UK (Partie 1), DB_FV (Partie 3), TI_IT et NTV (Partie 3bis — download séparé)
  const SKIP_IDS = new Set(['UK', 'AVANTI', 'DB_FV', 'DB_RV', 'TI_IT', 'NTV']);
  const filtered = ops.filter(op => !SKIP_IDS.has(op.id));

  for (const op of filtered) {
    try {
      if (op.gtfs_url) {
        downloadDirect(op);
      } else if (op.gtfs_nap_id) {
        await downloadNAP(op);
      } else {
        console.log('  SKIP ' + op.id + ' : aucune source configurée.');
      }
    } catch(err) {
      console.error('  ERREUR ' + op.id + ' : ' + err.message);
      process.exit(1);
    }
  }
})();
ENDNODE

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 3 — Allemagne Fernverkehr (ICE · IC · EC · NJ)
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement Allemagne Fernverkehr (ICE · IC · EC · NJ)..."
mkdir -p ./gtfs/db_fv
curl -L -s \
  "https://download.gtfs.de/germany/fv_free/latest.zip" \
  -o /tmp/gtfs_db_fv.zip
unzip -o /tmp/gtfs_db_fv.zip -d ./gtfs/db_fv > /dev/null

echo "⚙️  Filtrage Allemagne FV (exclusion non-ferroviaire)..."
node filter_germany.js

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 3bis — Trenitalia Italia (FR · FA · FB · IC · ICN · EC · EN)
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement Trenitalia Italia (FR · FA · FB · IC · ICN · EC)..."
mkdir -p ./gtfs/trenitalia_it

# URL directe opendata FS — portail dati.gov.it
# Si cette URL change, consulter : https://www.dati.gov.it/view-dataset/dataset?id=trenitalia-gtfs
curl -L -s \
  "https://www.dati.gov.it/view-dataset/dataset?id=trenitalia-gtfs" \
  -o /tmp/gtfs_ti_it.zip 2>/dev/null || true

# Fallback mirror TransitFeeds si l'URL officielle échoue
if [ ! -s /tmp/gtfs_ti_it.zip ]; then
  echo "  ⚠️  URL officielle inaccessible — tentative mirror transitfeeds..."
  curl -L -s \
    "https://transitfeeds.com/p/trenitalia/888/latest/download" \
    -o /tmp/gtfs_ti_it.zip
fi

unzip -o /tmp/gtfs_ti_it.zip -d ./gtfs/trenitalia_it > /dev/null
echo "⚙️  Filtrage Trenitalia Italia (exclusion Regionale/SFM)..."
node filter_trenitalia.js

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 3ter — NTV / Italo (haute vitesse italienne alternative)
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement NTV/Italo..."
mkdir -p ./gtfs/ntv

curl -L -s \
  "https://www.italotreno.it/sites/default/files/gtfs.zip" \
  -o /tmp/gtfs_ntv.zip

# Vérifier que le téléchargement a réussi avant d'extraire
if [ -s /tmp/gtfs_ntv.zip ]; then
  unzip -o /tmp/gtfs_ntv.zip -d ./gtfs/ntv > /dev/null
  echo "  ✅ NTV/Italo extrait dans ./gtfs/ntv"
else
  echo "  ⚠️  NTV/Italo : téléchargement vide ou inaccessible — opérateur ignoré pour cette mise à jour"
fi

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 4 — Ingestion RAPTOR + index stations
# ─────────────────────────────────────────────────────────────────────────────
echo "⚙️  Ingestion GTFS -> engine_data..."
node gtfs-ingest.js

echo "🗺️  Construction index stations..."
node build-stations-index.js

echo "✅ Mise à jour terminée."