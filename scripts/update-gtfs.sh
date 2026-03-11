#!/bin/bash
set -e

# --- PARTIE 1 : Téléchargement et Filtrage spécifique UK (Avanti) ---
echo "📥 Téléchargement spécifique : UK Rail (Avanti)..."
API_KEY="iSQvk8H4v8dTBm5rACmwsV6gLqks8laM"
UK_URL="https://transit.land/api/v2/rest/feeds/f-uk~rail/download_latest_feed_version?apikey=$API_KEY"

# On crée les dossiers nécessaires
mkdir -p ./gtfs/UK_Rail
mkdir -p ./gtfs/Avanti_Only

# Téléchargement du gros fichier UK avec options pour Windows/Render
curl -k -L "$UK_URL" -o /tmp/gtfs_uk_full.zip
unzip -o /tmp/gtfs_uk_full.zip -d ./gtfs/UK_Rail > /dev/null

echo "⚙️ Filtrage Avanti (VT)..."
# Recherche automatique du script Python (plus robuste sur Render)
PYTHON_SCRIPT=$(find . -name "filter_avanti.py" | head -n 1)

if [ -z "$PYTHON_SCRIPT" ]; then
    echo "❌ Erreur : filter_avanti.py introuvable !"
    exit 1
fi

python3 "$PYTHON_SCRIPT"

# --- PARTIE 2 : Logique Node.js d'origine pour les autres pays ---
echo "📥 Téléchargement des autres GTFS (SNCF, Eurostar, Renfe, etc.)..."

node << 'ENDNODE'
const https   = require('https');
const fs      = require('fs');
const path    = require('path');
const { execSync } = require('child_process');

const ops         = require('./operators.json');
const NAP_API_KEY = '5c51e865-2f81-4215-a1f0-3b73985a31fa';

/**
 * Téléchargement Direct (SNCF, Eurostar, Trenitalia...)
 */
function downloadDirect(op) {
  const dir = op.gtfs_dir;
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const tmp = '/tmp/gtfs_' + op.id + '.zip';
  console.log('  -> ' + op.id + ' (direct) : ' + op.gtfs_url);
  try {
    execSync('curl -L -s -o ' + tmp + ' "' + op.gtfs_url + '"');
    execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
    console.log('  OK ' + op.id + ' extrait dans ' + dir);
  } catch (e) {
    console.error('  ❌ Erreur sur ' + op.id + ': ' + e.message);
  }
}

/**
 * Téléchargement via NAP (Espagne - Renfe/Ouigo ES)
 */
function downloadNAP(op) {
  return new Promise((resolve, reject) => {
    const dir = op.gtfs_dir;
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const tmp = '/tmp/gtfs_' + op.id + '.zip';
    console.log('  -> ' + op.id + ' (NAP id=' + op.gtfs_nap_id + ')');
    
    const url = 'https://nap.transportes.gob.es/api/v1/datasets/' + op.gtfs_nap_id + '/download';
    const file = fs.createWriteStream(tmp);
    
    https.get(url, { headers: { 'X-API-KEY': NAP_API_KEY } }, (res) => {
      if (res.statusCode === 302 || res.statusCode === 301) {
        https.get(res.headers.location, (res2) => {
          res2.pipe(file);
          file.on('finish', () => {
            file.close();
            execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
            console.log('  OK ' + op.id + ' extrait via NAP');
            resolve();
          });
        });
      } else {
        res.pipe(file);
        file.on('finish', () => {
          file.close();
          execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
          console.log('  OK ' + op.id + ' extrait via NAP');
          resolve();
        });
      }
    }).on('error', reject);
  });
}

/**
 * BOUCLE PRINCIPALE
 */
(async function() {
  // On ignore l'ID "UK" ou "Avanti" car déjà traité en Partie 1
  const filteredOps = ops.filter(op => op.id !== 'UK' && op.id !== 'AVANTI');

  for (const op of filteredOps) {
    try {
      if (op.gtfs_url) {
        downloadDirect(op);
      } else if (op.gtfs_nap_id) {
        await downloadNAP(op);
      }
    } catch (err) {
      console.error('  ❌ Erreur fatale sur ' + op.id + ':', err.message);
    }
  }
  console.log('--- Fin de la boucle Node ---');
})();
ENDNODE

echo "✅ Tous les GTFS sont prêts et décompressés."