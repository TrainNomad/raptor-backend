#!/bin/bash
set -e

# --- PARTIE 1 : Téléchargement du Rail UK (Nouveau) ---
echo "📥 Téléchargement spécifique : UK Rail (Avanti)..."
API_KEY="iSQvk8H4v8dTBm5rACmwsV6gLqks8laM"
UK_URL="https://transit.land/api/v2/rest/feeds/f-uk~rail/download_latest_feed_version?apikey=$API_KEY"

# On crée les dossiers nécessaires
mkdir -p ./gtfs/UK_Rail
mkdir -p ./gtfs/Avanti_Only

# Téléchargement du gros fichier UK
curl -k -L "$UK_URL" -o /tmp/gtfs_uk_full.zip
unzip -o /tmp/gtfs_uk_full.zip -d ./gtfs/UK_Rail > /dev/null

# Lancement du filtrage Python pour ne garder qu'Avanti (VT)
echo "⚙️ Filtrage Avanti (VT)..."
python3 scripts/filter/filter_avanti.py

# --- PARTIE 2 : Votre logique Node.js d'origine (Conservée) ---
echo "📥 Téléchargement des autres GTFS (SNCF, Eurostar, etc.)..."

node << 'ENDNODE'
const https   = require('https');
const fs      = require('fs');
const path    = require('path');
const { execSync } = require('child_process');

const ops         = require('./operators.json');
const NAP_API_KEY = '5c51e865-2f81-4215-a1f0-3b73985a31fa';

// On filtre les opérateurs pour ne pas re-télécharger l'UK ici s'il est dans le JSON
// ou on laisse votre logique habituelle s'en charger
function downloadDirect(op) {
  const dir = op.gtfs_dir;
  fs.mkdirSync(dir, { recursive: true });
  const tmp = '/tmp/gtfs_' + op.id + '.zip';
  console.log('  -> ' + op.id + ' (direct) : ' + op.gtfs_url);
  execSync('curl -L -s -o ' + tmp + ' "' + op.gtfs_url + '"');
  execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
  console.log('  OK ' + op.id + ' extrait dans ' + dir);
}

// ... (Le reste de votre code Node.js d'origine que vous aviez dans update-gtfs.sh)
// Assurez-vous de garder vos fonctions downloadNAP, etc.
ENDNODE

echo "✅ Tous les GTFS sont prêts et décompressés."