#!/bin/bash
set -e

echo "📥 Téléchargement des GTFS..."

node << 'ENDNODE'
const https   = require('https');
const fs      = require('fs');
const path    = require('path');
const { execSync } = require('child_process');

const ops         = require('./operators.json');
const NAP_API_KEY = '5c51e865-2f81-4215-a1f0-3b73985a31fa';

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
    const tmp = '/tmp/gtfs_' + op.id + '.zip';
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
  for (const op of ops) {
    try {
      if (op.gtfs_url) {
        downloadDirect(op);
      } else if (op.gtfs_nap_id) {
        await downloadNAP(op);
      } else {
        console.log('  SKIP ' + op.id + ' : aucune source configuree.');
      }
    } catch(err) {
      console.error('  ERREUR ' + op.id + ' : ' + err.message);
      process.exit(1);
    }
  }
})();
ENDNODE

echo "⚙️  Ingestion GTFS -> engine_data..."
node gtfs-ingest.js

echo "🗺️  Construction index stations..."
node build-stations-index.js

echo "Mise à jour terminée."