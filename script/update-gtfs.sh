#!/bin/bash
set -e

echo "📥 Téléchargement des GTFS..."

node -e "
const ops = require('./operators.json');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

for (const op of ops) {
  if (!op.gtfs_url) continue;
  const dir = op.gtfs_dir;
  fs.mkdirSync(dir, { recursive: true });
  console.log('  → ' + op.id + ' : ' + op.gtfs_url);
  execSync('curl -L \"' + op.gtfs_url + '\" -o /tmp/gtfs_' + op.id + '.zip');
  execSync('unzip -o /tmp/gtfs_' + op.id + '.zip -d ' + dir);
  console.log('  ✅ ' + op.id + ' extrait dans ' + dir);
}
"

echo "⚙️  Ingestion GTFS → engine_data..."
node gtfs-ingest.js

echo "🗺️  Construction index stations..."
node build-stations-index.js

echo "✅ Mise à jour terminée."