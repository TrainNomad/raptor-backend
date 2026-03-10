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

// ─── Téléchargement + filtrage inline (UK Rail) ───────────────────────────────
function downloadAndFilter(op) {
  const dir = op.gtfs_dir;
  const tmp = '/tmp/gtfs_' + op.id + '_raw';
  const zip = '/tmp/gtfs_' + op.id + '.zip';
  fs.mkdirSync(dir, { recursive: true });
  fs.mkdirSync(tmp, { recursive: true });

  console.log('  -> ' + op.id + ' (download+filter) : ' + op.gtfs_source_url);
  execSync('curl -L -s -o ' + zip + ' "' + op.gtfs_source_url + '"', { stdio: 'inherit' });
  execSync('unzip -o ' + zip + ' -d ' + tmp + ' > /dev/null');
  execSync('rm ' + zip);

  // Filtrage Python inline — streaming, ne charge jamais tout en RAM
  const keep = JSON.stringify(op.gtfs_filter_agencies || []);
  execSync(`python3 - << 'PYEOF'
import os, csv, sys, time
SRC = "${tmp}"
DST = "${dir}"
KEEP = set(${keep})
t0 = time.time()
print("  Filtrage agences : " + ", ".join(sorted(KEEP)))

def rd(name):
    p = os.path.join(SRC, name)
    if not os.path.exists(p): return None, None
    f = open(p, "r", encoding="utf-8-sig", errors="replace")
    r = csv.DictReader(f)
    return f, r

def wr(name, fields):
    f = open(os.path.join(DST, name), "w", encoding="utf-8", newline="")
    w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    return f, w

# 1. agency
f, r = rd("agency.txt")
if r:
    rows = [x for x in r if x.get("agency_id","").strip() in KEEP]
    f.close(); _, w = wr("agency.txt", r.fieldnames); [w.writerow(x) for x in rows]
    print("  agency : " + str(len(rows)))

# 2. routes
f, r = rd("routes.txt")
route_ids = set()
if r:
    kept = []
    for x in r:
        rt = int(x.get("route_type","0") or 0)
        if x.get("agency_id","").strip() in KEEP and (rt==2 or 100<=rt<=199):
            kept.append(x); route_ids.add(x["route_id"].strip())
    f.close(); _, w = wr("routes.txt", r.fieldnames); [w.writerow(x) for x in kept]
    print("  routes : " + str(len(route_ids)))

# 3. trips
f, r = rd("trips.txt")
trip_ids = set(); svc_ids = set()
if r:
    kept = []
    for x in r:
        if x.get("route_id","").strip() in route_ids:
            kept.append(x); trip_ids.add(x["trip_id"].strip()); svc_ids.add(x["service_id"].strip())
    f.close(); _, w = wr("trips.txt", r.fieldnames); [w.writerow(x) for x in kept]
    print("  trips : " + str(len(trip_ids)))

# 4. stop_times (streaming)
f, r = rd("stop_times.txt")
stop_ids = set(); n = 0
if r:
    fo, w = wr("stop_times.txt", r.fieldnames)
    for x in r:
        if x.get("trip_id","").strip() in trip_ids:
            w.writerow(x); stop_ids.add(x.get("stop_id","").strip()); n += 1
    f.close(); fo.close()
    print("  stop_times : " + str(n))

# 5. stops
f, r = rd("stops.txt")
if r:
    kept = [x for x in r if x.get("stop_id","").strip() in stop_ids or x.get("location_type","") == "1"]
    f.close(); _, w = wr("stops.txt", r.fieldnames); [w.writerow(x) for x in kept]
    print("  stops : " + str(len(kept)))

# 6. calendar
f, r = rd("calendar.txt")
if r:
    kept = [x for x in r if x.get("service_id","").strip() in svc_ids]
    f.close(); _, w = wr("calendar.txt", r.fieldnames); [w.writerow(x) for x in kept]

# 7. calendar_dates
f, r = rd("calendar_dates.txt")
if r:
    kept = [x for x in r if x.get("service_id","").strip() in svc_ids]
    f.close(); _, w = wr("calendar_dates.txt", r.fieldnames); [w.writerow(x) for x in kept]

import subprocess
size = subprocess.check_output(["du","-sh", DST]).decode().split()[0]
print("  OK filtre terminé en %.1fs — %s" % (time.time()-t0, size))
PYEOF`, { stdio: 'inherit' });

  execSync('rm -rf ' + tmp);
  console.log('  OK ' + op.id + ' extrait dans ' + dir);
}


(async function() {
  for (const op of ops) {
    try {
      if (op.gtfs_source_url) {
        downloadAndFilter(op);
      } else if (op.gtfs_url) {
        downloadDirect(op);
      } else if (op.gtfs_nap_id) {
        await downloadNAP(op);
      } else if (op.gtfs_dir && fs.existsSync(op.gtfs_dir) && fs.readdirSync(op.gtfs_dir).length > 0) {
        console.log('  OK ' + op.id + ' : dossier pré-existant (' + op.gtfs_dir + ')');
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