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

function downloadDirect(op) {
  const dir = op.gtfs_dir;
  fs.mkdirSync(dir, { recursive: true });
  const tmp = '/tmp/gtfs_' + op.id + '.zip';
  console.log('  -> ' + op.id + ' (direct) : ' + op.gtfs_url);
  execSync('curl -L -s -o ' + tmp + ' "' + op.gtfs_url + '"');
  execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
  console.log('  OK ' + op.id + ' extrait dans ' + dir);
}

function downloadNAP(op) {
  return new Promise((resolve, reject) => {
    const dir = op.gtfs_dir;
    fs.mkdirSync(dir, { recursive: true });
    const tmp = '/tmp/gtfs_' + op.id + '.zip';
    console.log('  -> ' + op.id + ' (NAP id=' + op.gtfs_nap_id + ')');
    const file = fs.createWriteStream(tmp);
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

function downloadAndFilter(op) {
  const dir = op.gtfs_dir;
  const tmp = '/tmp/gtfs_' + op.id + '_raw';
  const zip = '/tmp/gtfs_' + op.id + '.zip';
  fs.mkdirSync(dir, { recursive: true });
  fs.mkdirSync(tmp, { recursive: true });

  console.log('  -> ' + op.id + ' (download+filter) : ' + op.gtfs_source_url);

  execSync(
    'curl -L --fail --retry 3 --retry-delay 5 --max-time 900 ' +
    '-H "User-Agent: TrainNomad/1.0" ' +
    '-w "  HTTP %{http_code} — %{size_download} bytes\\n" ' +
    '-o ' + zip + ' "' + op.gtfs_source_url + '"',
    { stdio: 'inherit' }
  );

  // Vérifier magic bytes PK (ZIP)
  const magic = Buffer.alloc(4);
  const fd = fs.openSync(zip, 'r');
  fs.readSync(fd, magic, 0, 4, 0);
  fs.closeSync(fd);
  if (magic[0] !== 0x50 || magic[1] !== 0x4B) {
    console.error('  Contenu reçu (500 premiers octets) :');
    console.error(fs.readFileSync(zip, 'utf8').substring(0, 500));
    throw new Error('Fichier téléchargé n\'est pas un ZIP');
  }

  console.log('  ZIP valide — décompression...');
  execSync('unzip -o ' + zip + ' -d ' + tmp + ' > /dev/null');
  execSync('rm -f ' + zip);

  const keep = JSON.stringify(op.gtfs_filter_agencies || []);
  execSync('python3 /tmp/gtfs_filter_' + op.id + '.py', {
    env: Object.assign({}, process.env, {
      GTFS_SRC: tmp,
      GTFS_DST: dir,
      GTFS_KEEP: keep,
    }),
    stdio: 'inherit'
  });

  execSync('rm -rf ' + tmp);
  console.log('  OK ' + op.id + ' filtré dans ' + dir);
}

// Écrire le script Python dans /tmp avant de l'appeler
const pyScript = `
import os, csv, time
SRC  = os.environ["GTFS_SRC"]
DST  = os.environ["GTFS_DST"]
import json; KEEP = set(json.loads(os.environ["GTFS_KEEP"]))
t0 = time.time()
print("  Agences : " + ", ".join(sorted(KEEP)))

def rd(name):
    p = os.path.join(SRC, name)
    if not os.path.exists(p): print("  skip " + name); return None, None
    f = open(p, "r", encoding="utf-8-sig", errors="replace")
    r = csv.DictReader(f); return f, r

def wr(name, fields):
    f = open(os.path.join(DST, name), "w", encoding="utf-8", newline="")
    w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
    w.writeheader(); return f, w

f, r = rd("agency.txt")
if r:
    rows=[x for x in r if x.get("agency_id","").strip() in KEEP]
    f.close(); _,w=wr("agency.txt",r.fieldnames); [w.writerow(x) for x in rows]
    print("  agency : "+str(len(rows)))

f, r = rd("routes.txt"); route_ids=set()
if r:
    kept=[]
    for x in r:
        rt=int(x.get("route_type","0") or 0)
        if x.get("agency_id","").strip() in KEEP and (rt==2 or 100<=rt<=199):
            kept.append(x); route_ids.add(x["route_id"].strip())
    f.close(); _,w=wr("routes.txt",r.fieldnames); [w.writerow(x) for x in kept]
    print("  routes : "+str(len(route_ids)))

f, r = rd("trips.txt"); trip_ids=set(); svc_ids=set()
if r:
    kept=[]
    for x in r:
        if x.get("route_id","").strip() in route_ids:
            kept.append(x); trip_ids.add(x["trip_id"].strip()); svc_ids.add(x["service_id"].strip())
    f.close(); _,w=wr("trips.txt",r.fieldnames); [w.writerow(x) for x in kept]
    print("  trips : "+str(len(trip_ids)))

f, r = rd("stop_times.txt"); stop_ids=set(); n=0
if r:
    fo,w=wr("stop_times.txt",r.fieldnames)
    for i,x in enumerate(r):
        if x.get("trip_id","").strip() in trip_ids:
            w.writerow(x); stop_ids.add(x.get("stop_id","").strip()); n+=1
        if i%1000000==0 and i>0: print("    "+str(i)+" lignes lues, "+str(n)+" gardées...")
    f.close(); fo.close()
    print("  stop_times : "+str(n))

f, r = rd("stops.txt")
if r:
    kept=[x for x in r if x.get("stop_id","").strip() in stop_ids or x.get("location_type","")=="1"]
    f.close(); _,w=wr("stops.txt",r.fieldnames); [w.writerow(x) for x in kept]
    print("  stops : "+str(len(kept)))

for fname in ["calendar.txt","calendar_dates.txt"]:
    f,r=rd(fname)
    if r:
        kept=[x for x in r if x.get("service_id","").strip() in svc_ids]
        f.close(); _,w=wr(fname,r.fieldnames); [w.writerow(x) for x in kept]

import subprocess
size=subprocess.check_output(["du","-sh",DST]).decode().split()[0]
print("  Terminé en %.1fs — %s" % (time.time()-t0, size))
`;

// On écrit le script Python pour chaque opérateur qui en a besoin
for (const op of ops) {
  if (op.gtfs_source_url) {
    fs.writeFileSync('/tmp/gtfs_filter_' + op.id + '.py', pyScript);
  }
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