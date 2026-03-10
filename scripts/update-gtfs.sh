#!/bin/bash

# 1. Configuration
API_KEY="iSQvk8H4v8dTBm5rACmwsV6gLqks8laM"
# Nouveau lien API v2 fonctionnel pour le rail UK
URL="https://transit.land/api/v2/rest/feeds/f-uk~rail/download_latest_feed_version?apikey=$API_KEY"

SOURCE_DIR="./gtfs/UK_Rail"
TARGET_DIR="./gtfs/Avanti_Only"
TEMP_ZIP="./gtfs_uk_full.zip"

echo "--- Début de la mise à jour TrainNomad (UK Rail) ---"

# 2. Nettoyage et préparation des dossiers
mkdir -p "$SOURCE_DIR"
mkdir -p "$TARGET_DIR"

# 3. Téléchargement sécurisé pour Windows
echo "📥 Téléchargement du GTFS UK complet..."
# -k : Ignore l'erreur de certificat SSL sur Windows
# -L : Suit la redirection vers le fichier ZIP réel
curl -k -L "$URL" -o "$TEMP_ZIP"

# 4. Extraction
echo "📦 Extraction des fichiers..."
unzip -o "$TEMP_ZIP" -d "$SOURCE_DIR"

# 5. Lancement du filtrage Python (Extraction d'Avanti West Coast)
echo "⚙️ Filtrage des données (Agence VT)..."
if [ -f "scripts/filter/filter_avanti.py" ]; then
    python scripts/filter/filter_avanti.py
else
    # Fallback si le script est à la racine
    python filter_avanti.py
fi

# 6. Création du ZIP final (Optionnel mais recommandé pour le backend)
echo "📚 Création de l'archive finale avanti_gtfs.zip..."
cd "$TARGET_DIR"
zip -r ../avanti_gtfs.zip ./*.txt
cd ../..

echo "--- Terminé ! ---"
echo "✅ Fichier prêt : ./gtfs/avanti_gtfs.zip"