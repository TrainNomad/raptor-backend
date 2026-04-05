# Configuration du Pipeline CI/CD

## Étape 1 : Modification de package.json
- [ ] Supprimer la lourde tâche de téléchargement du `build`.
- [ ] Garder uniquement `node server.js` pour le démarrage.
- [ ] Configurer Render pour qu'il ne build que si les fichiers dans `engine_data/` changent.

## Étape 2 : Création du workflow GitHub (.github/workflows/update_data.yml)
- [ ] **Trigger** : Tous les jours à 3h du matin (cron).
- [ ] **Job** : 
    - Installer Node.js.
    - Lancer `bash scripts/update-gtfs.sh`.
    - Lancer `node build-stations-index.js`.
- [ ] **Artifact** : Compresser le dossier `engine_data` et l'envoyer vers Render (via un déploiement automatique ou en committant sur une branche `deploy`).

## Étape 3 : Optimisation Render (Le "Runtime")
- [ ] Dans ton `server.js`, tu as mis `--max-old-space-size=450`. C'est serré pour 500MB de GTFS !
- [ ] **Action** : En passant tes données en **TypedArrays (Binaires)**, ces 450MB de RAM suffiront largement car Node.js ne créera plus des millions d'objets, mais gérera juste des blocs de mémoire brute (Buffer). 