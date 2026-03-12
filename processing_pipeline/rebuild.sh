#!/usr/bin/env bash
# rebuild.sh – arrête tout, purge les images workers, reconstruit et relance
set -e

echo "==> Arrêt de la stack..."
docker compose down --remove-orphans

echo "==> Suppression des images workers cachées..."
docker rmi -f \
  processing_pipeline-ingest \
  processing_pipeline-transcoder \
  processing_pipeline-audio \
  processing_pipeline-packager \
  ott_pipeline-ingest \
  ott_pipeline-transcoder \
  ott_pipeline-audio \
  ott_pipeline-packager \
  2>/dev/null || true

echo "==> Rebuild sans cache et démarrage..."
docker compose build --no-cache
docker compose up
