#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID=rfp-database-464609 REGION=europe-west1 SERVICE=data-enricher \
#   BQ_LOCATION=europe-southwest1 CONCURRENCY=1 ./scripts/deploy.sh

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-europe-west1}"
SERVICE="${SERVICE:-data-enricher}"
BQ_LOCATION="${BQ_LOCATION:-europe-southwest1}"
CONCURRENCY="${CONCURRENCY:-1}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/rfp-enricher/${SERVICE}:manual"

echo "🔨 Building ${IMAGE}"
gcloud builds submit --tag "${IMAGE}" .

echo "🚀 Deploying ${SERVICE} to ${REGION} (BQ_LOCATION=${BQ_LOCATION}, concurrency=${CONCURRENCY})"
gcloud run deploy "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated \
  --concurrency "${CONCURRENCY}" \
  --set-env-vars PROJECT_ID=${PROJECT_ID},DATASET_ID=rfpdata,TABLE=performing_arts_fixed,BQ_LOCATION=${BQ_LOCATION} \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest

echo "✅ Done"
