#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID=rfp-database-464609 REGION=europe-west1 SERVICE=data-enricher ./scripts/deploy.sh

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-europe-west1}"
SERVICE="${SERVICE:-data-enricher}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/rfp-enricher/${SERVICE}:manual"

echo "🔨 Building ${IMAGE}"
gcloud builds submit --tag "${IMAGE}" .

echo "🚀 Deploying ${SERVICE} to ${REGION}"
gcloud run deploy "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=${PROJECT_ID},DATASET_ID=rfpdata,TABLE=performing_arts_fixed \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest

echo "✅ Done"
