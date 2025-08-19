#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID=rfp-database-464609 REGION=europe-west1 SERVICE=data-enricher \
#   BQ_LOCATION=europe-southwest1 CONCURRENCY=1 MAX_INSTANCES=2 ./scripts/deploy.sh

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-europe-west1}"
SERVICE="${SERVICE:-data-enricher}"
BQ_LOCATION="${BQ_LOCATION:-europe-southwest1}"
CONCURRENCY="${CONCURRENCY:-1}"       # keep low to avoid 429s
MAX_INSTANCES="${MAX_INSTANCES:-2}"   # small autoscale for canaries
REPO="${REPO:-rfp-enricher}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:manual-$(date +%s)"

echo "🔨 Building ${IMAGE}"
gcloud artifacts repositories create "${REPO}" \
  --repository-format=DOCKER --location="${REGION}" \
  --description="RFP Enricher images" || true

gcloud builds submit --tag "${IMAGE}" --timeout=1200

echo "🚀 Deploying ${SERVICE} to ${REGION} (BQ_LOCATION=${BQ_LOCATION}, concurrency=${CONCURRENCY}, max-instances=${MAX_INSTANCES})"
gcloud run deploy "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated \
  --concurrency "${CONCURRENCY}" \
  --max-instances "${MAX_INSTANCES}" \
  --set-env-vars PROJECT_ID=${PROJECT_ID},DATASET_ID=rfpdata,TABLE=performing_arts_fixed,BQ_LOCATION=${BQ_LOCATION} \
  --set-env-vars OPENAI_MAX_RETRIES=5,OPENAI_TIMEOUT=30,ROW_DELAY_MIN_MS=50,ROW_DELAY_MAX_MS=250,BQ_MAX_RETRIES=5 \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest

echo "✅ Done"
