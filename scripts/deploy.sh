#!/usr/bin/env bash
set -euo pipefail

# Expect env vars to be already exported or set defaults
PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-europe-west1}"
SERVICE="${SERVICE:-data-enricher}"
REPO="${REPO:-rfp-enricher}"
BQ_LOCATION="${BQ_LOCATION:-europe-southwest1}"
CONCURRENCY="${CONCURRENCY:-1}"
MAX_INSTANCES="${MAX_INSTANCES:-2}"

gcloud config set project "$PROJECT_ID"

# Ensure Artifact Registry exists
gcloud artifacts repositories create "$REPO" \
  --repository-format=DOCKER \
  --location="$REGION" \
  --description="RFP Enricher images" || true

# Build & push
TAG="fix-$(date +%Y%m%d-%H%M%S)"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:${TAG}"
echo "Building $IMAGE ..."
gcloud builds submit . --tag "$IMAGE" --timeout=1200

# Deploy
echo "Deploying $SERVICE ..."
gcloud run deploy "$SERVICE" \
  --region "$REGION" \
  --image "$IMAGE" \
  --allow-unauthenticated \
  --concurrency "$CONCURRENCY" \
  --max-instances "$MAX_INSTANCES" \
  --set-env-vars "PROJECT_ID=${PROJECT_ID},DATASET_ID=rfpdata,TABLE=performing_arts_fixed,BQ_LOCATION=${BQ_LOCATION}" \
  --set-env-vars "OPENAI_MAX_RETRIES=5,OPENAI_TIMEOUT=30,ROW_DELAY_MIN_MS=50,ROW_DELAY_MAX_MS=250,BQ_MAX_RETRIES=5" \
  --set-secrets "OPENAI_API_KEY=openai-api-key:latest,MASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest"

echo
echo "Deployed image:"
gcloud run services describe "$SERVICE" --region "$REGION" --format='value(spec.template.spec.containers[0].image)'
echo
echo "Service URL:"
gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)'
