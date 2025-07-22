#!/usr/bin/env bash
set -euo pipefail

# You can edit these or export them beforehand
PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
DATASET_ID="${DATASET_ID:-rfpdata}"
SERVICE_NAME="${SERVICE_NAME:-data-enricher}"
IMAGE="gcr.io/$PROJECT_ID/$SERVICE_NAME:latest"

echo "🔨 Building image $IMAGE"
gcloud builds submit --tag "$IMAGE" .

echo "🚀 Deploying to Cloud Run service $SERVICE_NAME"
gcloud run deploy "$SERVICE_NAME" \
  --image "$IMAGE" \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars \
"PROJECT_ID=$PROJECT_ID,DATASET_ID=$DATASET_ID,RAW_TABLE=performing_arts_raw,STAGING_TABLE=performing_arts_enriched_staging,OPENAI_API_KEY=$OPENAI_API_KEY"

echo "✅ Deployment complete"
