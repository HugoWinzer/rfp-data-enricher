#!/usr/bin/env bash
set -euo pipefail

# No API keys or secrets hard-coded here!

SERVICE="data-enricher"
IMAGE="gcr.io/$PROJECT_ID/$SERVICE:latest"

echo "🔨 Building image $IMAGE"
gcloud builds submit --tag "$IMAGE" .

echo "🚀 Deploying to Cloud Run service $SERVICE"
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET_ID=$DATASET_ID,RAW_TABLE=$RAW_TABLE,STAGING_TABLE=$STAGING_TABLE,OPENAI_API_KEY=$OPENAI_API_KEY,TICKETMASTER_KEY=$TICKETMASTER_KEY,GOOGLE_PLACES_KEY=$GOOGLE_PLACES_KEY

echo "✅ Deployment complete"
