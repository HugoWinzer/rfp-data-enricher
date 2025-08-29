#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="rfp-data-enricher-madrid"
BQ_LOCATION="europe-southwest1"

gcloud config set project "$PROJECT_ID"

# Build image using alternate Dockerfile via Cloud Build config
gcloud builds submit --config cloudbuild.madrid.yaml --substitutions=_SERVICE="$SERVICE"

gcloud run deploy "$SERVICE" \
  --image "gcr.io/$PROJECT_ID/$SERVICE:latest" \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest,EVENTBRITE_TOKEN=eventbrite-token:latest \
  --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="rfpdata",TABLE="performing_arts_madrid",BQ_LOCATION="$BQ_LOCATION",OPENAI_MODEL="gpt-4o-mini",STOP_ON_GPT_QUOTA="1" \
  --max-instances=1 --concurrency=1 --min-instances=1

SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
echo "Service URL: $SERVICE_URL"

# Smoke tests (same endpoints as your main service)
curl -sS "$SERVICE_URL/ping"; echo
curl -sS "$SERVICE_URL/ready"; echo
curl -sS "$SERVICE_URL/?limit=5&dry=1"; echo
