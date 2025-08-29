#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="rfp-data-enricher-madrid"
BQ_LOCATION="europe-southwest1"

gcloud config set project "$PROJECT_ID"

# Build via Cloud Build using the repo Dockerfile (which now points to src.madrid_enricher:app)
gcloud builds submit --tag "gcr.io/$PROJECT_ID/$SERVICE:latest"

gcloud run deploy "$SERVICE" \
  --image "gcr.io/$PROJECT_ID/$SERVICE:latest" \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest \
  --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="rfpdata",TABLE="performing_arts_madrid",BQ_LOCATION="$BQ_LOCATION",OPENAI_MODEL="gpt-4o-mini",STOP_ON_GPT_QUOTA="1",ROW_DELAY_MIN_MS="30",ROW_DELAY_MAX_MS="180",ENABLE_TICKETMASTER="1",ENABLE_PLACES="1",ENABLE_EVENTBRITE="0",KEY_COL="name",NAME_COL="name",WEBSITE_COL="domain",ENRICH_STATUS_COL="enrichment_status" \
  --max-instances=1 --concurrency=1 --min-instances=1

SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
echo "Service URL: $SERVICE_URL"

# Smoke tests
curl -sS "$SERVICE_URL/ping"; echo
curl -sS "$SERVICE_URL/ready"; echo
curl -sS "$SERVICE_URL/?limit=5&dry=1"; echo
