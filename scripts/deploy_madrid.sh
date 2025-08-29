#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="rfp-data-enricher-madrid"
BQ_LOCATION="europe-southwest1"

# IMPORTANT: this app already serves /, /ping, /ready and reads these envs to pick rows and write back. 
# We target your new Madrid table and tell the app that the "website" column is actually `domain`,
# and that rows are keyed by `name`. (Matches your app's env contract.)  【turn3file50†source】【turn3file54†source】

gcloud config set project "$PROJECT_ID"

# Build with the DEFAULT Dockerfile (entrypoint is src.enrich_app:app)  【turn3file55†source】
gcloud builds submit --tag "gcr.io/$PROJECT_ID/$SERVICE:latest"

gcloud run deploy "$SERVICE" \
  --image "gcr.io/$PROJECT_ID/$SERVICE:latest" \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest \
  --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="rfpdata",TABLE="performing_arts_madrid",BQ_LOCATION="$BQ_LOCATION",OPENAI_MODEL="gpt-4o-mini",ROW_DELAY_MIN_MS="30",ROW_DELAY_MAX_MS="180",STOP_ON_GPT_QUOTA="1",ENABLE_TICKETMASTER="1",ENABLE_PLACES="1",ENABLE_EVENTBRITE="0",KEY_COL="name",NAME_COL="name",WEBSITE_COL="domain",ENRICH_STATUS_COL="enrichment_status" \
  --max-instances=1 --concurrency=1 --min-instances=1

SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
echo "Service URL: $SERVICE_URL"

# Smoke tests (same endpoints the app documents)  【turn3file54†source】
curl -sS "$SERVICE_URL/ping"; echo
curl -sS "$SERVICE_URL/ready"; echo
curl -sS "$SERVICE_URL/?limit=5&dry=1"; echo
