#!/usr/bin/env bash
set -euo pipefail


PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-rfp-data-enricher}"
BQ_LOCATION="${BQ_LOCATION:-europe-southwest1}"


gcloud config set project "$PROJECT_ID"


# Build
gcloud builds submit --tag "gcr.io/$PROJECT_ID/$SERVICE:latest"


# Deploy (secrets via Secret Manager, no plaintext keys)
gcloud run deploy "$SERVICE" \
--image "gcr.io/$PROJECT_ID/$SERVICE:latest" \
--region "$REGION" \
--allow-unauthenticated \
--set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest,EVENTBRITE_TOKEN=eventbrite-token:latest \
--set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="rfpdata",TABLE="performing_arts_fixed",BQ_LOCATION="$BQ_LOCATION",OPENAI_MODEL="gpt-4o-mini",ROW_DELAY_MIN_MS="30",ROW_DELAY_MAX_MS="180",STOP_ON_GPT_QUOTA="1",ENABLE_TICKETMASTER="1",ENABLE_PLACES="1",ENABLE_EVENTBRITE="1" \
--max-instances=1 --concurrency=1 --min-instances=1


# Print URL
SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
echo "Service URL: $SERVICE_URL"


# Smoke test
curl -sS "$SERVICE_URL/ping"; echo
curl -sS "$SERVICE_URL/ready"; echo
curl -sS "$SERVICE_URL/?limit=5&dry=1"; echo
curl -sS "$SERVICE_URL/?limit=5"; echo


# Tail logs for API hits
gcloud run services logs tail "$SERVICE" --region "$REGION" --since=20m | egrep -i 'API_TM|API_EB|API_PLACES|APPLY UPDATE|GPT quota|429'
