# scripts/deploy_madrid.sh
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-rfp-data-enricher-madrid}"
BQ_LOCATION="${BQ_LOCATION:-europe-southwest1}"

gcloud config set project "$PROJECT_ID"

# Build with alt Dockerfile so your main service is untouched
gcloud builds submit --tag "gcr.io/$PROJECT_ID/$SERVICE:latest" --gcs-log-dir="gs://$PROJECT_ID-cloudbuild-logs" --timeout=1200 \
  --config <(cat <<'YAML'
steps:
- name: 'gcr.io/cloud-builders/docker'
  args: ['build','-f','Dockerfile.madrid','-t','gcr.io/$PROJECT_ID/$SERVICE:latest','.']
images: ['gcr.io/$PROJECT_ID/$SERVICE:latest']
YAML
)

gcloud run deploy "$SERVICE" \
  --image "gcr.io/$PROJECT_ID/$SERVICE:latest" \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-secrets OPENAI_API_KEY=openai-api-key:latest,TICKETMASTER_KEY=ticketmaster-key:latest,GOOGLE_PLACES_KEY=google-places-key:latest,EVENTBRITE_TOKEN=eventbrite-token:latest \
  --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="rfpdata",TABLE="performing_arts_madrid",BQ_LOCATION="$BQ_LOCATION",OPENAI_MODEL="gpt-4o-mini",STOP_ON_GPT_QUOTA="1" \
  --max-instances=1 --concurrency=1 --min-instances=1

SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
echo "Service URL: $SERVICE_URL"

# Smoke tests (same pattern as your base service)
curl -sS "$SERVICE_URL/ping"; echo
curl -sS "$SERVICE_URL/ready"; echo
curl -sS "$SERVICE_URL/?limit=5&dry=1"; echo

