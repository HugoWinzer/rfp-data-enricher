cat > scripts/deploy.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

# You can override these by exporting beforehand
PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
DATASET_ID="${DATASET_ID:-rfpdata}"
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
  --set-env-vars \
"PROJECT_ID=$PROJECT_ID,DATASET_ID=$DATASET_ID,RAW_TABLE=performing_arts_raw,STAGING_TABLE=performing_arts_enriched_staging,OPENAI_API_KEY=$OPENAI_API_KEY"

echo "✅ Deployment complete"
EOF
