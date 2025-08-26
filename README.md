# RFP Data Enricher (Cloud Run)


Small Flask service that enriches BigQuery rows using OpenAI.


## Endpoints
- `GET /healthz` → `ok`
- `GET /?limit=25` → runs a batch (1–100)


## Required env
- `PROJECT_ID=rfp-database-464609`
- `DATASET_ID=rfpdata`
- `TABLE=performing_arts_fixed`
- `BQ_LOCATION=europe-southwest1`
- `OPENAI_API_KEY` (secret)
- `OPENAI_MODEL` (default `gpt-4o-mini`)


## Local run
```bash
pip install -r requirements.txt
export PROJECT_ID=rfp-database-464609 DATASET_ID=rfpdata TABLE=p...ming_arts_fixed BQ_LOCATION=europe-southwest1 OPENAI_API_KEY=...
python -m src.enrich_app
