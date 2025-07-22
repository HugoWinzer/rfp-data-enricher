# RFP Data Enricher

**Goal:**  
Read raw venue rows from BigQuery, call the OpenAI API to extract hard-to-find fields  
(avg_ticket_price, capacity, ticket_vendor, annual_revenue, ticketing_revenue),  
and write them back into an enriched staging table.

---

## Repo Layout

## Usage

1. **Edit code** in this GitHub repo.  
2. In Cloud Shell:
    ```bash
    git clone https://github.com/HugoWinzer/rfp-data-enricher.git
    cd rfp-data-enricher
    export OPENAI_API_KEY="sk-…"
    ./scripts/deploy.sh
    ```
   This builds the container and deploys to Cloud Run automatically.

## Required Environment Variables

- `PROJECT_ID` (e.g. `rfp-database-464609`)  
- `DATASET_ID` (e.g. `rfpdata`)  
- `RAW_TABLE` (e.g. `performing_arts_raw` or `museums_raw`)  
- `STAGING_TABLE` (e.g. `performing_arts_enriched_staging`)  
- `OPENAI_API_KEY`

