import os
import sys
import json
import datetime
import logging

from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import openai

# ── Logging setup ──────────────────────────────────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ── Env var config ────────────────────────────────────────────────────
required_vars = ["PROJECT_ID", "DATASET_ID", "RAW_TABLE", "STAGING_TABLE", "OPENAI_API_KEY"]
missing = [v for v in required_vars if v not in os.environ]
if missing:
    logger.error(f"Missing required env vars: {missing}")
    sys.exit(1)

PROJECT_ID    = os.environ["PROJECT_ID"]
DATASET_ID    = os.environ["DATASET_ID"]
RAW_TABLE     = os.environ["RAW_TABLE"]
STAGING_TABLE = os.environ["STAGING_TABLE"]
openai.api_key = os.environ["OPENAI_API_KEY"]

# ── BigQuery client ───────────────────────────────────────────────────
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    sys.exit(1)

app = Flask(__name__)

# ── Helper: fetch rows from raw table ─────────────────────────────────
def fetch_rows(limit: int):
    query = (
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}` "
        f"LIMIT {limit}"
    )
    try:
        job = bq_client.query(query)
        results = job.result()
        return [dict(row) for row in results]
    except GoogleAPIError as e:
        logger.error(f"BigQuery fetch_rows error: {e}")
        raise

# ── Helper: call OpenAI to extract JSON ───────────────────────────────
def call_gpt(row: dict):
    prompt = f"""
Extract these fields and return only valid JSON with keys:
  - avg_ticket_price
  - capacity
  - ticket_vendor
  - annual_revenue
  - ticketing_revenue

If unknown, set value to null.

Venue data:
Name: {row.get('name')}
Alt name: {row.get('alt_name') or ''}
Category: {row.get('category') or ''}
Sub-category: {row.get('sub_category') or ''}
Short description: {row.get('short_description') or ''}
Full description: {row.get('full_description') or ''}
Phone number: {row.get('phone_number') or ''}
Domain: {row.get('domain') or ''}
LinkedIn URL: {row.get('linkedin_url') or ''}
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data extraction assistant."},
                {"role": "user",   "content": prompt}
            ],
            temperature=0.0,
            max_tokens=200
        )
        text = resp.choices[0].message.content.strip()
        data = json.loads(text)
        # Ensure all keys present
        for k in ["avg_ticket_price", "capacity", "ticket_vendor",
                  "annual_revenue", "ticketing_revenue"]:
            if k not in data:
                data[k] = None
        return data
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e} | resp: {text}")
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
    # On any error, return all-fields-null
    return {k: None for k in [
        "avg_ticket_price", "capacity", "ticket_vendor",
        "annual_revenue", "ticketing_revenue"
    ]}

# ── Helper: write one row into staging table ──────────────────────────
def write_row(raw: dict, enriched: dict):
    record = raw.copy()
    record.update(enriched)
    record["enrichment_status"] = "DONE"
    record["last_updated"] = datetime.datetime.utcnow().isoformat()
    try:
        errors = bq_client.insert_rows_json(
            f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}",
            [record]
        )
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
    except Exception as e:
        logger.error(f"BigQuery insert exception: {e}")
        raise

# ── API endpoint: run one batch ───────────────────────────────────────
@app.route("/", methods=["GET"])
def run_batch():
    # Validate & parse limit
    limit_str = request.args.get("limit", "10")
    try:
        limit = int(limit_str)
        assert limit > 0
    except Exception:
        return jsonify(error="Invalid limit parameter"), 400

    try:
        rows = fetch_rows(limit)
    except Exception:
        return jsonify(error="Failed to fetch rows"), 500

    processed = 0
    for r in rows:
        enriched = call_gpt(r)
        try:
            write_row(r, enriched)
            processed += 1
        except Exception:
            # skip failed writes
            continue

    return jsonify(processed=processed, status="OK")

# ── Entrypoint ────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting server on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)
