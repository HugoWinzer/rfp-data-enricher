# src/enrich_app.py
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery

# ---- logging ----
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enricher")

# ---- env ----
PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("STAGING_TABLE", "performing_arts_fixed")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # good/cheap JSON model

# ---- clients ----
bq = bigquery.Client(project=PROJECT_ID)

# Resolve dataset location so all queries pin to the right region
try:
    _ds = bq.get_dataset(bigquery.DatasetReference(PROJECT_ID, DATASET_ID))
    BQ_LOCATION = _ds.location
except Exception as e:
    log.warning("Could not read dataset location; defaulting to None: %s", e)
    BQ_LOCATION = None

# OpenAI v1 client (optional)
_client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        _client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        log.warning("OpenAI client init failed: %s", e)
else:
    log.info("OPENAI_API_KEY not set; GPT fallback disabled.")

app = Flask(__name__)

PROMPT = """You are enriching a performing arts organization record.
Return a STRICT JSON object with these keys only:
- ticket_vendor (string or null; e.g., "Ticketmaster", "Eventbrite")
- capacity (integer or null)
- avg_ticket_price (number or null; typical single-ticket price)
- annual_revenue (number or null; whole org revenue)
- ticketing_revenue (number or null; ticket sales revenue)

Infer only if you are reasonably confident. Otherwise use null.
"""

def gpt_enrich(name: str, domain: Optional[str], description: Optional[str]) -> Dict[str, Any]:
    """Return dict with fields above or {} if GPT unavailable/error."""
    if not _client:
        return {}
    try:
        user_ctx = {
            "name": name,
            "domain": domain,
            "description": description
        }
        resp = _client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": json.dumps(user_ctx, ensure_ascii=False)}
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content
        log.info("GPT raw output for '%s': %s", name, content)
        data = json.loads(content)
        # sanitize types
        out = {
            "ticket_vendor": data.get("ticket_vendor"),
            "capacity": int(data["capacity"]) if data.get("capacity") is not None else None,
            "avg_ticket_price": float(data["avg_ticket_price"]) if data.get("avg_ticket_price") is not None else None,
            "annual_revenue": float(data["annual_revenue"]) if data.get("annual_revenue") is not None else None,
            "ticketing_revenue": float(data["ticketing_revenue"]) if data.get("ticketing_revenue") is not None else None,
        }
        return out
    except Exception as e:
        log.warning("gpt failed: %s", e)
        return {}

def update_in_place(row: Dict[str, Any], enriched: Dict[str, Any]) -> None:
    """Update a single row in BigQuery with CASTs to avoid type errors."""
    name = row["name"]

    # decide status and sources
    found_any = any(enriched.get(k) is not None for k in
                    ("ticket_vendor", "capacity", "avg_ticket_price", "annual_revenue", "ticketing_revenue"))
    status = "DONE" if found_any else "NO_DATA"

    # sources (put the model when present)
    vendor_src = OPENAI_MODEL if enriched.get("ticket_vendor") is not None else None
    cap_src = OPENAI_MODEL if enriched.get("capacity") is not None else None
    price_src = OPENAI_MODEL if enriched.get("avg_ticket_price") is not None else None
    ann_src = OPENAI_MODEL if enriched.get("annual_revenue") is not None else None
    tick_src = OPENAI_MODEL if enriched.get("ticketing_revenue") is not None else None

    q = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
    SET
      ticket_vendor = @ticket_vendor,
      ticket_vendor_source = @ticket_vendor_source,
      capacity = CAST(@capacity AS INT64),
      capacity_source = @capacity_source,
      avg_ticket_price = CAST(@avg_ticket_price AS NUMERIC),
      avg_ticket_price_source = @avg_ticket_price_source,
      annual_revenue = CAST(@annual_revenue AS NUMERIC),
      annual_revenue_source = @annual_revenue_source,
      ticketing_revenue = CAST(@ticketing_revenue AS NUMERIC),
      ticketing_revenue_source = @ticketing_revenue_source,
      enrichment_status = @enrichment_status,
      last_updated = CURRENT_TIMESTAMP()
    WHERE name = @name
    """
    params = [
        bigquery.ScalarQueryParameter("ticket_vendor", "STRING", enriched.get("ticket_vendor")),
        bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", vendor_src),
        bigquery.ScalarQueryParameter("capacity", "INT64", enriched.get("capacity")),
        bigquery.ScalarQueryParameter("capacity_source", "STRING", cap_src),
        bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", enriched.get("avg_ticket_price")),
        bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", price_src),
        bigquery.ScalarQueryParameter("annual_revenue", "NUMERIC", enriched.get("annual_revenue")),
        bigquery.ScalarQueryParameter("annual_revenue_source", "STRING", ann_src),
        bigquery.ScalarQueryParameter("ticketing_revenue", "NUMERIC", enriched.get("ticketing_revenue")),
        bigquery.ScalarQueryParameter("ticketing_revenue_source", "STRING", tick_src),
        bigquery.ScalarQueryParameter("enrichment_status", "STRING", status),
        bigquery.ScalarQueryParameter("name", "STRING", name),
    ]
    job_conf = bigquery.QueryJobConfig(query_parameters=params)
    log.info("APPLY UPDATE for %s -> %s", name, [k for k, v in enriched.items() if v is not None] + ["enrichment_status", "last_updated"])
    bq.query(q, job_config=job_conf, location=BQ_LOCATION).result()

def run_batch(limit: int) -> int:
    log.info("=== UPDATE MODE: no inserts; BigQuery UPDATE only ===")
    sel = f"""
    SELECT name, domain, full_description
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
    WHERE COALESCE(enrichment_status, 'PENDING') IN ('PENDING', 'RETRY', 'NO_DATA')
    ORDER BY last_updated NULLS FIRST
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    rows = list(bq.query(sel, job_config=bigquery.QueryJobConfig(query_parameters=params), location=BQ_LOCATION).result())
    log.info("Processing %d rows", len(rows))

    processed = 0
    for r in rows:
        record = {"name": r["name"], "domain": r.get("domain"), "full_description": r.get("full_description")}
        enriched = gpt_enrich(record["name"], record.get("domain"), record.get("full_description"))
        update_in_place(record, enriched)
        processed += 1
    return processed

@app.route("/", methods=["GET"])
def index():
    try:
        limit = int(request.args.get("limit", "20"))
    except Exception:
        limit = 20
    n = run_batch(limit)
    return jsonify({"status": "OK", "processed": n})

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
