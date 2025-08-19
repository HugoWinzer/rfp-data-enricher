# src/enrich_app.py
import os
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List

from flask import Flask, request, jsonify
from google.cloud import bigquery

from .gpt_client import call_gpt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("enrich")

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
TABLE = os.environ["STAGING_TABLE"]  # e.g. performing_arts_fixed
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"

bq = bigquery.Client(project=PROJECT_ID)

app = Flask(__name__)

def _to_decimal(v):
    if v is None:
        return None
    try:
        # Use string to avoid float precision issues; BigQuery NUMERIC expects Decimal
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None

def _to_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

def _select_rows(limit: int) -> List[bigquery.table.Row]:
    """
    Pull a small batch of rows that need enrichment.
    Assumes a column 'enrichment_status' exists, marking 'DONE' when processed.
    """
    sql = f"""
    SELECT name
    FROM `{TABLE_ID}`
    WHERE COALESCE(enrichment_status, 'PENDING') != 'DONE'
    ORDER BY last_updated IS NULL DESC, last_updated ASC
    LIMIT @limit
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
        ),
    )
    return list(job.result())

def _update_in_place(name: str, enriched: Dict[str, Any]):
    """
    Update only the fields we confidently have. Stays within known columns.
    Known updatable fields here:
      - ticket_vendor (STRING) + ticket_vendor_source (STRING)
      - capacity (INT64)       + capacity_source (STRING)
      - avg_ticket_price (NUMERIC) + avg_ticket_price_source (STRING)
      - enrichment_status (STRING), last_updated (TIMESTAMP)
    """
    set_clauses = ["enrichment_status = 'DONE'", "last_updated = CURRENT_TIMESTAMP()"]
    params: List[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("name", "STRING", name)
    ]

    # ticket_vendor
    vendor = enriched.get("ticket_vendor")
    if isinstance(vendor, str) and vendor.strip():
        set_clauses += [
            "ticket_vendor = @ticket_vendor",
            "ticket_vendor_source = 'GPT'",
        ]
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", vendor.strip()))

    # capacity
    cap_val = _to_int(enriched.get("capacity"))
    if cap_val is not None and cap_val >= 0:
        set_clauses += [
            "capacity = @capacity",
            "capacity_source = 'GPT'",
        ]
        params.append(bigquery.ScalarQueryParameter("capacity", "INT64", cap_val))

    # avg_ticket_price (NUMERIC)
    price_val = _to_decimal(enriched.get("avg_ticket_price"))
    if price_val is not None and price_val >= 0:
        set_clauses += [
            "avg_ticket_price = @avg_ticket_price",
            "avg_ticket_price_source = 'GPT'",
        ]
        params.append(bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", price_val))

    # Build and run update only if we have something to set (we always set status + timestamp)
    sql = f"""
    UPDATE `{TABLE_ID}`
    SET {", ".join(set_clauses)}
    WHERE name = @name
    """
    logger.info("APPLY UPDATE for %s -> %s", name, [c.split("=")[0].strip() for c in set_clauses])
    bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

@app.route("/", methods=["GET"])
def run_batch():
    limit = request.args.get("limit", "20")
    try:
        limit = max(1, int(limit))
    except Exception:
        limit = 20

    logger.info("=== UPDATE MODE: no inserts; BigQuery UPDATE only ===")
    rows = _select_rows(limit)
    logger.info("Processing %d rows", len(rows))

    processed = 0
    for r in rows:
        name = r["name"] if isinstance(r, dict) else r.name
        try:
            enriched = call_gpt(name)
            logger.info("GPT raw output for '%s': %s", name, enriched)
        except Exception as e:
            logger.warning("gpt failed for '%s': %s", name, e)
            enriched = {}

        try:
            _update_in_place(name, enriched)
            processed += 1
        except Exception as e:
            logger.error("Failed row: %s: %s", name, e, exc_info=True)

    return jsonify({"processed": processed, "status": "OK"})

if __name__ == "__main__":
    # Dev-only. In Cloud Run we use gunicorn (see Dockerfile).
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
