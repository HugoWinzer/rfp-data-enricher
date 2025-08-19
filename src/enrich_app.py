# src/enrich_app.py
import os
import json
import logging
import random
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Tuple

from flask import Flask, request, jsonify
from google.cloud import bigquery
from openai import OpenAI

from .gpt_client import enrich_with_gpt

# ------------------------------------------------------------------------------
# Config & globals
# ------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID", "rfpdata")
TABLE = os.environ.get("TABLE", "performing_arts_fixed")
TABLE_FQN = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
BQ_LOCATION = os.environ.get("BQ_LOCATION")  # e.g. "europe-southwest1"

ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "0"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "0"))

bq = bigquery.Client(project=PROJECT_ID)
oa = OpenAI(  # not used directly here, but triggers early key validation if missing
    max_retries=int(os.getenv("OPENAI_MAX_RETRIES", "5")),
    timeout=float(os.getenv("OPENAI_TIMEOUT", "30")),
)

app = Flask(__name__)

# Convenience kwargs for every BigQuery call
_BQ_KW = {"location": BQ_LOCATION} if BQ_LOCATION else {}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _to_decimal(val: Any):
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None


def gpt_enrich(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Run GPT and return (enriched_fields, source_fields)."""
    try:
        enriched = enrich_with_gpt(row, OPENAI_MODEL)
    except Exception as e:
        log.warning("gpt failed:\n%s", e)
        return {"enrichment_status": "NO_DATA"}, {}

    sources: Dict[str, Any] = {}
    if "ticket_vendor" in enriched:
        sources["ticket_vendor_source"] = "GPT"
    if "capacity" in enriched:
        sources["capacity_source"] = "GPT"
    if "avg_ticket_price" in enriched:
        sources["avg_ticket_price_source"] = "GPT"

    if any(k in enriched for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
        enriched["enrichment_status"] = "DONE"
    else:
        enriched["enrichment_status"] = "NO_DATA"

    return enriched, sources


def fetch_rows(limit: int):
    sql = f"""
    SELECT *
    FROM `{TABLE_FQN}`
    WHERE
      (ticket_vendor IS NULL OR capacity IS NULL OR avg_ticket_price IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status != 'NO_DATA')
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return list(bq.query(sql, job_config=job_config, **_BQ_KW).result())


def update_in_place(row: Dict[str, Any], enriched: Dict[str, Any], sources: Dict[str, Any]):
    sets = ["last_updated = CURRENT_TIMESTAMP()"]
    params = []

    # ticket_vendor
    if enriched.get("ticket_vendor") is not None:
        sets.append("ticket_vendor = @ticket_vendor")
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", enriched["ticket_vendor"]))
        src = sources.get("ticket_vendor_source")
        if src:
            sets.append("ticket_vendor_source = @ticket_vendor_source")
            params.append(bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", src))

    # capacity
    if enriched.get("capacity") is not None:
        sets.append("capacity = @capacity")
        params.append(bigquery.ScalarQueryParameter("capacity", "INT64", int(enriched["capacity"])))
        src = sources.get("capacity_source")
        if src:
            sets.append("capacity_source = @capacity_source")
            params.append(bigquery.ScalarQueryParameter("capacity_source", "STRING", src))

    # avg_ticket_price (NUMERIC-safe)
    if "avg_ticket_price" in enriched:
        price_dec = _to_decimal(enriched.get("avg_ticket_price"))
        if price_dec is not None:
            sets.append("avg_ticket_price = CAST(@avg_ticket_price AS NUMERIC)")
            params.append(bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", price_dec))
            src = sources.get("avg_ticket_price_source")
            if src:
                sets.append("avg_ticket_price_source = @avg_ticket_price_source")
                params.append(bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", src))
        else:
            log.info("Skip avg_ticket_price update: not a valid Decimal for row key candidate")

    # enrichment_status
    if enriched.get("enrichment_status") is not None:
        sets.append("enrichment_status = @enrichment_status")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", enriched["enrichment_status"]))

    if len(sets) == 1:
        # Nothing to update → preserve/mark status
        sets.append("enrichment_status = COALESCE(@enrichment_status, enrichment_status)")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", enriched.get("enrichment_status", "NO_DATA")))

    # Identify row
    where_col = "name"
    key_val = row.get(where_col) if isinstance(row, dict) else getattr(row, where_col, None)
    if key_val is None:
        where_col = "id"
        key_val = row.get(where_col) if isinstance(row, dict) else getattr(row, where_col, None)
    if key_val is None:
        raise RuntimeError("Cannot identify row key: expected 'name' or 'id' in table.")

    params.append(bigquery.ScalarQueryParameter("key", "STRING", str(key_val)))

    q = f"""
    UPDATE `{TABLE_FQN}`
    SET {", ".join(sets)}
    WHERE {where_col} = @key
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq.query(q, job_config=job_config, **_BQ_KW).result()

    changed_cols = [frag.split("=")[0].strip() for frag in sets]
    log.info("APPLY UPDATE for %s -> %s", key_val, changed_cols)


def run_batch(limit: int) -> int:
    rows = fetch_rows(limit)
    log.info("=== UPDATE MODE: no inserts; BigQuery UPDATE only ===")
    log.info("Processing %d rows", len(rows))

    processed = 0
    for r in rows:
        row_dict = dict(r.items()) if hasattr(r, "items") else dict(r)
        enriched, sources = gpt_enrich(row_dict)
        try:
            update_in_place(row_dict, enriched, sources)
            processed += 1
        except Exception as e:
            key = row_dict.get("name") or row_dict.get("id")
            log.error("Failed row: %s: %s", key, e)

        if ROW_DELAY_MAX_MS > 0:
            # Avoid hammering upstream APIs (why: 429s)
            jitter_ms = random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
            time.sleep(jitter_ms / 1000.0)

    return processed

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.get("/")
def root():
    try:
        limit = int(request.args.get("limit", "25"))
        limit = max(1, min(limit, 100))
    except Exception:
        limit = 25

    try:
        count = run_batch(limit)
        return jsonify({"processed": count, "status": "OK"}), 200
    except Exception as e:
        log.exception("Batch failed")
        return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
