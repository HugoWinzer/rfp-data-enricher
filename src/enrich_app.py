import os
import json
import logging
import random
import time
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Tuple

from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import TooManyRequests, ServiceUnavailable, GoogleAPIError

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

# MUST match dataset region (you told me it's europe-southwest1)
BQ_LOCATION = os.environ.get("BQ_LOCATION")

# Tuning knobs (safe defaults)
ROW_DELAY_MIN_MS = int(os.environ.get("ROW_DELAY_MIN_MS", "50"))
ROW_DELAY_MAX_MS = int(os.environ.get("ROW_DELAY_MAX_MS", "250"))
BQ_MAX_RETRIES = int(os.environ.get("BQ_MAX_RETRIES", "5"))
BQ_BACKOFF_BASE = float(os.environ.get("BQ_BACKOFF_BASE", "0.5"))
BQ_BACKOFF_CAP = float(os.environ.get("BQ_BACKOFF_CAP", "8.0"))

bq = bigquery.Client(project=PROJECT_ID)
app = Flask(__name__)


# ------------------------------------------------------------------------------ 
# Helpers
# ------------------------------------------------------------------------------

def _to_decimal(val):
    """Safe Decimal for BigQuery NUMERIC."""
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _bq_query(sql: str, job_config: bigquery.QueryJobConfig):
    """Query with exponential backoff on 429/503."""
    attempt = 0
    while True:
        try:
            return bq.query(sql, job_config=job_config).result()
        except (TooManyRequests, ServiceUnavailable) as e:
            if attempt >= BQ_MAX_RETRIES:
                raise
            sleep = min(BQ_BACKOFF_CAP, BQ_BACKOFF_BASE * (2 ** attempt)) * (0.5 + random.random())
            log.warning("BigQuery transient error (%s). Retry in %.2fs", type(e).__name__, sleep)
            time.sleep(sleep)
            attempt += 1
        except GoogleAPIError:
            # Non-retryable
            raise


def _gpt_enrich(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Wrap model call and tag sources."""
    try:
        out = enrich_with_gpt(row)
    except Exception as e:
        log.warning("gpt error: %s", e)
        return {"enrichment_status": "NO_DATA"}, {}

    enriched: Dict[str, Any] = {}
    sources: Dict[str, str] = {}

    for key in ("ticket_vendor", "capacity", "avg_ticket_price"):
        val = out.get(key)
        if val not in (None, ""):
            enriched[key] = val
            sources[f"{key}_source"] = "GPT"

    enriched["enrichment_status"] = (
        "DONE" if any(k in enriched for k in ("ticket_vendor", "capacity", "avg_ticket_price")) else "NO_DATA"
    )
    return enriched, sources


def fetch_rows(limit: int):
    sql = f"""
    SELECT * FROM `{TABLE_FQN}`
    WHERE (ticket_vendor IS NULL OR capacity IS NULL OR avg_ticket_price IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status != 'NO_DATA')
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    if BQ_LOCATION:
        job_config.location = BQ_LOCATION
    return list(_bq_query(sql, job_config))


def update_in_place(row, enriched: Dict[str, Any], sources: Dict[str, str]):
    sets = ["last_updated = CURRENT_TIMESTAMP()"]
    params = []

    if enriched.get("ticket_vendor") is not None:
        sets.append("ticket_vendor = @ticket_vendor")
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", enriched["ticket_vendor"]))
        src = sources.get("ticket_vendor_source")
        if src:
            sets.append("ticket_vendor_source = @ticket_vendor_source")
            params.append(bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", src))

    if enriched.get("capacity") is not None:
        sets.append("capacity = @capacity")
        params.append(bigquery.ScalarQueryParameter("capacity", "INT64", int(enriched["capacity"])))
        src = sources.get("capacity_source")
        if src:
            sets.append("capacity_source = @capacity_source")
            params.append(bigquery.ScalarQueryParameter("capacity_source", "STRING", src))

    if enriched.get("avg_ticket_price") is not None:
        price_dec = _to_decimal(enriched["avg_ticket_price"])
        sets.append("avg_ticket_price = CAST(@avg_ticket_price AS NUMERIC)")
        params.append(bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", price_dec))
        src = sources.get("avg_ticket_price_source")
        if src:
            sets.append("avg_ticket_price_source = @avg_ticket_price_source")
            params.append(bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", src))

    if enriched.get("enrichment_status") is not None:
        sets.append("enrichment_status = @enrichment_status")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", enriched["enrichment_status"]))

    if len(sets) == 1:
        sets.append("enrichment_status = COALESCE(@enrichment_status, enrichment_status)")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", enriched.get("enrichment_status", "NO_DATA")))

    # Identify row key; adjust if your PK differs
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
    if BQ_LOCATION:
        job_config.location = BQ_LOCATION

    _bq_query(q, job_config)

    changed_cols = [frag.split("=")[0].strip() for frag in sets]
    log.info("APPLY UPDATE for %s -> %s", key_val, changed_cols)


def run_batch(limit: int):
    rows = fetch_rows(limit)
    log.info("=== UPDATE MODE: BigQuery UPDATE only ===")
    log.info("Processing %d rows", len(rows))

    processed = 0
    for r in rows:
        row_dict = dict(r.items()) if hasattr(r, "items") else dict(r)
        enriched, sources = _gpt_enrich(row_dict)
        try:
            update_in_place(row_dict, enriched, sources)
            processed += 1
        except Exception as e:
            key = row_dict.get("name") or row_dict.get("id")
            log.error("Failed row: %s: %s", key, e)

        # small jitter to reduce 429s per instance
        delay_ms = random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
        time.sleep(delay_ms / 1000.0)

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
