#!/usr/bin/env python3
import os
import json
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from google.cloud import bigquery

try:
    from .gpt_client import enrich_with_gpt  # when run by gunicorn: package style
except Exception:
    from gpt_client import enrich_with_gpt  # when run directly

# ------------------------------------------------------------------------------
# Config & globals
# ------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID   = os.getenv("PROJECT_ID")
DATASET_ID   = os.getenv("DATASET_ID", "rfpdata")
TABLE        = os.getenv("TABLE", "performing_arts_fixed")
BQ_LOCATION  = os.getenv("BQ_LOCATION", "europe-southwest1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")

bq = bigquery.Client(project=PROJECT_ID)

app = Flask(__name__)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def table_fqdn() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"

def _to_decimal(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        # Ensure we never pass float to NUMERIC
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None

def fetch_rows(limit: int):
    """Fetch candidate rows to enrich."""
    sql = f"""
    SELECT *
    FROM {table_fqdn()}
    WHERE
      (avg_ticket_price IS NULL OR capacity IS NULL OR ticket_vendor IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status NOT IN ("LOCKED"))
    ORDER BY COALESCE(last_updated, TIMESTAMP('1970-01-01')) ASC
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    return list(job.result())

def _build_update_sql(for_fields):
    """Create an UPDATE statement with only the fields we want to set."""
    sets = []
    if "ticket_vendor" in for_fields:
        sets.append("ticket_vendor = @ticket_vendor, ticket_vendor_source = 'GPT'")
    if "capacity" in for_fields:
        sets.append("capacity = @capacity, capacity_source = 'GPT'")
    if "avg_ticket_price" in for_fields:
        sets.append("avg_ticket_price = @avg_ticket_price, avg_ticket_price_source = 'GPT'")
    if "enrichment_status" in for_fields:
        sets.append("enrichment_status = @enrichment_status")
    sets.append("last_updated = CURRENT_TIMESTAMP()")
    set_clause = ", ".join(sets)
    sql = f"""
    UPDATE {table_fqdn()}
    SET {set_clause}
    WHERE name = @name
    """
    return sql

def update_in_place(row, enriched: dict):
    """Update a single row with enriched values using parameterized query."""
    name = row.get("name") if isinstance(row, dict) else row["name"]
    fields_to_set = []
    params = [bigquery.ScalarQueryParameter("name", "STRING", name)]

    # ticket vendor
    tv = enriched.get("ticket_vendor")
    if tv:
        fields_to_set.append("ticket_vendor")
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", tv))

    # capacity
    cap = enriched.get("capacity")
    if cap is not None:
        try:
            cap_int = int(cap)
            fields_to_set.append("capacity")
            params.append(bigquery.ScalarQueryParameter("capacity", "INT64", cap_int))
        except Exception:
            pass

    # avg ticket price (NUMERIC)
    price = enriched.get("avg_ticket_price")
    if price is not None:
        dec = _to_decimal(price)
        if dec is not None:
            fields_to_set.append("avg_ticket_price")
            params.append(bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", dec))

    # enrichment_status
    status = enriched.get("enrichment_status", "OK")
    fields_to_set.append("enrichment_status")
    params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", status))

    if not fields_to_set:
        # still mark last_updated + status
        fields_to_set.append("enrichment_status")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", "NO_DATA"))

    sql = _build_update_sql(set(for_fields := fields_to_set))
    log.info("APPLY UPDATE for %s -> %s", name, for_fields)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq.query(sql, job_config=job_config, location=BQ_LOCATION).result()

def run_batch(limit: int) -> int:
    rows = fetch_rows(limit)
    processed = 0
    for r in rows:
        try:
            name = r["name"]
        except Exception:
            # fallback if column named differently
            name = r.get("organization_name") if isinstance(r, dict) else None
        enriched = {"enrichment_status": "NO_DATA"}
        try:
            suggestion = enrich_with_gpt(name=name, row=dict(r), model=OPENAI_MODEL)
            if suggestion:
                enriched.update({k: v for k, v in suggestion.items() if v not in (None, "", {})})
                if any(enriched.get(k) for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
                    enriched["enrichment_status"] = "OK"
        except Exception as e:
            log.warning("gpt failed: %s", e)

        try:
            update_in_place(r, enriched)
            processed += 1
        except Exception as e:
            log.error("Failed row: %s: %s", name, e, exc_info=True)
    return processed

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.get("/healthz")
def healthz():
    return "ok", 200, {"Content-Type": "text/plain; charset=utf-8"}

@app.get("/")
def root():
    try:
        limit = int(request.args.get("limit", "10"))
    except ValueError:
        limit = 10
    try:
        count = run_batch(limit)
        return jsonify({"processed": count, "status": "OK"}), 200
    except Exception as e:
        log.exception("Batch failed")
        return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500

# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # Local dev: run Flask directly
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
