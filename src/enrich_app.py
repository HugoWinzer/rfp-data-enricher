#!/usr/bin/env python3
import os
import time
import logging
from decimal import Decimal, InvalidOperation
from flask import Flask, request, jsonify
from google.cloud import bigquery

try:
    from .gpt_client import enrich_with_gpt  # package import (gunicorn)
except Exception:
    from gpt_client import enrich_with_gpt    # direct run


# ------------------------------------------------------------------------------
# Config & globals
# ------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID   = os.getenv("PROJECT_ID")
DATASET_ID   = os.getenv("DATASET_ID", "rfpdata")
TABLE        = os.getenv("TABLE", "performing_arts_fixed")
# REQUIRED: exact dataset location, e.g. "EU" or "europe-west1"
BQ_LOCATION  = os.getenv("BQ_LOCATION")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")
if not BQ_LOCATION:
    raise RuntimeError("BQ_LOCATION env var is required (e.g. 'EU' or 'europe-west1')")

bq = bigquery.Client(project=PROJECT_ID)

app = Flask(__name__)
# Accept both with/without trailing slashes for *all* routes
app.url_map.strict_slashes = False


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
        return Decimal(str(value))  # avoid passing float directly to NUMERIC
    except (InvalidOperation, ValueError, TypeError):
        return None

def _row_to_dict(row):
    try:
        return dict(row.items())
    except Exception:
        try:
            return dict(row)
        except Exception:
            return {}

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
    # IMPORTANT: pass location on the query call; do NOT set job_config.location
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    return list(job.result())

def _build_update_sql(for_fields):
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
    """Update one row with enriched values using parameterized query."""
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

    sql = _build_update_sql(set(fields_to_set))
    log.info("APPLY UPDATE for %s -> %s", name, fields_to_set)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    # IMPORTANT: pass location here too
    bq.query(sql, job_config=job_config, location=BQ_LOCATION).result()

def run_batch(limit: int) -> int:
    rows = fetch_rows(limit)
    processed = 0
    for r in rows:
        try:
            name = r["name"]
        except Exception:
            name = r.get("organization_name") if isinstance(r, dict) else None

        row_dict = _row_to_dict(r)
        enriched = {"enrichment_status": "NO_DATA"}
        try:
            suggestion = enrich_with_gpt(name=name, row=row_dict, model=OPENAI_MODEL)
            if suggestion:
                enriched.update({k: v for k, v in suggestion.items() if v not in (None, "", {})})
                if any(enriched.get(k) for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
                    enriched["enrichment_status"] = "OK"
        except Exception as e:
            log.warning("gpt failed: %s", e)

        try:
            update_in_place(r, enriched)
            # tiny pause to ease BQ DML pressure (tune/remove if desired)
            time.sleep(0.05)
            processed += 1
        except Exception as e:
            log.error("Failed row: %s: %s", name, e, exc_info=True)
    return processed


# ------------------------------------------------------------------------------
# Routes (no dependency on /healthz required)
# ------------------------------------------------------------------------------

# Simple liveness — use this instead of /healthz
@app.route("/ping", methods=["GET", "HEAD"])
def ping():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})

# Readiness — actually checks BigQuery connectivity in your dataset location
@app.get("/ready")
def ready():
    try:
        # fastest possible query; location must match dataset location
        bq.query("SELECT 1", location=BQ_LOCATION).result()
        return jsonify({"ready": True, "bq_location": BQ_LOCATION}), 200
    except Exception as e:
        log.warning("ready check failed: %s", e)
        return jsonify({"ready": False, "error": str(e)}), 503

# Main endpoint: process a batch
@app.route("/", methods=["GET", "HEAD"])
def root():
    # HEAD => OK without doing work
    if request.method == "HEAD":
        return ("", 200, {})
    try:
        limit = int(request.args.get("limit", "10"))
    except ValueError:
        limit = 10
    # optional dry-run (no updates) if you ever need it: /?limit=10&dry=1
    dry = request.args.get("dry") in ("1", "true", "True", "yes")
    try:
        if dry:
            count = len(fetch_rows(limit))
            return jsonify({"processed": 0, "candidates": count, "status": "DRY_OK"}), 200
        count = run_batch(limit)
        return jsonify({"processed": count, "status": "OK"}), 200
    except Exception as e:
        log.exception("Batch failed")
        return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500


# --- Compatibility aliases (you can ignore these) -----------------------------
# They just return 200 so external health checks never 404 again.
@app.get("/healthz")
@app.get("/healthz/")
@app.get("/_ah/health")
def _health_compat():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
