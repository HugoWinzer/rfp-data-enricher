#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, List

from flask import Flask, request, jsonify
from google.cloud import bigquery

try:
    from .gpt_client import enrich_with_gpt
    from .extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
    )
except Exception:
    from gpt_client import enrich_with_gpt
    from extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "performing_arts_fixed")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g. "EU" or "europe-west1"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")
if not BQ_LOCATION:
    raise RuntimeError("BQ_LOCATION env var is required (e.g. 'EU' or 'europe-west1')")

bq = bigquery.Client(project=PROJECT_ID)
app = Flask(__name__)
app.url_map.strict_slashes = False


def table_fqdn() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _row_to_dict(row: Any) -> Dict[str, Any]:
    try:
        return dict(row.items())
    except Exception:
        try:
            return dict(row)
        except Exception:
            return {}


def fetch_rows(limit: int):
    sql = f"""
    SELECT *
    FROM {table_fqdn()}
    WHERE
      (avg_ticket_price IS NULL OR capacity IS NULL OR ticket_vendor IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status NOT IN ('LOCKED'))
    ORDER BY COALESCE(last_updated, TIMESTAMP('1970-01-01')) ASC
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    return list(job.result())


def _build_update_sql(for_fields: List[str], use_id: bool) -> str:
    sets = []
    if "ticket_vendor" in for_fields:
        sets.append("ticket_vendor = @ticket_vendor")
    if "capacity" in for_fields:
        sets.append("capacity = @capacity")
    if "avg_ticket_price" in for_fields:
        sets.append("avg_ticket_price = @avg_ticket_price")
    if "enrichment_status" in for_fields:
        sets.append("enrichment_status = @enrichment_status")
    if "ticket_vendor_source" in for_fields:
        sets.append("ticket_vendor_source = @ticket_vendor_source")
    if "capacity_source" in for_fields:
        sets.append("capacity_source = @capacity_source")
    if "avg_ticket_price_source" in for_fields:
        sets.append("avg_ticket_price_source = @avg_ticket_price_source")

    sets.append("last_updated = CURRENT_TIMESTAMP()")
    where_clause = "id = @id" if use_id else "name = @name"
    sql = f"""
    UPDATE {table_fqdn()}
    SET {", ".join(sets)}
    WHERE {where_clause}
    """
    return sql


def update_in_place(row: Dict[str, Any], enriched: Dict[str, Any]):
    name = row.get("name") or row.get("organization_name")
    row_id = row.get("id")

    fields_to_set: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []
    if row_id is not None:
        params.append(bigquery.ScalarQueryParameter("id", "INT64", int(row_id)))
    else:
        params.append(bigquery.ScalarQueryParameter("name", "STRING", name))

    tv = enriched.get("ticket_vendor")
    if tv:
        fields_to_set.append("ticket_vendor")
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", tv))
        src = enriched.get("ticket_vendor_source")
        if src:
            fields_to_set.append("ticket_vendor_source")
            params.append(bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", src))

    cap = enriched.get("capacity")
    if cap is not None:
        try:
            cap_int = int(cap)
            fields_to_set.append("capacity")
            params.append(bigquery.ScalarQueryParameter("capacity", "INT64", cap_int))
            src = enriched.get("capacity_source")
            if src:
                fields_to_set.append("capacity_source")
                params.append(bigquery.ScalarQueryParameter("capacity_source", "STRING", src))
        except Exception:
            pass

    price = enriched.get("avg_ticket_price")
    if price is not None:
        dec = _to_decimal(price)
        if dec is not None:
            fields_to_set.append("avg_ticket_price")
            params.append(bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", dec))
            src = enriched.get("avg_ticket_price_source")
            if src:
                fields_to_set.append("avg_ticket_price_source")
                params.append(bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", src))

    status = enriched.get("enrichment_status", "OK")
    fields_to_set.append("enrichment_status")
    params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", status))

    sql = _build_update_sql(fields_to_set, use_id=row_id is not None)
    log.info("APPLY UPDATE for %s -> %s", row_id or name, sorted(fields_to_set))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq.query(sql, job_config=job_config, location=BQ_LOCATION).result()


def _combine_enrichment(row: Dict[str, Any], gpt_suggestion: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    website = row.get("website") or row.get("url")
    html, text = scrape_website_text(website)

    derived: Dict[str, Any] = {"enrichment_status": "NO_DATA"}

    signals = sniff_vendor_signals(html, website)
    vendor = choose_vendor(signals)
    if vendor:
        derived["ticket_vendor"] = vendor
        derived["ticket_vendor_source"] = "SCRAPE"

    avg_price = derive_price_from_text(text)
    if avg_price is not None:
        derived["avg_ticket_price"] = avg_price
        derived["avg_ticket_price_source"] = "SCRAPE"

    if gpt_suggestion:
        if "capacity" in gpt_suggestion and gpt_suggestion["capacity"] is not None:
            derived["capacity"] = gpt_suggestion["capacity"]
            derived["capacity_source"] = "GPT"
        if "ticket_vendor" in gpt_suggestion and "ticket_vendor" not in derived:
            derived["ticket_vendor"] = gpt_suggestion["ticket_vendor"]
            derived["ticket_vendor_source"] = "GPT"
        if "avg_ticket_price" in gpt_suggestion and "avg_ticket_price" not in derived:
            derived["avg_ticket_price"] = gpt_suggestion["avg_ticket_price"]
            derived["avg_ticket_price_source"] = "GPT"

    if any(derived.get(k) for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
        derived["enrichment_status"] = "OK"

    return derived


def run_batch(limit: int) -> int:
    rows = fetch_rows(limit)
    processed = 0
    for r in rows:
        row_dict = _row_to_dict(r)
        name = row_dict.get("name") or row_dict.get("organization_name")
        try:
            suggestion = enrich_with_gpt(name=name or "", row=row_dict, model=OPENAI_MODEL)
        except Exception as e:
            logging.warning("gpt failed: %s", e)
            suggestion = None

        enriched = _combine_enrichment(row_dict, suggestion)

        try:
            update_in_place(row_dict, enriched)
            time.sleep(random.uniform(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS) / 1000.0)  # why: ease BQ DML pressure
            processed += 1
        except Exception as e:
            log.error("Failed row: %s: %s", name, e, exc_info=True)
    return processed


@app.route("/ping", methods=["GET", "HEAD"])
def ping():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})


@app.get("/ready")
def ready():
    try:
        bq.query("SELECT 1", location=BQ_LOCATION).result()
        return jsonify({"ready": True, "bq_location": BQ_LOCATION}), 200
    except Exception as e:
        log.warning("ready check failed: %s", e)
        return jsonify({"ready": False, "error": str(e)}), 503


@app.route("/", methods=["GET", "HEAD"])
def root():
    if request.method == "HEAD":
        return ("", 200, {})
    try:
        limit = int(request.args.get("limit", "10"))
    except ValueError:
        limit = 10
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


@app.get("/healthz")
@app.get("/healthz/")
@app.get("/_ah/health")
def _health_compat():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
