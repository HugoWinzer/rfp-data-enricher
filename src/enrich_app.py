#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, List, Tuple

from flask import Flask, request, jsonify
from google.cloud import bigquery

# --- optional local imports (work both when run as a package or a script) ---
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

# ------------------------- logging -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich_app")

# ------------------------- env & clients -------------------------
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "performing_arts_fixed")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g. "US", "EU", "europe-southwest1"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Throttle between row updates to avoid BQ DML contention
ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")
if not BQ_LOCATION:
    raise RuntimeError("BQ_LOCATION env var is required (e.g. 'US'/'EU'/'region')")

bq = bigquery.Client(project=PROJECT_ID)
# Some of your snippets referenced BQ in uppercase—make it available too.
BQ = bq

app = Flask(__name__)
app.url_map.strict_slashes = False

# ------------------------- helpers -------------------------
def table_fqdn() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _row_to_dict(row: Any) -> Dict[str, Any]:
    # Works with google.cloud.bigquery.table.Row
    try:
        return dict(row.items())
    except Exception:
        try:
            return dict(row)
        except Exception:
            return {}


def _candidate_predicate() -> str:
    # Keep this in one place so root() and stats agree
    return """
      (avg_ticket_price IS NULL OR capacity IS NULL OR ticket_vendor IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status NOT IN ('LOCKED'))
    """


def fetch_rows(limit: int):
    sql = f"""
    SELECT *
    FROM {table_fqdn()}
    WHERE {_candidate_predicate()}
    ORDER BY COALESCE(last_updated, TIMESTAMP('1970-01-01')) ASC
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    return list(job.result())


# Backwards-compat with your snippet calling get_candidates()
def get_candidates(limit: int):
    return fetch_rows(limit)


def _build_update_sql(for_fields: List[str], use_entity_id: bool) -> str:
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
    where_clause = "entity_id = @entity_id" if use_entity_id else "name = @name"
    sql = f"""
    UPDATE {table_fqdn()}
    SET {", ".join(sets)}
    WHERE {where_clause}
    """
    return sql


def _extract_row_identity(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (entity_id, name). We prefer entity_id (STRING) if available,
    else we fall back to name.
    """
    entity_id = row.get("entity_id")
    name = row.get("name") or row.get("organization_name")
    return (entity_id, name)


def update_in_place(row: Dict[str, Any], enriched: Dict[str, Any]):
    entity_id, name = _extract_row_identity(row)
    if not entity_id and not name:
        log.warning("Row has no entity_id or name; skipping update.")
        return

    fields_to_set: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []

    # Where param
    if entity_id:
        params.append(bigquery.ScalarQueryParameter("entity_id", "STRING", entity_id))
    else:
        params.append(bigquery.ScalarQueryParameter("name", "STRING", name))

    # ticket_vendor (+ source)
    tv = enriched.get("ticket_vendor")
    if tv:
        fields_to_set.append("ticket_vendor")
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", tv))
        src = enriched.get("ticket_vendor_source")
        if src:
            fields_to_set.append("ticket_vendor_source")
            params.append(bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", src))

    # capacity (+ source)
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
            # ignore bad capacity
            pass

    # avg_ticket_price (+ source)
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

    # enrichment_status
    status = enriched.get("enrichment_status", "OK")
    fields_to_set.append("enrichment_status")
    params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", status))

    if not fields_to_set:
        # Still stamp last_updated + NO_DATA to avoid re-processing forever
        fields_to_set = ["enrichment_status"]
        params = params + [
            bigquery.ScalarQueryParameter("enrichment_status", "STRING", "NO_DATA"),
        ]

    sql = _build_update_sql(fields_to_set, use_entity_id=bool(entity_id))
    log.info("APPLY UPDATE for %s -> %s", entity_id or name, sorted(fields_to_set))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq.query(sql, job_config=job_config, location=BQ_LOCATION).result()


def _combine_enrichment(row: Dict[str, Any], gpt_suggestion: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer the 'domain' field if available
    site = row.get("domain") or row.get("website") or row.get("url")
    html, text = scrape_website_text(site)

    derived: Dict[str, Any] = {"enrichment_status": "NO_DATA"}

    # Vendor via signals
    signals = sniff_vendor_signals(html, site)
    vendor = choose_vendor(signals)
    if vendor:
        derived["ticket_vendor"] = vendor
        derived["ticket_vendor_source"] = "SCRAPE"

    # Average ticket price from page text
    avg_price = derive_price_from_text(text)
    if avg_price is not None:
        derived["avg_ticket_price"] = avg_price
        derived["avg_ticket_price_source"] = "SCRAPE"

    # GPT suggestion (usually for capacity; can backfill vendor/price if needed)
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
        name = row_dict.get("name") or row_dict.get("organization_name") or row_dict.get("domain") or "?"
        try:
            suggestion = enrich_with_gpt(name=name, row=row_dict, model=OPENAI_MODEL)
        except Exception as e:
            log.warning("GPT suggestion failed for %s: %s", name, e)
            suggestion = None

        enriched = _combine_enrichment(row_dict, suggestion)

        try:
            update_in_place(row_dict, enriched)
            # reduce DML contention
            time.sleep(random.uniform(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS) / 1000.0)
            processed += 1
        except Exception as e:
            log.error("Failed updating %s: %s", name, e, exc_info=True)
    return processed


# ------------------------- routes -------------------------
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
            count = len(get_candidates(limit))
            return jsonify({"processed": 0, "candidates": count, "status": "DRY_OK"}), 200
        count = run_batch(limit)
        return jsonify({"processed": count, "status": "OK"}), 200
    except Exception as e:
        log.exception("Batch failed")
        return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500


# Simple, useful stats for quick checks in Cloud Shell
@app.get("/stats")
def stats():
    try:
        # Overview rollup
        q1 = f"""
        SELECT
          COUNT(*) AS total,
          COUNTIF(enrichment_status = 'OK') AS ok,
          COUNTIF(enrichment_status IS NULL OR enrichment_status != 'OK') AS pending,
          COUNTIF(ticket_vendor IS NOT NULL) AS have_vendor,
          COUNTIF(capacity IS NOT NULL) AS have_capacity,
          COUNTIF(avg_ticket_price IS NOT NULL) AS have_price,
          MAX(last_updated) AS last_updated_max
        FROM {table_fqdn()}
        """
        overview_row = list(bq.query(q1, location=BQ_LOCATION).result())[0]
        overview = _row_to_dict(overview_row)

        # Backlog count (same predicate as candidate selection)
        q_backlog = f"SELECT COUNT(*) AS backlog FROM {table_fqdn()} WHERE {_candidate_predicate()}"
        backlog_row = list(bq.query(q_backlog, location=BQ_LOCATION).result())[0]
        backlog = _row_to_dict(backlog_row).get("backlog", 0)

        # Top vendors
        q2 = f"""
        SELECT ticket_vendor, COUNT(*) AS c
        FROM {table_fqdn()}
        WHERE ticket_vendor IS NOT NULL
        GROUP BY 1
        ORDER BY c DESC
        LIMIT 15
        """
        vendors = [
            {"ticket_vendor": r["ticket_vendor"], "count": r["c"]}
            for r in bq.query(q2, location=BQ_LOCATION).result()
        ]

        # Recent sample
        q_recent = f"""
        SELECT
          name, domain, ticket_vendor, ticket_vendor_source,
          capacity, capacity_source, avg_ticket_price, avg_ticket_price_source,
          enrichment_status, last_updated
        FROM {table_fqdn()}
        WHERE last_updated IS NOT NULL
        ORDER BY last_updated DESC
        LIMIT 10
        """
        recent = [_row_to_dict(r) for r in bq.query(q_recent, location=BQ_LOCATION).result()]

        return jsonify({"overview": overview, "backlog": backlog, "top_vendors": vendors, "recent": recent}), 200
    except Exception as e:
        log.exception("stats failed")
        return jsonify({"error": str(e)}), 500


@app.get("/healthz")
@app.get("/healthz/")
@app.get("/_ah/health")
def _health_compat():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})


# ------------------------- entrypoint -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
