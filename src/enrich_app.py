#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, List, Tuple

from flask import Flask, request, jsonify
from google.cloud import bigquery

# Local modules
try:
    from .gpt_client import enrich_with_gpt, GPTQuotaExceeded
    from .extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
    )
except Exception:
    from gpt_client import enrich_with_gpt, GPTQuotaExceeded
    from extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Env ---
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "performing_arts_fixed")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g. "US", "EU", "europe-southwest1"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Stop entire batch immediately on GPT quota/rate limits (service responds 429)
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1").lower() in ("1", "true", "yes")

# Gentle DML pacing (ms) between rows
ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))

# Soft budget to avoid Cloud Run 120s timeout; we 504 early so callers can downshift LIMIT
REQUEST_BUDGET_SEC = int(os.getenv("REQUEST_BUDGET_SEC", "100"))

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")
if not BQ_LOCATION:
    raise RuntimeError("BQ_LOCATION env var is required (e.g. 'US'/'EU'/'region')")

BQ = bigquery.Client(project=PROJECT_ID)
app = Flask(__name__)
app.url_map.strict_slashes = False


def table_fqdn() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _to_decimal(value: Any):
    if value is None:
        return None
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


def get_candidates(limit: int):
    """
    Fetch rows that still need enrichment. We only reference columns that exist
    across your table. Key is 'name' (stable).
    """
    sql = f"""
    SELECT name, domain, ticket_vendor, capacity, avg_ticket_price, enrichment_status, last_updated
    FROM {table_fqdn()}
    WHERE
      (ticket_vendor IS NULL OR capacity IS NULL OR avg_ticket_price IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status NOT IN ('LOCKED'))
    ORDER BY COALESCE(last_updated, TIMESTAMP('1970-01-01')) ASC
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = BQ.query(sql, job_config=job_config, location=BQ_LOCATION)
    return list(job.result())


def _build_update_sql(for_fields: List[str]) -> str:
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
    # Use `name` as the key; it exists on all rows in your table
    sql = f"""
    UPDATE {table_fqdn()}
    SET {", ".join(sets)}
    WHERE name = @name
    """
    return sql


def update_in_place(row: Dict[str, Any], enriched: Dict[str, Any]):
    """
    Applies the enrichment back to BigQuery for this row (by name).
    Sources are written *only if* the value is set so you never have a value
    without its provenance.
    """
    name = row.get("name")
    if not name:
        return  # cannot safely update without a stable key

    fields_to_set: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("name", "STRING", name),
    ]

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

    # Always set a status
    status = enriched.get("enrichment_status", "OK")
    fields_to_set.append("enrichment_status")
    params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", status))

    if not fields_to_set:
        # Nothing to write (should be rare)
        return

    sql = _build_update_sql(fields_to_set)
    log.info("APPLY UPDATE for %s -> %s", name, sorted(fields_to_set))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    BQ.query(sql, job_config=job_config, location=BQ_LOCATION).result()


def _combine_enrichment(row: Dict[str, Any], gpt_suggestion: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge scrape-derived values with GPT’s suggestion:
      - ticket_vendor = payment-funnel software (Ticketmaster, Fever, Eventbrite, etc.)
      - prefer SCRAPE results; fill gaps with GPT.
      - write matching *_source fields whenever a value is set.
    """
    site = row.get("domain") or row.get("website") or row.get("url")
    html, text = scrape_website_text(site)

    derived: Dict[str, Any] = {"enrichment_status": "NO_DATA"}

    # Vendor from payment / checkout signals in HTML
    signals = sniff_vendor_signals(html, site)
    vendor = choose_vendor(signals)
    if vendor:
        derived["ticket_vendor"] = vendor
        derived["ticket_vendor_source"] = "SCRAPE"

    # Price heuristics from text
    avg_price = derive_price_from_text(text)
    if avg_price is not None:
        derived["avg_ticket_price"] = avg_price
        derived["avg_ticket_price_source"] = "SCRAPE"

    # Fill the gaps with GPT
    if gpt_suggestion:
        if derived.get("capacity") is None and gpt_suggestion.get("capacity") is not None:
            derived["capacity"] = gpt_suggestion["capacity"]
            derived["capacity_source"] = "GPT"
        if not derived.get("ticket_vendor") and gpt_suggestion.get("ticket_vendor"):
            derived["ticket_vendor"] = gpt_suggestion["ticket_vendor"]
            derived["ticket_vendor_source"] = "GPT"
        if derived.get("avg_ticket_price") is None and gpt_suggestion.get("avg_ticket_price") is not None:
            derived["avg_ticket_price"] = gpt_suggestion["avg_ticket_price"]
            derived["avg_ticket_price_source"] = "GPT"

    if any(derived.get(k) is not None for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
        derived["enrichment_status"] = "OK"

    return derived


def run_batch(limit: int) -> Tuple[int, str]:
    """
    Process up to `limit` rows, respecting a soft time budget so we can return
    504 before Cloud Run's hard timeout. The caller maps "TIMEOUT" -> HTTP 504.
    """
    start = time.time()
    rows = get_candidates(limit)
    processed = 0

    for r in rows:
        if time.time() - start > REQUEST_BUDGET_SEC:
            log.warning("Time budget exceeded at %d rows; returning TIMEOUT", processed)
            return processed, "TIMEOUT"

        row_dict = _row_to_dict(r)
        name = row_dict.get("name") or row_dict.get("organization_name") or ""

        try:
            suggestion = enrich_with_gpt(name=name, row=row_dict, model=OPENAI_MODEL)
        except GPTQuotaExceeded as e:
            # Bubble up so the HTTP handler can return 429
            log.error("GPT quota exceeded after %d rows: %s", processed, e)
            raise
        except Exception as e:
            log.warning("GPT failed (non-quota) on %s: %s", name, e)
            suggestion = None

        enriched = _combine_enrichment(row_dict, suggestion)

        try:
            update_in_place(row_dict, enriched)
            processed += 1
            # Gentle DML pacing
            time.sleep(random.uniform(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS) / 1000.0)
        except Exception as e:
            log.error("Failed row update for '%s': %s", name, e, exc_info=True)

    return processed, "OK"


# -------------------- HTTP endpoints --------------------

@app.get("/ping")
def ping():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})


@app.get("/ready")
def ready():
    try:
        BQ.query("SELECT 1", location=BQ_LOCATION).result()
        return jsonify({"ready": True, "bq_location": BQ_LOCATION}), 200
    except Exception as e:
        log.warning("ready check failed: %s", e)
        return jsonify({"ready": False, "error": str(e)}), 503


@app.route("/", methods=["GET", "HEAD"])
def root():
    if request.method == "HEAD":
        return ("", 200, {})

    # Query params
    try:
        limit = int(request.args.get("limit", "10"))
    except ValueError:
        limit = 10
    dry = request.args.get("dry") in ("1", "true", "True", "yes")

    try:
        if dry:
            count = len(get_candidates(limit))
            return jsonify({"processed": 0, "candidates": count, "status": "DRY_OK"}), 200

        try:
            count, status = run_batch(limit)
            if status == "TIMEOUT":
                return jsonify({"processed": count, "status": status}), 504
            return jsonify({"processed": count, "status": status}), 200

        except GPTQuotaExceeded as e:
            # Return 429 so your shell loop/scheduler stops immediately
            return jsonify({"processed": 0, "status": "QUOTA", "error": str(e)}), 429

    except Exception as e:
        log.exception("Batch failed")
        return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500


@app.get("/stats")
def stats():
    try:
        q1 = f"""
        SELECT
          COUNT(*) AS total,
          COUNTIF(enrichment_status = 'OK') AS ok,
          COUNTIF(enrichment_status IS NULL OR enrichment_status != 'OK') AS pending,
          COUNTIF(ticket_vendor IS NOT NULL) AS have_vendor,
          COUNTIF(capacity IS NOT NULL) AS have_capacity,
          COUNTIF(avg_ticket_price IS NOT NULL) AS have_price
        FROM {table_fqdn()}
        """
        overview = list(BQ.query(q1, location=BQ_LOCATION).result())[0]
        ov = {k: overview[k] for k in overview.keys()}

        q2 = f"""
        SELECT ticket_vendor, COUNT(*) AS c
        FROM {table_fqdn()}
        WHERE ticket_vendor IS NOT NULL
        GROUP BY 1
        ORDER BY c DESC
        LIMIT 15
        """
        vendors = [{"ticket_vendor": r["ticket_vendor"], "count": r["c"]}
                   for r in BQ.query(q2, location=BQ_LOCATION).result()]

        return jsonify({"overview": ov, "top_vendors": vendors}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/healthz")
@app.get("/healthz/")
@app.get("/_ah/health")
def _health_compat():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
