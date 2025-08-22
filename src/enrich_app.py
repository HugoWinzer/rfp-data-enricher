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
    from .gpt_client import enrich_with_gpt
    from .extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
        normalize_vendor_name,
        is_true_ticketing_provider,
    )
except Exception:
    from gpt_client import enrich_with_gpt
    from extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
        normalize_vendor_name,
        is_true_ticketing_provider,
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich_app")

# --- Config ---
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "performing_arts_fixed")
STAGING_TABLE = os.getenv("STAGING_TABLE", "performing_arts_enriched_stage")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g. "US", "EU", "europe-southwest1"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Delays for pacing BigQuery job churn (applies between batches, not per-row now)
BATCH_DELAY_MIN_MS = int(os.getenv("BATCH_DELAY_MIN_MS", "100"))
BATCH_DELAY_MAX_MS = int(os.getenv("BATCH_DELAY_MAX_MS", "300"))

# Hard bounds to clamp GPT guesses
CAPACITY_MIN = int(os.getenv("CAPACITY_MIN", "30"))
CAPACITY_MAX = int(os.getenv("CAPACITY_MAX", "20000"))
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "5"))
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "500"))

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")
if not BQ_LOCATION:
    raise RuntimeError("BQ_LOCATION env var is required (e.g. 'US'/'EU'/'region')")

BQ = bigquery.Client(project=PROJECT_ID)
app = Flask(__name__)
app.url_map.strict_slashes = False


def table_fqdn() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def stage_fqdn() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}`"


def _to_decimal(value: Any) -> Optional[Decimal]:
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


def ensure_stage_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {stage_fqdn()} (
      id INT64,
      name STRING,
      ticket_vendor STRING,
      ticket_vendor_source STRING,
      capacity INT64,
      capacity_source STRING,
      avg_ticket_price NUMERIC,
      avg_ticket_price_source STRING,
      enrichment_status STRING,
      last_updated TIMESTAMP
    )
    """
    BQ.query(ddl, location=BQ_LOCATION).result()


def fetch_rows(limit: int):
    # Only rows missing at least one target field and not LOCKED
    sql = f"""
    SELECT id, name, domain, website, url
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


def _clamp_capacity(x: Optional[int]) -> Optional[int]:
    if x is None:
        return None
    try:
        v = int(x)
        v = max(CAPACITY_MIN, min(CAPACITY_MAX, v))
        return v
    except Exception:
        return None


def _clamp_price(x: Optional[Decimal]) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        v = _to_decimal(x)
        if v is None:
            return None
        v = max(PRICE_MIN, min(PRICE_MAX, v))
        # round to 2 decimals to fit BigQuery NUMERIC typical usage
        return v.quantize(Decimal("0.01"))
    except Exception:
        return None


def _combine_enrichment(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Force-fill: vendor, capacity, avg_ticket_price must be present.
    Sources: SCRAPE > GPT > FALLBACK
    """
    org_name = (row.get("name") or "").strip()
    site = row.get("domain") or row.get("website") or row.get("url")

    html, text = scrape_website_text(site)

    # 1) Try scraping signals
    signals = sniff_vendor_signals(html, site)
    vendor_scrape = choose_vendor(signals)
    vendor_scrape = normalize_vendor_name(vendor_scrape) if vendor_scrape else None

    price_scrape = derive_price_from_text(text)

    # 2) Ask GPT for all three fields (no empties)
    gpt = None
    try:
        gpt = enrich_with_gpt(
            name=org_name,
            site=site or "",
            scraped_text=text or "",
            model=OPENAI_MODEL,
        )
    except Exception as e:
        log.warning("GPT enrichment failed for %s: %s", org_name, e)

    # Extract GPT candidates
    vendor_gpt = normalize_vendor_name((gpt or {}).get("ticket_vendor"))
    capacity_gpt = _clamp_capacity((gpt or {}).get("capacity"))
    price_gpt = _clamp_price((gpt or {}).get("avg_ticket_price"))

    # 3) Decide vendor: must be a true ticketing provider (payment funnel), not an aggregator
    vendor_final: Optional[str] = None
    vendor_src: Optional[str] = None

    # Prefer scrape if it's a real provider
    if vendor_scrape and is_true_ticketing_provider(vendor_scrape):
        vendor_final, vendor_src = vendor_scrape, "SCRAPE"
    elif vendor_gpt and is_true_ticketing_provider(vendor_gpt):
        vendor_final, vendor_src = vendor_gpt, "GPT"

    # 4) Decide price
    price_final: Optional[Decimal] = None
    price_src: Optional[str] = None
    if price_scrape is not None:
        p = _clamp_price(price_scrape)
        if p is not None:
            price_final, price_src = p, "SCRAPE"
    if price_final is None and price_gpt is not None:
        price_final, price_src = price_gpt, "GPT"

    # 5) Decide capacity
    capacity_final: Optional[int] = None
    capacity_src: Optional[str] = None
    if capacity_gpt is not None:
        capacity_final, capacity_src = capacity_gpt, "GPT"

    # 6) Force-fill if still missing (very rare if GPT worked)
    if not vendor_final:
        vendor_final, vendor_src = "UNKNOWN_VENDOR", "FALLBACK"
    if capacity_final is None:
        capacity_final, capacity_src = CAPACITY_MIN, "FALLBACK"  # conservative
    if price_final is None:
        price_final, price_src = PRICE_MIN, "FALLBACK"

    # Final dictionary
    enriched = {
        "ticket_vendor": vendor_final,
        "ticket_vendor_source": vendor_src or "FALLBACK",
        "capacity": capacity_final,
        "capacity_source": capacity_src or "FALLBACK",
        "avg_ticket_price": price_final,
        "avg_ticket_price_source": price_src or "FALLBACK",
        "enrichment_status": "OK",  # Always OK because fields are force-filled
    }
    return enriched


def _records_for_stage(original: Dict[str, Any], enriched: Dict[str, Any]) -> Dict[str, Any]:
    # The update key: prefer id, else name
    rec = {
        "id": int(original["id"]) if original.get("id") is not None else None,
        "name": original.get("name"),
        "ticket_vendor": enriched["ticket_vendor"],
        "ticket_vendor_source": enriched.get("ticket_vendor_source"),
        "capacity": int(enriched["capacity"]) if enriched.get("capacity") is not None else None,
        "capacity_source": enriched.get("capacity_source"),
        "avg_ticket_price": str(enriched["avg_ticket_price"]) if enriched.get("avg_ticket_price") is not None else None,
        "avg_ticket_price_source": enriched.get("avg_ticket_price_source"),
        "enrichment_status": enriched.get("enrichment_status", "OK"),
        "last_updated": bigquery.ScalarQueryParameter("", "TIMESTAMP", None)  # ignored; set in UPDATE
    }
    return rec


def _merge_stage_into_main(use_id: bool):
    # When id exists, match on id; otherwise match on name
    on_clause = "T.id = S.id" if use_id else "T.id IS NULL AND T.name = S.name"
    sql = f"""
    MERGE {table_fqdn()} T
    USING {stage_fqdn()} S
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
      ticket_vendor = S.ticket_vendor,
      ticket_vendor_source = S.ticket_vendor_source,
      capacity = S.capacity,
      capacity_source = S.capacity_source,
      avg_ticket_price = S.avg_ticket_price,
      avg_ticket_price_source = S.avg_ticket_price_source,
      enrichment_status = S.enrichment_status,
      last_updated = CURRENT_TIMESTAMP()
    """
    BQ.query(sql, location=BQ_LOCATION).result()


def _truncate_stage():
    BQ.query(f"TRUNCATE TABLE {stage_fqdn()}", location=BQ_LOCATION).result()


def run_batch(limit: int) -> int:
    ensure_stage_table()
    rows = fetch_rows(limit)
    if not rows:
        return 0

    stage_records_with_id: List[Dict[str, Any]] = []
    stage_records_no_id: List[Dict[str, Any]] = []

    processed = 0
    for r in rows:
        row = _row_to_dict(r)
        enriched = _combine_enrichment(row)

        rec = _records_for_stage(row, enriched)
        if row.get("id") is not None:
            stage_records_with_id.append(rec)
        else:
            stage_records_no_id.append(rec)
        processed += 1

    # Insert into stage and MERGE in two passes (id and name)
    def _insert_and_merge(batch: List[Dict[str, Any]], use_id: bool):
        if not batch:
            return
        errors = BQ.insert_rows_json(
            f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}", batch, row_ids=[None] * len(batch)
        )
        if errors:
            raise RuntimeError(f"insert_rows_json errors: {errors}")
        _merge_stage_into_main(use_id)
        _truncate_stage()

    _insert_and_merge(stage_records_with_id, use_id=True)
    _insert_and_merge(stage_records_no_id, use_id=False)

    # Gentle delay between batches
    time.sleep(random.uniform(BATCH_DELAY_MIN_MS, BATCH_DELAY_MAX_MS) / 1000.0)
    return processed


# ------------------- HTTP Endpoints -------------------

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


@app.get("/stats")
def stats():
    try:
        q1 = f"""
        SELECT
          COUNT(*) AS total,
          COUNTIF(enrichment_status = 'OK') AS ok,
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
