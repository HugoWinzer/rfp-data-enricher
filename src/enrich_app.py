#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal
from typing import Dict, Any, List

from flask import Flask, request, jsonify
from google.cloud import bigquery

# Local imports — tolerate both package and flat execution
try:
    from .gpt_client import enrich_with_gpt, GPTQuotaExceeded
    from .extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
        normalize_vendor_name,
        is_true_ticketing_provider,
        vendor_from_ticketmaster,
        avg_price_from_google_places,
        extract_linkedin_url,
        extract_alt_name,
        extract_descriptions,
        extract_capacity,
    )
except Exception:
    from gpt_client import enrich_with_gpt, GPTQuotaExceeded
    from extractors import (
        scrape_website_text,
        sniff_vendor_signals,
        choose_vendor,
        derive_price_from_text,
        normalize_vendor_name,
        is_true_ticketing_provider,
        vendor_from_ticketmaster,
        avg_price_from_google_places,
        extract_linkedin_url,
        extract_alt_name,
        extract_descriptions,
        extract_capacity,
    )

# ------------------------------------------------------------------------------
# App / config
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "OUTPUT")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g., "europe-southwest1"

ENABLE_TICKETMASTER = os.getenv("ENABLE_TICKETMASTER", "1") == "1"
ENABLE_PLACES = os.getenv("ENABLE_PLACES", "1") == "1"
# Explicitly ignore Eventbrite
ENABLE_EVENTBRITE = False

ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "0") == "1"

# Optional column name overrides
KEY_COL = os.getenv("KEY_COL", "id")
NAME_COL = os.getenv("NAME_COL", "name")
WEBSITE_COL = os.getenv("WEBSITE_COL", "website")
ENRICH_STATUS_COL = os.getenv("ENRICH_STATUS_COL", "enrichment_status")

bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _tbl() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _sleep_jitter():
    ms = ROW_DELAY_MIN_MS if ROW_DELAY_MAX_MS <= ROW_DELAY_MIN_MS else random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
    time.sleep(ms / 1000.0)


def _pick_candidates(limit: int) -> List[Dict[str, Any]]:
    """
    Pick rows that are not LOCKED and still missing any target fields.
    Uses dynamic SQL so it won't break if a column is absent.
    """
    sql = f"""
    DECLARE has_status BOOL DEFAULT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = '{ENRICH_STATUS_COL}'
    );
    DECLARE has_vendor BOOL DEFAULT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = 'ticket_vendor'
    );
    DECLARE has_capacity BOOL DEFAULT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = 'capacity'
    );
    DECLARE has_price BOOL DEFAULT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = 'avg_ticket_price'
    );
    DECLARE has_website BOOL DEFAULT EXISTS (
      SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = '{WEBSITE_COL}'
    );

    DECLARE where_parts ARRAY<STRING> DEFAULT [
      IF(has_status, 'IFNULL({ENRICH_STATUS_COL}, "") != "LOCKED"', 'TRUE'),
      IF(has_website, '{WEBSITE_COL} IS NOT NULL', 'TRUE'),
      '(' ||
        IF(has_vendor, 'ticket_vendor IS NULL', 'FALSE') || ' OR ' ||
        IF(has_capacity, 'capacity IS NULL', 'FALSE') || ' OR ' ||
        IF(has_price, 'avg_ticket_price IS NULL', 'FALSE') ||
      ')'
    ];

    EXECUTE IMMEDIATE (
      'SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website' ||
      IF(has_status, ', {ENRICH_STATUS_COL} AS status', ', NULL AS status') ||
      ' FROM {tbl} WHERE ' || ARRAY_TO_STRING(where_parts, ' AND ') ||
      ' LIMIT {limit}'
    )
    USING tbl AS { _tbl() }, limit AS {limit};
    """
    job = bq_client.query(sql)
    return [dict(r) for r in job.result()]


def _bq_param(name: str, value: Any) -> bigquery.ScalarQueryParameter:
    if isinstance(value, bool):
        return bigquery.ScalarQueryParameter(name, "BOOL", value)
    if isinstance(value, int):
        return bigquery.ScalarQueryParameter(name, "INT64", value)
    if isinstance(value, float):
        return bigquery.ScalarQueryParameter(name, "FLOAT64", value)
    if isinstance(value, Decimal):
        # BigQuery NUMERIC via string to preserve precision
        return bigquery.ScalarQueryParameter(name, "NUMERIC", str(value))
    return bigquery.ScalarQueryParameter(name, "STRING", value)


def _update_row(row_id: Any, updates: Dict[str, Any], dry: bool) -> None:
    if dry:
        logging.info(f"[DRY] Would update {row_id} with {updates}")
        return

    set_clauses = []
    params = []
    i = 0
    for k, v in updates.items():
        i += 1
        set_clauses.append(f"`{k}` = @p{i}")
        params.append(_bq_param(f"p{i}", v))

    # Always bump last_updated if column exists
    set_sql = ", ".join(set_clauses) + ", last_updated = CURRENT_TIMESTAMP()"

    sql = f"""
    UPDATE { _tbl() }
       SET {set_sql}
     WHERE `{KEY_COL}` = @row_id
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[_bq_param("row_id", str(row_id)), *params])
    bq_client.query(sql, job_config=job_config).result()
    logging.info(f"UPDATED {row_id}: {list(updates.keys())}")


def _enrich_one(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combine heuristics, APIs and GPT. Return a dict of updates to write.
    Ensures no NULLs by supplying defaults when needed.
    """
    name = row.get("name") or row.get(NAME_COL)
    website = row.get("website") or row.get(WEBSITE_COL)

    # Scrape + extract heuristics (no phones)
    website_text = scrape_website_text(website) if website else ""
    alt_name = extract_alt_name(website_text)
    descriptions = extract_descriptions(website_text)
    linkedin = extract_linkedin_url(website_text)

    # External APIs
    places_price = None
    if ENABLE_PLACES and name:
        places_price = avg_price_from_google_places(name, website)  # may be None

    # Ticketing vendor sniff (Ticketmaster only; Eventbrite ignored)
    signals = sniff_vendor_signals(website_text)
    tm_vendor = vendor_from_ticketmaster(name, website) if ENABLE_TICKETMASTER else None
    vendor_guess = tm_vendor or choose_vendor(signals)
    vendor = normalize_vendor_name(vendor_guess) if vendor_guess else None
    vendor_source = "ticketmaster" if tm_vendor else ("heuristic" if vendor else None)

    # Heuristic price/capacity from site text
    price_heur, price_src = derive_price_from_text(website_text)
    capacity_heur = extract_capacity(website_text)

    # GPT enrichment (no phone field)
    try:
        gpt_update = enrich_with_gpt(
            row_dict={
                "name": name,
                "alt_name": alt_name,
                "website_url": website,
                "description": (descriptions[:512] if descriptions else None),
                "website_text": website_text,
                "linkedin_url": linkedin,
            }
        )
    except GPTQuotaExceeded as e:
        if STOP_ON_GPT_QUOTA:
            raise
        logging.error(f"GPT quota exceeded for {name}: {e}")
        gpt_update = {}

    # Merge with precedence: GPT > Places/heuristics, then defaults (no NULLs)
    updates: Dict[str, Any] = {}

    # --- avg_ticket_price ---
    avg_price = gpt_update.get("avg_ticket_price")
    if avg_price is None:
        avg_price = places_price or price_heur
        if avg_price is not None:
            updates["avg_ticket_price_source"] = "places" if places_price is not None else (price_src or "heuristic")
    else:
        updates["avg_ticket_price_source"] = "gpt"
    if avg_price is None:
        avg_price = 0
        updates["avg_ticket_price_source"] = updates.get("avg_ticket_price_source", "none")
    updates["avg_ticket_price"] = avg_price

    # --- capacity ---
    capacity = gpt_update.get("capacity")
    if capacity is None:
        capacity = capacity_heur if capacity_heur is not None else 0
        updates["capacity_source"] = "heuristic" if capacity_heur is not None else "none"
    else:
        updates["capacity_source"] = "gpt"
    try:
        updates["capacity"] = int(capacity)
    except Exception:
        updates["capacity"] = 0
        updates["capacity_source"] = "none"

    # --- ticket_vendor ---
    if vendor and is_true_ticketing_provider(vendor):
        updates["ticket_vendor"] = vendor
        updates["ticket_vendor_source"] = vendor_source or "heuristic"
    else:
        updates["ticket_vendor"] = "Unknown"
        updates["ticket_vendor_source"] = "none"

    # Optional nice-to-haves (never required)
    if linkedin:
        updates["linkedin_url"] = linkedin
    if alt_name:
        updates["alt_name"] = alt_name
    if descriptions:
        updates["description"] = descriptions[:1024]

    # Mark status
    updates["enrichment_status"] = "OK"

    return updates


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/ping")
def ping():
    return "ok"


@app.get("/ready")
def ready():
    return jsonify({"bq_location": BQ_LOCATION, "ready": True})


@app.get("/")
def run_enrichment():
    limit = int(request.args.get("limit", "50"))
    dry = request.args.get("dry", "0") in ("1", "true", "True", "yes")
    limit = max(1, min(limit, 500))

    try:
        rows = _pick_candidates(limit)
    except Exception as e:
        logging.exception("Failed to pick candidates")
        return jsonify({"status": "ERROR", "error": str(e)}), 500

    processed = 0
    for row in rows:
        try:
            updates = _enrich_one(row)
            if updates:
                _update_row(row.get("id") or row.get(KEY_COL), updates, dry)
            processed += 1
            _sleep_jitter()
        except GPTQuotaExceeded as e:
            logging.error(f"GPT quota exceeded after {processed} rows: {e}")
            if STOP_ON_GPT_QUOTA:
                return jsonify({"status": "QUOTA_HIT", "processed": processed}), 429
        except Exception as e:
            logging.exception(f"Row failed: {row}")

    return jsonify({
        "candidates": len(rows),
        "processed": processed,
        "status": "DRY_OK" if dry else "OK",
    })
