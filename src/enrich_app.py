#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, List, Tuple

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
        vendor_from_eventbrite,
        avg_price_from_google_places,
        phone_from_google_places,
        extract_linkedin_url,
        extract_phone_numbers,
        extract_alt_name,
        extract_descriptions,
        detect_rfp,
        extract_charge_pct,
        extract_revenues,
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
        vendor_from_eventbrite,
        avg_price_from_google_places,
        phone_from_google_places,
        extract_linkedin_url,
        extract_phone_numbers,
        extract_alt_name,
        extract_descriptions,
        detect_rfp,
        extract_charge_pct,
        extract_revenues,
        extract_capacity,
    )

# ------------------------------------------------------------------------------
# App / config
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "culture_merged")
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g., "europe-southwest1"

ENABLE_TICKETMASTER = os.getenv("ENABLE_TICKETMASTER", "1") == "1"
ENABLE_PLACES = os.getenv("ENABLE_PLACES", "1") == "1"
ENABLE_EVENTBRITE = os.getenv("ENABLE_EVENTBRITE", "0") == "1"

ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "0") == "1"

# Optional column name overrides to adapt to unknown schema
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
    if ROW_DELAY_MAX_MS <= ROW_DELAY_MIN_MS:
        ms = ROW_DELAY_MIN_MS
    else:
        ms = random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
    time.sleep(ms / 1000.0)


def _pick_candidates(limit: int) -> List[Dict[str, Any]]:
    """
    Conservative candidate picker:
      - Prefer rows not yet marked DONE in enrichment_status (if column exists).
      - Otherwise just pick recent rows with a website.
    """
    sql = f"""
    DECLARE has_status BOOL DEFAULT EXISTS (
      SELECT 1
      FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = '{ENRICH_STATUS_COL}'
    );

    EXECUTE IMMEDIATE (
      SELECT IF(
        has_status,
        'SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website, {ENRICH_STATUS_COL} AS status
           FROM { _tbl() }
          WHERE IFNULL({ENRICH_STATUS_COL}, "") != "DONE"
            AND {WEBSITE_COL} IS NOT NULL
          LIMIT {limit}',
        'SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website, NULL AS status
           FROM { _tbl() }
          WHERE {WEBSITE_COL} IS NOT NULL
          LIMIT {limit}'
      )
    )
    """
    job = bq_client.query(sql)
    return [dict(r) for r in job.result()]


def _update_row(row_id: Any, updates: Dict[str, Any], dry: bool) -> None:
    if dry:
        logging.info(f"[DRY] Would update {row_id} with {updates}")
        return

    # Build a parameterized UPDATE for only the provided keys
    set_clauses = []
    params = []
    for i, (k, v) in enumerate(updates.items(), start=1):
        set_clauses.append(f"`{k}` = @p{i}")
        params.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING" if isinstance(v, str) else "NUMERIC" if isinstance(v, (int, float, Decimal)) else "STRING", None if v is None else str(v) if isinstance(v, Decimal) else v))

    if not set_clauses:
        return

    sql = f"""
    UPDATE { _tbl() }
       SET {", ".join(set_clauses)}
     WHERE `{KEY_COL}` = @row_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("row_id", "STRING", str(row_id)),
            *params,
        ]
    )
    job = bq_client.query(sql, job_config=job_config)
    job.result()
    logging.info(f"UPDATED {row_id}: {list(updates.keys())}")


def _enrich_one(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combines extractor heuristics with GPT results.
    Returns the dict of updates to write.
    """
    name = row.get("name") or row.get(NAME_COL)
    website = row.get("website") or row.get(WEBSITE_COL)

    # Scrape + extract heuristics
    website_text = scrape_website_text(website) if website else ""
    alt_name = extract_alt_name(website_text)
    descriptions = extract_descriptions(website_text)
    phone_candidates = extract_phone_numbers(website_text)
    linkedin = extract_linkedin_url(website_text)

    # External APIs (guarded)
    places_price = None
    places_phone = None
    if ENABLE_PLACES and name:
        places_price = avg_price_from_google_places(name, website)  # may be None
        places_phone = phone_from_google_places(name, website)

    # Ticketing vendor sniff
    signals = sniff_vendor_signals(website_text)
    tm_vendor = vendor_from_ticketmaster(name, website) if ENABLE_TICKETMASTER else None
    eb_vendor = vendor_from_eventbrite(name, website) if ENABLE_EVENTBRITE else None
    vendor_guess = tm_vendor or eb_vendor or choose_vendor(signals)
    vendor = normalize_vendor_name(vendor_guess) if vendor_guess else None
    vendor_source = None
    if tm_vendor:
        vendor_source = "ticketmaster"
    elif eb_vendor:
        vendor_source = "eventbrite"
    elif vendor:
        vendor_source = "heuristic"

    # Heuristic price/capacity from site text
    price_heur, price_src = derive_price_from_text(website_text)
    capacity_heur = extract_capacity(website_text)

    # GPT enrichment (with router/fallbacks)
    try:
        gpt_update = enrich_with_gpt(
            row_dict={
                "name": name,
                "alt_name": alt_name,
                "website_url": website,
                "description": descriptions[:512] if descriptions else None,
                "website_text": website_text,
                "phone": places_phone or (phone_candidates[0] if phone_candidates else None),
            }
        )
    except GPTQuotaExceeded as e:
        if os.getenv("STOP_ON_GPT_QUOTA", "0") == "1":
            raise
        logging.error(f"GPT quota exceeded, continuing without GPT for {name}: {e}")
        gpt_update = {}

    # Merge all sources with precedence: GPT > Places/heuristics
    updates: Dict[str, Any] = {}

    # Price
    avg_price = gpt_update.get("avg_ticket_price")
    if avg_price is None:
        avg_price = places_price or price_heur
        if avg_price is not None:
            updates["avg_ticket_price_source"] = "places" if places_price is not None else (price_src or "heuristic")
    else:
        updates["avg_ticket_price_source"] = "gpt"
    if avg_price is not None:
        updates["avg_ticket_price"] = avg_price

    # Capacity
    capacity = gpt_update.get("capacity")
    if capacity is None and capacity_heur:
        capacity = capacity_heur
        updates["capacity_source"] = "heuristic"
    elif capacity is not None:
        updates["capacity_source"] = "gpt"
    if capacity is not None:
        updates["capacity"] = int(capacity)

    # Vendor
    if vendor and is_true_ticketing_provider(vendor):
        updates["ticket_vendor"] = vendor
        updates["ticket_vendor_source"] = vendor_source or "heuristic"

    # Always mark status when we did any work
    if updates:
        updates["enrichment_status"] = "DONE"

    return updates


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/ping")
def ping():
    return "ok"


@app.get("/ready")
def ready():
    # simple readiness + echo location so you can see it in curl
    return jsonify({"bq_location": BQ_LOCATION, "ready": True})


@app.get("/")
def run_enrichment():
    limit = int(request.args.get("limit", "50"))
    dry = request.args.get("dry", "0") in ("1", "true", "True", "yes")

    # Safety: clamp limit
    limit = max(1, min(limit, 500))

    # Fetch candidates
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
            # else continue to next row
        except Exception as e:
            logging.exception(f"Row failed: {row}")
            # continue processing

    return jsonify({
        "candidates": len(rows),
        "processed": processed,
        "status": "DRY_OK" if dry else "OK",
    })

