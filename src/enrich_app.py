#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
        # the following helpers are optional in your repo; guard use accordingly
        normalize_vendor_name,
        is_true_ticketing_provider,
        # external API helpers are gated by env flags
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
    )

# ------------------------------------------------------------------------------
# App / config
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "OUTPUT")  # default to your stated table
BQ_LOCATION = os.getenv("BQ_LOCATION")  # e.g., "europe-southwest1"

# Feature flags (Eventbrite disabled by default)
ENABLE_TICKETMASTER = os.getenv("ENABLE_TICKETMASTER", "1") == "1"
ENABLE_PLACES = os.getenv("ENABLE_PLACES", "1") == "1"
ENABLE_EVENTBRITE = os.getenv("ENABLE_EVENTBRITE", "0") == "1"  # <- off

ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1") == "1"

# Column name overrides to adapt to schema variants
KEY_COL = os.getenv("KEY_COL", "name")         # we update by name unless you change it
NAME_COL = os.getenv("NAME_COL", "name")
WEBSITE_COL = os.getenv("WEBSITE_COL", "domain")
ENRICH_STATUS_COL = os.getenv("ENRICH_STATUS_COL", "enrichment_status")

# Defaults to guarantee no NULLs
DEFAULT_CAPACITY = int(os.getenv("DEFAULT_CAPACITY", "200"))
DEFAULT_AVG_TICKET_PRICE = Decimal(os.getenv("DEFAULT_AVG_TICKET_PRICE", "25"))
DEFAULT_EVENTS_PER_YEAR = int(os.getenv("DEFAULT_EVENTS_PER_YEAR", "20"))
DEFAULT_LOAD_FACTOR = Decimal(os.getenv("DEFAULT_LOAD_FACTOR", "0.70"))

bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

# Cache schema to avoid updating non-existent columns
_SCHEMA_COLS_CACHE: Optional[set[str]] = None


def _tbl() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _load_schema_cols() -> set[str]:
    global _SCHEMA_COLS_CACHE
    if _SCHEMA_COLS_CACHE is not None:
        return _SCHEMA_COLS_CACHE
    cols = set()
    sql = f"""
        SELECT column_name
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{TABLE}'
    """
    for r in bq_client.query(sql).result():
        cols.add(str(r["column_name"]))
    _SCHEMA_COLS_CACHE = cols
    return cols


def _filter_to_existing_columns(updates: Dict[str, Any]) -> Dict[str, Any]:
    cols = _load_schema_cols()
    return {k: v for k, v in updates.items() if k in cols or k == ENRICH_STATUS_COL}


def _sleep_jitter():
    ms = ROW_DELAY_MIN_MS if ROW_DELAY_MAX_MS <= ROW_DELAY_MIN_MS else random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
    time.sleep(ms / 1000.0)


def _pick_candidates(limit: int) -> List[Dict[str, Any]]:
    """
    Candidates:
      - rows not marked DONE, OR (if revenues column exists) revenues IS NULL or = 0
      - must have a website/domain
    """
    has_status_sql = f"""
      SELECT EXISTS(
        SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{TABLE}' AND column_name = '{ENRICH_STATUS_COL}'
      )
    """
    has_revenues_sql = f"""
      SELECT EXISTS(
        SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{TABLE}' AND column_name = 'revenues'
      )
    """
    has_status = list(bq_client.query(has_status_sql).result())[0][0]
    has_revenues = list(bq_client.query(has_revenues_sql).result())[0][0]

    where_parts = [f"{WEBSITE_COL} IS NOT NULL"]
    if has_status:
        where_parts.append(f"IFNULL({ENRICH_STATUS_COL}, '') != 'DONE'")
    if has_revenues:
        where_parts.append("(revenues IS NULL OR revenues = 0)")

    where_clause = " OR ".join(where_parts) if has_status else " AND ".join(where_parts)

    sql = f"""
        SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website
        FROM { _tbl() }
        WHERE {where_clause}
        LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def _update_row(row_id: Any, updates: Dict[str, Any], dry: bool) -> None:
    if dry:
        logging.info(f"[DRY] Would update {row_id} with {updates}")
        return

    updates = _filter_to_existing_columns(updates)
    if not updates:
        return

    set_clauses = []
    params = []
    for i, (k, v) in enumerate(updates.items(), start=1):
        set_clauses.append(f"`{k}` = @p{i}")
        if isinstance(v, Decimal):
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "NUMERIC", str(v)))
        elif isinstance(v, (int, float)):
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "NUMERIC", v))
        else:
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING", v))

    sql = f"""
      UPDATE { _tbl() }
         SET {", ".join(set_clauses)},
             last_updated = CURRENT_TIMESTAMP()
       WHERE `{KEY_COL}` = @row_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("row_id", "STRING", str(row_id)), *params]
    )
    bq_client.query(sql, job_config=job_config).result()
    logging.info(f"UPDATED {row_id}: {list(updates.keys())}")


def _quantize_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _enrich_one(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrape/heuristics + GPT. Also computes revenues so it's never NULL.
    """
    name = row.get("name") or row.get(NAME_COL)
    website = row.get("website") or row.get(WEBSITE_COL)

    # Scrape
    html, website_text = ("", "")
    try:
        html, website_text = scrape_website_text(website) if website else ("", "")
    except Exception:
        pass

    # Vendor (heuristic only; optional)
    signals = sniff_vendor_signals(html, website) if html else {}
    vendor_guess = choose_vendor(signals) if signals else None
    try:
        vendor = normalize_vendor_name(vendor_guess) if vendor_guess else None
    except Exception:
        vendor = vendor_guess
    vendor_update: Dict[str, Any] = {}
    if vendor and is_true_ticketing_provider(vendor):
        vendor_update = {"ticket_vendor": vendor, "ticket_vendor_source": "heuristic"}

    # Heuristic price from text
    price_from_text = None
    price_src = None
    try:
        price_from_text = derive_price_from_text(website_text)
        if price_from_text:
            price_src = "heuristic"
    except Exception:
        pass

    # GPT (also estimates events_per_year & occupancy)
    try:
        gpt = enrich_with_gpt(
            row_dict={
                "name": name,
                "website_url": website,
                "website_text": website_text,
            }
        )
    except GPTQuotaExceeded as e:
        if STOP_ON_GPT_QUOTA:
            raise
        logging.error(f"GPT quota exceeded; continuing without GPT for {name}: {e}")
        gpt = {}

    # Merge: price
    avg_price = gpt.get("avg_ticket_price")
    src_price = "gpt" if avg_price is not None else None
    if avg_price is None:
        if price_from_text is not None:
            avg_price = Decimal(str(price_from_text))
            src_price = price_src or "heuristic"
        else:
            avg_price = DEFAULT_AVG_TICKET_PRICE
            src_price = "default"

    # Merge: capacity
    capacity = gpt.get("capacity")
    src_capacity = "gpt" if capacity is not None else None
    if capacity is None:
        capacity = DEFAULT_CAPACITY
        src_capacity = "default"
    capacity = int(capacity)

    # Merge: events_per_year (frequency)
    events = gpt.get("events_per_year")
    src_events = "gpt" if events is not None else "default"
    if events is None:
        events = DEFAULT_EVENTS_PER_YEAR
    events = int(events)

    # Merge: occupancy/load factor
    occ = gpt.get("occupancy")
    src_occ = "gpt" if occ is not None else "default"
    if occ is None:
        occ = float(DEFAULT_LOAD_FACTOR)
    try:
        occ = float(occ)
    except Exception:
        occ = float(DEFAULT_LOAD_FACTOR)
    if occ < 0:
        occ = 0.0
    if occ > 1:
        occ = 1.0

    # Compute revenues and sources
    revenue = _quantize_money(Decimal(str(avg_price)) * Decimal(capacity) * Decimal(events) * Decimal(str(occ)))
    revenue_src = f"formula[{src_price},{src_capacity},{src_events},{src_occ}]"

    updates: Dict[str, Any] = {
        "avg_ticket_price": Decimal(str(avg_price)),
        "avg_ticket_price_source": src_price,
        "capacity": capacity,
        "capacity_source": src_capacity,
        "revenues": revenue,
        "revenues_source": revenue_src,
        ENRICH_STATUS_COL: "DONE",
        **vendor_update,
    }

    # If your OUTPUT table has a frequency column, populate it too.
    schema_cols = _load_schema_cols()
    if "frequency_per_year" in schema_cols:
        updates["frequency_per_year"] = events
    elif "events_per_year" in schema_cols:
        updates["events_per_year"] = events

    return _filter_to_existing_columns(updates)


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
        except GPTQuotaExceeded:
            if STOP_ON_GPT_QUOTA:
                return jsonify({"status": "QUOTA_HIT", "processed": processed}), 429
        except Exception:
            logging.exception(f"Row failed: {row}")

    return jsonify({
        "candidates": len(rows),
        "processed": processed,
        "status": "DRY_OK" if dry else "OK",
    })
