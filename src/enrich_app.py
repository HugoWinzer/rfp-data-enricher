#!/usr/bin/env python3
# src/enrich_app.py
import os
import time
import random
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional, List

from flask import Flask, request, jsonify
from google.cloud import bigquery

# ------------------------------------------------------------------------------
# App / config
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

PROJECT_ID        = os.getenv("PROJECT_ID")
DATASET_ID        = os.getenv("DATASET_ID", "rfpdata")
TABLE             = os.getenv("TABLE", "OUTPUT")
BQ_LOCATION       = os.getenv("BQ_LOCATION")  # e.g., "europe-southwest1"

# Feature flags (Eventbrite off by default)
ENABLE_TICKETMASTER = os.getenv("ENABLE_TICKETMASTER", "1") == "1"
ENABLE_PLACES       = os.getenv("ENABLE_PLACES", "1") == "1"
ENABLE_EVENTBRITE   = os.getenv("ENABLE_EVENTBRITE", "0") == "1"  # keep disabled

ROW_DELAY_MIN_MS  = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS  = int(os.getenv("ROW_DELAY_MAX_MS", "180"))
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1") == "1"

# Column name overrides (adapt to your OUTPUT schema)
KEY_COL           = os.getenv("KEY_COL", "name")          # used in WHERE for UPDATE
NAME_COL          = os.getenv("NAME_COL", "name")
WEBSITE_COL       = os.getenv("WEBSITE_COL", "domain")
ENRICH_STATUS_COL = os.getenv("ENRICH_STATUS_COL", "enrichment_status")

# Backfill control (default OFF). When false, we do NOT revisit DONE rows to fill revenues.
BACKFILL_REVENUES = os.getenv("BACKFILL_REVENUES", "0").lower() in ("1", "true", "yes")

# Defaults to guarantee no NULLs
DEFAULT_CAPACITY         = int(os.getenv("DEFAULT_CAPACITY", "200"))
DEFAULT_AVG_TICKET_PRICE = Decimal(os.getenv("DEFAULT_AVG_TICKET_PRICE", "25"))
DEFAULT_EVENTS_PER_YEAR  = int(os.getenv("DEFAULT_EVENTS_PER_YEAR", "20"))
DEFAULT_LOAD_FACTOR      = Decimal(os.getenv("DEFAULT_LOAD_FACTOR", "0.70"))

bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

# ------------------------------------------------------------------------------
# Imports from local package (tolerate package/flat)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
_SCHEMA_COLS_CACHE: Optional[set[str]] = None


def _tbl() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _load_schema_cols() -> set[str]:
    """Cache OUTPUT table columns so we only write what exists."""
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
    # Always allow ENRICH_STATUS_COL even if named differently
    return {k: v for k, v in updates.items() if (k in cols) or (k == ENRICH_STATUS_COL)}


def _sleep_jitter():
    ms = ROW_DELAY_MIN_MS if ROW_DELAY_MAX_MS <= ROW_DELAY_MIN_MS else random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
    time.sleep(ms / 1000.0)


def _pick_candidates(limit: int, *, backfill: bool) -> List[Dict[str, Any]]:
    """
    Candidate picker:
      - Always include rows not yet marked DONE (if status column exists).
      - If backfill=True and revenues exists, ALSO include rows where revenues IS NULL or 0.
      - With backfill=False, we do NOT touch DONE rows even if revenues is NULL/0.
    """
    # Build two query bodies and let BigQuery choose by `has_status`
    q_not_done = (
        f"SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website, {ENRICH_STATUS_COL} AS status "
        f"FROM { _tbl() } "
        f"WHERE {WEBSITE_COL} IS NOT NULL "
        f"AND (IFNULL({ENRICH_STATUS_COL}, \"\") != \"DONE\""
        f"{' OR (has_revenues AND (revenues IS NULL OR revenues = 0))' if backfill else ''}) "
        f"LIMIT {limit}"
    )
    q_simple = (
        f"SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website, NULL AS status "
        f"FROM { _tbl() } "
        f"WHERE {WEBSITE_COL} IS NOT NULL"
        f"{' AND (revenues IS NULL OR revenues = 0)' if backfill else ''} "
        f"LIMIT {limit}"
    )

    sql = f"""
    DECLARE has_status BOOL DEFAULT EXISTS (
      SELECT 1
      FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = '{ENRICH_STATUS_COL}'
    );
    DECLARE has_revenues BOOL DEFAULT EXISTS (
      SELECT 1
      FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{TABLE}' AND column_name = 'revenues'
    );

    EXECUTE IMMEDIATE (
      SELECT IF(has_status, '{q_not_done}', '{q_simple}')
    );
    """
    job = bq_client.query(sql)
    return [dict(r) for r in job.result()]


def _update_row(row_id: Any, updates: Dict[str, Any], dry: bool) -> None:
    if dry:
        logging.info(f"[DRY] Would update {row_id} with {updates}")
        return

    updates = _filter_to_existing_columns(updates)
    if not updates:
        return

    set_clauses, params = [], []
    for i, (k, v) in enumerate(updates.items(), start=1):
        set_clauses.append(f"`{k}` = @p{i}")
        if isinstance(v, Decimal):
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "NUMERIC", str(v)))
        elif isinstance(v, (int, float)):
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "NUMERIC", v))
        elif v is None:
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING", None))
        else:
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING", str(v)))

    # Always bump last_updated if the column exists
    if "last_updated" in _load_schema_cols():
        set_clauses.append("`last_updated` = CURRENT_TIMESTAMP()")

    sql = f"""
      UPDATE { _tbl() }
         SET {", ".join(set_clauses)}
       WHERE `{KEY_COL}` = @row_id
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("row_id", "STRING", str(row_id)), *params]
    )
    bq_client.query(sql, job_config=cfg).result()
    logging.info(f"UPDATED {row_id}: {list(updates.keys())}")


def _q_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _enrich_one(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrape/heuristics + GPT, and ALWAYS compute `revenues`.
    """
    name = row.get("name") or row.get(NAME_COL)
    website = row.get("website") or row.get(WEBSITE_COL)

    # Scrape website text (for heuristics and GPT context)
    html, text = ("", "")
    try:
        html, text = scrape_website_text(website) if website else ("", "")
    except Exception as e:
        logging.warning(f"scrape failed for {website}: {e}")

    # Heuristic price from text
    price_heur = None
    try:
        price_heur = derive_price_from_text(text)
    except Exception:
        pass

    # GPT (returns avg_ticket_price, capacity, events_per_year, occupancy)
    try:
        gpt = enrich_with_gpt(
            row_dict={
                "name": name,
                "website_url": website,
                "website_text": text,
            }
        ) or {}
    except GPTQuotaExceeded as e:
        if STOP_ON_GPT_QUOTA:
            raise
        logging.error(f"GPT quota exceeded for {name}: {e}")
        gpt = {}

    # -------- Merge fields (prefer GPT, then heuristic, then defaults) --------
    # avg_ticket_price
    avg_price = gpt.get("avg_ticket_price")
    price_src = "gpt" if avg_price is not None else None
    if avg_price is None and price_heur is not None:
        avg_price = Decimal(str(price_heur))
        price_src = "heuristic"
    if avg_price is None:
        avg_price = DEFAULT_AVG_TICKET_PRICE
        price_src = "default"
    avg_price = Decimal(str(avg_price))

    # capacity
    cap = gpt.get("capacity")
    cap_src = "gpt" if cap is not None else None
    if cap is None:
        cap = DEFAULT_CAPACITY
        cap_src = "default"
    cap = int(cap)

    # events_per_year
    events = gpt.get("events_per_year")
    events_src = "gpt" if events is not None else "default"
    if events is None:
        events = DEFAULT_EVENTS_PER_YEAR
    events = int(events)

    # occupancy/load factor (0..1)
    occ = gpt.get("occupancy")
    occ_src = "gpt" if occ is not None else "default"
    try:
        occ = float(occ) if occ is not None else float(DEFAULT_LOAD_FACTOR)
    except Exception:
        occ = float(DEFAULT_LOAD_FACTOR)
    occ = max(0.0, min(1.0, occ))

    # --------------------- Compute revenues (never NULL) ----------------------
    revenues = _q_money(avg_price * Decimal(cap) * Decimal(events) * Decimal(str(occ)))
    rev_src = f"formula[{price_src},{cap_src},{events_src},{occ_src}]"

    updates: Dict[str, Any] = {
        "avg_ticket_price": avg_price,
        "avg_ticket_price_source": price_src,
        "capacity": cap,
        "capacity_source": cap_src,
        "revenues": revenues,
        "revenues_source": rev_src,
        ENRICH_STATUS_COL: "DONE",
    }

    # If OUTPUT has a frequency column, populate it too (best-effort)
    cols = _load_schema_cols()
    if "frequency_per_year" in cols:
        updates["frequency_per_year"] = events
    elif "events_per_year" in cols:
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
    dry = request.args.get("dry", "0").lower() in ("1", "true", "yes")
    backfill = BACKFILL_REVENUES or (request.args.get("backfill", "0").lower() in ("1", "true", "yes"))

    # Safety: clamp limit
    limit = max(1, min(limit, 500))

    # Fetch candidates
    try:
        rows = _pick_candidates(limit, backfill=backfill)
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
            logging.error(f"GPT quota exceeded after {processed} rows")
            if STOP_ON_GPT_QUOTA:
                return jsonify({"status": "QUOTA_HIT", "processed": processed}), 429
        except Exception:
            logging.exception(f"Row failed: {row}")

    return jsonify({
        "candidates": len(rows),
        "processed": processed,
        "status": "DRY_OK" if dry else "OK",
    })
