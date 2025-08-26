#!/usr/bin/env python3
import os
import time
import random
import logging
import json
from decimal import Decimal
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify
from google.cloud import bigquery

# OpenAI (direct call just for revenue estimation)
from openai import OpenAI
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
        extract_linkedin_url,
        extract_phone_numbers,
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
        vendor_from_eventbrite,
        avg_price_from_google_places,
        extract_linkedin_url,
        extract_phone_numbers,
        extract_alt_name,
        extract_descriptions,
        extract_capacity,
    )

# ------------------------------------------------------------------------------
# App / config
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

PROJECT_ID        = os.getenv("PROJECT_ID")
DATASET_ID        = os.getenv("DATASET_ID", "rfpdata")
TABLE             = os.getenv("TABLE", "culture_merged")
BQ_LOCATION       = os.getenv("BQ_LOCATION")  # e.g., "europe-southwest1"

ENABLE_TICKETMASTER = os.getenv("ENABLE_TICKETMASTER", "1") == "1"
ENABLE_PLACES       = os.getenv("ENABLE_PLACES", "1") == "1"
ENABLE_EVENTBRITE   = os.getenv("ENABLE_EVENTBRITE", "0") == "1"  # keep OFF

ROW_DELAY_MIN_MS  = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS  = int(os.getenv("ROW_DELAY_MAX_MS", "180"))
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "0") == "1"

# Column overrides
KEY_COL           = os.getenv("KEY_COL", "id")
NAME_COL          = os.getenv("NAME_COL", "name")
WEBSITE_COL       = os.getenv("WEBSITE_COL", "website")
ENRICH_STATUS_COL = os.getenv("ENRICH_STATUS_COL", "enrichment_status")

# Conservative defaults (only used if GPT completely unavailable)
DEFAULT_CAPACITY         = int(os.getenv("DEFAULT_CAPACITY", "200"))
DEFAULT_AVG_TICKET_PRICE = Decimal(os.getenv("DEFAULT_AVG_TICKET_PRICE", "25"))
DEFAULT_EVENTS_PER_YEAR  = int(os.getenv("DEFAULT_EVENTS_PER_YEAR", "20"))
DEFAULT_LOAD_FACTOR      = Decimal(os.getenv("DEFAULT_LOAD_FACTOR", "0.7"))

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
    Pick rows that still need work even if status is OK:
      - revenues IS NULL  (backfill revenues)
      - OR enrichment_status != 'OK'
    Always skip LOCKED rows.
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
          WHERE {WEBSITE_COL} IS NOT NULL
            AND IFNULL({ENRICH_STATUS_COL}, "") != "LOCKED"
            AND (revenues IS NULL OR IFNULL({ENRICH_STATUS_COL}, "") != "OK")
          LIMIT {limit}',
        'SELECT {KEY_COL} AS id, {NAME_COL} AS name, {WEBSITE_COL} AS website, NULL AS status
           FROM { _tbl() }
          WHERE {WEBSITE_COL} IS NOT NULL
            AND revenues IS NULL
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

    if not updates:
        return

    set_clauses, params = [], []
    for i, (k, v) in enumerate(updates.items(), start=1):
        set_clauses.append(f"`{k}` = @p{i}")
        # Let BQ coerce NUMERICs from strings when needed
        if isinstance(v, (int, float, Decimal)):
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "NUMERIC", str(v)))
        else:
            params.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING", None if v is None else str(v)))

    sql = f"""
    UPDATE { _tbl() }
       SET {", ".join(set_clauses)}
     WHERE `{KEY_COL}` = @row_id
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("row_id", "STRING", str(row_id)), *params])
    bq_client.query(sql, job_config=cfg).result()
    logging.info(f"UPDATED {row_id}: {list(updates.keys())}")

# --------------------- GPT revenue estimator (events/year aware) ----------------
def _gpt_estimate_revenue(
    name: str,
    website_text: str,
    avg_ticket_price: Optional[Decimal],
    capacity: Optional[int],
) -> Optional[int]:
    """
    Ask GPT to estimate yearly gross ticket revenue by inferring:
      - events_per_year
      - load_factor (0..1)
      - avg_ticket_price (use provided when available)
      - capacity (use provided when available)
    Returns an integer revenue estimate, or None on failure.
    """
    try:
        known = {
            "name": name,
            "avg_ticket_price": float(avg_ticket_price) if avg_ticket_price is not None else None,
            "capacity": int(capacity) if capacity is not None else None,
        }
        system = (
            "You estimate yearly gross TICKET REVENUE for an arts/culture org.\n"
            "When not explicitly stated, infer reasonable numbers from context.\n"
            "Prefer facts from the website text. If missing, infer typical values for similar orgs.\n"
            "Output strictly valid JSON with the following keys:\n"
            "  events_per_year (integer), load_factor (0..1), avg_ticket_price (number),\n"
            "  capacity (integer), revenue_estimate (integer, in same currency as price or USD if unknown).\n"
            "Use: revenue_estimate = events_per_year * capacity * load_factor * avg_ticket_price.\n"
            "Avoid explanations or extra text."
        )
        user = {
            "task": "Estimate yearly ticket revenue with reasonable assumptions.",
            "known_values": known,
            "website_text_excerpt": website_text[:1800],
        }
        resp = _openai.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        data = json.loads(raw)
        rev = data.get("revenue_estimate")
        if isinstance(rev, (int, float)) and rev >= 0:
            return int(rev)
        return None
    except Exception as e:
        logging.warning(f"Revenue GPT estimate failed: {e}")
        return None

# ------------------------------------------------------------------------------
# Core enrichment
# ------------------------------------------------------------------------------
def _enrich_one(row: Dict[str, Any]) -> Dict[str, Any]:
    name    = row.get("name")   or row.get(NAME_COL)
    website = row.get("website") or row.get(WEBSITE_COL)

    website_text  = scrape_website_text(website) if website else ""
    alt_name      = extract_alt_name(website_text)
    descriptions  = extract_descriptions(website_text)
    # We ignore phone enrichment per request, but we still parse LinkedIn
    linkedin      = extract_linkedin_url(website_text)

    # Google Places only for price (phone skipped)
    places_price = None
    if ENABLE_PLACES and name:
        places_price = avg_price_from_google_places(name, website)

    # Vendor detection (Eventbrite can be disabled)
    signals   = sniff_vendor_signals(website_text)
    tm_vendor = vendor_from_ticketmaster(name, website) if ENABLE_TICKETMASTER else None
    eb_vendor = vendor_from_eventbrite(name, website) if ENABLE_EVENTBRITE else None
    vendor_guess = tm_vendor or eb_vendor or choose_vendor(signals)
    vendor = normalize_vendor_name(vendor_guess) if vendor_guess else None

    # Heuristic price & capacity from site text
    price_heur, _price_src = derive_price_from_text(website_text)
    capacity_heur          = extract_capacity(website_text)

    # General GPT pass
    try:
        gpt_update = enrich_with_gpt(
            row_dict={
                "name": name,
                "alt_name": alt_name,
                "website_url": website,
                "description": descriptions[:512] if descriptions else None,
                "website_text": website_text,
                "request_revenues_estimate": True,
                "estimation_note": (
                    "Please estimate events_per_year, load/utilization, and output a revenue figure."
                ),
            }
        ) or {}
    except GPTQuotaExceeded as e:
        if STOP_ON_GPT_QUOTA:
            raise
        logging.error(f"GPT quota exceeded, continuing without GPT for {name}: {e}")
        gpt_update = {}

    # ----------------------------- merge fields ------------------------------
    updates: Dict[str, Any] = {}

    # avg_ticket_price
    avg_price_val: Optional[Decimal] = None
    if gpt_update.get("avg_ticket_price") is not None:
        try:
            avg_price_val = Decimal(str(gpt_update["avg_ticket_price"]))
        except Exception:
            avg_price_val = None
    if avg_price_val is None and places_price is not None:
        try:
            avg_price_val = Decimal(str(places_price))
        except Exception:
            pass
    if avg_price_val is None and price_heur is not None:
        try:
            avg_price_val = Decimal(str(price_heur))
        except Exception:
            pass
    if avg_price_val is not None:
        updates["avg_ticket_price"] = float(avg_price_val)

    # capacity
    cap_val: Optional[int] = None
    if gpt_update.get("capacity") is not None:
        try:
            cap_val = int(gpt_update["capacity"])
        except Exception:
            cap_val = None
    if cap_val is None and capacity_heur:
        try:
            cap_val = int(capacity_heur)
        except Exception:
            pass
    if cap_val is not None:
        updates["capacity"] = int(cap_val)

    # vendor
    if vendor and is_true_ticketing_provider(vendor):
        updates["ticket_vendor"] = vendor

    # linkedin
    if linkedin:
        updates["linkedin_url"] = linkedin

    # --------------------- revenues: GPT estimate (events/year aware) ----------
    revenues_val: Optional[int] = None

    # 1) take revenues from the general GPT if it already produced one
    if gpt_update.get("revenues") is not None:
        try:
            revenues_val = int(float(gpt_update["revenues"]))
        except Exception:
            revenues_val = None

    # 2) otherwise, run the dedicated estimator prompt that infers events/year
    if revenues_val is None:
        revenues_val = _gpt_estimate_revenue(
            name=name,
            website_text=website_text,
            avg_ticket_price=avg_price_val,
            capacity=cap_val,
        )

    # 3) hard fallback only if GPT fails completely
    if revenues_val is None:
        p = avg_price_val if avg_price_val is not None else DEFAULT_AVG_TICKET_PRICE
        c = cap_val if cap_val is not None else DEFAULT_CAPACITY
        e = DEFAULT_EVENTS_PER_YEAR
        lf = DEFAULT_LOAD_FACTOR
        revenues_val = int((p * c * lf * e).to_integral_value())

    updates["revenues"] = revenues_val

    # Done — keep marking OK when we update anything (skips LOCKED because we don't pick them)
    if updates:
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
        except Exception:
            logging.exception(f"Row failed: {row}")

    return jsonify({"candidates": len(rows), "processed": processed, "status": "DRY_OK" if dry else "OK"})
