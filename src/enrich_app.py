# src/enrich_app.py
import os, sys, json, decimal, logging
from typing import Dict, Any, Tuple, List

from flask import Flask, request, jsonify
from google.cloud import bigquery

from .gpt_client import enrich_with_gpt
from .extractors import (
    scrape_website_text,
    detect_vendor_signals,
    choose_best_vendor,
    places_text_search, places_details,
    tm_search_events, tm_median_min_price, tm_is_vendor_present,
    normalize_name, extract_capacity_from_html, extract_prices_from_html,
)

# ---------- env ----------
PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
TABLE = os.getenv("TABLE") or os.getenv("STAGING_TABLE", "performing_arts_fixed")
TABLE_FQN = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"

GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY", "")
TICKETMASTER_KEY = os.getenv("TICKETMASTER_KEY", "")
EVENTBRITE_TOKEN = os.getenv("EVENTBRITE_TOKEN", "")  # optional

DEBUG_LOG_N = int(os.getenv("DEBUG_LOG_N", "3"))

# ---------- setup ----------
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enricher")
BQ = bigquery.Client(project=PROJECT_ID)

# ---------- helpers ----------
def as_decimal(val):
    if val is None:
        return None
    return decimal.Decimal(str(val))

def fetch_rows(limit: int) -> List[Dict[str, Any]]:
    # pull not-DONE rows first by oldest last_updated
    sql = f"""
    SELECT *
    FROM `{TABLE_FQN}`
    WHERE COALESCE(enrichment_status,'PENDING')!='DONE'
    ORDER BY last_updated IS NULL DESC, last_updated ASC
    LIMIT {int(limit)}
    """
    return [dict(r) for r in BQ.query(sql).result()]

def update_in_place(row: Dict[str, Any], enriched: Dict[str, Any], sources: Dict[str, str], idx: int = 0):
    """
    - Only mark DONE if at least one of {ticket_vendor, capacity, avg_ticket_price} is set.
    - Otherwise mark NO_DATA.
    """
    name = row["name"]
    if idx < DEBUG_LOG_N:
        log.info("GPT parsed for '%s': %s", name, json.dumps(enriched, ensure_ascii=False))

    set_fields = []
    params = [bigquery.ScalarQueryParameter("name", "STRING", name)]

    if enriched.get("ticket_vendor"):
        set_fields += ["ticket_vendor=@ticket_vendor", "ticket_vendor_source=@ticket_vendor_source"]
        params += [
            bigquery.ScalarQueryParameter("ticket_vendor", "STRING", enriched["ticket_vendor"]),
            bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", sources.get("ticket_vendor_source", "GPT")),
        ]

    if enriched.get("capacity") is not None:
        set_fields += ["capacity=@capacity", "capacity_source=@capacity_source"]
        params += [
            bigquery.ScalarQueryParameter("capacity", "INT64", int(enriched["capacity"])),
            bigquery.ScalarQueryParameter("capacity_source", "STRING", sources.get("capacity_source", "GPT")),
        ]

    if enriched.get("avg_ticket_price") is not None:
        set_fields += [
            "avg_ticket_price=SAFE_CAST(@avg_ticket_price AS NUMERIC)",
            "avg_ticket_price_source=@avg_ticket_price_source",
        ]
        params += [
            bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", as_decimal(enriched["avg_ticket_price"])),
            bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", sources.get("avg_ticket_price_source", "GPT")),
        ]

    # status
    filled_any = any(enriched.get(k) not in (None, "") for k in ("ticket_vendor", "capacity", "avg_ticket_price"))
    status = "DONE" if filled_any else "NO_DATA"
    set_fields += ["enrichment_status=@enrichment_status", "last_updated=CURRENT_TIMESTAMP()"]
    params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", status))

    if not set_fields:
        # should never happen, but make sure we still bump last_updated
        set_fields = ["enrichment_status=@enrichment_status", "last_updated=CURRENT_TIMESTAMP()"]

    q = f"""
    UPDATE `{TABLE_FQN}`
    SET {", ".join(set_fields)}
    WHERE name=@name
    """
    BQ.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    log.info("APPLY UPDATE for %s -> %s", name, set_fields)

# ---------- enrichment pipeline ----------
def enrich_row(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    name = raw.get("name") or ""
    enriched: Dict[str, Any] = {}
    sources: Dict[str, str] = {}

    # 1) Try website vendor / capacity / price hints
    html, text = scrape_website_text(raw.get("domain"))
    if html:
        signals = detect_vendor_signals(html, f"http://{raw.get('domain')}" if raw.get("domain") else "")
        best = choose_best_vendor(signals)
        if best and not enriched.get("ticket_vendor"):
            enriched["ticket_vendor"] = best["vendor"]
            sources["ticket_vendor_source"] = "Website"

        cap = extract_capacity_from_html(html)
        if cap and enriched.get("capacity") is None:
            enriched["capacity"] = cap[0]
            sources["capacity_source"] = "Website"

        prices = extract_prices_from_html(html)
        if prices and enriched.get("avg_ticket_price") is None:
            enriched["avg_ticket_price"] = sum(prices) / len(prices)
            sources["avg_ticket_price_source"] = "Website"

    # 2) Google Places price_level → rough price proxy (optional)
    if GOOGLE_PLACES_KEY and not enriched.get("avg_ticket_price"):
        try:
            result = places_text_search(GOOGLE_PLACES_KEY, name) or {}
            if result.get("place_id"):
                details = places_details(GOOGLE_PLACES_KEY, result["place_id"]) or {}
                price_level = details.get("price_level")
                if isinstance(price_level, int):
                    # heuristic: 0..4 mapped to ~€10..€90
                    enriched["avg_ticket_price"] = float(price_level * 20 + 10)
                    sources["avg_ticket_price_source"] = "Google Places"
        except Exception:
            pass

    # 3) Ticketmaster events heuristics (optional)
    if TICKETMASTER_KEY:
        try:
            tm = tm_search_events(TICKETMASTER_KEY, name)
            if not enriched.get("ticket_vendor") and tm_is_vendor_present(tm, normalize_name(name)):
                enriched["ticket_vendor"] = "Ticketmaster"
                sources["ticket_vendor_source"] = "Ticketmaster"
            if not enriched.get("avg_ticket_price"):
                median_min = tm_median_min_price(tm)
                if median_min:
                    enriched["avg_ticket_price"] = median_min
                    sources["avg_ticket_price_source"] = "Ticketmaster"
        except Exception:
            pass

    # 4) GPT fallback to fill any remaining fields
    missing = [k for k in ("avg_ticket_price", "capacity", "ticket_vendor") if enriched.get(k) is None]
    if missing and os.getenv("OPENAI_API_KEY"):
        gpt_out = enrich_with_gpt(raw, web_context=text)
        for k in missing:
            if gpt_out.get(k) is not None and enriched.get(k) is None:
                enriched[k] = gpt_out[k]
                sources[f"{k}_source"] = "GPT"

    return enriched, sources

# ---------- Flask app ----------
app = Flask(__name__)

@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/", methods=["GET"])
def run_batch():
    try:
        limit = int(request.args.get("limit", "10"))
    except Exception:
        return jsonify(error="invalid limit"), 400

    log.info("=== UPDATE MODE: no inserts; BigQuery UPDATE only ===")
    rows = fetch_rows(limit)
    log.info("Processing %d rows", len(rows))

    processed = 0
    for idx, r in enumerate(rows):
        enriched, sources = enrich_row(r)
        update_in_place(r, enriched, sources, idx)
        processed += 1

    return jsonify(processed=processed, status="OK")

if __name__ == "__main__":
    # local dev only; Cloud Run uses gunicorn CMD in Dockerfile
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
