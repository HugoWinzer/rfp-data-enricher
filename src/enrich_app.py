# src/enrich_app.py
import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import Flask, request, jsonify
from google.cloud import bigquery

from .extractors import (
    normalize_name, parse_location_hint,
    places_text_search, places_details,
    fetch_html, detect_vendor_signals, choose_best_vendor,
    tm_search_events, tm_median_min_price, tm_is_vendor_present,
    extract_capacity_from_html, wikidata_find_qid, wikidata_capacity,
    extract_prices_from_html,
)

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enricher")

# ---- env ----
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE = os.getenv("STAGING_TABLE") or os.getenv("RAW_TABLE") or "performing_arts_fixed"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")  # not used for now
TM_KEY = os.getenv("TICKETMASTER_KEY", "")
PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY", "")

USE_GPT_FALLBACK = os.getenv("USE_GPT_FALLBACK", "false").lower() == "true"  # default OFF

bq = bigquery.Client(project=PROJECT_ID)
app = Flask(__name__)

def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def row_selector(limit: int) -> str:
    # prioritize rows with NULL vendor or prices/capacity missing, and not DONE
    return f"""
    SELECT row, name, alt_name, category, sub_category, short_description, full_description,
           phone_number, domain, linkedin_url,
           ticket_vendor, capacity, avg_ticket_price,
           ticket_vendor_source, capacity_source, avg_ticket_price_source,
           enrichment_status
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
    WHERE COALESCE(enrichment_status,'PENDING') <> 'DONE'
      AND (ticket_vendor IS NULL OR capacity IS NULL OR avg_ticket_price IS NULL)
    ORDER BY last_updated DESC NULLS LAST
    LIMIT {limit}
    """

def update_row(row_id: int, data: Dict[str, Any]) -> None:
    # Build dynamic SET clause only for provided fields
    fields = []
    params = [bigquery.ScalarQueryParameter("row_id", "INT64", row_id)]
    for key in [
        "ticket_vendor", "ticket_vendor_source",
        "capacity", "capacity_source",
        "avg_ticket_price", "avg_ticket_price_source",
        "enrichment_status"
    ]:
        if key in data and data[key] is not None:
            fields.append(f"{key} = @{key}")
            typ = "STRING"
            if key in ("capacity",):
                typ = "INT64"
            elif key in ("avg_ticket_price",):
                typ = "FLOAT64"
            params.append(bigquery.ScalarQueryParameter(key, typ, data[key]))
    # always update last_updated
    fields.append("last_updated = CURRENT_TIMESTAMP()")

    if not fields:
        return

    sql = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
    SET {", ".join(fields)}
    WHERE row = @row_id
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq.query(sql, job_config=job_config).result()

def enrich_one(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Returns dict of fields to update (may be empty). Logs reasons for misses."""
    row_id = rec["row"]
    name = (rec.get("name") or "").strip()
    domain = (rec.get("domain") or "").strip()
    desc = rec.get("full_description")
    name_norm = normalize_name(name)
    loc_hint = parse_location_hint(desc)

    log.info(f"[{row_id}] START name='{name}' city='{loc_hint.get('city')}' country='{loc_hint.get('country')}' domain='{domain}'")

    results: Dict[str, Any] = {}
    # ---------- 1) places → website ----------
    place = None
    website = None
    place_source = None
    if PLACES_KEY and name:
        q = name
        if loc_hint.get("city"):
            q += f" {loc_hint['city']}"
        if loc_hint.get("country"):
            q += f" {loc_hint['country']}"
        ps = places_text_search(PLACES_KEY, q)
        if ps:
            place_id = ps.get("place_id")
            det = places_details(PLACES_KEY, place_id) if place_id else None
            if det:
                place = det
                website = det.get("website")
                place_source = f"google_places:place_id={place_id}"
                if not domain and website:
                    # you could choose to write domain as well; keeping enrichment focused for now
                    pass
    # prefer explicit domain over places website
    site_url = website or (("http://" + domain) if domain and not domain.startswith(("http://", "https://")) else domain)

    # ---------- 2) website scan for vendor ----------
    vendor_signal = None
    html_main = None
    if site_url:
        html_main = fetch_html(site_url)
        sigs = detect_vendor_signals(html_main or "", site_url)
        vendor_signal = choose_best_vendor(sigs)
        if vendor_signal:
            results["ticket_vendor"] = vendor_signal["vendor"]
            results["ticket_vendor_source"] = vendor_signal["evidence"]
            log.info(f"[{row_id}] vendor via website: {vendor_signal}")

    # ---------- 3) Ticketmaster search (fallback / cross-check) ----------
    if TM_KEY and not results.get("ticket_vendor"):
        tm_json = tm_search_events(TM_KEY, name)
        if tm_is_vendor_present(tm_json, name_norm):
            results["ticket_vendor"] = "Ticketmaster"
            results["ticket_vendor_source"] = "ticketmaster:keyword=" + name
            log.info(f"[{row_id}] vendor via TM discovery")

        # Also try to compute a price median from TM (even if vendor not TM)
        med = tm_median_min_price(tm_json)
        if med and not rec.get("avg_ticket_price"):
            results["avg_ticket_price"] = float(round(med, 2))
            results["avg_ticket_price_source"] = f"ticketmaster:median_min_over_{len(tm_json.get('_embedded', {}).get('events', []))}_events"

    # ---------- 4) capacity: website first, then Wikidata ----------
    if html_main and not rec.get("capacity"):
        cap = extract_capacity_from_html(html_main)
        if cap:
            results["capacity"] = cap[0]
            results["capacity_source"] = f"website:{site_url}#{cap[1]}"
            log.info(f"[{row_id}] capacity via site: {results['capacity']}")

    if not results.get("capacity"):
        qid = wikidata_find_qid(name, loc_hint.get("city"))
        if qid:
            cap_wd = wikidata_capacity(qid)
            if cap_wd:
                results["capacity"] = cap_wd
                results["capacity_source"] = f"wikidata:{qid}"
                log.info(f"[{row_id}] capacity via wikidata {qid}: {cap_wd}")

    # ---------- 5) avg ticket price: website text (tarifs) ----------
    if html_main and not results.get("avg_ticket_price") and not rec.get("avg_ticket_price"):
        prices = extract_prices_from_html(html_main)
        if prices:
            prices.sort()
            mid = len(prices) // 2
            avg = prices[mid] if len(prices) % 2 else (prices[mid-1] + prices[mid]) / 2.0
            results["avg_ticket_price"] = float(round(avg, 2))
            results["avg_ticket_price_source"] = f"website:{site_url}"
            log.info(f"[{row_id}] avg_ticket_price via site median: {results['avg_ticket_price']}")

    # ---------- 6) status ----------
    if any(k in results for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
        results["enrichment_status"] = "DONE"
    else:
        results["enrichment_status"] = "PENDING"
        log.info(f"[{row_id}] no evidence found")

    return results

@app.route("/", methods=["GET"])
def run_once():
    try:
        limit = int(request.args.get("limit", "5"))
    except Exception:
        limit = 5
    dry_run = request.args.get("dry_run", "false").lower() in ("1", "true", "yes")

    rows = list(bq.query(row_selector(limit)).result())
    log.info(f"Processing {len(rows)} rows (dry_run={dry_run})")

    updated = 0
    for r in rows:
        try:
            rec = dict(r.items())
            patch = enrich_one(rec)
            if not dry_run and patch:
                update_row(rec["row"], patch)
            if patch:
                updated += 1
        except Exception as e:
            log.exception(f"row {r.get('row')} failed: {e}")

    return jsonify({"processed": len(rows), "updated": updated, "dry_run": dry_run, "status": "OK"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
