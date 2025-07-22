import os
import sys
import json
import datetime
import logging
from urllib.parse import quote_plus

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import openai

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger()

# ─── Env Vars ────────────────────────────────────────────────────────
REQUIRED = [
    "PROJECT_ID","DATASET_ID","RAW_TABLE","STAGING_TABLE","OPENAI_API_KEY",
    "TICKETMASTER_KEY","GOOGLE_PLACES_KEY"
]
missing = [v for v in REQUIRED if v not in os.environ]
if missing:
    logger.error(f"Missing env vars: {missing}")
    sys.exit(1)

PROJECT_ID        = os.environ["PROJECT_ID"]
DATASET_ID        = os.environ["DATASET_ID"]
RAW_TABLE         = os.environ["RAW_TABLE"]
STAGING_TABLE     = os.environ["STAGING_TABLE"]
openai.api_key    = os.environ["OPENAI_API_KEY"]
TM_KEY            = os.environ["TICKETMASTER_KEY"]
PLACES_KEY        = os.environ["GOOGLE_PLACES_KEY"]
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

# ─── BigQuery client ─────────────────────────────────────────────────
bq = bigquery.Client(project=PROJECT_ID)

# ─── Helper: fetch raw rows ──────────────────────────────────────────
def fetch_rows(limit: int):
    sql = f"""
      SELECT r.*
      FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}` AS r
      LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}` AS s
        ON r.name = s.name AND r.domain = s.domain
      WHERE s.name IS NULL
      LIMIT {limit}
    """
    try:
        job = bq.query(sql)
        return [dict(row) for row in job.result()]
    except GoogleAPIError as e:
        logger.error(f"BigQuery fetch error: {e}")
        raise

# ─── Ticketmaster lookup ────────────────────────────────────────────
def get_ticketmaster_info(name):
    url = (
      f"https://app.ticketmaster.com/discovery/v2/venues.json"
      f"?keyword={quote_plus(name)}&apikey={TM_KEY}"
    )
    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        venues = data.get("_embedded", {}).get("venues", [])
        if not venues:
            return {}
        v = venues[0]
        return {
          "ticket_vendor": "Ticketmaster",
          # priceRanges is a list of {min, max, currency}
          "avg_ticket_price": (
            sum((pr.get("min",0)+pr.get("max",0))/2 for pr in v.get("priceRanges",[]))
            / max(len(v.get("priceRanges",[])),1)
          )
        }
    except Exception as e:
        logger.warning(f"Ticketmaster lookup failed for {name}: {e}")
        return {}

# ─── Google Places lookup ───────────────────────────────────────────
def get_google_places_info(name):
    # find place_id
    find_url = (
      "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
      f"?input={quote_plus(name)}&inputtype=textquery&fields=place_id&key={PLACES_KEY}"
    )
    try:
        resp = requests.get(find_url, timeout=5).json()
        candidates = resp.get("candidates",[])
        if not candidates:
            return {}
        pid = candidates[0]["place_id"]
        # get details
        details_url = (
          "https://maps.googleapis.com/maps/api/place/details/json"
          f"?place_id={pid}&fields=price_level,types&key={PLACES_KEY}"
        )
        det = requests.get(details_url, timeout=5).json().get("result",{})
        # map price_level (0–4) to an approximate numeric USD price
        price_level = det.get("price_level")
        avg_price = None
        if isinstance(price_level, int):
            avg_price = float(price_level * 20) + 10  # e.g. "$"→30 USD
        return {
          "avg_ticket_price": avg_price,
          "capacity": None  # Google Places doesn’t give capacity; leave for GPT
        }
    except Exception as e:
        logger.warning(f"Google Places lookup failed for {name}: {e}")
        return {}

# ─── Wikidata SPARQL lookup ──────────────────────────────────────────
def get_wikidata_info(name):
    # Try to fetch venue capacity (P1082) and annual revenue (P2139)
    sparql = f"""
SELECT ?capacity ?revenue WHERE {{
  ?item rdfs:label "{name}"@en.
  OPTIONAL {{ ?item wdt:P1082 ?capacity. }}
  OPTIONAL {{ ?item wdt:P2139 ?revenue. }}
}} LIMIT 1
"""
    try:
        headers = {"Accept":"application/sparql-results+json"}
        r = requests.get(WIKIDATA_ENDPOINT, params={"query": sparql}, headers=headers, timeout=5).json()
        row = r["results"]["bindings"][0]
        cap = int(row["capacity"]["value"]) if "capacity" in row else None
        rev = float(row["revenue"]["value"]) if "revenue" in row else None
        return {"capacity": cap, "annual_revenue": rev}
    except Exception as e:
        logger.warning(f"Wikidata lookup failed for {name}: {e}")
        return {}

# ─── GPT fallback ────────────────────────────────────────────────────
def call_gpt_fallback(row):
    prompt = f"""
You have partial data for a venue. Fill in the missing fields in JSON:
  avg_ticket_price, capacity, ticket_vendor, annual_revenue, ticketing_revenue.

Known data:
Name: {row['name']}
Domain: {row.get('domain') or ''}
Alt name: {row.get('alt_name') or ''}
Category: {row.get('category') or ''}
Short desc: {row.get('short_description') or ''}
Full desc: {row.get('full_description') or ''}
Phone: {row.get('phone_number') or ''}

Also use these hints:
- The “ticket_vendor” is often Ticketmaster or the venue’s official site.
- If you have no data, return null.

Return _only_ JSON with the five keys.
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
              {"role":"system","content":"You are an expert data extractor."},
              {"role":"user","content":prompt}
            ],
            temperature=0,
            max_tokens=200
        )
        data = json.loads(resp.choices[0].message.content.strip())
        # ensure keys exist
        for k in ["avg_ticket_price","capacity","ticket_vendor","annual_revenue","ticketing_revenue"]:
            data.setdefault(k, None)
        return data
    except Exception as e:
        logger.warning(f"GPT fallback failed: {e}")
        return {k: None for k in
          ["avg_ticket_price","capacity","ticket_vendor","annual_revenue","ticketing_revenue"]}

# ─── Core enrichment ─────────────────────────────────────────────────
def enrich_row(row):
    # --- DUMMY TEST ENRICHMENT ---
    return {
        "avg_ticket_price": 42.0,
        "capacity": 1234,
        "ticket_vendor": "TEST_VENDOR",
        "annual_revenue": 999999.0,
        "ticketing_revenue": 111111.0
    }


# ─── Write enriched row ───────────────────────────────────────────────
def write_row(raw, enriched):
    rec = raw.copy()
    rec.update(enriched)
    rec["enrichment_status"] = "DONE"
    rec["last_updated"]     = datetime.datetime.utcnow().isoformat()
    try:
        errs = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}", [rec])
        if errs:
            logger.error(f"Insert errors: {errs}")
    except Exception as e:
        logger.error(f"BigQuery insert exception: {e}")

# ─── HTTP endpoint ───────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_batch():
    try:
        limit = int(request.args.get("limit","10"))
    except:
        return jsonify(error="invalid limit"), 400

    rows = fetch_rows(limit)
    logger.info(f"Processing {len(rows)} rows")
    processed = 0
    for r in rows:
        enriched = enrich_row(r)
        write_row(r, enriched)
        processed += 1
    return jsonify(processed=processed, status="OK")

if __name__=="__main__":
    port = int(os.getenv("PORT",8080))
    app.run(host="0.0.0.0", port=port)
