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

print("Environment:", dict(os.environ))


print("Booting enrich_app.py...")
print("Environment:", dict(os.environ))  # Log all env vars

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger()

# ─── Env Vars ────────────────────────────────────────────────────────
REQUIRED = [
    "PROJECT_ID", "DATASET_ID", "RAW_TABLE", "STAGING_TABLE",
    "OPENAI_API_KEY", "TICKETMASTER_KEY", "GOOGLE_PLACES_KEY"
]
missing = [v for v in REQUIRED if v not in os.environ]
if missing:
    logger.error(f"Missing env vars: {missing}")
    sys.exit(1)

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
RAW_TABLE = os.environ["RAW_TABLE"]
STAGING_TABLE = os.environ["STAGING_TABLE"]
openai.api_key = os.environ["OPENAI_API_KEY"]
TM_KEY = os.environ["TICKETMASTER_KEY"]
PLACES_KEY = os.environ["GOOGLE_PLACES_KEY"]
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

# ─── BigQuery client ─────────────────────────────────────────────────
bq = bigquery.Client(project=PROJECT_ID)

# ─── Helper: fetch raw rows ──────────────────────────────────────────
def fetch_rows(limit: int):
    sql = f"""
      SELECT *
      FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
      WHERE enrichment_status IS NULL
      LIMIT {limit}
    """
    try:
        job = bq.query(sql)
        return [dict(row) for row in job.result()]
    except GoogleAPIError as e:
        logger.error(f"BigQuery fetch error: {e}")
        raise


# ─── Google Places lookup ───────────────────────────────────────────
def get_google_places_info(name, domain=None):
    try:
        # Prefer domain if available
        search_text = domain if domain else name
        url = (
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
            f"?input={quote_plus(search_text)}&inputtype=textquery&fields=place_id&key={PLACES_KEY}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            return {}
        place_id = candidates[0]["place_id"]
        # Get details
        details_url = (
            "https://maps.googleapis.com/maps/api/place/details/json"
            f"?place_id={place_id}&fields=price_level,types,user_ratings_total&key={PLACES_KEY}"
        )
        details_resp = requests.get(details_url, timeout=10)
        det = details_resp.json().get("result", {})
        price_level = det.get("price_level")
        avg_price = None
        if isinstance(price_level, int):
            avg_price = float(price_level * 20) + 10  # crude mapping: $=30, $$=50, etc.
        return {
            "avg_ticket_price": avg_price,
            "google_places_ratings": det.get("user_ratings_total")
        }
    except Exception as e:
        logger.warning(f"Google Places lookup failed for {name}: {e}")
        return {}

# ─── Ticketmaster lookup ────────────────────────────────────────────
def get_ticketmaster_info(name):
    url = (
        f"https://app.ticketmaster.com/discovery/v2/venues.json"
        f"?keyword={quote_plus(name)}&apikey={TM_KEY}"
    )
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        venues = data.get("_embedded", {}).get("venues", [])
        if not venues:
            return {}
        v = venues[0]
        # Try to extract average ticket price if priceRanges is available (rare)
        price_ranges = v.get("priceRanges", [])
        avg_price = None
        if price_ranges:
            avg_price = sum(
                (pr.get("min", 0) + pr.get("max", 0)) / 2 for pr in price_ranges
            ) / max(len(price_ranges), 1)
        capacity = v.get("capacity")
        return {
            "ticket_vendor": "Ticketmaster",
            "avg_ticket_price": avg_price,
            "capacity": capacity
        }
    except Exception as e:
        logger.warning(f"Ticketmaster lookup failed for {name}: {e}")
        return {}

# ─── Wikidata SPARQL lookup ──────────────────────────────────────────
def get_wikidata_info(name):
    sparql = f"""
SELECT ?capacity ?revenue WHERE {{
  ?item rdfs:label "{name}"@en.
  OPTIONAL {{ ?item wdt:P1082 ?capacity. }}
  OPTIONAL {{ ?item wdt:P2139 ?revenue. }}
}} LIMIT 1
"""
    try:
        headers = {"Accept": "application/sparql-results+json"}
        r = requests.get(WIKIDATA_ENDPOINT, params={"query": sparql}, headers=headers, timeout=10)
        results = r.json()
        bindings = results["results"]["bindings"]
        if not bindings:
            return {}
        row = bindings[0]
        cap = int(row["capacity"]["value"]) if "capacity" in row else None
        rev = float(row["revenue"]["value"]) if "revenue" in row else None
        return {"capacity": cap, "annual_revenue": rev}
    except Exception as e:
        logger.warning(f"Wikidata lookup failed for {name}: {e}")
        return {}

# ─── GPT fallback ────────────────────────────────────────────────────
def call_gpt_fallback(row):
    prompt = f"""
Act as a data researcher. Provide as much accurate info as possible for this venue:
Venue name: "{row['name']}"

Respond ONLY with a JSON object like:
{{
  "avg_ticket_price": [average single ticket price in USD, or null],
  "capacity": [integer, or null],
  "ticket_vendor": [string, or null],
  "annual_revenue": [USD, or null],
  "ticketing_revenue": [USD, or null]
}}
Do not include commentary, only valid JSON!
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": prompt}],
            max_tokens=150,
            temperature=0.0,
        )
        raw = resp.choices[0].message['content']
        logger.info(f"GPT raw output for '{row['name']}': {raw}")
        data = json.loads(raw)
        # Ensure all keys exist
        for k in ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"]:
            data.setdefault(k, None)
        return data
    except Exception as e:
        logger.warning(f"GPT fallback failed for {row.get('name')}: {e}")
        # Always return keys
        return {k: None for k in
                ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"]}

# ─── Core enrichment ─────────────────────────────────────────────────
def enrich_row(row):
    enriched = {}
    # Try Google Places (by domain or name)
    enriched.update(get_google_places_info(row.get("name"), row.get("domain")))
    # Ticketmaster (by name)
    enriched.update(get_ticketmaster_info(row.get("name")))
    # Wikidata (by name)
    enriched.update(get_wikidata_info(row.get("name")))
    # Check for missing keys
    missing = [k for k in ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"]
               if enriched.get(k) is None]
    if missing:
        gpt_result = call_gpt_fallback(row)
        for k in missing:
            enriched[k] = gpt_result.get(k)
    return enriched

# ─── Write enriched row ───────────────────────────────────────────────
def write_row(raw, enriched):
    rec = raw.copy()
    rec.update(enriched)
    rec["enrichment_status"] = "DONE"
    rec["last_updated"] = datetime.datetime.utcnow().isoformat()
    try:
        errs = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}", [rec])
        if errs:
            logger.error(f"Insert errors for '{raw.get('name')}': {errs}")
    except Exception as e:
        logger.error(f"BigQuery insert exception for '{raw.get('name')}': {e}")

# ─── HTTP endpoint ───────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_batch():
    try:
        limit = int(request.args.get("limit", "10"))
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

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
