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
from bs4 import BeautifulSoup

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

# --- Eventbrite Credentials ---
EVENTBRITE_TOKEN = "553UU7UT3NAMJOUNMRJ7"

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
        details_url = (
            "https://maps.googleapis.com/maps/api/place/details/json"
            f"?place_id={place_id}&fields=price_level,types,user_ratings_total&key={PLACES_KEY}"
        )
        details_resp = requests.get(details_url, timeout=10)
        det = details_resp.json().get("result", {})
        price_level = det.get("price_level")
        avg_price = None
        if isinstance(price_level, int):
            avg_price = float(price_level * 20) + 10
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

# ─── Eventbrite lookup ───────────────────────────────────────────────
def get_eventbrite_info(name):
    try:
        headers = {"Authorization": f"Bearer {EVENTBRITE_TOKEN}"}
        url = f"https://www.eventbriteapi.com/v3/venues/search/?q={quote_plus(name)}"
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        venues = data.get("venues", [])
        if not venues:
            return {}
        v = venues[0]
        return {
            "ticket_vendor": "Eventbrite",
            "capacity": v.get("capacity"),
            "eventbrite_id": v.get("id")
        }
    except Exception as e:
        logger.warning(f"Eventbrite lookup failed for {name}: {e}")
        return {}

# ─── Web Scraping for context ────────────────────────────────────────
def scrape_venue_website(domain):
    if not domain:
        return ""
    try:
        url = domain
        if not url.startswith("http"):
            url = "http://" + url
        resp = requests.get(url, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        texts = soup.stripped_strings
        text = " ".join(list(texts)[:300])  # Take first 300 text chunks for prompt
        return text
    except Exception as e:
        logger.warning(f"Web scrape failed for domain {domain}: {e}")
        return ""

# ─── GPT fallback (with web context) ──────────────────────────────
def call_gpt_fallback(row, web_context=""):
    prompt = f"""
You are an expert on venues and ticket sales.
Fill in the following fields for "{row['name']}". Use as much context as possible.

Venue known details:
- Name: {row['name']}
- Alt name: {row.get('alt_name')}
- Category: {row.get('category')}
- Description: {row.get('short_description') or ''} {row.get('full_description') or ''}
- Domain: {row.get('domain')}
- Linkedin: {row.get('linkedin_url')}
- Phone: {row.get('phone_number') or ''}

Additional website context (scraped text): {web_context[:1000]}

Respond ONLY with a JSON object like:
{{
  "avg_ticket_price": [number in USD, or a reasonable guess],
  "capacity": [integer, or a reasonable guess],
  "ticket_vendor": [string, or "Unknown"],
  "annual_revenue": [number, or a reasonable guess],
  "ticketing_revenue": [number, or a reasonable guess]
}}
Do NOT add extra text or explanation—ONLY the JSON.
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": prompt}],
            max_tokens=200,
            temperature=0.1,
        )
        raw = resp.choices[0].message['content']
        logger.info(f"GPT raw output for '{row['name']}': {raw}")
        try:
            data = json.loads(raw)
        except Exception as parse_exc:
            logger.error(f"GPT output parse error for '{row['name']}': {parse_exc} | Raw: {raw}")
            import re
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(0))
                except Exception:
                    data = {}
            else:
                data = {}
        # Ensure all keys exist, with fallback guesses if not present
        data.setdefault("avg_ticket_price", 30)
        data.setdefault("capacity", 500)
        data.setdefault("ticket_vendor", "Unknown")
        data.setdefault("annual_revenue", 100000)
        data.setdefault("ticketing_revenue", 40000)
        return data
    except Exception as e:
        logger.warning(f"GPT fallback failed for {row.get('name')}: {e}")
        return {
            "avg_ticket_price": 30,
            "capacity": 500,
            "ticket_vendor": "Unknown",
            "annual_revenue": 100000,
            "ticketing_revenue": 40000
        }

# ─── Core enrichment ──────────────────────────────────────────────
def enrich_row(row):
    enriched = {}

    # Google Places
    enriched.update(get_google_places_info(row.get("name"), row.get("domain")))

    # Ticketmaster
    enriched.update(get_ticketmaster_info(row.get("name")))

    # Eventbrite
    enriched.update(get_eventbrite_info(row.get("name")))

    # Wikidata
    enriched.update(get_wikidata_info(row.get("name")))

    # Check for missing keys
    missing = [k for k in ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"]
               if enriched.get(k) is None]
    # Scrape context for GPT prompt
    web_context = scrape_venue_website(row.get("domain"))
    if missing:
        gpt_result = call_gpt_fallback(row, web_context=web_context)
        for k in missing:
            enriched[k] = gpt_result.get(k)
    return enriched

# ─── Write enriched row ───────────────────────────────────────────
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

# ─── HTTP endpoint ────────────────────────────────────────────────
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
