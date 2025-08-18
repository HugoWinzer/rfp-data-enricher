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

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger()

# Environment Variables
PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
RAW_TABLE = os.getenv("RAW_TABLE", "performing_arts_fixed")
STAGING_TABLE = os.getenv("STAGING_TABLE", RAW_TABLE)  # same as RAW
openai.api_key = os.getenv("OPENAI_API_KEY")
TM_KEY = os.getenv("TICKETMASTER_KEY")
PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY")
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
EVENTBRITE_TOKEN = os.getenv("EVENTBRITE_TOKEN", "553UU7UT3NAMJOUNMRJ7")

bq = bigquery.Client(project=PROJECT_ID)

# --------------------
# BigQuery Helpers
# --------------------
def fetch_rows(limit: int):
    sql = f"""
      SELECT *
      FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
      WHERE enrichment_status IS NULL OR enrichment_status = 'PENDING'
      LIMIT {limit}
    """
    try:
        job = bq.query(sql)
        return [dict(row) for row in job.result()]
    except GoogleAPIError as e:
        logger.error(f"BigQuery fetch error: {e}")
        raise

# --------------------
# External Sources
# --------------------
def get_google_places_info(name, domain=None):
    try:
        search_text = domain if domain else name
        url = (
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
            f"?input={quote_plus(search_text)}&inputtype=textquery&fields=place_id&key={PLACES_KEY}"
        )
        resp = requests.get(url, timeout=10)
        candidates = resp.json().get("candidates", [])
        if not candidates:
            return {}, {}
        place_id = candidates[0]["place_id"]
        details_url = (
            "https://maps.googleapis.com/maps/api/place/details/json"
            f"?place_id={place_id}&fields=price_level,user_ratings_total&key={PLACES_KEY}"
        )
        det = requests.get(details_url, timeout=10).json().get("result", {})
        avg_price = None
        if isinstance(det.get("price_level"), int):
            avg_price = float(det["price_level"] * 20) + 10
        if avg_price:
            return {"avg_ticket_price": avg_price}, {"avg_ticket_price_source": "Google Places"}
        return {}, {}
    except Exception as e:
        logger.warning(f"Google Places lookup failed for {name}: {e}")
        return {}, {}

def get_ticketmaster_info(name):
    url = f"https://app.ticketmaster.com/discovery/v2/venues.json?keyword={quote_plus(name)}&apikey={TM_KEY}"
    try:
        r = requests.get(url, timeout=10).json()
        venues = r.get("_embedded", {}).get("venues", [])
        if not venues:
            return {}, {}
        v = venues[0]
        out, src = {}, {}
        if v.get("capacity"):
            out["capacity"] = v["capacity"]
            src["capacity_source"] = "Ticketmaster"
        out["ticket_vendor"] = "Ticketmaster"
        src["ticket_vendor_source"] = "Ticketmaster"
        return out, src
    except Exception as e:
        logger.warning(f"Ticketmaster lookup failed for {name}: {e}")
        return {}, {}

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
        r = requests.get(WIKIDATA_ENDPOINT, params={"query": sparql}, headers=headers, timeout=10).json()
        bindings = r["results"]["bindings"]
        if not bindings:
            return {}, {}
        row = bindings[0]
        out, src = {}, {}
        if "capacity" in row:
            out["capacity"] = int(row["capacity"]["value"])
            src["capacity_source"] = "Wikidata"
        if "revenue" in row:
            out["annual_revenue"] = float(row["revenue"]["value"])
            src["annual_revenue_source"] = "Wikidata"
        return out, src
    except Exception as e:
        logger.warning(f"Wikidata lookup failed for {name}: {e}")
        return {}, {}

# --------------------
# GPT Fallback
# --------------------
def scrape_venue_website(domain):
    if not domain:
        return ""
    try:
        url = domain if domain.startswith("http") else "http://" + domain
        resp = requests.get(url, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        return " ".join(list(soup.stripped_strings)[:300])
    except Exception as e:
        logger.warning(f"Web scrape failed for {domain}: {e}")
        return ""

def call_gpt_fallback(row, web_context="", filled=None):
    prompt = f"""
You are a cautious data enrichment agent.
Fill ONLY missing fields for "{row['name']}".
Known values: {filled if filled else {}}

Venue details:
- Name: {row['name']}
- Domain: {row.get('domain')}
- Linkedin: {row.get('linkedin_url')}
- Phone: {row.get('phone_number') or ''}
Scraped text: {web_context[:1000]}

Respond strictly in JSON:
{{
  "avg_ticket_price": number or null,
  "capacity": integer or null,
  "ticket_vendor": string or null,
  "annual_revenue": number or null,
  "ticketing_revenue": number or null
}}
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-4.1-mini",
            messages=[{"role": "system", "content": prompt}],
            max_tokens=200,
            temperature=0.2,
        )
        raw = resp.choices[0].message["content"]
        data = json.loads(raw)
        sources = {f"{k}_source": "GPT" for k, v in data.items() if v is not None}
        return data, sources
    except Exception as e:
        logger.warning(f"GPT fallback failed: {e}")
        return {}, {}

# --------------------
# Enrichment Logic
# --------------------
def enrich_row(row):
    enriched, sources = {}, {}

    for fn in [get_google_places_info, get_ticketmaster_info, get_wikidata_info]:
        result, src = fn(row.get("name"), row.get("domain")) if fn == get_google_places_info else fn(row.get("name"))
        enriched.update({k: v for k, v in result.items() if v is not None})
        sources.update(src)

    # GPT fallback
    missing = [k for k in ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"] if not enriched.get(k)]
    if missing:
        web_context = scrape_venue_website(row.get("domain"))
        gpt_result, gpt_sources = call_gpt_fallback(row, web_context, enriched)
        for k in missing:
            if gpt_result.get(k) is not None:
                enriched[k] = gpt_result[k]
                sources[f"{k}_source"] = "GPT"

    return enriched, sources

def write_row(raw, enriched, sources):
    rec = raw.copy()
    rec.update(enriched)
    rec.update(sources)
    rec["enrichment_status"] = "DONE"
    rec["last_updated"] = datetime.datetime.utcnow().isoformat()
    try:
        errs = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}", [rec])
        if errs:
            logger.error(f"Insert errors: {errs}")
    except Exception as e:
        logger.error(f"BigQuery insert exception: {e}")

# --------------------
# Flask App
# --------------------
app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_batch():
    limit = int(request.args.get("limit", "10"))
    rows = fetch_rows(limit)
    processed = 0
    for r in rows:
        enriched, sources = enrich_row(r)
        write_row(r, enriched, sources)
        processed += 1
    return jsonify(processed=processed, status="OK")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
