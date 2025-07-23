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
EVENTBRITE_TOKEN = "553UU7UT3NAMJOUNMRJ7"

bq = bigquery.Client(project=PROJECT_ID)

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
            return {}, {}
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
        out = {"avg_ticket_price": avg_price} if avg_price is not None else {}
        source = {"avg_ticket_price_source": "Google Places"} if avg_price is not None else {}
        return out, source
    except Exception as e:
        logger.warning(f"Google Places lookup failed for {name}: {e}")
        return {}, {}

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
            return {}, {}
        v = venues[0]
        price_ranges = v.get("priceRanges", [])
        avg_price = None
        if price_ranges:
            avg_price = sum(
                (pr.get("min", 0) + pr.get("max", 0)) / 2 for pr in price_ranges
            ) / max(len(price_ranges), 1)
        capacity = v.get("capacity")
        out = {}
        source = {}
        if avg_price is not None:
            out["avg_ticket_price"] = avg_price
            source["avg_ticket_price_source"] = "Ticketmaster"
        if capacity is not None:
            out["capacity"] = capacity
            source["capacity_source"] = "Ticketmaster"
        out["ticket_vendor"] = "Ticketmaster"
        source["ticket_vendor_source"] = "Ticketmaster"
        return out, source
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
        r = requests.get(WIKIDATA_ENDPOINT, params={"query": sparql}, headers=headers, timeout=10)
        results = r.json()
        bindings = results["results"]["bindings"]
        if not bindings:
            return {}, {}
        row = bindings[0]
        out = {}
        source = {}
        if "capacity" in row:
            out["capacity"] = int(row["capacity"]["value"])
            source["capacity_source"] = "Wikidata"
        if "revenue" in row:
            out["annual_revenue"] = float(row["revenue"]["value"])
            source["annual_revenue_source"] = "Wikidata"
        return out, source
    except Exception as e:
        logger.warning(f"Wikidata lookup failed for {name}: {e}")
        return {}, {}

def get_eventbrite_info(name):
    try:
        headers = {"Authorization": f"Bearer {EVENTBRITE_TOKEN}"}
        url = f"https://www.eventbriteapi.com/v3/venues/search/?q={quote_plus(name)}"
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        venues = data.get("venues", [])
        if not venues:
            return {}, {}
        v = venues[0]
        out = {}
        source = {}
        if v.get("capacity"):
            out["capacity"] = v.get("capacity")
            source["capacity_source"] = "Eventbrite"
        out["ticket_vendor"] = "Eventbrite"
        source["ticket_vendor_source"] = "Eventbrite"
        return out, source
    except Exception as e:
        logger.warning(f"Eventbrite lookup failed for {name}: {e}")
        return {}, {}

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
        text = " ".join(list(texts)[:300])
        return text
    except Exception as e:
        logger.warning(f"Web scrape failed for domain {domain}: {e}")
        return ""

def call_gpt_fallback(row, web_context="", filled=None):
    prompt = f"""
You are an expert on venues and ticket sales.
Fill in ONLY the fields that are missing below for "{row['name']}".
Known fields: {filled if filled else {}}

Venue details:
- Name: {row['name']}
- Alt name: {row.get('alt_name')}
- Category: {row.get('category')}
- Description: {row.get('short_description') or ''} {row.get('full_description') or ''}
- Domain: {row.get('domain')}
- Linkedin: {row.get('linkedin_url')}
- Phone: {row.get('phone_number') or ''}
Additional website context (scraped text): {web_context[:1000]}

Respond ONLY with a JSON object. 
If you do not know a value, set it to null. You may guess the ticket_vendor if you can infer from web or scraped content.
Example:
{{
  "avg_ticket_price": [number or null],
  "capacity": [integer or null],
  "ticket_vendor": [string or null],
  "annual_revenue": [number or null],
  "ticketing_revenue": [number or null]
}}
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": prompt}],
            max_tokens=200,
            temperature=0.2,
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
        # Build sources dict:
        sources = {}
        for k in ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"]:
            if data.get(k) is not None:
                sources[f"{k}_source"] = "GPT"
        return data, sources
    except Exception as e:
        logger.warning(f"GPT fallback failed for {row.get('name')}: {e}")
        return {
            "avg_ticket_price": None,
            "capacity": None,
            "ticket_vendor": None,
            "annual_revenue": None,
            "ticketing_revenue": None
        }, {}

def enrich_row(row):
    enriched = {}
    sources = {}

    # Google Places
    result, source = get_google_places_info(row.get("name"), row.get("domain"))
    enriched.update(result)
    sources.update(source)

    # Ticketmaster
    result, source = get_ticketmaster_info(row.get("name"))
    enriched.update({k: v for k, v in result.items() if v is not None})
    sources.update(source)

    # Eventbrite
    result, source = get_eventbrite_info(row.get("name"))
    enriched.update({k: v for k, v in result.items() if v is not None and k not in enriched})
    sources.update({k: v for k, v in source.items() if k not in sources})

    # Wikidata
    result, source = get_wikidata_info(row.get("name"))
    enriched.update({k: v for k, v in result.items() if v is not None and k not in enriched})
    sources.update({k: v for k, v in source.items() if k not in sources})

    # Now GPT for any missing fields (all fields allowed, but with source tracked)
    missing = [k for k in ["avg_ticket_price", "capacity", "ticket_vendor", "annual_revenue", "ticketing_revenue"]
               if enriched.get(k) is None]
    web_context = scrape_venue_website(row.get("domain"))
    if missing:
        gpt_result, gpt_sources = call_gpt_fallback(row, web_context=web_context, filled=enriched)
        for k in missing:
            enriched[k] = gpt_result.get(k)
            if gpt_result.get(k) is not None:
                sources[f"{k}_source"] = "GPT"

    return enriched, sources

def write_row(raw, enriched, sources):
    rec = raw.copy()
    rec.update(enriched)
    for k, v in sources.items():
        rec[k] = v
    rec["enrichment_status"] = "DONE"
    rec["last_updated"] = datetime.datetime.utcnow().isoformat()
    try:
        errs = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}", [rec])
        if errs:
            logger.error(f"Insert errors for '{raw.get('name')}': {errs}")
    except Exception as e:
        logger.error(f"BigQuery insert exception for '{raw.get('name')}': {e}")

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
        enriched, sources = enrich_row(r)
        write_row(r, enriched, sources)
        processed += 1
    return jsonify(processed=processed, status="OK")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
