import os
import sys
import json
import datetime
import logging
from urllib.parse import quote_plus, urljoin, urlparse
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import openai

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enricher")

# ---------------------------
# Environment
# ---------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
RAW_TABLE = os.getenv("RAW_TABLE", "performing_arts_fixed")
STAGING_TABLE = os.getenv("STAGING_TABLE", RAW_TABLE)  # in-place update
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # set to gpt-3.5-turbo if your client is old
TM_KEY = os.getenv("TICKETMASTER_KEY", "")
PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY", "")
BING_KEY = os.getenv("BING_KEY", "")  # <— add this for better vendor detection
EVENTBRITE_TOKEN = os.getenv("EVENTBRITE_TOKEN", "")  # optional
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

openai.api_key = OPENAI_API_KEY
bq = bigquery.Client(project=PROJECT_ID)

# ---------------------------
# Constants
# ---------------------------
VENDOR_PATTERNS = {
    "Ticketmaster": ["ticketmaster.", "ticketmaster.fr", "ticketmaster.com", "universalticketing."],
    "Eventbrite": ["eventbrite.", "universe.com"],          # Universe is Eventbrite
    "SeeTickets": ["seetickets.", "see-tickets."],
    "DICE": ["dice.fm"],
    "Shotgun": ["shotgun.live"],
    "Weezevent": ["weezevent."],
    "Billetweb": ["billetweb.fr"],
    "HelloAsso": ["helloasso."],  # sometimes used for associations
}

MIN_VENDOR_SCORE = 0.75  # threshold to accept a vendor

# ---------------------------
# Utilities
# ---------------------------
def get_table_columns(project, dataset, table):
    """Fetch and cache column names so we only write allowed fields."""
    global _COL_CACHE
    key = f"{project}.{dataset}.{table}"
    if "_COL_CACHE" not in globals():
        _COL_CACHE = {}
    if key not in _COL_CACHE:
        rows = bq.query(f"""
            SELECT column_name
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table}'
        """).result()
        _COL_CACHE[key] = {r["column_name"] for r in rows}
    return _COL_CACHE[key]

def filter_to_table(record: dict) -> dict:
    allowed = get_table_columns(PROJECT_ID, DATASET_ID, STAGING_TABLE)
    return {k: v for k, v in record.items() if k in allowed}

def safe_get(url, headers=None, timeout=15):
    try:
        return requests.get(url, headers=headers, timeout=timeout)
    except Exception:
        return None

def normalize_domain(d: str | None) -> str | None:
    if not d:
        return None
    d = d.strip()
    if not d:
        return None
    if not d.startswith("http"):
        d = "http://" + d
    return d

# ---------------------------
# Data fetch (queue)
# ---------------------------
def fetch_rows(limit: int):
    sql = f"""
      SELECT *
      FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
      WHERE enrichment_status IS NULL OR enrichment_status='PENDING'
      LIMIT {limit}
    """
    return [dict(r) for r in bq.query(sql).result()]

# ---------------------------
# Deterministic sources
# ---------------------------
def google_places_avg_price(name, domain=None):
    """Very weak proxy; keep low weight. We DO NOT write any extra columns."""
    if not PLACES_KEY:
        return {}, {}
    try:
        search_text = domain or name
        url = ("https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
               f"?input={quote_plus(search_text)}&inputtype=textquery&fields=place_id&key={PLACES_KEY}")
        r = safe_get(url)
        if not r or r.status_code != 200:
            return {}, {}
        candidates = r.json().get("candidates", [])
        if not candidates:
            return {}, {}
        pid = candidates[0]["place_id"]
        det_url = ("https://maps.googleapis.com/maps/api/place/details/json"
                   f"?place_id={pid}&fields=price_level&key={PLACES_KEY}")
        det = safe_get(det_url)
        if not det or det.status_code != 200:
            return {}, {}
        price_level = det.json().get("result", {}).get("price_level")
        if isinstance(price_level, int):
            avg = float(price_level * 20 + 10)  # crude heuristic; will be low-weighted
            return {"avg_ticket_price": avg}, {"avg_ticket_price_source": "google_places"}
        return {}, {}
    except Exception as e:
        log.warning(f"google_places failed: {e}")
        return {}, {}

def ticketmaster_lookup(name, country_hint=None):
    if not TM_KEY:
        return {}, {}, []
    q = f"https://app.ticketmaster.com/discovery/v2/venues.json?keyword={quote_plus(name)}&apikey={TM_KEY}"
    r = safe_get(q)
    if not r or r.status_code != 200:
        return {}, {}, []
    venues = r.json().get("_embedded", {}).get("venues", [])
    if not venues:
        return {}, {}, []
    v = venues[0]
    out, src = {}, {}
    ev = []
    if v.get("capacity"):
        out["capacity"] = v["capacity"]
        src["capacity_source"] = "ticketmaster_api"
    out["ticket_vendor"] = "Ticketmaster"
    src["ticket_vendor_source"] = "ticketmaster_api"
    # evidence (homepage link, venue id if any)
    if v.get("url"):
        ev.append(v["url"])
    return out, src, ev

def eventbrite_lookup(name):
    if not EVENTBRITE_TOKEN:
        return {}, {}, []
    url = f"https://www.eventbriteapi.com/v3/venues/search/?q={quote_plus(name)}"
    r = safe_get(url, headers={"Authorization": f"Bearer {EVENTBRITE_TOKEN}"})
    if not r or r.status_code != 200:
        return {}, {}, []
    venues = r.json().get("venues", [])
    if not venues:
        return {}, {}, []
    v = venues[0]
    out, src = {}, {}
    ev = []
    if v.get("capacity"):
        out["capacity"] = int(v["capacity"])
        src["capacity_source"] = "eventbrite_api"
    out["ticket_vendor"] = "Eventbrite"
    src["ticket_vendor_source"] = "eventbrite_api"
    if v.get("resource_uri"):
        ev.append(v["resource_uri"])
    return out, src, ev

def wikidata_lookup(name):
    sparql = f"""
SELECT ?capacity ?revenue WHERE {{
  ?item rdfs:label "{name}"@en.
  OPTIONAL {{ ?item wdt:P1083 ?capacity. }}      # capacity (venues)
  OPTIONAL {{ ?item wdt:P2139 ?revenue. }}       # revenue
}} LIMIT 1
"""
    try:
        r = requests.get(WIKIDATA_ENDPOINT, params={"query": sparql},
                         headers={"Accept": "application/sparql-results+json"}, timeout=15)
        if r.status_code != 200:
            return {}, {}, []
        bindings = r.json()["results"]["bindings"]
        if not bindings:
            return {}, {}, []
        row = bindings[0]
        out, src, ev = {}, {}, []
        if "capacity" in row:
            out["capacity"] = int(row["capacity"]["value"])
            src["capacity_source"] = "wikidata"
        if "revenue" in row:
            out["annual_revenue"] = float(row["revenue"]["value"])
            src["annual_revenue_source"] = "wikidata"
        return out, src, ev
    except Exception as e:
        log.warning(f"wikidata failed: {e}")
        return {}, {}, []

# ---------------------------
# Website parsing for vendor
# ---------------------------
def scrape_site(domain):
    url = normalize_domain(domain)
    if not url:
        return "", [], []
    r = safe_get(url, timeout=20)
    if not r or r.status_code >= 500:
        return "", [], []
    soup = BeautifulSoup(r.text, "html.parser")
    text = " ".join(list(soup.stripped_strings)[:1200])
    anchors = []
    evidence = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(url, href)
        anchors.append(full)
        evidence.append(full)
    # also look at script/link tags for known vendor substrings
    for tag in soup.find_all(["script", "link"], src=True):
        evidence.append(urljoin(url, tag["src"]))
    return text, anchors, evidence

def detect_vendor_from_links(links):
    scores = defaultdict(float)
    hits = defaultdict(list)
    for link in links:
        L = link.lower()
        for vendor, patterns in VENDOR_PATTERNS.items():
            if any(p in L for p in patterns):
                scores[vendor] += 0.9  # very strong when on official site
                hits[vendor].append(link)
    if not scores:
        return None, 0.0, []
    vendor = max(scores, key=scores.get)
    return vendor, scores[vendor], hits[vendor]

# ---------------------------
# Bing search as backstop
# ---------------------------
def bing_search(query, count=5):
    if not BING_KEY:
        return []
    url = "https://api.bing.microsoft.com/v7.0/search"
    r = safe_get(url + f"?q={quote_plus(query)}&count={count}&mkt=fr-FR",
                 headers={"Ocp-Apim-Subscription-Key": BING_KEY})
    if not r or r.status_code != 200:
        return []
    values = r.json().get("webPages", {}).get("value", [])
    return [{"name": v.get("name"), "url": v.get("url"), "snippet": v.get("snippet")} for v in values]

def detect_vendor_with_bing(name, country_hint="FR"):
    scores = defaultdict(float)
    hits = defaultdict(list)
    for vendor, patterns in VENDOR_PATTERNS.items():
        q = f'{name} site:{patterns[0].rstrip(".")}'  # use main domain
        for item in bing_search(q, count=6):
            scores[vendor] += 0.6  # weaker than onsite link
            hits[vendor].append(item["url"])
    if not scores:
        return None, 0.0, []
    vendor = max(scores, key=scores.get)
    return vendor, scores[vendor], hits[vendor]

# ---------------------------
# GPT fallback (strict, abstain-friendly)
# ---------------------------
def gpt_extract(row, web_text, evidence_urls):
    if not OPENAI_API_KEY:
        return {}, {}
    prompt = f"""
You are an evidence-based enrichment agent. Use ONLY the text below (from the official site or listings).
If a value is not clearly supported, return null for it.

Return STRICT JSON with exactly these keys:
{{
  "avg_ticket_price": number or null,
  "capacity": integer or null,
  "ticket_vendor": string or null,
  "annual_revenue": number or null,
  "ticketing_revenue": number or null
}}

Venue: {row.get('name')}
Domain: {row.get('domain')}
LinkedIn: {row.get('linkedin_url')}

TEXT (may be French):
{web_text[:4000]}

EVIDENCE URLS:
{json.dumps(evidence_urls[:10])}
"""
    try:
        # old OpenAI python client (openai<1.0) uses ChatCompletion
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": prompt}],
            temperature=0,
            max_tokens=220,
        )
        raw = resp.choices[0].message["content"]
        data = json.loads(raw)
        sources = {f"{k}_source": "gpt" for k, v in data.items() if v is not None}
        return data, sources
    except Exception as e:
        log.warning(f"gpt extraction failed: {e}")
        return {}, {}

# ---------------------------
# Orchestrate one row
# ---------------------------
def enrich_row(row):
    enriched, sources = {}, {}
    evidence = []

    # 1) Parse official website (if any) for vendor links
    web_text, links, web_evidence = scrape_site(row.get("domain"))
    evidence.extend(web_evidence)
    v_from_site, score_site, site_hits = detect_vendor_from_links(links)

    # 2) Ticketmaster / Eventbrite APIs (capacity/vendor)
    tm_out, tm_src, tm_ev = ticketmaster_lookup(row.get("name"))
    ev_out, ev_src, ev_ev = eventbrite_lookup(row.get("name")) if EVENTBRITE_TOKEN else ({}, {}, [])
    enriched.update({k: v for k, v in tm_out.items() if v is not None})
    sources.update(tm_src)
    evidence.extend(tm_ev)
    # only add EB if we don't already have capacity/vendor from TM
    for k, v in ev_out.items():
        if v is not None and not enriched.get(k):
            enriched[k] = v
    for k, v in ev_src.items():
        if k not in sources:
            sources[k] = v
    evidence.extend(ev_ev)

    # 3) Wikidata (capacity/revenue)
    wd_out, wd_src, wd_ev = wikidata_lookup(row.get("name"))
    for k, v in wd_out.items():
        if v is not None and not enriched.get(k):
            enriched[k] = v
    for k, v in wd_src.items():
        if k not in sources:
            sources[k] = v
    evidence.extend(wd_ev)

    # 4) Google Places avg price (weak)
    gp_out, gp_src = google_places_avg_price(row.get("name"), row.get("domain"))
    for k, v in gp_out.items():
        if v is not None and not enriched.get(k):
            enriched[k] = v
    for k, v in gp_src.items():
        if k not in sources:
            sources[k] = v

    # 5) Vendor consensus with Bing if still uncertain
    final_vendor = None
    vendor_evidence = []
    vendor_score = 0.0
    if v_from_site:
        final_vendor, vendor_score, vendor_evidence = v_from_site, score_site, site_hits
    else:
        v_bing, score_bing, bing_hits = detect_vendor_with_bing(row.get("name"))
        if v_bing:
            final_vendor, vendor_score, vendor_evidence = v_bing, score_bing, bing_hits

    if final_vendor and vendor_score >= MIN_VENDOR_SCORE:
        enriched["ticket_vendor"] = final_vendor
        sources["ticket_vendor_source"] = "website_links" if score_site >= score_bing else "bing_search"
        evidence.extend(vendor_evidence)

    # 6) GPT only for still-missing fields, with real context
    missing = [k for k in ["avg_ticket_price", "capacity", "ticket_vendor",
                           "annual_revenue", "ticketing_revenue"] if not enriched.get(k)]
    if missing:
        gpt_out, gpt_src = gpt_extract(row, web_text, evidence)
        for k in missing:
            if gpt_out.get(k) is not None:
                enriched[k] = gpt_out[k]
                sources[f"{k}_source"] = "gpt"

    return enriched, sources

# ---------------------------
# Write back (safe UPDATE)
# ---------------------------
def update_row_in_place(row_key_value, enriched: dict, sources: dict):
    """Dynamically build UPDATE … SET only for columns that exist & we actually have."""
    payload = {}
    payload.update(enriched)
    payload.update(sources)
    payload["enrichment_status"] = "DONE"
    payload["last_updated"] = datetime.datetime.utcnow()

    # Filter to actual table columns
    payload = filter_to_table(payload)
    if not payload:
        return

    # Build dynamic SETs
    set_clauses = []
    params = []
    for i, (k, v) in enumerate(payload.items(), start=1):
        set_clauses.append(f"{k} = @p{i}")
        # infer BQ type
        kind = "STRING"
        if isinstance(v, int):
            kind = "INT64"
        elif isinstance(v, float):
            kind = "FLOAT64"
        elif isinstance(v, datetime.datetime):
            kind = "TIMESTAMP"
        params.append(bigquery.ScalarQueryParameter(f"p{i}", kind, v))

    q = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}`
    SET {", ".join(set_clauses)}
    WHERE row = @row_id
    """
    params.append(bigquery.ScalarQueryParameter("row_id", "INT64", int(row_key_value)))
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()

# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def run_batch():
    try:
        limit = int(request.args.get("limit", "10"))
    except Exception:
        return jsonify(error="invalid limit"), 400

    rows = fetch_rows(limit)
    log.info(f"Processing {len(rows)} rows")
    processed = 0
    for r in rows:
        try:
            enriched, sources = enrich_row(r)
            if enriched or sources:
                update_row_in_place(r["row"], enriched, sources)
            else:
                # mark as done to avoid retry loop, or leave as pending if you prefer
                update_row_in_place(r["row"], {}, {})
            processed += 1
        except Exception as e:
            log.exception(f"row {r.get('row')} failed: {e}")
    return jsonify(processed=processed, status="OK")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
