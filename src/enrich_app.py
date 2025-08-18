# --- src/enrich_app.py ---
cat > src/enrich_app.py <<'PY'
<PASTE START>
import os, sys, json, datetime, logging
from urllib.parse import quote_plus, urljoin
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import openai

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enricher")

PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("STAGING_TABLE", os.getenv("RAW_TABLE", "performing_arts_fixed"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
TM_KEY = os.getenv("TICKETMASTER_KEY", "")
PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY", "")
BING_KEY = os.getenv("BING_KEY", "")
EVENTBRITE_TOKEN = os.getenv("EVENTBRITE_TOKEN", "")
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

openai.api_key = OPENAI_API_KEY
bq = bigquery.Client(project=PROJECT_ID)

VENDOR_PATTERNS = {
    "Ticketmaster": ["ticketmaster.", "ticketmaster.fr", "ticketmaster.com"],
    "Eventbrite": ["eventbrite.", "universe.com"],
    "SeeTickets": ["seetickets.", "see-tickets."],
    "DICE": ["dice.fm"],
    "Shotgun": ["shotgun.live"],
    "Weezevent": ["weezevent."],
    "Billetweb": ["billetweb.fr"],
    "HelloAsso": ["helloasso."]
}
MIN_VENDOR_SCORE = 0.75

_COL_CACHE = {}
def table_columns():
    key = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"
    if key not in _COL_CACHE:
        rows = bq.query(f"""
            SELECT column_name
            FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{TABLE.split('.')[-1]}'
        """).result()
        _COL_CACHE[key] = {r["column_name"] for r in rows}
    return _COL_CACHE[key]

def filter_to_table(d: dict) -> dict:
    return {k: v for k, v in d.items() if k in table_columns()}

def safe_get(url, headers=None, timeout=15):
    try:
        return requests.get(url, headers=headers, timeout=timeout)
    except Exception:
        return None

def normalize_url(domain: str | None):
    if not domain: return None
    domain = domain.strip()
    if not domain: return None
    if not domain.startswith("http"): domain = "http://" + domain
    return domain

def fetch_rows(limit: int):
    sql = f"""
      SELECT *
      FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
      WHERE (enrichment_status IS NULL OR enrichment_status='PENDING')
      LIMIT {limit}
    """
    return [dict(r) for r in bq.query(sql).result()]

def google_places_avg_price(name, domain=None):
    if not PLACES_KEY: return {}, {}
    try:
        search = domain or name
        url = ("https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
               f"?input={quote_plus(search)}&inputtype=textquery&fields=place_id&key={PLACES_KEY}")
        r = safe_get(url)
        if not r or r.status_code != 200: return {}, {}
        cands = r.json().get("candidates", [])
        if not cands: return {}, {}
        pid = cands[0]["place_id"]
        det = safe_get("https://maps.googleapis.com/maps/api/place/details/json"
                       f"?place_id={pid}&fields=price_level&key={PLACES_KEY}")
        if not det or det.status_code != 200: return {}, {}
        pl = det.json().get("result", {}).get("price_level")
        if isinstance(pl, int):
            return {"avg_ticket_price": float(pl*20 + 10)}, {"avg_ticket_price_source":"google_places"}
        return {}, {}
    except Exception as e:
        log.warning(f"google places failed: {e}")
        return {}, {}

def ticketmaster_lookup(name):
    if not TM_KEY: return {}, {}, []
    url = f"https://app.ticketmaster.com/discovery/v2/venues.json?keyword={quote_plus(name)}&apikey={TM_KEY}"
    r = safe_get(url)
    if not r or r.status_code != 200: return {}, {}, []
    venues = r.json().get("_embedded", {}).get("venues", [])
    if not venues: return {}, {}, []
    v = venues[0]; out, src, ev = {}, {}, []
    if v.get("capacity"):
        out["capacity"] = v["capacity"]; src["capacity_source"] = "ticketmaster_api"
    out["ticket_vendor"] = "Ticketmaster"; src["ticket_vendor_source"] = "ticketmaster_api"
    if v.get("url"): ev.append(v["url"])
    return out, src, ev

def eventbrite_lookup(name):
    if not EVENTBRITE_TOKEN: return {}, {}, []
    url = f"https://www.eventbriteapi.com/v3/venues/search/?q={quote_plus(name)}"
    r = safe_get(url, headers={"Authorization": f"Bearer {EVENTBRITE_TOKEN}"})
    if not r or r.status_code != 200: return {}, {}, []
    venues = r.json().get("venues", [])
    if not venues: return {}, {}, []
    v = venues[0]; out, src, ev = {}, {}, []
    if v.get("capacity"):
        out["capacity"] = int(v["capacity"]); src["capacity_source"] = "eventbrite_api"
    out["ticket_vendor"] = "Eventbrite"; src["ticket_vendor_source"] = "eventbrite_api"
    if v.get("resource_uri"): ev.append(v["resource_uri"])
    return out, src, ev

def wikidata_lookup(name):
    sparql = f"""
SELECT ?capacity ?revenue WHERE {{
  ?item rdfs:label "{name}"@en.
  OPTIONAL {{ ?item wdt:P1083 ?capacity. }}
  OPTIONAL {{ ?item wdt:P2139 ?revenue. }}
}} LIMIT 1
"""
    try:
        r = requests.get(WIKIDATA_ENDPOINT, params={"query": sparql},
                         headers={"Accept":"application/sparql-results+json"}, timeout=15)
        if r.status_code != 200: return {}, {}, []
        b = r.json()["results"]["bindings"]
        if not b: return {}, {}, []
        row = b[0]; out, src = {}, {}
        if "capacity" in row: out["capacity"]=int(row["capacity"]["value"]); src["capacity_source"]="wikidata"
        if "revenue" in row: out["annual_revenue"]=float(row["revenue"]["value"]); src["annual_revenue_source"]="wikidata"
        return out, src, []
    except Exception as e:
        log.warning(f"wikidata failed: {e}")
        return {}, {}, []

def scrape_site(domain):
    url = normalize_url(domain)
    if not url: return "", []
    r = safe_get(url, timeout=20)
    if not r or r.status_code >= 500: return "", []
    soup = BeautifulSoup(r.text, "html.parser")
    text = " ".join(list(soup.stripped_strings)[:1200])
    links = []
    for a in soup.find_all("a", href=True):
        links.append(urljoin(url, a["href"]))
    for tag in soup.find_all(["script","link"], src=True):
        links.append(urljoin(url, tag["src"]))
    return text, links

def detect_vendor_from_links(links):
    scores = defaultdict(float); hits = defaultdict(list)
    for link in links:
        L = link.lower()
        for vendor, patterns in VENDOR_PATTERNS.items():
            if any(p in L for p in patterns):
                scores[vendor]+=0.9; hits[vendor].append(link)
    if not scores: return None, 0.0, []
    v = max(scores, key=scores.get); return v, scores[v], hits[v]

def bing_search(query, count=5):
    if not BING_KEY: return []
    url = "https://api.bing.microsoft.com/v7.0/search"
    r = safe_get(url+f"?q={quote_plus(query)}&count={count}",
                 headers={"Ocp-Apim-Subscription-Key":BING_KEY})
    if not r or r.status_code!=200: return []
    return [{"url": v.get("url")} for v in r.json().get("webPages", {}).get("value", [])]

def detect_vendor_with_bing(name):
    if not BING_KEY: return None, 0.0, []
    scores = defaultdict(float); hits = defaultdict(list)
    for vendor, patterns in VENDOR_PATTERNS.items():
        q = f'{name} site:{patterns[0].rstrip(".")}'
        for item in bing_search(q, count=6):
            scores[vendor]+=0.6; hits[vendor].append(item["url"])
    if not scores: return None, 0.0, []
    v = max(scores, key=scores.get); return v, scores[v], hits[v]

def gpt_extract(row, web_text, evidence_urls):
    if not OPENAI_API_KEY: return {}, {}
    prompt = f"""
Use ONLY the provided website text/evidence. If unsure, return null.
Return JSON with exactly these keys:
{{"avg_ticket_price": number|null, "capacity": integer|null, "ticket_vendor": string|null,
  "annual_revenue": number|null, "ticketing_revenue": number|null}}
Venue: {row.get('name')}
Domain: {row.get('domain')}
TEXT:
{web_text[:3500]}
Evidence: {json.dumps(evidence_urls[:10])}
"""
    try:
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":prompt}],
            temperature=0, max_tokens=220,
        )
        data = json.loads(resp.choices[0].message["content"])
        src = {f"{k}_source":"gpt" for k,v in data.items() if v is not None}
        return data, src
    except Exception as e:
        log.warning(f"gpt failed: {e}")
        return {}, {}

def enrich_row(row):
    enriched, sources = {}, {}
    evidence = []

    web_text, links = scrape_site(row.get("domain"))
    v_site, score_site, site_hits = detect_vendor_from_links(links)
    evidence += site_hits

    tm_out, tm_src, tm_ev = ticketmaster_lookup(row.get("name"))
    ev_out, ev_src, ev_ev = eventbrite_lookup(row.get("name"))
    wd_out, wd_src, _ = wikidata_lookup(row.get("name"))
    gp_out, gp_src = google_places_avg_price(row.get("name"), row.get("domain"))

    for D,S in [(tm_out,tm_src),(ev_out,ev_src),(wd_out,wd_src),(gp_out,gp_src)]:
        for k,v in D.items():
            if v is not None and k not in enriched: enriched[k]=v
        for k,v in S.items():
            if k not in sources: sources[k]=v

    final_vendor = None; vendor_source = None; vendor_score = 0.0
    if v_site:
        final_vendor, vendor_score, vendor_source = v_site, score_site, "website_links"
    else:
        v_bing, score_bing, _ = detect_vendor_with_bing(row.get("name"))
        if v_bing: final_vendor, vendor_score, vendor_source = v_bing, score_bing, "bing_search"

    if final_vendor and vendor_score >= MIN_VENDOR_SCORE:
        enriched["ticket_vendor"] = final_vendor
        sources["ticket_vendor_source"] = vendor_source

    missing = [k for k in ["avg_ticket_price","capacity","ticket_vendor","annual_revenue","ticketing_revenue"]
               if not enriched.get(k)]
    if missing:
        gpt_out, gpt_src = gpt_extract(row, web_text, evidence)
        for k in missing:
            if gpt_out.get(k) is not None:
                enriched[k]=gpt_out[k]; sources[f"{k}_source"]="gpt"

    return enriched, sources

def update_in_place(row_dict, enriched, sources):
    payload = {**enriched, **sources,
               "enrichment_status":"DONE",
               "last_updated": datetime.datetime.utcnow()}
    payload = filter_to_table(payload)
    if not payload: return

    sets, params = [], []
    i = 0
    for k,v in payload.items():
        i += 1
        kind = "STRING"
        if isinstance(v,int): kind="INT64"
        elif isinstance(v,float): kind="FLOAT64"
        elif isinstance(v,datetime.datetime): kind="TIMESTAMP"
        sets.append(f"{k}=@p{i}")
        params.append(bigquery.ScalarQueryParameter(f"p{i}", kind, v))

    if row_dict.get("entity_id"):
        where = "entity_id=@row_id"
        params.append(bigquery.ScalarQueryParameter("row_id","STRING",row_dict["entity_id"]))
    else:
        where = "LOWER(name)=@nm AND LOWER(IFNULL(domain,''))=@dm"
        params.append(bigquery.ScalarQueryParameter("nm","STRING",(row_dict.get("name") or "").lower()))
        params.append(bigquery.ScalarQueryParameter("dm","STRING",(row_dict.get("domain") or "").lower()))

    q = f"UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE}` SET {', '.join(sets)} WHERE {where}"
    bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

app = Flask(__name__)

@app.get("/health")
def health(): return {"ok": True}

@app.get("/")
def run_batch():
    limit = int(request.args.get("limit","10"))
    rows = fetch_rows(limit)
    log.info(f"Processing {len(rows)} rows")
    processed = 0
    for r in rows:
        try:
            enriched, sources = enrich_row(r)
            update_in_place(r, enriched, sources)
            processed += 1
        except Exception as e:
            log.exception(f"Failed row: {r.get('name')}: {e}")
    return jsonify(processed=processed, status="OK")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
<PASTE END>
PY

# --- requirements.txt ---
cat > requirements.txt <<'REQ'
google-cloud-bigquery<4.0.0
openai<1.0.0
flask<3.0.0
requests
beautifulsoup4
REQ

# --- Dockerfile ---
cat > Dockerfile <<'DOCK'
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PORT=8080
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
EXPOSE 8080
CMD ["python", "src/enrich_app.py"]
DOCK
