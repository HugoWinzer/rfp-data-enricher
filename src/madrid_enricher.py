# src/madrid_enricher.py
import os, json, logging
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import bigquery

# Reuse your existing helpers in src/
from gpt_client import ask_gpt             # your GPT caller
from extractors import (                   # your scraper/vendor detector
    scrape_website_text, sniff_vendor_signals, choose_vendor
)
from segmenter import size_segment
from category_rules import infer_category_and_subcategory
from profile_prompt import PROFILE_SYSTEM, build_user_payload

# Keep the same lightweight Flask+Gunicorn shape as your base service.
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

PROJECT_ID   = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET_ID   = os.getenv("DATASET_ID", "rfpdata")
TABLE        = os.getenv("TABLE", "performing_arts_madrid")  # <— new table
BQ_LOCATION  = os.getenv("BQ_LOCATION", "europe-southwest1")
STOP_ON_429  = os.getenv("STOP_ON_GPT_QUOTA", "1") == "1"

bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

PENDING = f"""
SELECT
  name, domain, city, country, source_url,
  CAST(capacity AS FLOAT64) AS capacity,
  CAST(avg_ticket_price AS FLOAT64) AS avg_ticket_price,
  category, sub_category, website, ticket_vendor,
  annual_visitors, capacity_final, atp, gtv,
  size_segment, ownership, rfp
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
WHERE city='Madrid'
  AND (enrichment_status IS NULL OR enrichment_status != 'LOCKED')
  AND (
    website IS NULL OR ticket_vendor IS NULL OR annual_visitors IS NULL OR
    capacity_final IS NULL OR atp IS NULL OR gtv IS NULL OR
    size_segment IS NULL OR ownership IS NULL OR rfp IS NULL OR
    category IS NULL OR sub_category IS NULL
  )
LIMIT @limit
"""

def _num(v):
    try:
        if v is None: return None
        return float(v)
    except Exception:
        return None

def update_row(name: str, payload: dict, status: str):
    table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"
    q = f"""
    UPDATE `{table}`
    SET
      category=@category,
      sub_category=@sub_category,
      website=@website,
      ticket_vendor=@ticket_vendor,
      annual_visitors=@annual_visitors,
      capacity_final=@capacity_final,
      atp=@atp,
      gtv=@gtv,
      size_segment=@size_segment,
      ownership=@ownership,
      rfp=@rfp,
      notes=@notes,
      enrichment_status=@status,
      last_updated=CURRENT_TIMESTAMP()
    WHERE name=@name AND city='Madrid'
    """
    params = [
        bigquery.ScalarQueryParameter("category","STRING", payload.get("category")),
        bigquery.ScalarQueryParameter("sub_category","STRING", payload.get("sub_category")),
        bigquery.ScalarQueryParameter("website","STRING", payload.get("website")),
        bigquery.ScalarQueryParameter("ticket_vendor","STRING", payload.get("ticket_vendor")),
        bigquery.ScalarQueryParameter("annual_visitors","NUMERIC", payload.get("annual_visitors")),
        bigquery.ScalarQueryParameter("capacity_final","NUMERIC", payload.get("capacity_final")),
        bigquery.ScalarQueryParameter("atp","NUMERIC", payload.get("atp")),
        bigquery.ScalarQueryParameter("gtv","NUMERIC", payload.get("gtv")),
        bigquery.ScalarQueryParameter("size_segment","STRING", payload.get("size_segment")),
        bigquery.ScalarQueryParameter("ownership","STRING", payload.get("ownership")),
        bigquery.ScalarQueryParameter("rfp","STRING", payload.get("rfp")),
        bigquery.ScalarQueryParameter("notes","STRING", payload.get("notes")),
        bigquery.ScalarQueryParameter("status","STRING", status),
        bigquery.ScalarQueryParameter("name","STRING", name),
    ]
    bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

@app.get("/ping")
def ping(): return "pong"

@app.get("/ready")
def ready(): return "ok"

@app.get("/")
def run_batch():
    try:
        limit = int(request.args.get("limit","25"))
    except ValueError:
        limit = 25
    dry = request.args.get("dry") in ("1","true","True")

    rows = list(bq.query(
        PENDING,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("limit","INT64", limit)]
        ),
    ).result())

    processed, out = 0, []
    for r in rows:
        name, domain = r["name"], r["domain"]

        # 1) Scrape + vendor sniff
        html, text = scrape_website_text(domain)
        vendor_signals = sniff_vendor_signals(html, domain)
        vendor_from_html = choose_vendor(vendor_signals)

        # 2) Category/Sub-category heuristics
        category, subcat = infer_category_and_subcategory(name, domain)

        # 3) GPT fill for missing fields
        user = build_user_payload({
            "name": name, "city": r["city"], "country": r["country"], "domain": domain,
            "capacity": r["capacity"], "avg_ticket_price": r["avg_ticket_price"],
            "vendor_signals": vendor_signals, "text_excerpt": (text or "")[:4000],
        })

        try:
            gpt = ask_gpt(PROFILE_SYSTEM, user)  # uses your configured model
            data = json.loads(gpt.text)

            website        = data.get("website") or domain
            ticket_vendor  = data.get("ticket_vendor") or vendor_from_html
            annual_visitors= _num(data.get("annual_visitors"))
            capacity_final = _num(data.get("capacity_final")) or r["capacity"]
            atp            = _num(data.get("atp")) or r["avg_ticket_price"]
            ownership      = data.get("ownership") or "Unknown"
            rfp            = data.get("rfp") or "Unknown"
            notes          = (data.get("notes") or "")[:900]

            # 4) Compute GTV and segment
            gtv = None
            if annual_visitors and atp:
                gtv = float(annual_visitors) * float(atp)
            elif capacity_final and atp:
                # conservative events/year heuristic
                evts = 25 if (capacity_final and capacity_final >= 10000) else 60
                gtv = float(capacity_final) * float(atp) * evts
            seg = size_segment(gtv) if gtv is not None else None

            payload = {
                "category": category, "sub_category": subcat, "website": website,
                "ticket_vendor": ticket_vendor, "annual_visitors": annual_visitors,
                "capacity_final": capacity_final, "atp": atp, "gtv": gtv,
                "size_segment": seg, "ownership": ownership, "rfp": rfp,
                "notes": notes
            }

            if dry:
                out.append({"name": name, **{k: payload[k] for k in ("website","ticket_vendor","annual_visitors","capacity_final","atp","gtv","size_segment","ownership","rfp")}})
            else:
                update_row(name, payload, status="OK")
                processed += 1
                out.append({"name": name, "gtv": gtv})

        except Exception as e:
            if "429" in str(e) and STOP_ON_429:
                logging.error("GPT quota (429). Stopping batch.")
                return jsonify({"status":"stopped_on_quota","processed":processed,"error":str(e)[:400]}), 429
            logging.exception("row failed")
            if not dry:
                update_row(name, {
                    "category": category, "sub_category": subcat, "website": domain,
                    "ticket_vendor": vendor_from_html, "annual_visitors": None,
                    "capacity_final": r["capacity"], "atp": r["avg_ticket_price"],
                    "gtv": None, "size_segment": None, "ownership": None, "rfp": None,
                    "notes": f"error: {str(e)[:600]}"
                }, status="ERROR")
            out.append({"name": name, "error": str(e)[:300]})

    return jsonify({"status":"ok","processed":processed,"items":out})
