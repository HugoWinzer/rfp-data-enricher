# src/enrich_app.py
import os, json, re, time, random
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from google.cloud import bigquery
from openai import OpenAI

app = Flask(__name__)

# ---- Env ----
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID", "rfpdata")
TABLE = os.environ.get("TABLE", "OUTPUT")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "europe-southwest1")

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
QUALITY_MIN_CONF = float(os.environ.get("QUALITY_MIN_CONF", "0.60"))  # only overwrite if >= this
DEFAULT_LOAD_FACTOR = float(os.environ.get("DEFAULT_LOAD_FACTOR", "0.70"))
ROW_DELAY_MIN_MS = int(os.environ.get("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.environ.get("ROW_DELAY_MAX_MS", "180"))

# ---- Clients ----
bq = bigquery.Client(project=PROJECT_ID)
oai = OpenAI()  # uses OPENAI_API_KEY from Secret Manager via env

# ---- Utils ----
def _table_fq() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"

def run_query(sql: str, params: Optional[list] = None):
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    return job.result()

def fetch_page_text(url: Optional[str], timeout_sec: int = 8, max_chars: int = 6000) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": "rfp-data-enricher/1.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        txt = soup.get_text(separator=" ", strip=True)
        return txt[:max_chars]
    except Exception:
        return ""

def safe_json_from_text(txt: str) -> Optional[Dict[str, Any]]:
    # Try to extract the first JSON object
    m = re.search(r"\{.*\}", txt, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

def baseline_revenue(avg_price: Optional[float], capacity: Optional[int], freq_per_year: Optional[int]) -> Optional[float]:
    try:
        if avg_price is None or capacity is None or freq_per_year is None:
            return None
        return round(float(avg_price) * float(capacity) * float(freq_per_year) * DEFAULT_LOAD_FACTOR, 2)
    except Exception:
        return None

def gpt_improve_revenue_and_rfp(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[bool], str]:
    """
    Returns (revenue, currency, confidence, rfp_detected, rationale)
    Only use revenue when confidence >= QUALITY_MIN_CONF and revenue > 0.
    """
    website_text = fetch_page_text(row.get("website_url"))
    base = baseline_revenue(row.get("avg_ticket_price"), row.get("capacity"), row.get("frequency_per_year"))

    sys = (
        "You are a data quality assistant for performing arts organizations. "
        "Estimate ANNUAL ticket revenues realistically and detect if the organization is soliciting RFPs. "
        "Be conservative. If the website suggests a different scale than baseline, adjust accordingly."
    )
    usr = {
        "organization": {
            "name": row.get("name"),
            "domain": row.get("domain"),
            "website_url": row.get("website_url"),
        },
        "known_fields": {
            "avg_ticket_price": row.get("avg_ticket_price"),
            "capacity": row.get("capacity"),
            "frequency_per_year": row.get("frequency_per_year"),
            "baseline_revenue_estimate": base,
            "current_revenues": row.get("revenues"),
            "current_revenues_source": row.get("revenues_source"),
        },
        "website_text_snippet": website_text[:3000],  # keep prompt light
        "instructions": (
            "Return strict JSON with keys: revenue(number), currency(string|null), "
            "confidence(number 0..1), rfp_detected(boolean), rationale(string <= 280 chars). "
            "Revenue is annual ticketing revenue. If very uncertain, lower confidence."
        ),
    }

    try:
        resp = oai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys},
                {"role": "user", "content": json.dumps(usr, ensure_ascii=False)},
            ],
            temperature=0.2,
        )
        content = resp.choices[0].message.content or ""
        data = safe_json_from_text(content)
        if not data:
            return None, None, None, None, "no_json"

        revenue = data.get("revenue")
        currency = data.get("currency")
        confidence = data.get("confidence")
        rfp_detected = data.get("rfp_detected")
        rationale = data.get("rationale", "")[:280] if isinstance(data.get("rationale"), str) else ""

        # Validate revenue
        if isinstance(revenue, (int, float)) and revenue > 0:
            rev_val = float(revenue)
        else:
            rev_val = None

        conf_val = float(confidence) if isinstance(confidence, (int, float)) else None
        rfp_val = bool(rfp_detected) if isinstance(rfp_detected, bool) else None

        return rev_val, currency, conf_val, rfp_val, rationale or "ok"
    except Exception as e:
        return None, None, None, None, f"gpt_err:{type(e).__name__}"

def update_row(name_key: str,
               revenues: Optional[float],
               rfp_detected: Optional[bool],
               rev_source_note: Optional[str]) -> None:
    # Only update provided fields; keep others intact.
    sets = ["last_updated = CURRENT_TIMESTAMP()"]
    params = [bigquery.ScalarQueryParameter("name", "STRING", name_key)]

    if revenues is not None:
        sets.append("revenues = @revenues")
        params.append(bigquery.ScalarQueryParameter("revenues", "NUMERIC", round(revenues, 2)))
    if rfp_detected is not None:
        sets.append("rfp_detected = @rfp")
        params.append(bigquery.ScalarQueryParameter("rfp", "BOOL", rfp_detected))
    if rev_source_note is not None:
        sets.append("revenues_source = @src")
        params.append(bigquery.ScalarQueryParameter("src", "STRING", rev_source_note))

    sql = f"""
    UPDATE {_table_fq()}
    SET {", ".join(sets)}
    WHERE name = @name
    """
    run_query(sql, params)

def pick_quality_candidates(limit: int, force_all: bool = False):
    # Already enriched rows; prefer sql-fallback or missing rfp_detected
    where_parts = [
        "(enrichment_status IS NULL OR enrichment_status != 'LOCKED')",
        "revenues IS NOT NULL"
    ]
    if not force_all:
        where_parts.append("(revenues_source LIKE 'sql-fallback%' OR rfp_detected IS NULL)")
    where_sql = " AND ".join(where_parts)

    sql = f"""
    SELECT
      name, domain, website_url,
      avg_ticket_price, capacity, frequency_per_year,
      revenues, revenues_source, rfp_detected
    FROM {_table_fq()}
    WHERE {where_sql}
    ORDER BY last_updated ASC
    LIMIT @limit
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])
    return [dict(r) for r in rows]

# ------------ Endpoints ------------
@app.get("/ping")
def ping():
    return "pong"

@app.get("/ready")
def ready():
    return jsonify({"ready": True, "bq_location": BQ_LOCATION})

@app.get("/quality")
def quality():
    """
    Quality pass:
      - Try to improve revenues using GPT (overwrite only if confidence >= QUALITY_MIN_CONF).
      - Fill rfp_detected based on website signal + GPT.
    Query params:
      limit: int (default 20)
      dry: 1 to simulate only
      force_all: 1 to consider all non-null revenue rows (not just sql-fallback/missing rfp_detected)
    """
    try:
        limit = int(request.args.get("limit", "20"))
    except Exception:
        limit = 20
    dry = request.args.get("dry") == "1"
    force_all = request.args.get("force_all") == "1"

    rows = pick_quality_candidates(limit, force_all=force_all)
    improved = 0
    only_rfp = 0
    processed = 0
    skipped = 0
    details = []

    for r in rows:
        processed += 1
        rev_new, currency, conf, rfp_new, reason = gpt_improve_revenue_and_rfp(r)

        overwrite = False
        rev_note = None
        rev_to_write = None

        if rev_new is not None and conf is not None and conf >= QUALITY_MIN_CONF:
            overwrite = True
            rev_to_write = rev_new
            # capture currency/conf in the source note
            rev_note = f"GPT_QUALITY[conf={conf:.2f}{', cur='+currency if currency else ''}]"
        else:
            # Keep existing revenue; but we might still fill rfp_detected
            rev_to_write = None
            rev_note = None

        if dry:
            details.append({
                "name": r.get("name"),
                "overwrite_revenues": overwrite,
                "rfp_detected_new": rfp_new,
                "confidence": conf,
                "reason": reason[:120] if isinstance(reason, str) else str(reason),
            })
        else:
            # Apply update if anything to change
            if overwrite or (rfp_new is not None and rfp_new != r.get("rfp_detected")):
                update_row(r.get("name"), rev_to_write, rfp_new, rev_note)

        if overwrite:
            improved += 1
        elif (rfp_new is not None) and (rfp_new != r.get("rfp_detected")):
            only_rfp += 1
        else:
            skipped += 1

        # gentle pacing
        sleep_ms = random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
        time.sleep(sleep_ms / 1000.0)

    return jsonify({
        "status": "DRY_OK" if dry else "OK",
        "processed": processed,
        "improved_revenues": improved,
        "rfp_detected_updates": only_rfp,
        "skipped": skipped,
        "quality_min_conf": QUALITY_MIN_CONF,
        "details": details[:10] if dry else [],
    })

# Root can still be your batch endpoint if you already have it elsewhere in your codebase.
# (We don't redefine it here to avoid accidental behavior changes.)
