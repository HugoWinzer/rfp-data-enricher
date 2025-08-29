# src/enrich_app.py
import os, json, re, time, random, logging
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
from google.cloud import bigquery
from openai import OpenAI
import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------------------
# App & Logging
# --------------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("rfp-data-enricher")

# --------------------------------------------------------------------------------------
# Environment / Config
# --------------------------------------------------------------------------------------
PROJECT_ID   = os.environ.get("PROJECT_ID")
DATASET_ID   = os.environ.get("DATASET_ID", "rfpdata")
TABLE        = os.environ.get("TABLE", "OUTPUT")
BQ_LOCATION  = os.environ.get("BQ_LOCATION", "europe-southwest1")

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

ROW_DELAY_MIN_MS   = int(os.environ.get("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS   = int(os.environ.get("ROW_DELAY_MAX_MS", "180"))
QUALITY_MIN_CONF   = float(os.environ.get("QUALITY_MIN_CONF", "0.60"))
DEFAULT_CAPACITY   = int(os.environ.get("DEFAULT_CAPACITY", "200"))
DEFAULT_AVG_TICKET_PRICE = float(os.environ.get("DEFAULT_AVG_TICKET_PRICE", "25"))
DEFAULT_EVENTS_PER_YEAR  = int(os.environ.get("DEFAULT_EVENTS_PER_YEAR", "20"))
DEFAULT_LOAD_FACTOR      = float(os.environ.get("DEFAULT_LOAD_FACTOR", "0.70"))

STOP_ON_GPT_QUOTA  = os.environ.get("STOP_ON_GPT_QUOTA", "1").lower() in ("1", "true", "yes", "y")
BACKFILL_REVENUES  = os.environ.get("BACKFILL_REVENUES", "1").lower() in ("1", "true", "yes", "y")

# --------------------------------------------------------------------------------------
# Clients
# --------------------------------------------------------------------------------------
bq  = bigquery.Client(project=PROJECT_ID)
oai = OpenAI()  # Reads OPENAI_API_KEY from Secret Manager via env var

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _table_fq() -> str:
    # backquoted fully-qualified table
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"

def run_query(sql: str, params: Optional[List[bigquery.ScalarQueryParameter]] = None):
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    return job.result()

def fetch_page_text(url: Optional[str], timeout_sec: int = 8, max_chars: int = 6000) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": "rfp-enricher/1.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        return soup.get_text(" ", strip=True)[:max_chars]
    except Exception as e:
        log.debug(f"fetch_page_text error for {url}: {e}")
        return ""

def safe_json_from_text(txt: str) -> Optional[Dict[str, Any]]:
    if not txt:
        return None
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception as e:
        log.debug(f"safe_json_from_text parse error: {e}")
        return None

def baseline_revenue(avg_price: Optional[float], capacity: Optional[int], freq_per_year: Optional[int]) -> Optional[float]:
    try:
        if avg_price is None:
            avg_price = DEFAULT_AVG_TICKET_PRICE
        if capacity is None:
            capacity = DEFAULT_CAPACITY
        if freq_per_year is None:
            freq_per_year = DEFAULT_EVENTS_PER_YEAR
        val = float(avg_price) * float(capacity) * float(freq_per_year) * float(DEFAULT_LOAD_FACTOR)
        return round(val, 2)
    except Exception as e:
        log.debug(f"baseline_revenue error: {e}")
        return None

def sleep_jitter():
    ms = random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
    time.sleep(ms / 1000.0)

def _pk_where(params: List[bigquery.ScalarQueryParameter], row: Dict[str, Any]) -> str:
    """
    Prefer 'id' if present; fallback to 'name'. Both are strings in your schema.
    """
    if row.get("id"):
        params.append(bigquery.ScalarQueryParameter("pk_id", "STRING", row["id"]))
        return "id = @pk_id"
    # Fallback (not ideal, but matches current dataset)
    params.append(bigquery.ScalarQueryParameter("pk_name", "STRING", row.get("name")))
    return "name = @pk_name"

# --------------------------------------------------------------------------------------
# OpenAI calls (Responses API with JSON mode)
# --------------------------------------------------------------------------------------
def oai_json(system_text: str, user_payload: Dict[str, Any], temperature: float = 0.2) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (parsed_json, raw_text | error). Uses JSON mode to avoid malformed outputs.
    """
    try:
        resp = oai.responses.create(
            model=OPENAI_MODEL,
            input=f"{system_text}\n\nUSER:\n{json.dumps(user_payload, ensure_ascii=False)}",
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        raw = (getattr(resp, "output_text", None) or "").strip()
        data = safe_json_from_text(raw)
        if not data:
            return None, raw or "empty"
        return data, raw
    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:200]}"

# --------------------------------------------------------------------------------------
# GPT logic
# --------------------------------------------------------------------------------------
def gpt_enrich_revenue(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[float], str]:
    """
    For NEW or missing revenues: estimate annual ticket revenues.
    Returns: (revenue, currency, confidence, rationale_or_error)
    """
    website_text = fetch_page_text(row.get("website_url"))
    base = baseline_revenue(row.get("avg_ticket_price"), row.get("capacity"), row.get("frequency_per_year"))

    sys = ("You are enriching performing arts org data. "
           "Estimate realistic ANNUAL ticket revenues. Prefer concrete signals from the website text; "
           "otherwise use the provided baseline. Output JSON keys: "
           "revenue(number), currency(string|null), confidence(0..1), rationale(string<=280).")

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
        },
        "website_text_snippet": (website_text or "")[:3000],
    }

    data, raw = oai_json(sys, usr, temperature=0.2)
    if not data:
        return None, None, None, f"gpt_err:{raw}"

    revenue    = data.get("revenue")
    currency   = data.get("currency")
    confidence = data.get("confidence")
    rationale  = (data.get("rationale") or "")[:280]

    rev_val  = float(revenue) if isinstance(revenue, (int, float)) and revenue and revenue > 0 else None
    conf_val = float(confidence) if isinstance(confidence, (int, float)) else None

    return rev_val, currency if isinstance(currency, str) else None, conf_val, rationale or "ok"

def gpt_improve_revenue_and_rfp(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[bool], str]:
    """
    For rows that already have revenues (e.g., SQL baseline or previous runs), try to improve with GPT
    and detect RFP presence. Only overwrite revenues if confidence >= QUALITY_MIN_CONF and > 0.
    Returns: (revenue, currency, confidence, rfp_detected, rationale_or_error)
    """
    website_text = fetch_page_text(row.get("website_url"))
    base = baseline_revenue(row.get("avg_ticket_price"), row.get("capacity"), row.get("frequency_per_year"))

    sys = ("You are a data quality assistant for performing arts organizations. "
           "Estimate ANNUAL ticket revenues realistically and detect if the org is soliciting RFPs. "
           "Return JSON keys: revenue(number), currency(string|null), confidence(0..1), "
           "rfp_detected(boolean), rationale(string<=280).")

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
        "website_text_snippet": (website_text or "")[:3000],
    }

    data, raw = oai_json(sys, usr, temperature=0.2)
    if not data:
        return None, None, None, None, f"gpt_err:{raw}"

    revenue      = data.get("revenue")
    currency     = data.get("currency")
    confidence   = data.get("confidence")
    rfp_detected = data.get("rfp_detected")
    rationale    = (data.get("rationale") or "")[:280]

    rev_val  = float(revenue) if isinstance(revenue, (int, float)) and revenue and revenue > 0 else None
    conf_val = float(confidence) if isinstance(confidence, (int, float)) else None
    rfp_val  = rfp_detected if isinstance(rfp_detected, bool) else None

    return rev_val, currency if isinstance(currency, str) else None, conf_val, rfp_val, rationale or "ok"

# --------------------------------------------------------------------------------------
# BigQuery selection & updates
# --------------------------------------------------------------------------------------
def pick_enrichment_candidates(limit: int, backfill: bool) -> List[Dict[str, Any]]:
    """
    NEW enrichment pass: rows missing revenues, or (if backfill) also rows with sql-fallback source.
    """
    where = [
        "(enrichment_status IS NULL OR enrichment_status != 'LOCKED')",
    ]
    if backfill:
        where.append("(revenues IS NULL OR revenues = 0 OR revenues_source LIKE 'sql-fallback%')")
    else:
        where.append("(revenues IS NULL OR revenues = 0)")

    sql = f"""
    SELECT id, name, domain, website_url,
           avg_ticket_price, capacity, frequency_per_year,
           revenues, revenues_source
    FROM {_table_fq()}
    WHERE {' AND '.join(where)}
    ORDER BY enriched_at ASC NULLS FIRST, last_updated ASC NULLS FIRST
    LIMIT @limit
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])
    return [dict(r) for r in rows]

def pick_quality_candidates(limit: int, force_all: bool = False) -> List[Dict[str, Any]]:
    """
    QUALITY pass: rows that already have revenues (esp. sql-fallback) or missing rfp_detected.
    """
    where_parts = [
        "(enrichment_status IS NULL OR enrichment_status != 'LOCKED')",
        "revenues IS NOT NULL"
    ]
    if not force_all:
        where_parts.append("(revenues_source LIKE 'sql-fallback%' OR rfp_detected IS NULL)")

    sql = f"""
    SELECT id, name, domain, website_url,
           avg_ticket_price, capacity, frequency_per_year,
           revenues, revenues_source, rfp_detected
    FROM {_table_fq()}
    WHERE {' AND '.join(where_parts)}
    ORDER BY last_updated ASC NULLS FIRST
    LIMIT @limit
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])
    return [dict(r) for r in rows]

def update_row(row: Dict[str, Any], updates: Dict[str, Any]):
    """
    updates: dict of column -> value (None means set NULL)
    Always sets last_updated = CURRENT_TIMESTAMP().
    """
    sets_sql = ["last_updated = CURRENT_TIMESTAMP()"]
    params: List[bigquery.ScalarQueryParameter] = []

    for col, val in updates.items():
        if val is None:
            sets_sql.append(f"{col} = NULL")
        else:
            # infer BQ type
            if isinstance(val, bool):
                bqtype = "BOOL"
            elif isinstance(val, int):
                bqtype = "INT64"
            elif isinstance(val, float):
                bqtype = "NUMERIC"
            else:
                bqtype = "STRING"
            pname = f"p_{col}"
            params.append(bigquery.ScalarQueryParameter(pname, bqtype, val))
            sets_sql.append(f"{col} = @{pname}")

    where_sql = _pk_where(params, row)
    sql = f"UPDATE {_table_fq()} SET {', '.join(sets_sql)} WHERE {where_sql}"
    run_query(sql, params)

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
@app.get("/ping")
def ping():
    return "ok"

@app.get("/ready")
def ready():
    return jsonify({"ready": True, "bq_location": BQ_LOCATION})

@app.get("/routes")
def routes():
    return jsonify(sorted([r.rule for r in app.url_map.iter_rules()]))

@app.get("/version")
def version():
    return jsonify({"app": "rfp-data-enricher", "version": os.environ.get("APP_VERSION", "unknown")})

# ------------------------------- MAIN ENRICHMENT --------------------------------------
@app.get("/")
def enrich():
    """
    Enrich revenues for rows missing revenue. If GPT gives confident estimate, use it.
    Otherwise (when BACKFILL_REVENUES=1), use the baseline SQL-like fallback.
    Params:
      - limit (int)
      - dry=1 (no writes)
      - backfill=1 (also touch rows with sql-fallback or 0 revenues)
    """
    try:
        limit = int(request.args.get("limit", "50"))
    except Exception:
        limit = 50
    dry = request.args.get("dry") == "1"
    backfill = request.args.get("backfill") == "1"

    rows = pick_enrichment_candidates(limit, backfill=backfill)
    processed = 0
    wrote_gpt = 0
    wrote_fallback = 0
    skipped = 0
    details = []

    for r in rows:
        processed += 1

        # Always compute baseline fallback for safety
        baseline_val = baseline_revenue(r.get("avg_ticket_price"), r.get("capacity"), r.get("frequency_per_year"))

        # Ask GPT first
        rev_new, currency, conf, rationale = gpt_enrich_revenue(r)

        write_updates = {}
        rev_source = None

        if rev_new is not None and conf is not None and conf >= QUALITY_MIN_CONF:
            write_updates["revenues"] = round(rev_new, 2)
            rev_source = f"GPT_ENRICH[conf={conf:.2f}{', cur='+currency if currency else ''}]"
            write_updates["revenues_source"] = rev_source
            write_updates["enrichment_status"] = "ENRICHED"
            write_updates["enriched_at"] = None  # leave NULL; (or add a TIMESTAMP via SQL NOW() if you track it separately)
            if dry:
                details.append({"name": r.get("name"), "action": "gpt", "conf": conf})
                wrote_gpt += 1
            else:
                update_row(r, write_updates)
                wrote_gpt += 1

        elif BACKFILL_REVENUES and baseline_val and (r.get("revenues") in (None, 0)):
            # Safe deterministic fallback
            write_updates["revenues"] = round(baseline_val, 2)
            write_updates["revenues_source"] = "sql-fallback[price,capacity,events,occupancy]"
            write_updates["enrichment_status"] = "ENRICHED_BASELINE"
            if dry:
                details.append({"name": r.get("name"), "action": "fallback"})
                wrote_fallback += 1
            else:
                update_row(r, write_updates)
                wrote_fallback += 1
        else:
            skipped += 1
            details.append({"name": r.get("name"), "action": "skip"})

        sleep_jitter()

    return jsonify({
        "status": "DRY_OK" if dry else "OK",
        "processed": processed,
        "wrote_gpt": wrote_gpt,
        "wrote_fallback": wrote_fallback,
        "skipped": skipped,
        "quality_min_conf": QUALITY_MIN_CONF,
        "details": details[:10] if dry else [],
    })

# ------------------------------- QUALITY PASS -----------------------------------------
@app.get("/quality")
def quality():
    """
    Improve existing revenues and set rfp_detected.
    - Only overwrite revenues if confidence >= QUALITY_MIN_CONF and > 0.
    Params:
      - limit (int)
      - dry=1 (no writes)
      - force_all=1 (ignore revenues_source filter)
    """
    try:
        limit = int(request.args.get("limit", "20"))
    except Exception:
        limit = 20
    dry = request.args.get("dry") == "1"
    force_all = request.args.get("force_all") == "1"

    rows = pick_quality_candidates(limit, force_all=force_all)
    improved = only_rfp = processed = skipped = 0
    details = []

    for r in rows:
        processed += 1
        rev_new, currency, conf, rfp_new, reason = gpt_improve_revenue_and_rfp(r)

        overwrite = False
        updates: Dict[str, Any] = {}

        if rev_new is not None and conf is not None and conf >= QUALITY_MIN_CONF:
            overwrite = True
            updates["revenues"] = round(rev_new, 2)
            updates["revenues_source"] = f"GPT_QUALITY[conf={conf:.2f}{', cur='+currency if currency else ''}]"
            updates["enrichment_status"] = "QUALITY_REVIEWED"

        if rfp_new is not None and rfp_new != r.get("rfp_detected"):
            updates["rfp_detected"] = bool(rfp_new)

        if dry:
            details.append({
                "name": r.get("name"),
                "overwrite_revenues": overwrite,
                "rfp_detected_new": updates.get("rfp_detected"),
                "confidence": conf,
                "reason": (reason or "")[:120],
            })
        else:
            if updates:
                update_row(r, updates)

        if overwrite:
            improved += 1
        elif "rfp_detected" in updates:
            only_rfp += 1
        else:
            skipped += 1

        sleep_jitter()

    return jsonify({
        "status": "DRY_OK" if dry else "OK",
        "processed": processed,
        "improved_revenues": improved,
        "rfp_detected_updates": only_rfp,
        "skipped": skipped,
        "quality_min_conf": QUALITY_MIN_CONF,
        "details": details[:10] if dry else [],
    })
