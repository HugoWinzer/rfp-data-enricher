# src/enrich_app.py
import os
import json
import re
import time
import random
from typing import Any, Dict, Optional, Tuple, List

from flask import Flask, request, jsonify
from google.cloud import bigquery
from openai import OpenAI
import requests
from bs4 import BeautifulSoup
from decimal import Decimal
from datetime import date, datetime

app = Flask(__name__)

# ---------------- Env ----------------
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID", "rfpdata")
TABLE = os.environ.get("TABLE", "OUTPUT")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "europe-southwest1")

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
QUALITY_MIN_CONF = float(os.environ.get("QUALITY_MIN_CONF", "0.60"))
DEFAULT_LOAD_FACTOR = float(os.environ.get("DEFAULT_LOAD_FACTOR", "0.70"))

ROW_DELAY_MIN_MS = int(os.environ.get("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.environ.get("ROW_DELAY_MAX_MS", "180"))

# ---------------- Clients ----------------
bq = bigquery.Client(project=PROJECT_ID)
oai = OpenAI()  # reads OPENAI_API_KEY from env (Secret Manager, etc.)

# ---------------- Helpers ----------------
def _table_fq() -> str:
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
        r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": "rfp-quality/1.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        return soup.get_text(" ", strip=True)[:max_chars]
    except Exception:
        return ""

def safe_json_from_text(txt: str) -> Optional[Dict[str, Any]]:
    # try strict parse first
    try:
        if isinstance(txt, dict):
            return txt
        return json.loads(txt)
    except Exception:
        pass
    # fallback: extract first {...}
    if not isinstance(txt, str):
        return None
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

def to_json_safe(x):
    """Convert BQ Decimals, datetimes, etc. into JSON-safe primitives."""
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, dict):
        return {k: to_json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_json_safe(v) for v in x]
    return x

def baseline_revenue(avg_price, capacity, freq_per_year) -> Optional[float]:
    try:
        if avg_price is None or capacity is None or freq_per_year is None:
            return None
        ap = float(avg_price)
        cap = float(capacity)
        freq = float(freq_per_year)
        return round(ap * cap * freq * DEFAULT_LOAD_FACTOR, 2)
    except Exception:
        return None

# ---------------- GPT (tool calling) ----------------
_TOOL_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "set_estimates",
            "description": "Return the final estimates for annual ticket revenue and RFP detection.",
            "parameters": {
                "type": "object",
                "properties": {
                    "revenue": {"type": "number", "description": "Estimated annual ticket revenue (>0)."},
                    "currency": {"type": ["string", "null"], "description": "Currency code (e.g., USD, EUR) or null if unknown."},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1, "description": "Confidence in the revenue estimate (0..1)."},
                    "rfp_detected": {"type": "boolean", "description": "Whether the org is soliciting RFPs."},
                    "rationale": {"type": "string", "description": "≤280 chars rationale."}
                },
                "required": ["revenue", "confidence", "rfp_detected"]
            }
        }
    }
]

def _coerce_tool_arguments(args) -> Dict[str, Any]:
    """
    OpenAI python 1.x sometimes returns function.arguments as a dict (already parsed),
    others as a JSON string. Normalize both.
    """
    if args is None:
        return {}
    if isinstance(args, dict):
        return args
    if isinstance(args, str):
        try:
            return json.loads(args)
        except Exception:
            m = re.search(r"\{.*\}", args, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(0))
                except Exception:
                    return {}
            return {}
    # if the SDK ever returns other JSON-native types
    return {}

def chat_tools_json(system_text: str, user_payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Call Chat Completions with tool calling. Supports both dict and str tool arguments.
    Returns: (json_or_none, raw_text_or_reason)
    """
    try:
        safe_payload = to_json_safe(user_payload)
        resp = oai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_text},
                {"role": "user", "content": json.dumps(safe_payload, ensure_ascii=False)},
                {"role": "system", "content": "You MUST call the function set_estimates with your final answer."}
            ],
            tools=_TOOL_SPEC,
            tool_choice="auto",
            temperature=0.2,
        )

        choice = resp.choices[0]
        msg = choice.message

        # Preferred: tool call
        tool_calls = getattr(msg, "tool_calls", None)
        if tool_calls:
            for tc in tool_calls:
                if getattr(tc, "type", None) == "function" and getattr(tc, "function", None):
                    if getattr(tc.function, "name", None) == "set_estimates":
                        raw_args = getattr(tc.function, "arguments", None)
                        data = _coerce_tool_arguments(raw_args)
                        if isinstance(data, dict) and data:
                            return data, "tool_call_ok"
                        return None, f"ToolArgsParseError:{type(raw_args).__name__}"

        # Fallback: try message content as JSON
        content = msg.content or ""
        data = safe_json_from_text(content)
        if data:
            return data, "content_json_ok"

        return None, "no_tool_call_and_no_json"
    except Exception as e:
        return None, f"{type(e).__name__}:{str(e)[:300]}"

def gpt_improve_revenue_and_rfp(row: Dict[str, Any]):
    """
    Returns: (revenue, currency, confidence, rfp_detected, rationale)
    Only adopt revenue when confidence >= QUALITY_MIN_CONF and revenue > 0.
    """
    website_text = fetch_page_text(row.get("website_url"))
    base = baseline_revenue(row.get("avg_ticket_price"), row.get("capacity"), row.get("frequency_per_year"))

    system_msg = (
        "You are a data quality assistant for performing arts organizations. "
        "Estimate ANNUAL ticket revenues realistically and detect if the organization is actively soliciting RFPs. "
        "If you only have partial information, make a conservative estimate; avoid unrealistic numbers."
    )
    user_payload = {
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
        "instructions": (
            "Return via function call set_estimates with keys: "
            "revenue(number), currency(string|null), confidence(number 0..1), "
            "rfp_detected(boolean), rationale(string <= 280 chars)."
        ),
    }

    data, raw = chat_tools_json(system_msg, user_payload)
    if not data:
        return None, None, None, None, f"gpt_err:{raw or 'no_json'}"

    try:
        revenue = data.get("revenue")
        currency = data.get("currency")
        confidence = data.get("confidence")
        rfp_detected = data.get("rfp_detected")
        rationale = (data.get("rationale") or "")[:280]

        rev_val = float(revenue) if isinstance(revenue, (int, float)) and revenue and revenue > 0 else None
        conf_val = float(confidence) if isinstance(confidence, (int, float)) else None
        rfp_val = bool(rfp_detected) if isinstance(rfp_detected, bool) else None
        return rev_val, currency, conf_val, rfp_val, rationale or "ok"
    except Exception as e:
        return None, None, None, None, f"gpt_err:{type(e).__name__}:{str(e)[:200]}"

# ---------------- DB writes ----------------
def update_row(name_key: str, revenues, rfp_detected, rev_source_note):
    sets = ["last_updated = CURRENT_TIMESTAMP()"]
    params: List[bigquery.ScalarQueryParameter] = [bigquery.ScalarQueryParameter("name", "STRING", name_key)]
    if revenues is not None:
        sets.append("revenues = @revenues")
        params.append(bigquery.ScalarQueryParameter("revenues", "NUMERIC", round(float(revenues), 2)))
    if rfp_detected is not None:
        sets.append("rfp_detected = @rfp")
        params.append(bigquery.ScalarQueryParameter("rfp", "BOOL", bool(rfp_detected)))
    if rev_source_note is not None:
        sets.append("revenues_source = @src")
        params.append(bigquery.ScalarQueryParameter("src", "STRING", rev_source_note))

    sql = f"UPDATE {_table_fq()} SET {', '.join(sets)} WHERE name = @name"
    run_query(sql, params)

def pick_quality_candidates(limit: int, force_all: bool = False):
    """
    Pick rows whose revenues exist (fallback or otherwise) and/or rfp_detected is missing,
    so we can improve them with GPT.
    """
    where_parts = [
        "(enrichment_status IS NULL OR enrichment_status != 'LOCKED')",
        "revenues IS NOT NULL"
    ]
    if not force_all:
        # prioritize rows with fallback revenues and/or missing rfp_detected
        where_parts.append("(revenues_source LIKE 'sql-fallback%' OR rfp_detected IS NULL)")
    where_sql = " AND ".join(where_parts)
    sql = f"""
        SELECT name, domain, website_url,
               avg_ticket_price, capacity, frequency_per_year,
               revenues, revenues_source, rfp_detected
        FROM {_table_fq()}
        WHERE {where_sql}
        ORDER BY last_updated ASC
        LIMIT @limit
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])
    return [dict(r) for r in rows]

# ---------------- Routes ----------------
@app.get("/")
def index():
    return jsonify({
        "service": "rfp-data-enricher",
        "routes": [r.rule for r in app.url_map.iter_rules()],
        "hint": "Use /quality?limit=20[&dry=1][&force_all=1] to improve revenues and rfp_detected."
    })

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
    return jsonify({
        "app": "rfp-data-enricher",
        "version": os.environ.get("APP_VERSION", "unknown")
    })

@app.get("/quality")
def quality():
    # Query params
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
        rev_note = None
        rev_to_write = None

        if rev_new is not None and conf is not None and conf >= QUALITY_MIN_CONF:
            overwrite = True
            rev_to_write = rev_new
            rev_note = f"GPT_QUALITY[conf={conf:.2f}{', cur='+currency if currency else ''}]"

        if dry:
            details.append({
                "name": r.get("name"),
                "overwrite_revenues": overwrite,
                "rfp_detected_new": rfp_new,
                "confidence": conf,
                "reason": (reason or "")[:200],
            })
        else:
            if overwrite or (rfp_new is not None and rfp_new != r.get("rfp_detected")):
                update_row(r.get("name"), rev_to_write, rfp_new, rev_note)

        if overwrite:
            improved += 1
        elif (rfp_new is not None) and (rfp_new != r.get("rfp_detected")):
            only_rfp += 1
        else:
            skipped += 1

        # polite pacing
        time.sleep(random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS) / 1000.0)

    return jsonify({
        "status": "DRY_OK" if dry else "OK",
        "processed": processed,
        "improved_revenues": improved,
        "rfp_detected_updates": only_rfp,
        "skipped": skipped,
        "quality_min_conf": QUALITY_MIN_CONF,
        "details": details[:10] if dry else [],
    })
