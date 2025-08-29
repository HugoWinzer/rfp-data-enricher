# enrich_app.py (updated to improve revenue estimation with GPT)

import os, json, re, time, random, logging
from typing import Any, Dict, Optional, Tuple, List
from flask import Flask, request, jsonify
from google.cloud import bigquery
from openai import OpenAI
import requests
from bs4 import BeautifulSoup
from decimal import Decimal
from datetime import date, datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID", "rfpdata")
TABLE = os.environ.get("TABLE", "OUTPUT")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "europe-southwest1")

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
QUALITY_MIN_CONF = float(os.environ.get("QUALITY_MIN_CONF", "0.60"))
DEFAULT_LOAD_FACTOR = float(os.environ.get("DEFAULT_LOAD_FACTOR", "0.70"))

ROW_DELAY_MIN_MS = int(os.environ.get("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.environ.get("ROW_DELAY_MAX_MS", "180"))

bq = bigquery.Client(project=PROJECT_ID)
oai = OpenAI()

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
    except Exception as e:
        logging.warning("scrape failed for %s: %s", url, repr(e))
        return ""

def safe_json_from_text(txt: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(txt) if isinstance(txt, str) else txt
    except Exception:
        m = re.search(r"\{.*\}", txt, re.DOTALL)
        return json.loads(m.group(0)) if m else None

def to_json_safe(x):
    if isinstance(x, Decimal): return float(x)
    if isinstance(x, (datetime, date)): return x.isoformat()
    if isinstance(x, dict): return {k: to_json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)): return [to_json_safe(v) for v in x]
    return x

def baseline_revenue(avg_price, capacity, freq_per_year) -> Optional[float]:
    try:
        return round(float(avg_price) * float(capacity) * float(freq_per_year) * DEFAULT_LOAD_FACTOR, 2)
    except Exception:
        return None

def gpt_improve_revenue_and_rfp(row: Dict[str, Any]):
    website_text = fetch_page_text(row.get("website_url"))
    base = baseline_revenue(row.get("avg_ticket_price"), row.get("capacity"), row.get("frequency_per_year"))

    system_msg = (
        "You are a data enrichment agent estimating realistic ANNUAL ticket revenue.\n"
        "Use average ticket price, capacity, and event frequency when available,\n"
        "or infer heuristically from website text. Respond ONLY with strict JSON: \n"
        "revenue, currency, confidence, rfp_detected, rationale."
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
        "website_text_snippet": website_text[:3000],
        "output_schema": {
            "revenue": "number (> 0)",
            "currency": "string or null",
            "confidence": "number between 0 and 1",
            "rfp_detected": "boolean",
            "rationale": "string, <= 280 chars"
        }
    }

    try:
        resp = oai.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": json.dumps(to_json_safe(user_payload), ensure_ascii=False)},
            ],
            temperature=0.2,
        )
        content = resp.choices[0].message.content or ""
        data = safe_json_from_text(content)
        if not data:
            raise ValueError("GPT returned non-JSON format")
    except Exception as e:
        return None, None, None, None, f"gpt_err:{type(e).__name__}:{str(e)[:160]}"

    try:
        revenue = float(data.get("revenue", 0)) or None
        confidence = float(data.get("confidence", 0)) or None
        return revenue, data.get("currency"), confidence, bool(data.get("rfp_detected")), data.get("rationale")
    except Exception as e:
        return None, None, None, None, f"gpt_parse_error:{type(e).__name__}:{str(e)[:160]}"

# ... rest of file unchanged (routes, update_row, pick_quality_candidates, etc)
