# src/madrid_enricher.py
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request
from google.cloud import bigquery

from gpt_client import ask_gpt, GPTResult
from revenue_prompt import SYSTEM_PROMPT, build_user_prompt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("madrid")

# --- Env ---
PROJECT_ID = os.environ.get("PROJECT_ID", "rfp-database-464609")
DATASET_ID = os.environ.get("DATASET_ID", "rfpdata")
TABLE = os.environ.get("TABLE", "performing_arts_madrid")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "europe-southwest1")

# polite defaults
ROW_DELAY_MIN_MS = int(os.environ.get("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.environ.get("ROW_DELAY_MAX_MS", "180"))

# --- BQ client ---
bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

# --- Queries (Madrid schema) ---
# We ONLY select columns that exist in performing_arts_madrid
PENDING_QUERY = f"""
SELECT
  name,
  domain,
  city,
  country,
  CAST(capacity AS FLOAT64) AS capacity,
  CAST(avg_ticket_price AS FLOAT64) AS avg_ticket_price,
  CAST(annual_visitors AS FLOAT64) AS annual_visitors,
  source_url,
  notes,
  gtv
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
WHERE gtv IS NULL
  AND (enrichment_status IS NULL OR enrichment_status != 'LOCKED')
LIMIT @limit
"""

UPDATE_GTV = f"""
UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
SET
  gtv = @gtv,
  notes = IFNULL(CONCAT(IFNULL(notes,''), CASE WHEN @notes IS NOT NULL THEN CONCAT(' | ', @notes) ELSE '' END), @notes),
  enrichment_status = 'OK',
  last_updated = CURRENT_TIMESTAMP()
WHERE name = @name
"""

app = Flask(__name__)

# --- Helpers ---
def _sleep_backoff(iter_idx: int) -> None:
    # tiny jitter to behave nicely
    base = ROW_DELAY_MIN_MS + (iter_idx % max(1, (ROW_DELAY_MAX_MS - ROW_DELAY_MIN_MS)))
    time.sleep(max(0.0, base) / 1000.0)

def fetch_pending(limit: int) -> List[bigquery.table.Row]:
    job = bq_client.query(
        PENDING_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
        ),
    )
    rows = list(job)
    return rows

def update_row(name: str, gtv_value: float, notes: Optional[str], dry: bool) -> None:
    if dry:
        logger.info(f"[DRY] Would update {name} -> gtv={gtv_value}, notes+={notes!r}")
        return
    job = bq_client.query(
        UPDATE_GTV,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", name),
                bigquery.ScalarQueryParameter("gtv", "FLOAT64", float(gtv_value)),
                bigquery.ScalarQueryParameter("notes", "STRING", notes if notes else None),
            ]
        ),
    )
    job.result()
    logger.info(f"APPLY UPDATE name={name} gtv={gtv_value}")

def build_ctx(row: bigquery.table.Row) -> Dict[str, Any]:
    # Convert BQ row to dict context expected by the prompt
    return {
        "name": row.get("name"),
        "domain": row.get("domain"),
        "city": row.get("city"),
        "country": row.get("country"),
        "capacity": row.get("capacity"),
        "avg_ticket_price": row.get("avg_ticket_price"),
        "annual_visitors": row.get("annual_visitors"),
        "source_url": row.get("source_url"),
        "notes": row.get("notes"),
    }

def estimate_revenue(ctx: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Call GPT to estimate annual revenue (USD). Return (revenue_usd, note)."""
    user_prompt = build_user_prompt(ctx)
    result: GPTResult = ask_gpt(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        temperature=0.2,
        max_tokens=350,
    )
    raw = (result.text or "").strip()
    revenue_val: Optional[float] = None
    note = ""

    try:
        data = json.loads(raw)
        revenue_val = float(data.get("revenue_usd")) if data.get("revenue_usd") is not None else None
        conf = (data.get("confidence") or "").lower()
        assump = data.get("assumptions") or ""
        note = f"GPT revenue_usd={revenue_val} confidence={conf} assumptions={assump}"
    except Exception as e:
        note = f"GPT parse_error; raw={raw[:250]}"
        logger.warning(f"JSON parse failed: {e}; raw={raw}")

    return (revenue_val, note)

# --- Routes ---
@app.get("/ping")
def ping():
    return "pong", 200

@app.get("/ready")
def ready():
    # simple BQ check
    _ = list(bq_client.query("SELECT 1").result())
    return jsonify({"status":"ok","bq_location":BQ_LOCATION,"table":f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"}), 200

@app.get("/")
def run_batch():
    # /?limit=5&dry=1
    limit = int(request.args.get("limit", "5"))
    dry = request.args.get("dry", "0") in ("1", "true", "True")

    rows = fetch_pending(limit)
    processed = 0
    updated = 0

    for i, row in enumerate(rows):
        processed += 1
        _sleep_backoff(i)

        ctx = build_ctx(row)
        revenue_val, note = estimate_revenue(ctx)

        if revenue_val is not None:
            update_row(ctx["name"], revenue_val, note, dry)
            updated += 1
        else:
            logger.info(f"Skipped (no revenue) name={ctx['name']} note={note}")

    return jsonify({"processed": processed, "updated": updated, "dry": dry}), 200
