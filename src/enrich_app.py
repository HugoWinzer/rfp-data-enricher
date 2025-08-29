import os
import json
import time
import logging
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from google.cloud import bigquery

from gpt_client import ask_gpt, GPTResult
from revenue_prompt import SYSTEM_PROMPT, build_user_prompt

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "OUTPUT")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-southwest1")
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1") == "1"

client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

PENDING_QUERY = f"""
SELECT
  name, domain,
  CAST(capacity AS FLOAT64) AS capacity,
  CAST(avg_ticket_price AS FLOAT64) AS avg_ticket_price,
  city, country, run_dates, source_url,
  Revenues
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE}`
WHERE (Revenues IS NULL)
  AND (enrichment_status IS NULL OR enrichment_status != 'LOCKED')
LIMIT @limit
"""

def update_row(name: str, revenues: float, source: str, notes: str, status: str = "OK"):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"
    query = f"""
    UPDATE `{table_ref}`
    SET Revenues=@revenues,
        revenues_source=@source,
        revenues_notes=@notes,
        enrichment_status=@status,
        last_updated=CURRENT_TIMESTAMP()
    WHERE name=@name
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("revenues", "NUMERIC", revenues),
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("notes", "STRING", notes[:1500] if notes else None),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("name", "STRING", name),
            ]
        ),
    )
    job.result()

@app.get("/ping")
def ping():
    return "pong"

@app.get("/ready")
def ready():
    return "ok"

@app.get("/")
def run_batch():
    """
    GET /?limit=N&dry=1
    - Picks rows with Revenues IS NULL
    - Builds a structured prompt using existing capacity/avg_ticket_price as hints
    - Calls GPT and writes Revenues back to BigQuery
    """
    try:
        limit = int(request.args.get("limit", "20"))
    except ValueError:
        limit = 20
    dry = request.args.get("dry") in ("1", "true", "True")

    job = client.query(
        PENDING_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
        ),
    )
    rows = list(job.result())
    processed = 0
    results = []

    for r in rows:
        name = r["name"]
        row_ctx = {
            "name": r.get("name"),
            "domain": r.get("domain"),
            "capacity": r.get("capacity"),
            "avg_ticket_price": r.get("avg_ticket_price"),
            "city": r.get("city"),
            "country": r.get("country"),
            "run_dates": r.get("run_dates"),
            "extra_context": r.get("source_url"),
        }
        user_prompt = build_user_prompt(row_ctx)

        try:
            gpt_result: GPTResult = ask_gpt(SYSTEM_PROMPT, user_prompt)
            data = json.loads(gpt_result.text)
            revenue_val = float(data.get("revenue_usd"))
            confidence = str(data.get("confidence", ""))
            assumptions = str(data.get("assumptions", ""))[:1000]
            note = f"confidence={confidence}; assumptions={assumptions}"

            if dry:
                results.append(
                    {"name": name, "revenues_dry": revenue_val, "notes": note}
                )
            else:
                update_row(
                    name=name,
                    revenues=revenue_val,
                    source="GPT",
                    notes=note,
                    status="OK",
                )
                results.append({"name": name, "revenues": revenue_val})
                processed += 1

        except RuntimeError as e:
            # This captures OpenAI 429 quota stops.
            if "429" in str(e) and STOP_ON_GPT_QUOTA:
                logging.error("GPT quota hit (429). Stopping batch.")
                return jsonify(
                    {
                        "status": "stopped_on_quota",
                        "processed": processed,
                        "error": str(e),
                    }
                ), 429
            logging.exception("GPT error")
            if not dry:
                # Mark row as attempted but leave Revenues NULL
                update_row(
                    name=name,
                    revenues=None,
                    source="GPT",
                    notes=f"error: {str(e)[:900]}",
                    status="ERROR",
                )
            results.append({"name": name, "error": str(e)})
        except Exception as e:
            logging.exception("Unhandled error")
            if not dry:
                update_row(
                    name=name,
                    revenues=None,
                    source="GPT",
                    notes=f"error: {str(e)[:900]}",
                    status="ERROR",
                )
            results.append({"name": name, "error": str(e)})

    return jsonify({"status": "ok", "processed": processed, "items": results})
