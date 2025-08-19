import os
import json
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from google.cloud import bigquery
from openai import OpenAI

# ------------------------------------------------------------------------------
# Config & globals
# ------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID", "rfpdata")
TABLE = os.environ.get("TABLE", "performing_arts_fixed")
TABLE_FQN = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
BQ_LOCATION = os.environ.get("BQ_LOCATION")  # e.g. "EU" or "europe-west1"

# BigQuery client (ADC)
bq = bigquery.Client(project=PROJECT_ID)
# OpenAI client (reads OPENAI_API_KEY from env)
oa = OpenAI()

app = Flask(__name__)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _to_decimal(val):
    """Safely convert to Decimal for BigQuery NUMERIC."""
    if val is None:
        return None
    try:
        # stringify to avoid float artifacts
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None


def gpt_enrich(row):
    """
    Call OpenAI Chat Completions (v1 API) to try to infer missing fields.
    Returns: (enriched: dict, sources: dict)

    enriched keys we may set:
      - ticket_vendor (str)
      - capacity (int)
      - avg_ticket_price (Decimal/str/float -> converted later)
      - enrichment_status ('DONE' | 'NO_DATA')

    sources keys:
      - ticket_vendor_source
      - capacity_source
      - avg_ticket_price_source
    """
    messages = [
        {"role": "system", "content": (
            "You are an assistant that enriches venue/company rows for performing arts. "
            "If you cannot determine a field, leave it blank."
        )},
        {"role": "user", "content": (
            "Given this row JSON, fill any missing fields: ticket_vendor (string name), "
            "capacity (integer), avg_ticket_price (numeric, average local currency). "
            "Return strict JSON with keys you know. Row: "
            + json.dumps(row, ensure_ascii=False)
        )}
    ]

    try:
        resp = oa.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.2,
        )
        content = resp.choices[0].message.content or ""
    except Exception as e:
        log.warning("gpt failed: \n%s", e)
        # fall back: we couldn't enrich
        return (
            {"enrichment_status": "NO_DATA"},
            {}
        )

    # Parse JSON if possible
    try:
        data = json.loads(content)
        enriched = {}
        sources = {}

        if isinstance(data, dict):
            tv = data.get("ticket_vendor")
            cap = data.get("capacity")
            price = data.get("avg_ticket_price")

            if tv:
                enriched["ticket_vendor"] = str(tv).strip()
                sources["ticket_vendor_source"] = "GPT"

            if cap not in (None, ""):
                try:
                    enriched["capacity"] = int(cap)
                    sources["capacity_source"] = "GPT"
                except Exception:
                    pass

            if price not in (None, ""):
                # leave as raw; we'll cast to Decimal in update
                enriched["avg_ticket_price"] = price
                sources["avg_ticket_price_source"] = "GPT"

        if any(k in enriched for k in ("ticket_vendor", "capacity", "avg_ticket_price")):
            enriched["enrichment_status"] = "DONE"
        else:
            enriched["enrichment_status"] = "NO_DATA"

        return enriched, sources

    except Exception:
        # model responded non-JSON; mark as no data
        return ({"enrichment_status": "NO_DATA"}, {})


def fetch_rows(limit: int):
    """
    Pull a small batch of rows that still need enrichment.
    Strategy: any record missing any of the 3 fields and not already NO_DATA recently.
    """
    sql = f"""
    SELECT
      *
    FROM `{TABLE_FQN}`
    WHERE
      (ticket_vendor IS NULL OR capacity IS NULL OR avg_ticket_price IS NULL)
      AND (enrichment_status IS NULL OR enrichment_status != 'NO_DATA')
    LIMIT @limit
    """

    params = [bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    if BQ_LOCATION:
        job_config.location = BQ_LOCATION

    return list(bq.query(sql, job_config=job_config).result())


def update_in_place(row, enriched: dict, sources: dict):
    """
    Build a single UPDATE with only the fields we have, including sources and last_updated.
    Uses typed parameters (NUMERIC via Decimal) and pins job location.
    """
    sets = ["last_updated = CURRENT_TIMESTAMP()"]
    params = []

    # ticket_vendor
    if enriched.get("ticket_vendor") is not None:
        sets.append("ticket_vendor = @ticket_vendor")
        params.append(bigquery.ScalarQueryParameter("ticket_vendor", "STRING", enriched["ticket_vendor"]))
        # source column
        src = sources.get("ticket_vendor_source")
        if src:
            sets.append("ticket_vendor_source = @ticket_vendor_source")
            params.append(bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", src))

    # capacity (INT64)
    if enriched.get("capacity") is not None:
        sets.append("capacity = @capacity")
        params.append(bigquery.ScalarQueryParameter("capacity", "INT64", int(enriched["capacity"])))
        src = sources.get("capacity_source")
        if src:
            sets.append("capacity_source = @capacity_source")
            params.append(bigquery.ScalarQueryParameter("capacity_source", "STRING", src))

    # avg_ticket_price (NUMERIC) — must be Decimal
    if enriched.get("avg_ticket_price") is not None:
        price_dec = _to_decimal(enriched["avg_ticket_price"])
        sets.append("avg_ticket_price = CAST(@avg_ticket_price AS NUMERIC)")
        params.append(bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", price_dec))
        src = sources.get("avg_ticket_price_source")
        if src:
            sets.append("avg_ticket_price_source = @avg_ticket_price_source")
            params.append(bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", src))

    # enrichment_status (STRING)
    if enriched.get("enrichment_status") is not None:
        sets.append("enrichment_status = @enrichment_status")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", enriched["enrichment_status"]))

    if len(sets) == 1:
        # nothing to update except timestamp; still write timestamp + NO_DATA if we can
        sets.append("enrichment_status = COALESCE(@enrichment_status, enrichment_status)")
        params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", enriched.get("enrichment_status", "NO_DATA")))

    # Define the primary key to identify the row. Adjust this WHERE clause to your schema (id vs name, etc).
    # Here we assume there is a stable string key column called "name".
    where_col = "name"
    key_val = row.get(where_col) if isinstance(row, dict) else getattr(row, where_col, None)
    if key_val is None:
        # fallback: try id
        where_col = "id"
        key_val = row.get(where_col) if isinstance(row, dict) else getattr(row, where_col, None)

    if key_val is None:
        raise RuntimeError("Cannot identify row key: expected 'name' or 'id' in table.")

    params.append(bigquery.ScalarQueryParameter("key", "STRING", str(key_val)))

    q = f"""
    UPDATE `{TABLE_FQN}`
    SET {", ".join(sets)}
    WHERE {where_col} = @key
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    if BQ_LOCATION:
        job_config.location = BQ_LOCATION

    bq.query(q, job_config=job_config).result()

    changed_cols = [frag.split("=")[0].strip() for frag in sets]
    log.info("APPLY UPDATE for %s -> %s", key_val, changed_cols)


def run_batch(limit: int):
    rows = fetch_rows(limit)
    log.info("=== UPDATE MODE: no inserts; BigQuery UPDATE only ===")
    log.info("Processing %d rows", len(rows))

    processed = 0
    for r in rows:
        # convert Row to dict for prompt + updates
        row_dict = dict(r.items()) if hasattr(r, "items") else dict(r)
        enriched, sources = gpt_enrich(row_dict)
        try:
            update_in_place(row_dict, enriched, sources)
            processed += 1
        except Exception as e:
            key = row_dict.get("name") or row_dict.get("id")
            log.error("Failed row: %s: %s", key, e)

    return processed

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/")
def root():
    """
    Trigger a small enrichment batch.
    Example: GET /?limit=25
    """
    try:
        limit = int(request.args.get("limit", "25"))
        limit = max(1, min(limit, 100))  # safety clamp
    except Exception:
        limit = 25

    try:
        count = run_batch(limit)
        return jsonify({"processed": count, "status": "OK"}), 200
    except Exception as e:
        log.exception("Batch failed")
        return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # Local dev: run Flask directly
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
