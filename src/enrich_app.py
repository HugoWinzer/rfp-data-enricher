# src/enrich_app.py
import os, json, decimal, logging
from google.cloud import bigquery

BQ = bigquery.Client()
PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
TABLE = os.environ.get("STAGING_TABLE", "performing_arts_fixed")
TABLE_FQN = f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"

# Log first N rows’ GPT output per request
DEBUG_LOG_N = int(os.getenv("DEBUG_LOG_N", "3"))

def as_decimal(val):
    if val is None:
        return None
    # ensure NUMERIC in BQ, avoid float -> NUMERIC issues
    return decimal.Decimal(str(val))

def update_in_place(row, enriched, sources, idx_in_batch=0):
    """
    - Only mark DONE if at least one of {ticket_vendor, capacity, avg_ticket_price} is set.
    - Otherwise mark NO_DATA (or leave PENDING if you prefer).
    """
    name = row["name"]

    # Light debug log for the first few items
    if idx_in_batch < DEBUG_LOG_N:
        logging.info("GPT parsed for '%s': %s", name, json.dumps(enriched, ensure_ascii=False))

    set_fields = []
    params = [
        bigquery.ScalarQueryParameter("name", "STRING", name),
    ]

    # ticket_vendor
    if enriched.get("ticket_vendor"):
        set_fields += ["ticket_vendor=@ticket_vendor", "ticket_vendor_source=@ticket_vendor_source"]
        params += [
            bigquery.ScalarQueryParameter("ticket_vendor", "STRING", enriched["ticket_vendor"]),
            bigquery.ScalarQueryParameter("ticket_vendor_source", "STRING", (sources or {}).get("ticket_vendor") or "GPT"),
        ]

    # capacity (INT64)
    if enriched.get("capacity") is not None:
        set_fields += ["capacity=@capacity", "capacity_source=@capacity_source"]
        params += [
            bigquery.ScalarQueryParameter("capacity", "INT64", int(enriched["capacity"])),
            bigquery.ScalarQueryParameter("capacity_source", "STRING", (sources or {}).get("capacity") or "GPT"),
        ]

    # avg_ticket_price (NUMERIC) – ALWAYS SAFE_CAST for extra safety
    if enriched.get("avg_ticket_price") is not None:
        set_fields += ["avg_ticket_price=SAFE_CAST(@avg_ticket_price AS NUMERIC)", "avg_ticket_price_source=@avg_ticket_price_source"]
        params += [
            bigquery.ScalarQueryParameter("avg_ticket_price", "NUMERIC", as_decimal(enriched["avg_ticket_price"])),
            bigquery.ScalarQueryParameter("avg_ticket_price_source", "STRING", (sources or {}).get("avg_ticket_price") or "GPT"),
        ]

    # Decide status
    filled_any = any(k in enriched and enriched[k] is not None for k in ("ticket_vendor", "capacity", "avg_ticket_price"))
    status = "DONE" if filled_any else "NO_DATA"   # change to "PENDING" if you prefer to retry later

    set_fields += ["enrichment_status=@enrichment_status", "last_updated=CURRENT_TIMESTAMP()"]
    params.append(bigquery.ScalarQueryParameter("enrichment_status", "STRING", status))

    set_clause = ", ".join(set_fields)
    q = f"""
    UPDATE `{TABLE_FQN}`
    SET {set_clause}
    WHERE name=@name
    """
    BQ.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    logging.info("APPLY UPDATE for %s -> %s", name, set_fields)
