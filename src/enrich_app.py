# src/enrich_app.py
# Cloud Run app that enriches performing-arts rows in BigQuery.
# Guarantees `revenues` is NEVER left NULL by coalescing GPT → heuristics → defaults.
#
# Env (defaults shown where sensible):
#   PROJECT_ID=rfp-database-464609
#   DATASET_ID=rfpdata
#   TABLE=OUTPUT
#   BQ_LOCATION=europe-southwest1
#   OPENAI_API_KEY=projects/.../secrets/OPENAI_API_KEY:latest (provided via Secret Manager at deploy)
#   OPENAI_MODEL=gpt-4o-mini
#   STOP_ON_GPT_QUOTA=1            # return HTTP 429 and stop batch when OpenAI rate-limit is hit
#   BACKFILL_REVENUES=0|1          # when 1, selector includes rows with NULL/0 revenues even if status OK
#   ROW_DELAY_MIN_MS=30            # jitter between rows
#   ROW_DELAY_MAX_MS=180
#   DEFAULT_CAPACITY=200
#   DEFAULT_AVG_TICKET_PRICE=25
#   DEFAULT_EVENTS_PER_YEAR=20
#   DEFAULT_LOAD_FACTOR=0.70
#   SCHEMA_TTL_SEC=60              # how long to cache the BQ schema in the container
#
# Contract:
# - Uses `name` as the update key.
# - Skips rows where enrichment_status='LOCKED'.
# - ticket vendor = payment-funnel software powering checkout.

import os
import re
import sys
import json
import time
import random
import logging
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP

from flask import Flask, request, jsonify, Response
from google.cloud import bigquery

# ---------- Optional: OpenAI (GPT fallback) ----------
try:
    from openai import OpenAI  # openai>=1.40.0
    _HAS_OPENAI = True
except Exception:
    _HAS_OPENAI = False


# ---------- Configuration ----------
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "performing_arts_fixed")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-southwest1")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1") == "1"
BACKFILL_REVENUES_ENV = os.getenv("BACKFILL_REVENUES", "0") == "1"

ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))

DEFAULT_CAPACITY = int(os.getenv("DEFAULT_CAPACITY", "200"))
DEFAULT_AVG_TICKET_PRICE = Decimal(os.getenv("DEFAULT_AVG_TICKET_PRICE", "25"))
DEFAULT_EVENTS_PER_YEAR = int(os.getenv("DEFAULT_EVENTS_PER_YEAR", "20"))
DEFAULT_LOAD_FACTOR = Decimal(os.getenv("DEFAULT_LOAD_FACTOR", "0.70"))

SCHEMA_TTL_SEC = int(os.getenv("SCHEMA_TTL_SEC", "60"))

# Column names (adjust here if your schema differs)
NAME_COL = "name"
STATUS_COL = "enrichment_status"

# App
app = Flask(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

bq_client = bigquery.Client(project=PROJECT_ID) if PROJECT_ID else bigquery.Client()

# cache of table columns
_TABLE_COLS_CACHE: Optional[set] = None
_TABLE_COLS_CACHE_AT: float = 0.0


# ---------- Helpers ----------
class GPTQuotaExceeded(Exception):
    pass


def _table_fq() -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"


def _load_schema_cols(force: bool = False) -> set:
    """Cache table columns with a short TTL; allow explicit refresh when unknown cols appear."""
    global _TABLE_COLS_CACHE, _TABLE_COLS_CACHE_AT
    now = time.time()
    if not force and _TABLE_COLS_CACHE and (now - _TABLE_COLS_CACHE_AT) < SCHEMA_TTL_SEC:
        return _TABLE_COLS_CACHE
    table = bq_client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE}")
    _TABLE_COLS_CACHE = {c.name for c in table.schema}
    _TABLE_COLS_CACHE_AT = now
    return _TABLE_COLS_CACHE


def _filter_to_existing_columns(updates: Dict[str, Any]) -> Dict[str, Any]:
    cols = _load_schema_cols()
    missing = [k for k in updates.keys() if k not in cols]
    if missing:
        # maybe schema changed recently; refresh once
        cols = _load_schema_cols(force=True)
    return {k: v for k, v in updates.items() if k in cols}


def _q_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _pick_websiteish(row: Dict[str, Any]) -> Optional[str]:
    # Prefer explicit website_url, then domain, then other common keys
    for k in ("website_url", "website", "domain", "url", "homepage"):
        if k in row and row[k]:
            val = str(row[k]).strip()
            if not val:
                continue
            if not val.lower().startswith(("http://", "https://")):
                # looks like a bare domain
                if "." in val:
                    return "https://" + val
            return val
    return None


# ---------------- Vendor detection (heuristic only; "ticket vendor = payment-funnel software powering checkout") ------------
_VENDOR_PATTERNS: List[Tuple[str, str]] = [
    (r"ticketmaster|tm\.ticketmaster|amptickets|livenation", "Ticketmaster"),
    (r"\beventbrite\b|eventbrite\.com", "Eventbrite"),
    (r"\buniverse\.com\b", "Universe"),
    (r"\bspektrix\b|system\.spektrix|spektrix\.com", "Spektrix"),
    (r"\btessitura\b|tessituranetwork|tn\.tessitura", "Tessitura"),
    (r"\bfeverup\.com\b|\bfever\b", "Fever"),
    (r"\betix\.com\b|\betix\b", "Etix"),
    (r"\baudienceview\b|\bovationtix\b", "AudienceView"),
    (r"\bvivenu\.com\b|\bvivenu\b", "vivenu"),
    (r"\bseetickets\b|see\.tickets", "See Tickets"),
]

def detect_ticket_vendor(text: str, url: Optional[str]) -> Optional[str]:
    hay = " ".join([text or "", url or ""]).lower()
    for pat, label in _VENDOR_PATTERNS:
        if re.search(pat, hay):
            return label
    return None


# ---------------- Scrape & heuristics ----------------
def _http_get_text(url: str, timeout: int = 12) -> Tuple[str, str]:
    """Return (html, visible_text) best-effort."""
    import requests
    from bs4 import BeautifulSoup  # beautifulsoup4
    if not url:
        return "", ""
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 rfp-data-enricher"})
        html = resp.text or ""
        soup = BeautifulSoup(html, "html.parser")
        # Simple text extraction
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        text = " ".join((soup.get_text(separator=" ") or "").split())
        return html, text[:200_000]  # bound size
    except Exception as e:
        logging.warning(f"scrape failed for {url}: {e}")
        return "", ""


# Prices like $25, €18.50, £12, or with comma decimals 12,50
_CURRENCY = r"(?:[$€£])"
def derive_price_from_text(text: str) -> Optional[Decimal]:
    """Pick a plausible average ticket price from text by scanning currency amounts."""
    if not text:
        return None
    amts: List[Decimal] = []
    # $12.50 or €12 or £12
    for m in re.finditer(rf"{_CURRENCY}\s?(\d{{1,3}}(?:[.]\d{{1,2}})?)", text):
        try:
            val = Decimal(m.group(1))
            if Decimal("5") <= val <= Decimal("250"):
                amts.append(val)
        except Exception:
            pass
    # 12,50 € (comma decimals)
    for m in re.finditer(r"(\d{1,3},\d{1,2})\s*€", text):
        try:
            val = Decimal(m.group(1).replace(",", "."))
            if Decimal("5") <= val <= Decimal("250"):
                amts.append(val)
        except Exception:
            pass
    if not amts:
        return None
    amts.sort()
    return amts[len(amts)//2]  # median-ish


def derive_capacity_from_text(text: str) -> Optional[int]:
    """Find phrases like '300-seat', 'capacity 450', 'seats: 120'."""
    if not text:
        return None
    candidates: List[int] = []
    # '300-seat' or '300 seat'
    for m in re.finditer(r"\b(\d{2,5})\s*[- ]?\s*seat[s]?\b", text, flags=re.IGNORECASE):
        candidates.append(int(m.group(1)))
    # 'capacity 450' or 'capacity: 450'
    for m in re.finditer(r"\bcapacity\s*[:\-]?\s*(\d{2,5})\b", text, flags=re.IGNORECASE):
        candidates.append(int(m.group(1)))
    # 'seats: 120'
    for m in re.finditer(r"\bseats?\s*[:\-]?\s*(\d{2,5})\b", text, flags=re.IGNORECASE):
        candidates.append(int(m.group(1)))
    candidates = [c for c in candidates if 30 <= c <= 100000]
    if not candidates:
        return None
    return max(candidates)  # pick largest plausible (main hall)


# ---------------- GPT fallback ----------------
def enrich_with_gpt(name: str, website_url: Optional[str], text: str) -> Dict[str, Any]:
    """Ask GPT for avg_ticket_price, capacity, events_per_year, occupancy, ticket_vendor (best-effort)."""
    if not _HAS_OPENAI:
        return {}
    try:
        client = OpenAI()
        sys_prompt = (
            "You're enriching performing arts venue data. "
            "ticket vendor = payment-funnel software powering checkout (e.g., Ticketmaster, Eventbrite, Fever, Spektrix, Tessitura, Universe). "
            "Return a compact JSON with keys: avg_ticket_price (number), capacity (int), "
            "events_per_year (int), occupancy (0..1), ticket_vendor (string, vendor brand only). "
            "If unsure, guess conservatively; omit keys you cannot infer."
        )
        content = (text or "")[:8000]  # keep prompt manageable
        user_payload = json.dumps({
            "name": name,
            "website_url": website_url,
            "snippet": content,
        })
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_payload},
            ],
            temperature=0.2,
        )
        msg = resp.choices[0].message.content or "{}"
        # Extract JSON block if present
        m = re.search(r"\{.*\}", msg, flags=re.DOTALL)
        payload = json.loads(m.group(0) if m else msg)
        return payload if isinstance(payload, dict) else {}
    except Exception as e:
        s = str(e)
        if "429" in s or "RateLimit" in s:
            raise GPTQuotaExceeded(s)
        logging.warning(f"GPT fallback failed: {e}")
        return {}


# ---------------- Core merge logic (NEVER leaves revenues NULL) ----------------
def _merge_fields(row: Dict[str, Any], text: str) -> Dict[str, Any]:
    name = row.get(NAME_COL) or row.get("Name") or row.get("NAME") or "unknown"
    website = _pick_websiteish(row)

    # Heuristics
    price_heur = derive_price_from_text(text)
    cap_heur = derive_capacity_from_text(text)
    vend_heur = detect_ticket_vendor(text, website)

    # GPT (optional)
    gpt: Dict[str, Any] = {}
    try:
        gpt = enrich_with_gpt(name=name, website_url=website, text=text) or {}
    except GPTQuotaExceeded:
        raise
    except Exception:
        gpt = {}

    # avg_ticket_price
    avg_price = gpt.get("avg_ticket_price")
    price_src = "gpt" if avg_price is not None else None
    if avg_price is None and price_heur is not None:
        avg_price = price_heur; price_src = "heuristic"
    if avg_price is None:
        avg_price = DEFAULT_AVG_TICKET_PRICE; price_src = "default"
    avg_price = Decimal(str(avg_price))

    # capacity
    cap = gpt.get("capacity")
    cap_src = "gpt" if cap is not None else None
    if cap is None and cap_heur is not None:
        cap = cap_heur; cap_src = "heuristic"
    if cap is None:
        cap = DEFAULT_CAPACITY; cap_src = "default"
    cap = int(cap)

    # events_per_year / frequency_per_year (table uses frequency_per_year)
    events = gpt.get("events_per_year")
    events_src = "gpt" if events is not None else "default"
    if events is None:
        events = DEFAULT_EVENTS_PER_YEAR
    events = int(events)

    # occupancy/load factor (0..1) — internal only; not necessarily a column in your schema
    occ = gpt.get("occupancy")
    occ_src = "gpt" if occ is not None else "default"
    if occ is None:
        occ = DEFAULT_LOAD_FACTOR
    occ = Decimal(str(occ))

    # ticket_vendor
    vendor = gpt.get("ticket_vendor")
    vendor_src = "gpt" if vendor else None
    if not vendor and vend_heur:
        vendor = vend_heur; vendor_src = "heuristic"
    vendor = vendor or None

    # revenues = price × capacity × frequency(events) × load factor
    revenues = _q_money(avg_price * Decimal(cap) * Decimal(events) * occ)
    rev_src = f"formula[{price_src},{cap_src},{events_src},{occ_src}]"

    updates: Dict[str, Any] = {
        "avg_ticket_price": avg_price,
        "avg_ticket_price_source": price_src,
        "capacity": cap,
        "capacity_source": cap_src,
        "ticket_vendor": vendor,
        "ticket_vendor_source": vendor_src,
        "revenues": revenues,
        "revenues_source": rev_src,
        STATUS_COL: "OK",
    }

    # Optional cols if present
    cols = _load_schema_cols()
    if "frequency_per_year" in cols:
        updates["frequency_per_year"] = events
        if "frequency_source" in cols:
            updates["frequency_source"] = events_src
    elif "events_per_year" in cols:
        updates["events_per_year"] = events
    if "occupancy" in cols:
        updates["occupancy"] = float(occ)
    if "enriched_at" in cols:
        # set by app for easy windowing
        updates["enriched_at"] = bigquery.ScalarQueryParameter  # placeholder so _bq_type_for picks NUMERIC? No. We'll set in UPDATE clause as CURRENT_TIMESTAMP()

    return _filter_to_existing_columns(updates)


# ---------------- BigQuery ops ----------------
def _select_rows(limit: int, backfill_revenues: bool) -> List[Dict[str, Any]]:
    cols = _load_schema_cols()
    not_locked = "(enrichment_status IS NULL OR enrichment_status != 'LOCKED')"

    need_fields = [f for f in ["ticket_vendor", "capacity", "avg_ticket_price", "revenues"] if f in cols]
    missing_any = " OR ".join([f"{f} IS NULL" for f in need_fields]) or "FALSE"
    backfill = "(revenues IS NULL OR revenues = 0)" if (backfill_revenues and "revenues" in cols) else "FALSE"

    sql = f"""
    SELECT * FROM {_table_fq()}
    WHERE {not_locked} AND (({missing_any}) OR ({backfill}))
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return [dict(r) for r in bq_client.query(sql, job_config=job_config, location=BQ_LOCATION)]


def _bq_type_for(v: Any) -> str:
    if isinstance(v, bool):
        return "BOOL"
    if isinstance(v, int):
        return "INT64"
    if isinstance(v, Decimal) or isinstance(v, float):
        return "NUMERIC"
    return "STRING"


def _update_row_by_name(name: str, updates: Dict[str, Any]) -> None:
    if not updates:
        return
    updates = _filter_to_existing_columns(updates)

    # Build SET clause and params
    set_parts = []
    params = [bigquery.ScalarQueryParameter("name", "STRING", name)]
    idx = 0
    for k, v in updates.items():
        # enriched_at handled below as CURRENT_TIMESTAMP()
        if k == "enriched_at":
            continue
        idx += 1
        pname = f"p{idx}"
        set_parts.append(f"{k} = @{pname}")
        params.append(bigquery.ScalarQueryParameter(pname, _bq_type_for(v), v))

    # Always update timestamps if present
    cols = _load_schema_cols()
    if "last_updated" in cols:
        set_parts.append("last_updated = CURRENT_TIMESTAMP()")
    if "enriched_at" in cols:
        set_parts.append("enriched_at = CURRENT_TIMESTAMP()")

    set_clause = ", ".join(set_parts) if set_parts else "last_updated = CURRENT_TIMESTAMP()"

    sql = f"""
    UPDATE {_table_fq()}
    SET {set_clause}
    WHERE {NAME_COL} = @name
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq_client.query(sql, job_config=job_config, location=BQ_LOCATION).result()


# ---------------- HTTP Handlers ----------------
@app.get("/ping")
def ping() -> Response:
    return Response("pong", mimetype="text/plain")


@app.get("/ready")
def ready() -> Response:
    try:
        _load_schema_cols(force=True)
        return Response("ok", mimetype="text/plain")
    except Exception as e:
        logging.error(f"readiness failed: {e}")
        return Response("not ready", status=503, mimetype="text/plain")


@app.get("/")
def run_batch():
    try:
        limit = int(request.args.get("limit", "20"))
        dry = request.args.get("dry", "0") == "1"
        backfill = request.args.get("backfill", "0") == "1" or BACKFILL_REVENUES_ENV

        # ensure fresh schema at batch start
        _load_schema_cols(force=True)

        rows = _select_rows(limit=limit, backfill_revenues=backfill)
        processed = 0
        updates_made = 0

        for row in rows:
            processed += 1

            # fetch website text for heuristics/GPT
            url = _pick_websiteish(row)
            html, text = _http_get_text(url) if url else ("", "")

            try:
                updates = _merge_fields(row=row, text=text)
            except GPTQuotaExceeded as e:
                if STOP_ON_GPT_QUOTA:
                    logging.error(f"GPT quota exceeded; stopping batch. {e}")
                    return jsonify({"status": "rate_limited", "processed": processed - 1}), 429
                else:
                    updates = {}  # proceed without GPT-derived values

            name_val = row.get(NAME_COL)
            if not name_val:
                logging.warning("row without name; skipping")
                continue

            if not dry:
                _update_row_by_name(name=name_val, updates=updates)
                updates_made += 1

                # jitter to be polite with websites / GPT
                delay_ms = random.randint(ROW_DELAY_MIN_MS, ROW_DELAY_MAX_MS)
                time.sleep(delay_ms / 1000.0)

        return jsonify({
            "status": "ok",
            "processed": processed,
            "updated": updates_made if not dry else 0,
            "dry": dry,
            "backfill_revenues": backfill
        })
    except Exception as e:
        logging.exception("batch failed")
        return jsonify({"status": "error", "error": str(e)}), 500


# --------------- Gunicorn entrypoint ---------------
# In Dockerfile: CMD exec gunicorn src.enrich_app:app --bind 0.0.0.0:${PORT:-8080} --workers 2 --threads 8 --timeout 120
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=True)
