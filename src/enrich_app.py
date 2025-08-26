#!/usr/bin/env python3
import os
import time
import random
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, List, Tuple


from flask import Flask, request, jsonify
from google.cloud import bigquery


try:
from .gpt_client import enrich_with_gpt, GPTQuotaExceeded
from .extractors import (
scrape_website_text,
sniff_vendor_signals,
choose_vendor,
derive_price_from_text,
normalize_vendor_name,
is_true_ticketing_provider,
vendor_from_ticketmaster,
vendor_from_eventbrite,
avg_price_from_google_places,
)
except Exception:
from gpt_client import enrich_with_gpt, GPTQuotaExceeded
from extractors import (
scrape_website_text,
sniff_vendor_signals,
choose_vendor,
derive_price_from_text,
normalize_vendor_name,
is_true_ticketing_provider,
vendor_from_ticketmaster,
vendor_from_eventbrite,
avg_price_from_google_places,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "rfpdata")
TABLE = os.getenv("TABLE", "performing_arts_fixed")
BQ_LOCATION = os.getenv("BQ_LOCATION") # e.g. "US", "EU", "europe-southwest1"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


# Stop entire batch immediately on GPT quota/rate limits
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1").lower() in ("1", "true", "yes")


# gentle DML pacing
ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))


if not PROJECT_ID:
raise RuntimeError("PROJECT_ID env var is required")
if not BQ_LOCATION:
raise RuntimeError("BQ_LOCATION env var is required (e.g. 'US'/'EU'/'region')")


BQ = bigquery.Client(project=PROJECT_ID)
app = Flask(__name__)
app.url_map.strict_slashes = False




def table_fqdn() -> str:
return f"`{PROJECT_ID}.{DATASET_ID}.{TABLE}`"




def _row_to_dict(row) -> Dict[str, Any]:
try:
return dict(row.items())
except Exception:
try:
return dict(row)
app.run(host="0.0.0.0", port=port)
