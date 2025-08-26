# src/enrich_app.py
from __future__ import annotations
import os, time, random, logging
from typing import Dict, Any, List, Tuple
from decimal import Decimal, InvalidOperation


from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core import retry, exceptions


# Local imports — keep your existing extractors
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
phone_from_google_places,
extract_linkedin_url,
extract_phone_numbers,
extract_alt_name,
extract_descriptions,
detect_rfp,
extract_charge_pct,
extract_revenues,
extract_capacity,
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
phone_from_google_places,
extract_linkedin_url,
extract_phone_numbers,
extract_alt_name,
extract_descriptions,
detect_rfp,
extract_charge_pct,
extract_revenues,
extract_capacity,
)


# --- App / logging ---
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# --- Config ---
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE = os.getenv("TABLE")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")


ENABLE_TICKETMASTER = os.getenv("ENABLE_TICKETMASTER", "1") == "1"
ENABLE_PLACES = os.getenv("ENABLE_PLACES", "1") == "1"
ENABLE_EVENTBRITE = os.getenv("ENABLE_EVENTBRITE", "0") == "1" # default disabled
STOP_ON_GPT_QUOTA = os.getenv("STOP_ON_GPT_QUOTA", "1") == "1"
ROW_DELAY_MIN_MS = int(os.getenv("ROW_DELAY_MIN_MS", "30"))
ROW_DELAY_MAX_MS = int(os.getenv("ROW_DELAY_MAX_MS", "180"))


# BigQuery Client
app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
