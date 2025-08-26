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


app.run(host="0.0.0.0", port=port)
