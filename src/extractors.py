# src/extractors.py
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

from .vendor_patterns import VENDOR_SIGNATURES, VENDOR_PRIORITY

REQ_TIMEOUT = (8, 20)  # (connect, read) seconds
HDRS = {
    "User-Agent": "Mozilla/5.0 (compatible; rfp-enricher/1.0; +https://example.net)"
}

def normalize_name(name: str) -> str:
    s = unidecode(name or "")
    s = s.lower()
    s = re.sub(r"[’'`´]", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def parse_location_hint(full_description: Optional[str]) -> Dict[str, Optional[str]]:
    # tries to extract "City, Region, Country" from free text like "31240 L'Union, Occitanie France"
    if not full_description:
        return {"city": None, "region": None, "country": None}
    txt = unidecode(full_description)
    parts = [p.strip() for p in re.split(r"[,\n]+", txt) if p.strip()]
    city = region = country = None
    # heuristic: last token containing "France|Belgium|Belgique|Netherlands|Nederland|Germany|Deutschland|Spain|Espana|Italia|Italy|UK|United Kingdom|USA"
    for p in reversed(parts):
        if re.search(r"\b(france|belgium|belgique|netherlands|nederland|germany|deutschland|spain|espana|italy|italia|united kingdom|uk|usa|australia)\b", p, re.I):
            country = p
            break
    # city ~ first token that looks like "word word" and not numeric
    for p in parts:
        if not re.search(r"\d", p) and len(p.split()) <= 4:
            city = p
            break
    if len(parts) >= 2:
        region = parts[-2] if country else None
    return {"city": city, "region": region, "country": country}

# ----------------- Google Places -----------------
def places_text_search(api_key: str, query: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": api_key}
    r = requests.get(url, params=params, headers=HDRS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("results"):
        return None
    return data["results"][0]

def places_details(api_key: str, place_id: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "place_id,name,formatted_address,international_phone_number,website,url,types"
    params = {"place_id": place_id, "fields": fields, "key": api_key}
    r = requests.get(url, params=params, headers=HDRS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("result"):
        return None
    return data["result"]

# ----------------- Website fetch & scanning -----------------
def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HDRS, timeout=REQ_TIMEOUT, allow_redirects=True)
        if r.status_code >= 200 and r.status_code < 400 and "text/html" in r.headers.get("Content-Type", ""):
            return r.text
    except requests.RequestException:
        return None
    return None

def absolute_link(href: str, base: str) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def detect_vendor_signals(html: str, base_url: str) -> List[Dict[str, str]]:
    """Return a list of vendor sig
