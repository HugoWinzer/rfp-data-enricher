# src/extractors.py
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

from .vendor_patterns import VENDOR_SIGNATURES, VENDOR_PRIORITY

REQ_TIMEOUT = (8, 20)  # (connect, read) seconds
HDRS = {"User-Agent": "Mozilla/5.0 (compatible; rfp-enricher/1.0; +https://example.net)"}


# ---------- Normalizers ----------
def normalize_name(name: str) -> str:
    s = unidecode(name or "")
    s = s.lower()
    s = re.sub(r"[’'`´]", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def parse_location_hint(full_description: Optional[str]) -> Dict[str, Optional[str]]:
    """best-effort city/region/country extraction from free text"""
    if not full_description:
        return {"city": None, "region": None, "country": None}
    txt = unidecode(full_description)
    parts = [p.strip() for p in re.split(r"[,\n]+", txt) if p.strip()]
    city = region = country = None
    for p in reversed(parts):
        if re.search(
            r"\b(france|belgium|belgique|netherlands|nederland|germany|deutschland|spain|espana|italy|italia|united kingdom|uk|usa|australia)\b",
            p,
            re.I,
        ):
            country = p
            break
    for p in parts:
        if not re.search(r"\d", p) and len(p.split()) <= 4:
            city = p
            break
    if len(parts) >= 2:
        region = parts[-2] if country else None
    return {"city": city, "region": region, "country": country}


# ---------- Google Places ----------
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
    fields = "place_id,name,formatted_address,international_phone_number,website,url,types,price_level"
    params = {"place_id": place_id, "fields": fields, "key": api_key}
    r = requests.get(url, params=params, headers=HDRS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("result"):
        return None
    return data["result"]


# ---------- Website fetch & scanning ----------
def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HDRS, timeout=REQ_TIMEOUT, allow_redirects=True)
        if 200 <= r.status_code < 400 and "text/html" in r.headers.get("Content-Type", ""):
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
    """Return a list of vendor signal dicts: {'vendor':..., 'evidence':..., 'type': 'link|script'}"""
    signals: List[Dict[str, str]] = []
    if not html:
        return signals
    soup = BeautifulSoup(html, "html.parser")

    # scripts
    for s in soup.find_all("script", src=True):
        src = s.get("src", "")
        for sig in VENDOR_SIGNATURES:
            if any(x in src for x in sig.script_substrings):
                signals.append({"vendor": sig.name, "evidence": absolute_link(src, base_url), "type": "script"})

    # anchors
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").lower().strip()
        href_l = href.lower()
        for sig in VENDOR_SIGNATURES:
            if any(d in href_l for d in sig.domains) or any(k in text for k in sig.link_keywords):
                signals.append({"vendor": sig.name, "evidence": absolute_link(href, base_url), "type": "link"})

    # unique-ify
    seen = set()
    uniq = []
    for s in signals:
        key = (s["vendor"], s["evidence"])
        if key not in seen:
            seen.add(key)
            uniq.append(s)
    return uniq


def choose_best_vendor(signals: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not signals:
        return None
    purchase_keywords = ("ticket", "billet", "billetterie", "acheter", "buy", "book")
    def score(sig: Dict[str, str]) -> int:
        base = VENDOR_PRIORITY.get(sig["vendor"], 1)
        ev = sig.get("evidence", "").lower()
        if sig["type"] == "link" and any(k in ev for k in purchase_keywords):
            base += 5
        return base
    signals_sorted = sorted(signals, key=score, reverse=True)
    return signals_sorted[0]


# ---------- Ticketmaster Discovery ----------
def tm_search_events(api_key: str, keyword: str) -> Dict[str, Any]:
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {"apikey": api_key, "keyword": keyword, "size": 50, "sort": "date,asc"}
    r = requests.get(url, params=params, headers=HDRS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}


def tm_median_min_price(tm_json: Dict[str, Any]) -> Optional[float]:
    if not tm_json or "_embedded" not in tm_json:
        return None
    events = tm_json["_embedded"].get("events", [])
    prices = []
    for ev in events:
        for pr in ev.get("priceRanges", []):
            try:
                mn = float(pr.get("min"))
                if mn > 0:
                    prices.append(mn)
            except Exception:
                continue
    if not prices:
        return None
    prices.sort()
    n = len(prices)
    mid = n // 2
    return float(prices[mid] if n % 2 == 1 else (prices[mid - 1] + prices[mid]) / 2.0)


def tm_is_vendor_present(tm_json: Dict[str, Any], venue_name_norm: str) -> bool:
    if not tm_json or "_embedded" not in tm_json:
        return False
    events = tm_json["_embedded"].get("events", [])
    for ev in events:
        title = ev.get("name", "")
        ven_name = ""
        try:
            ven = ev["_embedded"]["venues"][0]
            ven_name = ven.get("name", "")
        except Exception:
            pass
        blob = f"{title} {ven_name}".lower()
        if any(tok in blob for tok in venue_name_norm.split()):
            return True
    return False


# ---------- Capacity & price extraction from HTML ----------
CAPACITY_PATTERNS = [
    r"\b(capacit[eé]|jauge|places?|seats?)\D{0,30}(\d{2,5})\b",
    r"\b(\d{2,5})\s+(places?|seats?)\b",
]

def extract_capacity_from_html(html: str) -> Optional[Tuple[int, str]]:
    if not html:
        return None
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    text = unidecode(text.lower())
    for pat in CAPACITY_PATTERNS:
        m = re.search(pat, text, re.I)
        if m:
            try:
                num = int(m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1))
                if 20 <= num <= 100_000:
                    return num, pat
            except Exception:
                continue
    return None


def extract_prices_from_html(html: str) -> List[float]:
    if not html:
        return []
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    text = unidecode(text.lower())
    prices = []
    for m in re.finditer(r"(?:(?:eur|euro|euros|€)\s*|\s*)(\d{1,3}(?:[.,]\d{1,2})?)\s*(?:eur|euro|euros|€)?", text):
        try:
            num = float(m.group(1).replace(",", "."))
            if 3 <= num <= 800:
                prices.append(num)
        except Exception:
            continue
    return prices


def scrape_website_text(domain: Optional[str]) -> Tuple[str, str]:
    """Return (html, text) for domain; empty strings if none."""
    if not domain:
        return "", ""
    url = domain
    if not url.startswith("http"):
        url = "http://" + url
    html = fetch_html(url)
    if not html:
        return "", ""
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    text = " ".join(text.split())[:50_000]
    return html, text
