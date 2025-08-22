# src/extractors.py
from __future__ import annotations
import json
import math
import random
import re
import time
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

from .vendor_patterns import detect_vendor

# ---------- HTTP helpers ----------

_UAS = [
    # A small rotation of desktop user agents
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
]

_HEADERS = {
    "User-Agent": random.choice(_UAS),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en,en-GB,en-US,fr,es,de,nl;q=0.5",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_CANDIDATE_PATHS = [
    "/", "/events", "/event", "/tickets", "/billetterie", "/programmation", "/programme",
    "/agenda", "/whats-on", "/calendar", "/cartelera", "/veranstaltungen", "/termine",
    "/bilhetes", "/ingressos", "/evenement", "/evenements",
]

_JSONLD_TYPES_EVENT = {"Event", "TheaterEvent", "MusicEvent", "Festival", "ExhibitionEvent"}
_PRICE_CURR_SYM = {
    "€": "EUR", "$": "USD", "£": "GBP", "R$": "BRL", "CHF": "CHF", "PLN": "PLN", "A$": "AUD", "AU$": "AUD",
    "NZ$": "NZD", "CA$": "CAD", "C$": "CAD", "kr": "SEK", "DKK": "DKK", "NOK": "NOK",
}

_TIMEOUT = (8, 15)  # (connect, read)
_MAX_BYTES = 2_000_000  # 2 MB cap per page

@dataclass
class ExtractResult:
    avg_ticket_price: Optional[float] = None
    avg_ticket_price_source: Optional[str] = None
    capacity: Optional[int] = None
    capacity_source: Optional[str] = None
    ticket_vendor: Optional[str] = None
    ticket_vendor_source: Optional[str] = None

def _normalize_domain_to_url(domain: str) -> Optional[str]:
    if not domain:
        return None
    d = domain.strip()
    if d.startswith("http://") or d.startswith("https://"):
        return d
    return f"https://{d}"

def _limited_text(content: bytes) -> str:
    return content[:_MAX_BYTES].decode("utf-8", errors="ignore")

def fetch_first_ok(base_url: str) -> Tuple[Optional[str], Optional[str], Optional[BeautifulSoup], List[str]]:
    """
    Try a handful of common pages and return (final_url, html_str, soup, links)
    """
    session = requests.Session()
    session.headers.update(_HEADERS)
    parsed = urlparse(base_url)
    if not parsed.scheme:
        base_url = "https://" + base_url
        parsed = urlparse(base_url)

    tried = []
    for path in _CANDIDATE_PATHS:
        url = urljoin(base_url, path)
        tried.append(url)
        try:
            resp = session.get(url, timeout=_TIMEOUT, allow_redirects=True)
            if resp.status_code >= 200 and resp.status_code < 400 and "text/html" in resp.headers.get("Content-Type",""):
                html = _limited_text(resp.content)
                soup = BeautifulSoup(html, "html.parser")
                links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("/"):
                        href = urljoin(resp.url, href)
                    links.append(href)
                return resp.url, html, soup, links
        except requests.RequestException:
            # brief backoff
            time.sleep(0.3 + random.random() * 0.5)
            continue
    return None, None, None, []

# ---------- Price parsing ----------

_PRICE_RX = re.compile(
    r"(?:€|EUR|£|GBP|\$|USD|R\$|BRL|CHF|PLN|A\$|AU\$|NZ\$|CA\$|C\$|kr|DKK|NOK)\s*[\d]+(?:[.,]\d{1,2})?",
    re.I,
)

def _to_float_price(token: str) -> Optional[float]:
    # Normalize currency+amount; we only return the numeric part for averaging
    # Examples: "€12", "12,50 €", "EUR 15.00", "R$ 30", "CHF 25.-"
    # Strip currency
    t = token.strip()
    # Keep digits, commas, dots
    m = re.findall(r"[\d.,]+", t)
    if not m:
        return None
    num = m[0]
    # Case like "25.-"
    num = num.replace(".-", "")
    # European decimals
    if num.count(",") == 1 and num.count(".") == 0:
        num = num.replace(",", ".")
    # Thousands separators
    if num.count(".") > 1:
        parts = num.split(".")
        num = parts[0] + "." + "".join(parts[1:])
    try:
        val = float(num)
        if val <= 0 or not math.isfinite(val):
            return None
        return round(val, 2)
    except ValueError:
        return None

def _prices_from_text(soup: BeautifulSoup) -> List[float]:
    text = soup.get_text(" ", strip=True)
    vals = []
    for m in _PRICE_RX.finditer(text):
        v = _to_float_price(m.group(0))
        if v is not None:
            vals.append(v)
    return vals[:50]  # cap

def _prices_from_jsonld(soup: BeautifulSoup) -> List[float]:
    vals = []
    for node in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(node.text)
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for o in objs:
            t = o.get("@type")
            if isinstance(t, list):
                types = set(t)
            else:
                types = {t} if t else set()
            if _JSONLD_TYPES_EVENT & types:
                offers = o.get("offers")
                offers_list = offers if isinstance(offers, list) else [offers] if offers else []
                for off in offers_list:
                    if not isinstance(off, dict):
                        continue
                    price = off.get("price")
                    if isinstance(price, (int, float)):
                        vals.append(float(price))
                    elif isinstance(price, str):
                        v = _to_float_price(price)
                        if v is not None:
                            vals.append(v)
    return vals

def extract_average_price_from_soup(soup: BeautifulSoup) -> Optional[float]:
    vals = _prices_from_jsonld(soup)
    if not vals:
        vals = _prices_from_text(soup)
    if not vals:
        return None
    return round(sum(vals) / len(vals), 2)

# ---------- Capacity parsing ----------

_CAPACITY_PATTERNS = [
    r"\bcapacity\s*[:\-]?\s*(\d{2,6})\b",
    r"\bcapacit[eé]\s*[:\-]?\s*(\d{2,6})\b",
    r"\bcapacidad\s*[:\-]?\s*(\d{2,6})\b",
    r"\baforo\s*[:\-]?\s*(\d{2,6})\b",
    r"\bseating\s*capacity\s*[:\-]?\s*(\d{2,6})\b",
    r"\b(\d{2,6})\s*(?:seats|places|sitze|plazas)\b",
]
_CAPACITY_RX = [re.compile(p, re.I) for p in _CAPACITY_PATTERNS]

def extract_capacity_from_text(text: str) -> Optional[int]:
    t = " ".join(text.split())  # squash whitespace
    for rx in _CAPACITY_RX:
        m = rx.search(t)
        if m:
            try:
                val = int(m.group(1))
                if 20 <= val <= 100000:
                    return val
            except Exception:
                continue
    return None

# ---------- Vendor detection wrapper ----------

def find_vendor(domain: str, html: str, links: List[str]) -> Tuple[Optional[str], Optional[str]]:
    return detect_vendor(domain, html, links)

# ---------- Orchestration for one domain ----------

def enrich_from_domain(domain: str) -> ExtractResult:
    result = ExtractResult()
    base = _normalize_domain_to_url(domain)
    if not base:
        return result

    final_url, html, soup, links = fetch_first_ok(base)
    if not soup or not html:
        return result

    # Vendor
    vendor, v_src = find_vendor(domain, html, links)
    if vendor:
        result.ticket_vendor = vendor
        result.ticket_vendor_source = v_src or (final_url or base)

    # Price
    price = extract_average_price_from_soup(soup)
    if price is not None:
        result.avg_ticket_price = price
        result.avg_ticket_price_source = final_url or base

    # Capacity
    cap = extract_capacity_from_text(soup.get_text(" ", strip=True))
    if cap is not None:
        result.capacity = cap
        result.capacity_source = final_url or base

    # If we didn’t get price/capacity, try a couple more pages (secondary paths)
    if (result.avg_ticket_price is None or result.capacity is None) and final_url:
        origin = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(final_url))
        # try up to 3 more likely pages
        extra = [p for p in _CANDIDATE_PATHS if p not in ("/", "/event")]
        random.shuffle(extra)
        for path in extra[:3]:
            try:
                url = urljoin(origin, path)
                r = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
                if r.ok and "text/html" in r.headers.get("Content-Type",""):
                    h = _limited_text(r.content)
                    s = BeautifulSoup(h, "html.parser")
                    if result.avg_ticket_price is None:
                        p = extract_average_price_from_soup(s)
                        if p is not None:
                            result.avg_ticket_price = p
                            result.avg_ticket_price_source = url
                    if result.capacity is None:
                        c = extract_capacity_from_text(s.get_text(" ", strip=True))
                        if c is not None:
                            result.capacity = c
                            result.capacity_source = url
                    # short-circuit if both found
                    if result.avg_ticket_price is not None and result.capacity is not None:
                        break
            except requests.RequestException:
                continue

    return result
