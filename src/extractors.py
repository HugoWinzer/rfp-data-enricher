import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

from .vendor_patterns import VENDOR_SIGNATURES, VENDOR_PRIORITY

REQ_TIMEOUT = (8, 20)
HDRS = {"User-Agent": "Mozilla/5.0 (compatible; rfp-enricher/1.0; +https://example.net)"}


# ---------- Normalizers ----------
def normalize_name(name: str) -> str:
    s = unidecode(name or "")
    s = s.lower()
    s = re.sub(r"[’'`´]", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def absolute_link(href: str, base_url: Optional[str]) -> str:
    try:
        return urljoin(base_url or "", href or "")
    except Exception:
        return href or ""


# ---------- Fetch ----------
def fetch_html(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, headers=HDRS, timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return ""
        return r.text or ""
    except Exception:
        return ""


# ---------- Vendor sniffing ----------
def sniff_vendor_signals(html: str, base_url: Optional[str]) -> List[Dict[str, str]]:
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
            uniq.append(s)
            seen.add(key)
    return uniq


def choose_vendor(signals: List[Dict[str, str]]) -> Optional[str]:
    if not signals:
        return None
    best = None
    best_score = -1
    for s in signals:
        score = VENDOR_PRIORITY.get(s["vendor"], 0)
        if score > best_score:
            best = s["vendor"]
            best_score = score
    return best


# ---------- Prices ----------
def parse_prices(text: str) -> List[int]:
    """Very rough price extraction; returns plausible whole numbers."""
    if not text:
        return []
    prices: List[int] = []
    for m in re.finditer(r"(\d{1,3})([,.]\d{2})?", text):
        try:
            num = int(m.group(1).replace(",", ""))
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
