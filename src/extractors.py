import re
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

from .vendor_patterns import VENDOR_SIGNATURES, VENDOR_PRIORITY

REQ_TIMEOUT = (8, 20)
HDRS = {"User-Agent": "Mozilla/5.0 (compatible; rfp-enricher/1.0; +https://example.net)"}


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


def sniff_vendor_signals(html: str, base_url: Optional[str]) -> List[Dict[str, str]]:
    signals: List[Dict[str, str]] = []
    if not html:
        return signals

    soup = BeautifulSoup(html, "html.parser")

    for s in soup.find_all("script", src=True):
        src = s.get("src", "")
        for sig in VENDOR_SIGNATURES:
            if any(x in src for x in sig.script_substrings):
                signals.append({"vendor": sig.name, "evidence": absolute_link(src, base_url), "type": "script"})

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").lower().strip()
        href_l = href.lower()
        for sig in VENDOR_SIGNATURES:
            if any(d in href_l for d in sig.domains) or any(k in text for k in sig.link_keywords):
                signals.append({"vendor": sig.name, "evidence": absolute_link(href, base_url), "type": "link"})

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


PRICE_RE = re.compile(r"(?:€|eur|euro|£|gbp|usd|\$)?\s*(\d{1,3})(?:[,.](\d{2}))?", re.IGNORECASE)


def parse_prices(text: str) -> List[int]:
    if not text:
        return []
    prices: List[int] = []
    for m in PRICE_RE.finditer(text):
        try:
            whole = int(m.group(1))
            if 3 <= whole <= 800:
                prices.append(whole)
        except Exception:
            continue
    return prices


def scrape_website_text(domain: Optional[str]) -> Tuple[str, str]:
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


def derive_price_from_text(text: str) -> Optional[int]:
    vals = parse_prices(text)
    if not vals:
        return None
    try:
        return int(round(mean(vals)))
    except Exception:
        return None
