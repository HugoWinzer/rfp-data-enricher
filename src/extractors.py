import logging
import re
from typing import Tuple, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

from .vendor_patterns import PROVIDER_PATTERNS, AGGREGATOR_KEYWORDS

log = logging.getLogger("extractors")
UA = "Mozilla/5.0 (compatible; rfp-enricher/1.0)"

def scrape_website_text(site: Optional[str]) -> Tuple[str, str]:
    if not site:
        return "", ""
    url = site
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + site.lstrip("/")
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": UA})
        resp.raise_for_status()
        html = resp.text or ""
        soup = BeautifulSoup(html, "html.parser")
        # Text without scripts/styles
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        text = " ".join((soup.get_text(separator=" ") or "").split())
        return html, text[:200000]  # cap
    except Exception as e:
        log.warning("scrape failed for %s: %s", site, e)
        return "", ""


def sniff_vendor_signals(html: str, site: Optional[str]) -> Dict[str, int]:
    """
    Return a counter {vendor_slug: hits} based on known embed/script/widget patterns.
    """
    html_l = (html or "").lower()
    signals: Dict[str, int] = {}
    for vendor, pats in PROVIDER_PATTERNS.items():
        hits = 0
        for p in pats:
            if re.search(p, html_l):
                hits += 1
        if hits:
            signals[vendor] = hits
    return signals


def choose_vendor(signals: Dict[str, int]) -> Optional[str]:
    if not signals:
        return None
    # choose vendor with most signals
    return max(signals.items(), key=lambda kv: kv[1])[0]


def derive_price_from_text(text: str) -> Optional[float]:
    """
    Very light heuristic: find frequent price-like tokens and pick the median-ish.
    """
    if not text:
        return None
    # €12, 12 €, 12.50, $15, CHF 30, etc.
    prices = re.findall(r"(?:€|\$|£|chf|eur|usd)?\s?(\d{1,3}(?:[.,]\d{2})?)\s?(?:€|\$|£|chf|eur|usd)?", text, flags=re.I)
    if not prices:
        return None
    vals = []
    for p in prices:
        p = p.replace(",", ".")
        try:
            v = float(p)
            if 3 <= v <= 500:
                vals.append(v)
        except Exception:
            pass
    if not vals:
        return None
    vals.sort()
    return vals[len(vals)//2]


def normalize_vendor_name(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    v = v.strip()
    # simple canonicalization
    aliases = {
        "event brite": "Eventbrite",
        "eventbrite": "Eventbrite",
        "ticket master": "Ticketmaster",
        "ticketmaster": "Ticketmaster",
        "see tickets": "See Tickets",
        "seetickets": "See Tickets",
        "feverup": "Fever",
        "fever": "Fever",
        "pretix": "Pretix",
        "weezevent": "Weezevent",
        "ticket tailor": "Ticket Tailor",
        "tickettailor": "Ticket Tailor",
        "spektrix": "Spektrix",
        "eventix": "Eventix",
        "universe": "Universe",
        "ticketone": "TicketOne",
        "billetto": "Billetto",
        "yoyo": "YoYo",
        "yoyotickets": "YoYo",
    }
    key = v.lower()
    return aliases.get(key, v)


def is_true_ticketing_provider(vendor_name: Optional[str]) -> bool:
    """
    True if this looks like a checkout/payment platform, not an aggregator/search portal.
    """
    if not vendor_name:
        return False
    low = vendor_name.lower()
    for bad in AGGREGATOR_KEYWORDS:
        if bad in low:
            return False
    # allow everything else; PROVIDER_PATTERNS keys are definitely true providers
    return True
