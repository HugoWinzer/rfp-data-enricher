import logging, os, json
import re
from typing import Tuple, Dict, Any, Optional


import requests
from bs4 import BeautifulSoup


from .vendor_patterns import PROVIDER_PATTERNS, AGGREGATOR_KEYWORDS


log = logging.getLogger("extractors")
UA = "Mozilla/5.0 (compatible; rfp-enricher/1.0)"
TM_KEY = os.getenv("TICKETMASTER_KEY")
PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY")
EB_TOKEN = os.getenv("EVENTBRITE_TOKEN")
ENABLE_TM = os.getenv("ENABLE_TICKETMASTER") in ("1","true","True")
ENABLE_PLACES = os.getenv("ENABLE_PLACES") in ("1","true","True")
ENABLE_EB = os.getenv("ENABLE_EVENTBRITE") in ("1","true","True")


def scrape_website_text(site: Optional[str]) -> Tuple[str, str]:
if not site:
return "", ""
url = site
if not url.startswith("http"):
url = "https://" + url
try:
r = requests.get(url, headers={"User-Agent": UA}, timeout=10)
if r.status_code >= 400:
return "", ""
html = r.text or ""
soup = BeautifulSoup(html, "html.parser")
# Text without scripts/styles
for t in soup(["script", "style", "noscript"]):
t.decompose()
text = " ".join((soup.get_text(separator=" ") or "").split())
return html, text[:200000] # cap
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
return True
