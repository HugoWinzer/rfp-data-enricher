# src/vendor_patterns.py
import re
from typing import Optional, Tuple

# Map regex (compiled) -> canonical vendor name
_VENDOR_PATTERNS = [
    # Global majors
    (r"(?:^|/|\.)(ticketmaster|livenation|universe)\.(?:com|co\.uk|ca|de|fr|au|nz)", "Ticketmaster"),
    (r"(?:^|/|\.)(seetickets)\.(?:com|us|uk|nl|de|fr|it)", "See Tickets"),
    (r"(?:^|/|\.)(eventbrite)\.(?:com|co\.[a-z]{2}|de|fr|it|nl|es|pt|au|ca|uk)", "Eventbrite"),
    (r"(?:^|/|\.)(dice)\.fm", "DICE"),
    (r"(?:^|/|\.)(feverup)\.com", "Fever"),
    (r"(?:^|/|\.)(tickettailor)\.com", "Ticket Tailor"),
    (r"(?:^|/|\.)(eventim)\.(?:de|it|pl|cz|sk|hu|se|dk|fi|ro|nl|no|com|co\.uk)", "Eventim"),
    (r"(?:^|/|\.)(ticketek)\.(?:com|com\.au|co\.nz|ar|cl)", "Ticketek"),
    (r"(?:^|/|\.)(trybooking)\.(?:com|com\.au|co\.nz)", "TryBooking"),
    (r"(?:^|/|\.)(oztix)\.com\.au", "Oztix"),
    (r"(?:^|/|\.)(ra|residentadvisor)\.(?:co\.uk|net)", "Resident Advisor"),
    (r"(?:^|/|\.)(ticketone)\.it", "TicketOne"),
    (r"(?:^|/|\.)(billetto)\.(?:dk|se|no|fi|co\.uk|com)", "Billetto"),

    # France/Benelux
    (r"(?:^|/|\.)(weezevent)\.com", "Weezevent"),
    (r"(?:^|/|\.)(billetweb)\.fr", "Billetweb"),
    (r"(?:^|/|\.)(shotgun)\.live", "Shotgun"),
    (r"(?:^|/|\.)(yurplan)\.com", "Yurplan"),
    (r"(?:^|/|\.)(passculture)\.app", "pass Culture (FR)"),
    (r"(?:^|/|\.)(fnacspectacles|billetterie\.fnac)\.(?:com|fr)", "Fnac Spectacles"),

    # Germany/Austria/CH
    (r"(?:^|/|\.)(eventim-light)\.com", "Eventim Light"),
    (r"(?:^|/|\.)(reservix|adticket)\.de", "Reservix / ADticket"),
    (r"(?:^|/|\.)(ticketino)\.com", "TICKETINO"),

    # Iberia/LatAm
    (r"(?:^|/|\.)(entradium)\.com", "Entradium"),
    (r"(?:^|/|\.)(ticketea)\.com", "Ticketea"),
    (r"(?:^|/|\.)(sympla)\.com\.br", "Sympla"),
    (r"(?:^|/|\.)(ingressorapido)\.com\.br", "Ingresso Rápido"),
    (r"(?:^|/|\.)(eventim)\.com\.br", "Eventim Brasil"),

    # Nordics/CEE odds & ends
    (r"(?:^|/|\.)(tixly)\.(?:com|eu)", "Tixly"),
    (r"(?:^|/|\.)(bilet|ebilet)\.pl", "eBilet"),
    (r"(?:^|/|\.)(bilet)\.ro", "Bilete.ro"),
]

_VENDOR_REGEX = [(re.compile(p, re.I), v) for p, v in _VENDOR_PATTERNS]

# Textual clues (HTML text/scripts) when no outbound URL match is found
_TEXT_CLUES = [
    (re.compile(r"\bTicketmaster\b", re.I), "Ticketmaster"),
    (re.compile(r"\bEventbrite\b", re.I), "Eventbrite"),
    (re.compile(r"\bSee\s*Tickets\b", re.I), "See Tickets"),
    (re.compile(r"\bWeezevent\b", re.I), "Weezevent"),
    (re.compile(r"\bBilletweb\b", re.I), "Billetweb"),
    (re.compile(r"\bFever\b", re.I), "Fever"),
    (re.compile(r"\bDICE\b", re.I), "DICE"),
    (re.compile(r"\bTicket\s*Tailor\b", re.I), "Ticket Tailor"),
    (re.compile(r"\bEventim\b", re.I), "Eventim"),
    (re.compile(r"\bTicketek\b", re.I), "Ticketek"),
    (re.compile(r"\bUniverse\b", re.I), "Ticketmaster"),  # Universe is Ticketmaster
]

def _match_vendor_from_url(url: str) -> Optional[str]:
    for rx, vendor in _VENDOR_REGEX:
        if rx.search(url):
            return vendor
    return None

def detect_vendor(domain: str, html: str, all_links: list[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (vendor_name, evidence_url) if we can see a vendor, else (None, None).
    - domain: the venue's own domain (may or may not be a vendor)
    - html: full HTML string of the page we fetched
    - all_links: hrefs (absolute or relative) we saw on the page
    """
    # 1) If the venue *is* on a vendor domain (subpages or store pages)
    if domain:
        v = _match_vendor_from_url(domain)
        if v:
            return v, f"https://{domain}"

    # 2) Look at all outbound links
    for href in all_links:
        v = _match_vendor_from_url(href)
        if v:
            return v, href

    # 3) Fall back to text clues in the HTML
    if html:
        for rx, v in _TEXT_CLUES:
            if rx.search(html):
                return v, None

    return None, None
