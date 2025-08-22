import os
import json
import logging
from typing import Dict, Any, Optional

from openai import OpenAI

log = logging.getLogger("gpt_client")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

def _client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set (env var or Secret Manager).")
    return OpenAI(api_key=api_key)

SYSTEM_INSTRUCTIONS = """
You are a data enricher for a performing arts organizations database.

CRUCIAL DEFINITIONS:
- "ticket_vendor": the software/payment platform that powers the checkout funnel
  for the organization’s tickets (e.g., Ticketmaster, Eventbrite, See Tickets,
  Fever, Eventix, Universe, Spektrix, Pretix, Weezevent, Ticket Tailor,
  YoYo, etc.). It is NOT an aggregator/search site.

TASK:
Given the org name, website and some scraped text, you must ALWAYS produce:
- ticket_vendor: string (best guess; pick the actual payment platform; never empty)
- capacity: integer (estimated if unknown; a plausible venue capacity)
- avg_ticket_price: number (typical per-ticket price in local currency; estimate if unknown)

CONSTRAINTS:
- Never return empty values. If uncertain, pick the most plausible guess based on the text.
- Avoid search/aggregator brands as vendor. Prefer embedded checkout providers/platforms.
- Return pure JSON with keys exactly: ticket_vendor, capacity, avg_ticket_price.
"""

def enrich_with_gpt(name: str, site: str, scraped_text: str, model: Optional[str] = None) -> Dict[str, Any]:
    """
    Ask GPT to ALWAYS fill vendor, capacity, avg_ticket_price.
    Returns a dict with those keys. Values may be guesses.
    """
    model = model or OPENAI_MODEL
    client = _client()

    user_prompt = f"""
Organization name: {name}
Website: {site}

SCRAPED_TEXT (may be partial or noisy):
{scraped_text[:6000]}  # keep token usage reasonable

Return JSON only.
"""

    resp = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_INSTRUCTIONS.strip()},
            {"role": "user", "content": user_prompt.strip()},
        ],
        temperature=0.3,
    )

    content = resp.choices[0].message.content
    try:
        data = json.loads(content)
    except Exception:
        log.warning("GPT returned non-JSON; content=%r", content)
        data = {}

    # Normalize shapes
    tv = data.get("ticket_vendor")
    cap = data.get("capacity")
    price = data.get("avg_ticket_price")

    return {
        "ticket_vendor": tv if isinstance(tv, str) and tv.strip() else None,
        "capacity": cap if isinstance(cap, int) else None,
        "avg_ticket_price": price,
    }
