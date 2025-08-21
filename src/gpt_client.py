import os
import json
from typing import Optional, Dict, Any

from openai import OpenAI

_client: Optional[OpenAI] = None


def _client_or_none() -> Optional[OpenAI]:
    """Avoid crashing if OPENAI_API_KEY is not set."""
    global _client
    if _client is not None:
        return _client
    if not os.getenv("OPENAI_API_KEY"):
        return None
    _client = OpenAI()
    return _client


SYS = (
    "You are a careful data extractor. "
    "Given a performing-arts organization, extract: "
    "ticket_vendor (string like Ticketmaster, Eventbrite, Universe, SeeTickets, etc.), "
    "capacity (integer), and avg_ticket_price (decimal in local currency). "
    "If unsure, leave fields null. Output strict JSON with keys "
    '["ticket_vendor","capacity","avg_ticket_price"].'
)


def enrich_with_gpt(*, name: str, row: Dict[str, Any], model: str = "gpt-4o-mini") -> Optional[Dict[str, Any]]:
    """
    Ask GPT for structured hints. Safe to call with missing API key – returns None.
    Why: GPT is useful for capacity and as a backfill when scraping fails.
    """
    client = _client_or_none()
    if client is None:
        return None

    website = row.get("website") or row.get("url") or ""
    city = row.get("city") or row.get("town") or ""
    country = row.get("country") or ""
    desc = f"Name: {name}\nWebsite: {website}\nCity: {city}\nCountry: {country}"

    msg = [
        {"role": "system", "content": SYS},
        {"role": "user", "content": f"Extract fields for:\n{desc}\nReturn JSON only."},
    ]

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=msg,
            temperature=0.0,
            response_format={"type": "json_object"},
            timeout=int(os.getenv("OPENAI_TIMEOUT", "30")),
        )
        text = resp.choices[0].message.content.strip()
    except Exception:
        # Fallback (future-proof)
        resp = client.responses.create(
            model=model,
            input=msg,
            temperature=0.0,
            response_format={"type": "json_object"},
            timeout=int(os.getenv("OPENAI_TIMEOUT", "30")),
        )
        text = (getattr(resp, "output_text", "") or "").strip()

    try:
        data = json.loads(text)
    except Exception:
        return None

    out: Dict[str, Any] = {}
    tv = data.get("ticket_vendor")
    if isinstance(tv, str) and tv.strip():
        out["ticket_vendor"] = tv.strip()

    cap = data.get("capacity")
    if isinstance(cap, (int, float, str)):
        try:
            out["capacity"] = int(float(cap))
        except Exception:
            pass

    price = data.get("avg_ticket_price")
    if isinstance(price, (int, float, str)):
        try:
            out["avg_ticket_price"] = str(price)
        except Exception:
            pass

    return out or None
