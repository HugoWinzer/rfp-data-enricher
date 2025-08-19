# src/gpt_client.py
import os, json, logging
from typing import Dict, Any, Optional

from openai import OpenAI

log = logging.getLogger("enricher.gpt")
_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

_client: Optional[OpenAI] = None
def _client_once() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()
    return _client

def enrich_with_gpt(row: Dict[str, Any], web_context: str = "") -> Dict[str, Any]:
    """
    Ask GPT for (avg_ticket_price: number, capacity: integer, ticket_vendor: string|empty)
    Always returns a dict; values may be None if unknown.
    """
    name = (row or {}).get("name", "")
    domain = (row or {}).get("domain", "") or ""
    prompt = f"""
You enrich sparse performing-arts org records.

Return STRICT JSON with keys:
- "avg_ticket_price": a single number in local currency if you can infer it, else null
- "capacity": integer typical audience capacity if you can infer it, else null
- "ticket_vendor": one of ["Ticketmaster","Eventbrite","See Tickets","Dice","Universe","Local Box Office","Other",""] — empty string if unknown

Context (may be partial or noisy):
- name: {name}
- domain: {domain}
- website_text: {web_context[:3000]}
"""

    try:
        resp = _client_once().chat.completions.create(
            model=_MODEL,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a careful data enricher. Only output valid JSON."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = resp.choices[0].message.content or "{}"
        data = json.loads(raw)
    except Exception as e:
        log.warning("GPT call failed: %s", str(e))
        return {}

    out: Dict[str, Any] = {}
    # avg_ticket_price
    try:
        v = data.get("avg_ticket_price", None)
        if isinstance(v, (int, float)) and v > 0:
            out["avg_ticket_price"] = float(v)
    except Exception:
        pass
    # capacity
    try:
        c = data.get("capacity", None)
        if isinstance(c, (int, float)) and int(c) > 0:
            out["capacity"] = int(c)
    except Exception:
        pass
    # vendor
    tv = data.get("ticket_vendor")
    if isinstance(tv, str):
        tv = tv.strip()
        if tv:
            out["ticket_vendor"] = tv

    return out
