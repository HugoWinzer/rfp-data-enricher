import os
import json
import logging
from typing import Dict, Any, Optional

from openai import OpenAI

log = logging.getLogger("enricher")

# You can override the model via env if needed
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_client = OpenAI()


def _to_int(x) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        if isinstance(x, str):
            # keep digits only (tolerate "1,200 seats")
            s = "".join(ch for ch in x if ch.isdigit())
            if not s:
                return None
            x = int(s)
        return int(x)
    except Exception:
        return None


def _to_float(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        if isinstance(x, str):
            # keep digits and punctuation, normalize comma to dot
            s = "".join(ch for ch in x if ch.isdigit() or ch in ".,").replace(",", ".")
            if not s or s.count(".") > 1:
                return None
            x = float(s)
        return float(x)
    except Exception:
        return None


def _extract_output_text(resp) -> str:
    """Best-effort text extraction that works across minor SDK variations."""
    try:
        t = getattr(resp, "output_text", None)
        if t:
            return t
    except Exception:
        pass
    try:
        # Fallback path
        outputs = getattr(resp, "output", None) or getattr(resp, "outputs", None)
        if outputs:
            first = outputs[0]
            content = getattr(first, "content", None)
            if content and len(content) > 0 and hasattr(content[0], "text"):
                return content[0].text
    except Exception:
        pass
    return ""


def enrich_with_gpt(raw: Dict[str, Any], web_context: Optional[str] = None) -> Dict[str, Any]:
    """
    Ask the model to fill:
      - ticket_vendor: string|null
      - capacity: integer|null
      - avg_ticket_price: number|null
    Only fill when confident; otherwise return nulls.
    """
    name = (raw.get("name") or "").strip()
    domain = (raw.get("domain") or "").strip()
    city = (raw.get("city") or "").strip()
    country = (raw.get("country") or "").strip()
    ctx = (web_context or "").strip()

    system = (
        "You enrich performing arts venue data. "
        "Only fill a field if you are highly confident; otherwise return null. "
        "Do not invent data. If the website text suggests a range, return the most typical value."
    )

    # Strict JSON schema to make downstream parsing robust
    schema = {
        "name": "venue_enrichment",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "ticket_vendor": {"type": ["string", "null"]},
                "capacity": {"type": ["integer", "null"]},
                "avg_ticket_price": {"type": ["number", "null"]},
            },
            "required": ["ticket_vendor", "capacity", "avg_ticket_price"],
            "additionalProperties": False,
        },
    }

    user = (
        "Fill these fields for the venue record only if confident; else return nulls.\n\n"
        f"Record:\n"
        f"- name: {name}\n"
        f"- domain: {domain}\n"
        f"- city: {city}\n"
        f"- country: {country}\n\n"
        f"Website text (may be empty):\n{ctx[:8000]}"
    )

    try:
        resp = _client.responses.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_schema", "json_schema": schema},
            input=[
                {"role": "system", "content": [{"type": "text", "text": system}]},
                {"role": "user", "content": [{"type": "text", "text": user}]},
            ],
            max_output_tokens=300,
        )
        text = _extract_output_text(resp)
        data = json.loads(text or "{}")
    except Exception as e:
        log.warning("GPT enrichment failed for '%s': %s", name, e)
        return {}

    out: Dict[str, Any] = {}

    # ticket_vendor
    tv = (data.get("ticket_vendor") or "").strip() if isinstance(data.get("ticket_vendor"), str) else None
    if tv:
        out["ticket_vendor"] = tv[:64]

    # capacity
    cap = _to_int(data.get("capacity"))
    if cap and cap > 0:
        out["capacity"] = cap

    # avg_ticket_price
    price = _to_float(data.get("avg_ticket_price"))
    if price and price > 0:
        out["avg_ticket_price"] = round(price, 2)

    return out
