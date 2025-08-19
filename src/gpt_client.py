import os, json, logging
from typing import Dict, Any, Optional
from openai import OpenAI

log = logging.getLogger("enricher")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_client = OpenAI()

def _to_int(x) -> Optional[int]:
    try:
        if x is None or x == "": return None
        if isinstance(x, str):
            s = "".join(ch for ch in x if ch.isdigit())
            if not s: return None
            x = int(s)
        return int(x)
    except Exception:
        return None

def _to_float(x) -> Optional[float]:
    try:
        if x is None or x == "": return None
        if isinstance(x, str):
            s = "".join(ch for ch in x if ch.isdigit() or ch in ".,").replace(",", ".")
            if not s or s.count(".") > 1: return None
            x = float(s)
        return float(x)
    except Exception:
        return None

def enrich_with_gpt(raw: Dict[str, Any], web_context: Optional[str] = None) -> Dict[str, Any]:
    name = (raw.get("name") or "").strip()
    domain = (raw.get("domain") or "").strip()
    city = (raw.get("city") or "").strip()
    country = (raw.get("country") or "").strip()
    ctx = (web_context or "").strip()[:8000]

    system = (
        "You enrich venue data. Fill fields only if highly confident; else null.\n"
        "Fields: ticket_vendor (string|null), capacity (int|null), avg_ticket_price (number|null)."
    )
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
        f"Record:\n- name: {name}\n- domain: {domain}\n- city: {city}\n- country: {country}\n\n"
        f"Website text:\n{ctx}"
    )

    try:
        r = _client.responses.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_schema", "json_schema": schema},
            input=[
                {"role": "system", "content": [{"type":"text","text": system}]},
                {"role": "user",   "content": [{"type":"text","text": user}]},
            ],
            max_output_tokens=300,
        )
        data = json.loads(r.output_text or "{}")
    except Exception as e:
        log.warning("GPT fail for '%s': %s", name, e)
        return {}

    out: Dict[str, Any] = {}
    tv = (data.get("ticket_vendor") or "").strip()
    if tv: out["ticket_vendor"] = tv[:64]
    cap = _to_int(data.get("capacity"))
    if cap and cap > 0: out["capacity"] = cap
    price = _to_float(data.get("avg_ticket_price"))
    if price and price > 0: out["avg_ticket_price"] = round(price, 2)
    return out
