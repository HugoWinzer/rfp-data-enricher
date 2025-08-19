import os, json, logging
from typing import Dict, Any, Optional
from openai import OpenAI

log = logging.getLogger("enricher")

# Allow override via env; default to cost-efficient general model
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

_client = OpenAI()  # uses OPENAI_API_KEY from env


def _coerce_int(x) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        # strings like "1,200" or "1200 seats"
        if isinstance(x, str):
            digits = "".join(ch for ch in x if ch.isdigit())
            if not digits:
                return None
            x = int(digits)
        return int(x)
    except Exception:
        return None


def _coerce_float(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        if isinstance(x, str):
            # keep digits and dot/comma, normalize comma to dot
            s = "".join(ch for ch in x if (ch.isdigit() or ch in ".,"))
            s = s.replace(",", ".")
            if s.count(".") > 1 or not s:
                return None
            x = float(s)
        return float(x)
    except Exception:
        return None


def enrich_with_gpt(raw: Dict[str, Any], web_context: Optional[str] = None) -> Dict[str, Any]:
    """
    Ask GPT to fill any of: ticket_vendor (string), capacity (int), avg_ticket_price (float).
    Must return null for unknown. No hallucinated URLs or currency conversions.
    """
    name = (raw.get("name") or "").strip()
    domain = (raw.get("domain") or "").strip()
    city = (raw.get("city") or "").strip()
    country = (raw.get("country") or "").strip()

    system_prompt = (
        "You are a careful data enricher for a performing-arts venues table.\n"
        "- Only answer fields you can infer with high confidence.\n"
        "- If unknown, output null.\n"
        "- Fields:\n"
        "  ticket_vendor: string, e.g. Ticketmaster, Eventbrite, Universe, Billetto, or null if unclear.\n"
        "  capacity: integer seat capacity for the venue (not an event), or null.\n"
        "  avg_ticket_price: typical single-ticket price (local currency numeric), or null.\n"
        "- Do NOT invent facts. Prefer hints in provided website text if present."
    )

    # Trim long website dumps so we stay within token limits
    context = (web_context or "").strip()
    if len(context) > 8000:
        context = context[:8000]

    # Strict JSON schema so we get machine-safe output
    json_schema = {
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

    # Build a compact, deterministic input
    user_payload = (
        f"Record:\n"
        f"- name: {name}\n"
        f"- domain: {domain}\n"
        f"- city: {city}\n"
        f"- country: {country}\n\n"
        f"Website text (may be empty):\n{context}"
    )

    try:
        resp = _client.responses.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_schema", "json_schema": json_schema},
            input=[
                {"role": "system", "content": [{"type": "text", "text": system_prompt}]},
                {"role": "user", "content": [{"type": "text", "text": user_payload}]},
            ],
            max_output_tokens=300,
        )
        raw_json = resp.output_text  # responses API convenience property
        data = json.loads(raw_json) if raw_json else {}
    except Exception as e:
        log.warning("GPT call failed for '%s': %s", name, e)
        return {}

    # Coerce types and sanitize
    out: Dict[str, Any] = {}

    tv = data.get("ticket_vendor")
    if isinstance(tv, str):
        tv = tv.strip()
    if tv:
        # keep it short & tidy
        out["ticket_vendor"] = tv[:64]

    cap = _coerce_int(data.get("capacity"))
    if cap and cap > 0:
        out["capacity"] = cap

    price = _coerce_float(data.get("avg_ticket_price"))
    if price and price > 0:
        out["avg_ticket_price"] = round(float(price), 2)

    return out
