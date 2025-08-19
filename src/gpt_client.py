# src/gpt_client.py
import json
from typing import Dict, Any, Optional

from openai import OpenAI

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

_client: Optional[OpenAI] = None

def _client_lazy() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()
    return _client

def enrich_with_gpt(row: Dict[str, Any], web_context: str = "") -> Dict[str, Any]:
    """
    Use OpenAI Chat Completions (>=1.0 API) to fill:
    - ticket_vendor: str | null
    - capacity: int | null
    - avg_ticket_price: number | null
    Return empty dict on failure.
    """
    name = row.get("name") or ""
    domain = row.get("domain") or ""

    system = (
        "You are a careful data enricher. "
        "Return only strict JSON with keys: ticket_vendor, capacity, avg_ticket_price. "
        "Use null for unknown."
    )

    user = f"""
    Organization: {name}
    Domain: {domain or "unknown"}
    Any scraped text (may be empty):
    ---
    {web_context or ""}
    ---

    Instructions:
    - Infer a likely ticketing vendor if the text strongly suggests one (e.g., Ticketmaster, Eventbrite, Universe, Tickets.com, etc). Else null.
    - If the venue capacity is stated or strongly implied, return an integer; else null.
    - If typical ticket prices are mentioned (or you can infer a representative single price), return a number; else null.
    Respond with pure JSON only.
    """

    try:
        resp = _client_lazy().chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0,
        )
        text = (resp.choices[0].message.content or "").strip()
        # Best-effort JSON extraction
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            text = text[start:end+1]

        data = json.loads(text)
        out: Dict[str, Any] = {
            "ticket_vendor": data.get("ticket_vendor"),
            "capacity": data.get("capacity"),
            "avg_ticket_price": data.get("avg_ticket_price"),
        }
        # Normalize bad types from model
        if out["capacity"] is not None:
            try:
                out["capacity"] = int(out["capacity"])
            except Exception:
                out["capacity"] = None
        if out["avg_ticket_price"] is not None:
            try:
                out["avg_ticket_price"] = float(out["avg_ticket_price"])
            except Exception:
                out["avg_ticket_price"] = None
        if out["ticket_vendor"]:
            out["ticket_vendor"] = str(out["ticket_vendor"]).strip()
        return out
    except Exception:
        # Silent failure -> let caller mark NO_DATA
        return {}
