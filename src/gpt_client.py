# src/gpt_client.py
import json
import os
from typing import Any, Dict, Optional

from openai import OpenAI

# Singleton OpenAI client; honors OPENAI_MAX_RETRIES and OPENAI_TIMEOUT
_client: Optional[OpenAI] = None

def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(
            max_retries=int(os.getenv("OPENAI_MAX_RETRIES", "5")),
            timeout=float(os.getenv("OPENAI_TIMEOUT", "30")),
        )
    return _client

def _parse_json(text: str) -> Dict[str, Any]:
    try:
        data = json.loads(text or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def enrich_with_gpt(row: Dict[str, Any], model: str) -> Dict[str, Any]:
    """
    Minimal wrapper around Chat Completions (OpenAI v1).
    Returns only the fields we care about if present.
    """
    client = _get_client()
    messages = [
        {"role": "system", "content": (
            "You enrich venue/company rows for performing arts. "
            "If you cannot determine a field, omit it."
        )},
        {"role": "user", "content": (
            "Given this row JSON, fill any missing fields: "
            "ticket_vendor (string), capacity (integer), avg_ticket_price (number). "
            "Return strict JSON with only known keys. Row: " + json.dumps(row, ensure_ascii=False)
        )},
    ]

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.2,
    )
    content = resp.choices[0].message.content or ""
    data = _parse_json(content)

    out: Dict[str, Any] = {}
    if not isinstance(data, dict):
        return out

    tv = data.get("ticket_vendor")
    cap = data.get("capacity")
    price = data.get("avg_ticket_price")

    if tv not in (None, ""):
        out["ticket_vendor"] = str(tv).strip()

    try:
        if cap not in (None, ""):
            out["capacity"] = int(cap)
    except Exception:
        pass

    # Keep as-is; caller converts to Decimal safely.
    try:
        if price not in (None, ""):
            out["avg_ticket_price"] = float(price)
    except Exception:
        pass

    return out
