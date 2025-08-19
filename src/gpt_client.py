# src/gpt_client.py
import json
import os
import re
from typing import Dict, Any, Optional

from openai import OpenAI

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_client: Optional[OpenAI] = None

def _client_instance() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()  # reads OPENAI_API_KEY from env
    return _client

SYSTEM_PROMPT = (
    "You are a data enrichment helper. "
    "Given an arts organization, return ONLY a compact JSON object with any of the following keys if confidently known: "
    "avg_ticket_price (number), capacity (integer), ticket_vendor (string). "
    "If a value is unknown, omit that key. No prose—just a JSON object."
)

def _first_json_block(text: str) -> Optional[str]:
    m = re.search(r"\{.*\}", text, re.S)
    return m.group(0) if m else None

def enrich_with_gpt(row: Dict[str, Any], web_context: str = "") -> Dict[str, Any]:
    """Call GPT; return dict of possibly-known fields (omit unknowns)."""
    user = (
        f"Organization: {row.get('name')}\n"
        f"Alt name: {row.get('alt_name')}\n"
        f"Category: {row.get('category')}\n"
        f"Domain: {row.get('domain')}\n"
        f"Phone: {row.get('phone_number') or ''}\n"
        f"Description: {(row.get('short_description') or '')} {(row.get('full_description') or '')}\n"
        f"Website context (may be noisy): {web_context[:2000]}"
    )
    resp = _client_instance().chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
    )
    text = resp.choices[0].message.content or ""
    try:
        return json.loads(text)
    except Exception:
        block = _first_json_block(text)
        if not block:
            return {}
        try:
            return json.loads(block)
        except Exception:
            return {}
