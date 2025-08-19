# src/gpt_client.py
import json
import re
from typing import Dict, Any, Optional
from openai import OpenAI

client = OpenAI()  # reads OPENAI_API_KEY from env

SYSTEM_PROMPT = (
    "You are a data enrichment helper. "
    "Given an arts organization name, return ONLY a compact JSON object with any of the following keys if confidently known: "
    "avg_ticket_price (number), capacity (integer), ticket_vendor (string). "
    "If a value is unknown, omit that key. No prose, no markdown—just a JSON object."
)

def _extract_first_json_block(text: str) -> Optional[str]:
    """Find the first {...} block—tolerant of extra text."""
    m = re.search(r"\{.*\}", text, re.S)
    return m.group(0) if m else None

def call_gpt(name: str, city: Optional[str] = None, country: Optional[str] = None) -> Dict[str, Any]:
    user = f"Organization: {name}"
    if city:
        user += f"\nCity: {city}"
    if country:
        user += f"\nCountry: {country}"

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
    )

    text = resp.choices[0].message.content or ""
    # Try to parse JSON strictly, else try to carve the first JSON block.
    try:
        return json.loads(text)
    except Exception:
        block = _extract_first_json_block(text)
        if not block:
            return {}
        try:
            return json.loads(block)
        except Exception:
            return {}
