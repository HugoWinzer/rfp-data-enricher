# src/gpt_client.py
import json
import os
from typing import Any, Dict

from openai import OpenAI

_client = None

def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()  # uses OPENAI_API_KEY from env
    return _client

def _parse_json(text: str) -> Dict[str, Any]:
    """
    Try to parse a JSON object out of the model output.
    Accepts bare JSON or a fenced code block.
    """
    if not text:
        return {}
    text = text.strip()

    # Extract JSON from ```json ... ``` if present
    if "```" in text:
        parts = text.split("```")
        for p in parts:
            pt = p.strip()
            if pt.lower().startswith("json"):
                pt = pt[4:].strip()
            if pt.startswith("{") and pt.endswith("}"):
                try:
                    return json.loads(pt)
                except Exception:
                    pass

    # Fall back to direct parse
    if text.startswith("{") and text.endswith("}"):
        try:
            return json.loads(text)
        except Exception:
            pass

    return {}

def enrich_with_gpt(row: Dict[str, Any], web_context: str = "") -> Dict[str, Any]:
    """
    Ask the model to fill any of:
      - ticket_vendor (str or null)
      - capacity (int or null)
      - avg_ticket_price (float or null)

    Returns a dict with some/all of those keys.
    """
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Single-string prompt works well with the Responses API
    prompt = f"""
You are a precise data enricher for performing arts venues.
Given a row of metadata and (optional) scraped site text, infer:
- ticket_vendor: standard vendor name if confidently present, else null
- capacity: integer if confidently present, else null
- avg_ticket_price: typical single-ticket price in local currency as a float if confidently present, else null

Return ONLY compact JSON with exactly these keys:
{{
  "ticket_vendor": <string or null>,
  "capacity": <int or null>,
  "avg_ticket_price": <float or null>
}}

Row JSON:
{json.dumps(row, ensure_ascii=False, indent=2)}

Scraped site text (may be empty):
{web_context[:6000]}
"""

    client = _get_client()
    resp = client.responses.create(model=model, input=prompt)

    text = getattr(resp, "output_text", None)
    if not text and getattr(resp, "output", None):
        try:
            # Older SDK objects may expose a list of content parts
            text = resp.output[0].content[0].text
        except Exception:
            text = None

    data = _parse_json(text or "")

    out: Dict[str, Any] = {}
    if isinstance(data, dict):
        tv = data.get("ticket_vendor")
        cap = data.get("capacity")
        price = data.get("avg_ticket_price")

        if isinstance(tv, str) and tv.strip():
            out["ticket_vendor"] = tv.strip()

        # capacity → int
        try:
            if cap is not None:
                out["capacity"] = int(cap)
        except Exception:
            pass

        # avg_ticket_price → float
        try:
            if price is not None:
                out["avg_ticket_price"] = float(price)
        except Exception:
            pass

    return out
