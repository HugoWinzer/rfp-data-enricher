import json
import os
import random
import time
from typing import Any, Dict

from openai import OpenAI

_client = None


def _get_client() -> OpenAI:
    """Singleton OpenAI client; reads OPENAI_API_KEY from env."""
    global _client
    if _client is None:
        _client = OpenAI()
    return _client


def _extract_json(text: str) -> Dict[str, Any]:
    """
    Robustly extract a JSON object from model output.
    Accepts bare JSON or ```json ...``` fenced blocks.
    """
    if not text:
        return {}
    s = text.strip()

    # Try fenced blocks first
    if "```" in s:
        parts = s.split("```")
        for p in parts:
            t = p.strip()
            if t.lower().startswith("json"):
                t = t[4:].strip()
            if t.startswith("{") and t.endswith("}"):
                try:
                    return json.loads(t)
                except Exception:
                    continue

    # Fallback: bare JSON
    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except Exception:
            pass

    return {}


def _retry_call(fn, *, max_retries: int, base: float, cap: float):
    """
    Simple exponential backoff with jitter.
    Retries on any exception; caller controls max_retries.
    """
    attempt = 0
    while True:
        try:
            return fn()
        except Exception:
            if attempt >= max_retries:
                raise
            sleep = min(cap, base * (2 ** attempt)) * (0.5 + random.random())
            time.sleep(sleep)
            attempt += 1


def enrich_with_gpt(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ask the model to fill: ticket_vendor (str), capacity (int), avg_ticket_price (float).
    Returns a dict with any subset of these keys present.
    """
    client = _get_client()
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    max_retries = int(os.environ.get("OPENAI_MAX_RETRIES", "5"))
    timeout = float(os.environ.get("OPENAI_TIMEOUT", "30"))  # seconds
    backoff_base = float(os.environ.get("OPENAI_BACKOFF_BASE", "0.5"))
    backoff_cap = float(os.environ.get("OPENAI_BACKOFF_CAP", "8.0"))

    system = (
        "You enrich performing-arts venue/company rows. "
        "Only return JSON. If you cannot determine a field, omit it."
    )
    user = (
        "Given this row JSON, fill any missing fields: "
        "ticket_vendor (string), capacity (integer), avg_ticket_price (number). "
        "Return STRICT JSON with only known keys.\n\nRow:\n" + json.dumps(row, ensure_ascii=False)
    )

    def _op():
        return client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
            timeout=timeout,
        )

    resp = _retry_call(_op, max_retries=max_retries, base=backoff_base, cap=backoff_cap)
    content = resp.choices[0].message.content or ""
    data = _extract_json(content)
    out: Dict[str, Any] = {}

    if not isinstance(data, dict):
        return out

    tv = data.get("ticket_vendor")
    cap = data.get("capacity")
    price = data.get("avg_ticket_price")

    if isinstance(tv, str) and tv.strip():
        out["ticket_vendor"] = tv.strip()

    try:
        if cap is not None and str(cap).strip() != "":
            out["capacity"] = int(cap)
    except Exception:
        pass

    try:
        if price is not None and str(price).strip() != "":
            out["avg_ticket_price"] = float(price)
    except Exception:
        pass

    return out
