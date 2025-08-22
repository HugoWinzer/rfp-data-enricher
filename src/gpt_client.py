# src/gpt_client.py
import os
import json
import logging
from typing import Any, Dict, Optional

from openai import OpenAI
from openai import (
    RateLimitError,
    PermissionDeniedError,
    APIStatusError,
    APIConnectionError,
    APIError,
)

log = logging.getLogger(__name__)

class GPTQuotaExceeded(Exception):
    """Raised when OpenAI indicates insufficient quota / hard rate limiting."""
    def __init__(self, message: str, processed_so_far: int = 0):
        super().__init__(message)
        self.processed_so_far = processed_so_far

_client: Optional[OpenAI] = None

def _client() -> OpenAI:
    global _client
    if _client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        _client = OpenAI(api_key=api_key)
    return _client

def _looks_like_quota_error(err: Exception) -> bool:
    s = str(err).lower()
    return (
        "insufficient_quota" in s
        or "exceeded your current quota" in s
        or "rate limit" in s
        or "429" in s
    )

def enrich_with_gpt(*, name: str, row: Dict[str, Any], model: str) -> Optional[Dict[str, Any]]:
    """
    Returns a dict like:
      {
        "capacity": 300,
        "ticket_vendor": "Ticketmaster",
        "avg_ticket_price": 48
      }
    or None if GPT couldn't infer non-quota reasons.
    """
    prompt = f"""
You are enriching performing-arts organizations.

Definition (important): "ticket vendor" = the **software company that handles the payment funnel** (e.g., Ticketmaster, See Tickets, Eventbrite, Fever), not the venue itself.

Given the row:
- name: {name}
- domain/url: {row.get('domain') or row.get('url') or row.get('website') or ''}

Return compact JSON with possible keys: capacity (int), ticket_vendor (string), avg_ticket_price (int).
If unsure, omit that key. Do NOT add commentary. Just JSON.
"""

    try:
        resp = _client().chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a precise data enricher."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
    except (RateLimitError, PermissionDeniedError, APIStatusError) as e:
        if _looks_like_quota_error(e):
            # Signal the app to stop the batch immediately.
            raise GPTQuotaExceeded(str(e))
        raise
    except (APIConnectionError, APIError) as e:
        # Transient/other API failures: treat as 'no suggestion' and continue
        log.warning("GPT transient error: %s", e)
        return None

    try:
        content = resp.choices[0].message.content.strip()
        # Try to extract JSON
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1:
            return None
        data = json.loads(content[start : end + 1])

        # sanitize a bit
        out: Dict[str, Any] = {}
        if "capacity" in data:
            try:
                out["capacity"] = int(data["capacity"])
            except Exception:
                pass
        if "ticket_vendor" in data and isinstance(data["ticket_vendor"], str) and data["ticket_vendor"].strip():
            out["ticket_vendor"] = data["ticket_vendor"].strip()
        if "avg_ticket_price" in data:
            try:
                out["avg_ticket_price"] = int(data["avg_ticket_price"])
            except Exception:
                pass
        return out or None
    except Exception as e:
        log.warning("Failed parsing GPT output: %s", e)
        return None
