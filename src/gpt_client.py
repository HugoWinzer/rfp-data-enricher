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
    pass


_client_singleton: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    global _client_singleton
    if _client_singleton is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        _client_singleton = OpenAI(api_key=api_key)
    return _client_singleton


def _looks_like_quota_error(err: Exception) -> bool:
    s = str(err).lower()
    return (
        "insufficient_quota" in s
        or "exceeded your current quota" in s
        or "you exceeded your current quota" in s
        or "quota" in s
        or "rate limit" in s
        or "429" in s
    )


def enrich_with_gpt(*, name: str, row: Dict[str, Any], model: str) -> Optional[Dict[str, Any]]:
    """
    Returns a dict like (any subset):
      {
        "capacity": 300,                # int
        "ticket_vendor": "Ticketmaster",# str
        "avg_ticket_price": 48          # int
      }
    or None if GPT couldn't infer (non-quota reasons).
    """
    domain = row.get("domain") or row.get("url") or row.get("website") or ""
    prompt = f"""
You are enriching performing-arts organizations.

Definition (IMPORTANT): "ticket vendor" = the **software company that handles the payment/checkout funnel** for tickets
(e.g. Ticketmaster, See Tickets, Eventbrite, Fever, AXS, Etix, Universe), not the venue itself.

Row:
- name: {name}
- domain/url: {domain}

Return ONLY compact JSON with *possible* keys: capacity (int), ticket_vendor (string), avg_ticket_price (int).
If unsure for a key, omit it. No commentary. Just JSON.
""".strip()

    try:
        resp = _get_client().chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a precise data enricher."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
    except (RateLimitError, PermissionDeniedError, APIStatusError) as e:
        if _looks_like_quota_error(e):
            raise GPTQuotaExceeded(str(e))
        raise
    except (APIConnectionError, APIError) as e:
        # Transient/other API failures: treat as 'no suggestion' and continue
        log.warning("GPT transient error: %s", e)
        return None

    try:
        content = (resp.choices[0].message.content or "").strip()
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1:
            return None
        data = json.loads(content[start : end + 1])

        out: Dict[str, Any] = {}
        if "capacity" in data:
            try:
                out["capacity"] = int(data["capacity"])
            except Exception:
                pass
        tv = data.get("ticket_vendor")
        if isinstance(tv, str) and tv.strip():
            out["ticket_vendor"] = tv.strip()
        if "avg_ticket_price" in data:
            try:
                out["avg_ticket_price"] = int(data["avg_ticket_price"])
            except Exception:
                pass

        return out or None
    except Exception as e:
        log.warning("Failed parsing GPT output: %s", e)
        return None
