#!/usr/bin/env python3
import os
import json
import math
import logging
from typing import Any, Dict, Optional

from openai import OpenAI
from openai import APIError
try:
    # Available in openai>=1.x
    from openai import RateLimitError
except Exception:
    RateLimitError = APIError  # fallback

log = logging.getLogger(__name__)
_client: Optional[OpenAI] = None


class GPTQuotaExceeded(Exception):
    """Raised when the OpenAI API indicates a rate/quota limit (HTTP 429)."""
    pass


def _client_singleton() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()  # reads OPENAI_API_KEY from env (in Secret Manager)
    return _client


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


CAPACITY_MIN = _env_int("CAPACITY_MIN", 50)
CAPACITY_MAX = _env_int("CAPACITY_MAX", 1200)
PRICE_MIN = _env_int("PRICE_MIN", 10)
PRICE_MAX = _env_int("PRICE_MAX", 120)


def _clamp(n: Optional[float], lo: float, hi: float) -> Optional[float]:
    if n is None or (isinstance(n, float) and (math.isnan(n) or math.isinf(n))):
        return None
    try:
        n = float(n)
    except Exception:
        return None
    return max(lo, min(hi, n))


def _log_rate_limit_headers(err: Exception, where: str = "openai"):
    """
    Logs key rate-limit headers when a 429 happens so you know when to restart.
    Works with OpenAI v1 exceptions that expose `response.headers` (httpx style).
    """
    headers = {}
    resp = getattr(err, "response", None)
    if resp is not None:
        try:
            headers = dict(resp.headers or {})
        except Exception:
            headers = {}

    def h(name: str):
        lname = name.lower()
        for k, v in headers.items():
            if k.lower() == lname:
                return v
        return None

    log.error(
        "[429 %s] Retry-After=%s | "
        "x-ratelimit-remaining-requests=%s x-ratelimit-remaining-tokens=%s | "
        "x-ratelimit-reset-requests=%s x-ratelimit-reset-tokens=%s | "
        "x-ratelimit-limit-requests=%s x-ratelimit-limit-tokens=%s",
        where,
        h("Retry-After") or h("retry-after"),
        h("x-ratelimit-remaining-requests"), h("x-ratelimit-remaining-tokens"),
        h("x-ratelimit-reset-requests"),     h("x-ratelimit-reset-tokens"),
        h("x-ratelimit-limit-requests"),     h("x-ratelimit-limit-tokens"),
    )


def _build_prompt(name: str, row: Dict[str, Any]) -> str:
    """
    We instruct GPT to ALWAYS return JSON with *all* fields filled (estimates OK).
    """
    domain = row.get("domain") or row.get("website") or row.get("url") or ""
    country = row.get("country") or row.get("country_code") or ""
    city = row.get("city") or row.get("location") or ""

    return f"""
You are enriching a performing arts venue dataset.

Definitions:
- "ticket_vendor" = the **software company that handles the ticketing/payment funnel** (e.g., Ticketmaster, Fever, Eventbrite, Universe, Ticket Tailor, See Tickets, etc.). Not a local reseller; the underlying software used on the venue's site.
- "capacity" = best-guess seated/standing capacity of the venue (not city capacity).
- "avg_ticket_price" = typical average ticket price for standard events at the venue in local currency (if unknown, estimate a reasonable typical price).

Rules:
- You MUST return a JSON object with **all** keys present: ticket_vendor, capacity, avg_ticket_price.
- If unsure, make a **reasonable estimate** (do not leave nulls).
- Keep capacity within [{CAPACITY_MIN}, {CAPACITY_MAX}] and avg_ticket_price within [{PRICE_MIN}, {PRICE_MAX}] unless you are highly confident it should exceed bounds; otherwise clamp.
- If you infer the vendor from common providers used in the region/vertical, that's ok—state it confidently.

Context:
- name: "{name}"
- domain: "{domain}"
- city: "{city}"
- country: "{country}"

Return ONLY valid JSON like:
{{
  "ticket_vendor": "Fever",
  "capacity": 450,
  "avg_ticket_price": 28
}}
""".strip()


def _parse_json_object(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        # Occasionally models wrap with ```json fences; attempt to extract
        t = text.strip()
        if t.startswith("```"):
            t = t.strip("`")
            if t.lower().startswith("json"):
                t = t[4:].strip()
        return json.loads(t)


def enrich_with_gpt(name: str, row: Dict[str, Any], model: str = "gpt-4o-mini") -> Optional[Dict[str, Any]]:
    """
    Ask GPT for all three fields, force estimates, and clamp to env ranges.
    Returns dict with keys: ticket_vendor, capacity, avg_ticket_price
    May raise GPTQuotaExceeded on 429.
    """
    prompt = _build_prompt(name, row)
    client = _client_singleton()

    try:
        # Use Chat Completions with JSON response_format for broad model support
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a precise data enricher that outputs strict JSON."},
                {"role": "user", "content": prompt},
            ],
            timeout=30,
        )

        content = (resp.choices[0].message.content or "").strip()
        if not content:
            log.warning("Empty GPT content for %s", name)
            return None

        data = _parse_json_object(content)

        # sanitize + clamp
        vendor = (data.get("ticket_vendor") or "").strip()
        cap = _clamp(data.get("capacity"), CAPACITY_MIN, CAPACITY_MAX)
        price = _clamp(data.get("avg_ticket_price"), PRICE_MIN, PRICE_MAX)

        # If model returned out-of-bounds values and was None after clamp, use midpoints
        if cap is None:
            cap = float((CAPACITY_MIN + CAPACITY_MAX) // 2)
        if price is None:
            price = float((PRICE_MIN + PRICE_MAX) // 2)
        if not vendor:
            vendor = "Ticketmaster"  # conservative/common default if truly unknown

        # integers for capacity / price as number
        result = {
            "ticket_vendor": vendor,
            "capacity": int(round(cap)),
            "avg_ticket_price": float(round(price, 2)),
        }

        # log usage to help you estimate pace/quota
        try:
            usage = getattr(resp, "usage", None)
            if usage:
                log.info("GPT usage model=%s prompt_tokens=%s completion_tokens=%s total_tokens=%s",
                         model, usage.prompt_tokens, usage.completion_tokens, usage.total_tokens)
        except Exception:
            pass

        return result

    except RateLimitError as e:
        _log_rate_limit_headers(e, where=f"model={model}")
        raise GPTQuotaExceeded("OpenAI rate limit hit") from e

    except APIError as e:
        # Some SDK versions wrap 429 as APIError with response/status_code
        status_code = getattr(e, "status_code", None) or getattr(getattr(e, "response", None), "status_code", None)
        if status_code == 429:
            _log_rate_limit_headers(e, where=f"model={model}")
            raise GPTQuotaExceeded("OpenAI rate limit hit") from e
        log.error("OpenAI APIError: %s", e)
        raise

    except Exception as e:
        # Not a quota error; let caller handle as a soft failure
        log.warning("OpenAI general failure for %s: %s", name, e)
        return None
