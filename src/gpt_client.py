# src/gpt_client.py
import json
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple

from openai import OpenAI
from .model_router import QuotaAwareRouter


@dataclass
class GPTQuotaExceeded(Exception):
    """Raised when all models are exhausted due to rate limits."""
    message: str = "OpenAI rate limit hit"


def _to_decimal_safe(x: Any) -> Decimal | None:
    if x is None or x == "":
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _to_int_safe(x: Any) -> int | None:
    try:
        if x is None or x == "":
            return None
        return int(str(x).strip())
    except Exception:
        return None


def _to_float01_safe(x: Any) -> float | None:
    try:
        if x is None or x == "":
            return None
        v = float(str(x).strip())
        if v < 0:
            v = 0.0
        if v > 1:
            v = 1.0
        return v
    except Exception:
        return None


def _build_messages(context: Dict[str, Any]) -> List[Dict[str, str]]:
    # Keep it compact to minimize tokens.
    sys = (
        "You enrich venue/performing-arts org records. "
        "Return strict JSON with keys:\n"
        "- avg_ticket_price (number or null)\n"
        "- capacity (integer or null)\n"
        "- events_per_year (integer or null)  # approximate typical annual frequency\n"
        "- occupancy (number 0..1 or null)    # typical load factor\n"
        "If unknown, use null. Do not invent URLs. Keep numbers realistic."
    )
    parts = []
    for key in ("name", "alt_name", "website_url", "phone", "city", "state", "country"):
        if context.get(key):
            parts.append(f"{key}: {context.get(key)}")
    if context.get("website_text"):
        txt = str(context["website_text"])
        if len(txt) > 4000:
            txt = txt[:4000]
        parts.append(f"website_text:\n{txt}")
    if context.get("description"):
        parts.append(f"description:\n{context['description']}")
    user = " \n".join(parts) if parts else "No context."

    return [
        {"role": "system", "content": sys},
        {"role": "user", "content": user},
    ]


class GPTClient:
    def __init__(self, client: OpenAI | None = None):
        self.client = client or OpenAI()
        self.router = QuotaAwareRouter(self.client)

    def enrich(self, context: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Returns (update_dict, model_used).
        update_dict has keys:
          - avg_ticket_price (Decimal|None), capacity (int|None),
          - events_per_year (int|None), occupancy (float|None in 0..1)
          - *_source fields for price/capacity if set by GPT
          - enrichment_status ("DONE")
        """
        messages = _build_messages(context)
        try:
            resp, used_model = self.router.chat(
                messages=messages,
                response_format={"type": "json_object"},
                temperature=0,
                max_tokens=220,
            )
        except Exception as e:
            raise GPTQuotaExceeded(str(e))

        text = resp.choices[0].message.content or "{}"
        try:
            data = json.loads(text)
        except Exception:
            logging.warning("Non-JSON response from model; falling back to empty object")
            data = {}

        price = _to_decimal_safe(data.get("avg_ticket_price"))
        cap = _to_int_safe(data.get("capacity"))
        events = _to_int_safe(data.get("events_per_year"))
        occ = _to_float01_safe(data.get("occupancy"))

        update: Dict[str, Any] = {
            "avg_ticket_price": price,
            "avg_ticket_price_source": ("gpt" if price is not None else None),
            "capacity": cap,
            "capacity_source": ("gpt" if cap is not None else None),
            "events_per_year": events,
            "occupancy": occ,
            "enrichment_status": "DONE",
        }
        return update, used_model


# Backwards-compatible shim
def enrich_with_gpt(*args, **kwargs) -> Dict[str, Any]:
    """
    Accepts either:
      - enrich_with_gpt(row_dict=..., **extras)
      - enrich_with_gpt(context_dict)
      - enrich_with_gpt(**context_fields)
    Returns `update` dict only (for existing call sites).
    """
    if args and isinstance(args[0], dict) and not kwargs:
        context: Dict[str, Any] = args[0]
    else:
        context = {}
        if "row_dict" in kwargs and isinstance(kwargs["row_dict"], dict):
            context.update(kwargs.pop("row_dict"))
        context.update(kwargs)

    client = GPTClient()
    update, used_model = client.enrich(context)
    name = context.get("name") or context.get("alt_name") or context.get("website_url") or "row"
    updated_keys = [k for k, v in update.items() if v is not None]
    logging.info(f"APPLY UPDATE for {name} -> {updated_keys} (model={used_model})")
    return update
