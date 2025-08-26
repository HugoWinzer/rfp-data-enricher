#!/usr/bin/env python3
import os
import json
import math
import logging
from typing import Any, Dict, Optional


from openai import OpenAI
from openai import APIError
try:
from openai import RateLimitError
except Exception:
RateLimitError = APIError


log = logging.getLogger(__name__)




def _env_int(name: str, default: int) -> int:
try:
return int(os.getenv(name, str(default)))
except Exception:
return default


CAPACITY_MIN = _env_int("CAPACITY_MIN", 30)
CAPACITY_MAX = _env_int("CAPACITY_MAX", 20000)
PRICE_MIN = _env_int("PRICE_MIN", 5)
PRICE_MAX = _env_int("PRICE_MAX", 250)
REV_MAX = float(os.getenv("REV_MAX", "2000000000")) # 2B
PCT_MAX = float(os.getenv("PCT_MAX", "25"))




class GPTQuotaExceeded(Exception):
pass




def _client_singleton() -> OpenAI:
global _CLIENT
try:
return _CLIENT
except NameError:
_CLIENT = OpenAI()
return _CLIENT




def _clamp_num(n: Optional[float], lo: float, hi: float) -> Optional[float]:
if n is None:
return None
try:
f = float(n)
except Exception:
return None
if math.isnan(f) or math.isinf(f):
return None
return max(lo, min(hi, f))




def _build_prompt(name: str, row: Dict[str, Any]) -> str:
domain = row.get("domain") or ""
category = row.get("category") or ""
return f"""
You enrich cultural venue records. If data is not public, estimate plausibly.


Definitions:
- ticket vendor = payment-funnel software powering checkout (Ticketmaster, Eventbrite, Fever, Spektrix, Tessitura, Universe, See Tickets, Pretix, etc.). Not a marketplace/aggregator.
- size tiers by annual_revenue: Diamond ≥ 20M; Gold 4–<20M; Silver 2–<4M; Bronze < 2M.
- charge_pct = % commission per ticket (typical 2–10%%; clamp 0–25%%).
- rfp = entity has ever issued a Request for Proposal.


Always output ALL fields as JSON (no comments):
return None
