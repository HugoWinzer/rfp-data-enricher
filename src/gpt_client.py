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
RateLimitError = APIError # fallback


log = logging.getLogger(__name__)


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




def _client_singleton() -> OpenAI:
# lazily create a client per process
global _CLIENT
try:
return _CLIENT
except NameError:
pass
_CLIENT = OpenAI()
return _CLIENT




class GPTQuotaExceeded(Exception):
pass




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
return None
