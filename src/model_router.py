# src/model_router.py
import os, time, logging, random
from typing import Tuple, Dict, Any, List
from openai import OpenAI
from openai import RateLimitError, APIStatusError




def _parse_model_list(primary: str, fallbacks: str) -> List[str]:
order = [m.strip() for m in [primary, *fallbacks.split(",")] if m and m.strip()]
seen, out = set(), []
for m in order:
if m not in seen:
seen.add(m)
out.append(m)
return out




class QuotaAwareRouter:
"""
Simple router:
- Try primary model first.
- On 429 / rate limit headers, mark that model cooling down and try the next.
- Respects Retry-After / x-ratelimit-reset-requests when provided.
"""


def __init__(self, client: OpenAI | None = None):
self.client = client or OpenAI()
primary = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
fallbacks = os.getenv("OPENAI_MODEL_FALLBACKS", "gpt-4o-mini,gpt-5-mini")
self.models = _parse_model_list(primary, fallbacks)


# cooldown timestamp per model (epoch seconds)
self.cooldown_until: Dict[str, float] = {m: 0.0 for m in self.models}


# hard caps
self.max_global_sleep_s = float(os.getenv("ROUTER_MAX_GLOBAL_SLEEP_S", "30"))
self.per_request_timeout_s = float(os.getenv("OPENAI_TIMEOUT_S", "120"))


def _apply_retry_after(self, model: str, headers: Dict[str, str]) -> None:
h = {k.lower(): v for k, v in (headers or {}).items()}
wait_s = 0.0
if "retry-after" in h:
try:
wait_s = float(h["retry-after"]) # seconds
except Exception:
pass
elif "x-ratelimit-reset-requests" in h:
val = h["x-ratelimit-reset-requests"]
try:
if val.endswith("s") and val[:-1].replace(".", "", 1).isdigit():
wait_s = float(val[:-1])
elif val.endswith("ms") and val[:-2].isdigit():
wait_s = float(val[:-2]) / 1000.0
else:
seconds = 0.0
num = ""
for ch in val:
if ch.isdigit() or ch == ".":
num += ch
elif ch == "h" and num:
seconds += float(num) * 3600; num = ""
elif ch == "m" and num:
raise last_err or RuntimeError("All models exhausted due to rate limits")
