import os, time
from dataclasses import dataclass


class QuotaExhaustedError(Exception):
"""Raised when we intentionally stop because the daily limit is reached."""
pass


@dataclass
class Limits:
rpm: int | None
tpm: int | None
rpd: int | None
stop_on_daily: bool


class RateLimiter:
"""
Lightweight RPM/TPM/RPD gate. Unused by default; wire into GPT client if you
want client-side throttling in addition to server-side 429 handling.
"""
def __init__(self):
def _read_int(name: str, default: int | None) -> int | None:
try:
i = int(os.getenv(name, "0"))
return i if i > 0 else None
except Exception:
return None


self.limits = Limits(
rpm=_read_int("OPENAI_RPM", 0),
tpm=_read_int("OPENAI_TPM", 0),
rpd=_read_int("OPENAI_RPD", 0),
stop_on_daily=os.getenv("STOP_ON_GPT_QUOTA", "0") == "1",
)


now = time.time()
self._minute_start = now
self._minute_tokens += max(0, used_input_tokens) + max(0, used_output_tokens)
