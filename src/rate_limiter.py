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
    Fixed-window limiter for:
      • RPM (requests per minute)
      • TPM (tokens per minute) – based on our own estimates/usage
      • RPD (requests per day)
    We run with max-instances=1 + concurrency=1 on Cloud Run, so in-process counters are enough.
    """
    def __init__(self):
        def _read_int(name, default):
            v = os.getenv(name, str(default)).strip()
            try:
                i = int(v)
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
        self._minute_requests = 0
        self._minute_tokens = 0

        # Day window is UTC midnight to midnight
        self._day_epoch = self._utc_midnight_epoch(now)
        self._day_requests = 0

    @staticmethod
    def _utc_midnight_epoch(ts: float) -> int:
        g = time.gmtime(ts)
        return int(time.mktime((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0, 0, 0, 0)))

    def _tick(self):
        now = time.time()
        # reset minute window
        if now - self._minute_start >= 60.0:
            self._minute_start = now
            self._minute_requests = 0
            self._minute_tokens = 0
        # reset daily window
        cur_midnight = self._utc_midnight_epoch(now)
        if cur_midnight != self._day_epoch:
            self._day_epoch = cur_midnight
            self._day_requests = 0

    def _secs_until_next_minute(self) -> float:
        return max(0.0, 60.0 - (time.time() - self._minute_start))

    def _secs_until_tomorrow(self) -> float:
        # 86400 secs/day
        return max(0.0, self._day_epoch + 86400 - time.time())

    def wait_for_slot(self, expected_input_tokens: int, expected_output_tokens: int):
        """
        Block (sleep) until sending one more request with the expected tokens
        would not exceed the configured RPM/TPM; stop immediately on daily cap.
        """
        exp_tokens = max(0, expected_input_tokens) + max(0, expected_output_tokens)
        while True:
            self._tick()

            # RPD check
            if self.limits.rpd is not None and self._day_requests >= self.limits.rpd:
                if self.limits.stop_on_daily:
                    raise QuotaExhaustedError("Daily request cap (RPD) reached.")
                # otherwise, wait to tomorrow
                time.sleep(self._secs_until_tomorrow())
                continue

            # RPM check
            if self.limits.rpm is not None and (self._minute_requests + 1) > self.limits.rpm:
                time.sleep(self._secs_until_next_minute())
                continue

            # TPM check (based on our estimate)
            if self.limits.tpm is not None and (self._minute_tokens + exp_tokens) > self.limits.tpm:
                time.sleep(self._secs_until_next_minute())
                continue

            # Allowed to proceed
            return

    def account_after_success(self, used_input_tokens: int, used_output_tokens: int):
        """Record actual usage after a successful API call."""
        self._tick()
        self._minute_requests += 1
        self._day_requests += 1
        self._minute_tokens += max(0, used_input_tokens) + max(0, used_output_tokens)
