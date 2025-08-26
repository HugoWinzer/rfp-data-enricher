# src/model_router.py
import os, time, logging, random
from typing import Tuple, Dict, Any, List
from openai import OpenAI, RateLimitError, APIStatusError

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
        # Normalize keys
        h = {k.lower(): v for k, v in (headers or {}).items()}
        wait_s = 0.0

        if "retry-after" in h:
            try:
                wait_s = float(h["retry-after"])
            except Exception:
                pass
        elif "x-ratelimit-reset-requests" in h:
            # values like "24h0m50.581s" or "2s"
            val = h["x-ratelimit-reset-requests"]
            try:
                if val.endswith("s") and val.replace(".", "", 1).replace("s", "").isdigit():
                    wait_s = float(val[:-1])
                elif val.endswith("ms") and val[:-2].isdigit():
                    wait_s = float(val[:-2]) / 1000.0
                else:
                    # rough parse of h/m/s — we cap at 1h anyway
                    if "h" in val or "m" in val or "s" in val:
                        seconds = 0.0
                        num = ""
                        for ch in val:
                            if ch.isdigit() or ch == ".":
                                num += ch
                            elif ch == "h" and num:
                                seconds += float(num) * 3600; num = ""
                            elif ch == "m" and num:
                                seconds += float(num) * 60; num = ""
                            elif ch == "s" and num:
                                seconds += float(num); num = ""
                        wait_s = seconds
            except Exception:
                pass

        # Cap cooldown (1h) to avoid wedging
        if wait_s <= 0:
            wait_s = random.uniform(5.0, 15.0)
        self.cooldown_until[model] = time.time() + min(wait_s, 3600.0)

    def _pick_model(self) -> str:
        now = time.time()
        ready = [m for m in self.models if now >= self.cooldown_until.get(m, 0)]
        if ready:
            return ready[0]
        # none ready — wait for the earliest one, but don’t block too long
        earliest = min(self.models, key=lambda m: self.cooldown_until.get(m, 0))
        sleep_for = max(0.0, self.cooldown_until[earliest] - now)
        sleep_for = min(sleep_for, self.max_global_sleep_s)
        if sleep_for > 0:
            logging.info(f"[router] all models cooling down; sleeping {sleep_for:.1f}s for {earliest}")
            time.sleep(sleep_for)
        return earliest

    def chat(self, *, messages: List[Dict[str, Any]], **kwargs) -> Tuple[Any, str]:
        """
        Calls chat.completions with router logic.
        Returns (response, model_used).
        """
        last_err: Exception | None = None
        attempts = 0
        max_attempts = len(self.models) + 2  # allow a brief global sleep once

        while attempts < max_attempts:
            attempts += 1
            model = self._pick_model()
            try:
                resp = self.client.chat.completions.create(
                    model=model,
                    messages=messages,
                    timeout=self.per_request_timeout_s,
                    **kwargs,
                )
                usage = getattr(resp, "usage", None)
                if usage:
                    logging.info(
                        f"GPT usage model={model} prompt_tokens={usage.prompt_tokens} "
                        f"completion_tokens={usage.completion_tokens} total_tokens={usage.total_tokens}"
                    )
                return resp, model

            except RateLimitError as e:
                headers = getattr(getattr(e, "response", None), "headers", {}) or {}
                self._apply_retry_after(model, headers)
                logging.error(
                    f"[429 model={model}] Retry-After={headers.get('Retry-After') or headers.get('retry-after')} | "
                    f"x-ratelimit-remaining-requests={headers.get('x-ratelimit-remaining-requests')} "
                    f"x-ratelimit-reset-requests={headers.get('x-ratelimit-reset-requests')} | "
                    f"x-ratelimit-limit-requests={headers.get('x-ratelimit-limit-requests')}"
                )
                last_err = e
                continue

            except APIStatusError as e:
                if getattr(e, "status_code", None) == 429:
                    headers = getattr(getattr(e, "response", None), "headers", {}) or {}
                    self._apply_retry_after(model, headers)
                    logging.error(f"[429 model={model}] {e}")
                    last_err = e
                    continue
                raise  # non-rate-limit HTTP errors should bubble

        raise last_err or RuntimeError("All models exhausted due to rate limits")
