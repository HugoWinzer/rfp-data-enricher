import os
import time
from typing import List, Dict, Any

from openai import OpenAI
from openai import RateLimitError, APIError, APIStatusError
import tiktoken

from .rate_limiter import RateLimiter, QuotaExhaustedError

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
# A safe upper bound you allow per call – still enforced by the endpoint.
DEFAULT_MAX_OUTPUT_TOKENS = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "400"))

# Try to use the latest tokenizer for 4o; fall back to cl100k_base if not present
def _get_encoder():
    try:
        return tiktoken.get_encoding("o200k_base")
    except Exception:
        return tiktoken.get_encoding("cl100k_base")

_ENCODER = _get_encoder()

def estimate_message_tokens(messages: List[Dict[str, str]]) -> int:
    """
    Very rough estimate: sum of tokenized role + content for each message.
    This is good enough to stay under TPM proactively.
    """
    total = 0
    for m in messages:
        total += len(_ENCODER.encode(m.get("role", "")))
        total += len(_ENCODER.encode(m.get("content", "")))
    return total

class GPTClient:
    def __init__(self):
        # key comes from Secret Manager via env
        self.client = OpenAI()
        self.model = DEFAULT_MODEL
        self.stop_on_quota = os.getenv("STOP_ON_GPT_QUOTA", "0") == "1"
        self.limiter = RateLimiter()
        # soft retry for transient minute-level throttles (not daily caps)
        self.max_retries = int(os.getenv("OPENAI_SOFT_RETRIES", "3"))

    def complete_json(self, messages: List[Dict[str, str]], max_output_tokens: int | None = None, temperature: float = 0.0) -> Dict[str, Any]:
        """
        Makes a chat.completions call expecting JSON output.
        Returns dict with { 'ok': bool, 'content': str|None, 'usage': {...}, 'error': str|None }
        """
        max_tokens = max_output_tokens or DEFAULT_MAX_OUTPUT_TOKENS

        # 1) Proactive throttle (RPM/TPM/RPD)
        expected_in = estimate_message_tokens(messages)
        expected_out = max_tokens
        self.limiter.wait_for_slot(expected_in, expected_out)

        attempt = 0
        last_err = None
        while attempt <= self.max_retries:
            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=temperature,
                    response_format={"type": "json_object"},
                    max_tokens=max_tokens,
                )

                # 2) Account actual usage
                usage = getattr(resp, "usage", None) or {}
                used_in = usage.get("prompt_tokens") or usage.get("input_tokens") or 0
                used_out = usage.get("completion_tokens") or usage.get("output_tokens") or 0
                self.limiter.account_after_success(used_in, used_out)

                content = resp.choices[0].message.content if resp.choices else ""
                return {"ok": True, "content": content, "usage": dict(usage), "error": None}

            except RateLimitError as e:
                # If you want to hard stop on *any* 429 (e.g., paid quota / daily cap),
                # honor the STOP_ON_GPT_QUOTA flag.
                if self.stop_on_quota:
                    raise QuotaExhaustedError("OpenAI 429 rate limit / quota.")
                last_err = f"429 rate limit: {e}"
                # Back off for minute-level limits
                time.sleep(min(20, 2 ** attempt + 1))
                attempt += 1
                continue

            except APIStatusError as e:
                # 429 can also arrive as APIStatusError; treat the same way
                if e.status_code == 429 and self.stop_on_quota:
                    raise QuotaExhaustedError("OpenAI 429 rate limit / quota.")
                if e.status_code >= 500:
                    # transient server error -> short backoff retry
                    last_err = f"{e.status_code} server: {e}"
                    time.sleep(min(15, 2 ** attempt + 1))
                    attempt += 1
                    continue
                # other status codes: don't loop
                return {"ok": False, "content": None, "usage": {}, "error": f"OpenAI HTTP {e.status_code}: {e}"}

            except APIError as e:
                # generic APIError: retry a little
                last_err = f"APIError: {e}"
                time.sleep(min(10, 2 ** attempt + 1))
                attempt += 1
                continue

            except QuotaExhaustedError:
                # bubble up (handled by Flask layer to return HTTP 429)
                raise

            except Exception as e:
                return {"ok": False, "content": None, "usage": {}, "error": f"Unhandled OpenAI error: {e}"}

        # exhausted soft retries
        return {"ok": False, "content": None, "usage": {}, "error": last_err or "OpenAI retry budget exhausted"}
