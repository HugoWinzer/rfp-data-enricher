#!/usr/bin/env python3
- city: {city}
- country: {country}


Strictly output **only** valid JSON (no code fences, no comments), like:
{{"ticket_vendor":"Eventbrite","capacity":450,"avg_ticket_price":28}}
"""




def _parse_json(s: str) -> Optional[Dict[str, Any]]:
try:
return json.loads(s)
except Exception:
try:
# Occasionally models wrap JSON in code fences
t = s.strip()
if t.startswith("```"):
t = t.strip("`")
if t.lower().startswith("json"):
t = t[4:].strip()
return json.loads(t)
except Exception:
return None




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
data = _parse_json(content) or {}
result = {
"ticket_vendor": data.get("ticket_vendor"),
"capacity": _clamp(data.get("capacity"), CAPACITY_MIN, CAPACITY_MAX),
"avg_ticket_price": _clamp(data.get("avg_ticket_price"), PRICE_MIN, PRICE_MAX),
}


# if everything missing, treat as failure
if not any(result.values()):
return None


# normalize obvious nonsense
if result["capacity"] is None:
result["capacity"] = CAPACITY_MIN
if result["avg_ticket_price"] is None:
result["avg_ticket_price"] = PRICE_MIN


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
