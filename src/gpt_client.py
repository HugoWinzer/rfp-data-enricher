# src/gpt_client.py
from __future__ import annotations
import json
import logging
from typing import Dict, Any, Tuple, List
from openai import OpenAI
from .model_router import QuotaAwareRouter


class GPTQuotaExceeded(Exception):
pass


_client = OpenAI()
_router = QuotaAwareRouter(_client)


_SYSTEM = (
"You are a data enrichment assistant for a cultural venues dataset. "
"Given the venue name, website text, and hints, extract missing fields. "
"When you don't know, return null."
)


# We ask for concise JSON to simplify parsing
_USER_TMPL = (
"Name: {name}\n"
"City: {city}\n"
"Website: {website}\n"
"Hints: {hints}\n\n"
"Website text (truncated):\n{website_text}\n\n"
"Return strict JSON with keys: \n"
" avg_ticket_price:number|null, avg_ticket_price_source:string|null,\n"
" capacity:integer|null, capacity_source:string|null,\n"
" ticket_vendor:string|null, ticket_vendor_source:string|null,\n"
" short_description:string|null, long_description:string|null,\n"
" phone:string|null, phone_source:string|null,\n"
" linkedin_url:string|null.\n"
)


_DEF_MAXTOK = 400




def _call(messages: List[Dict[str, Any]], max_tokens: int = _DEF_MAXTOK) -> Tuple[str, str]:
try:
resp, used_model = _router.chat(messages=messages, max_tokens=max_tokens, temperature=0)
text = resp.choices[0].message.content or "{}"
return text, used_model
except Exception as e:
# For calling code, treat this as quota to reuse existing STOP_ON_GPT_QUOTA logic
raise GPTQuotaExceeded(str(e))




def enrich_with_gpt(row: Dict[str, Any], website_text: str, hints: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
name = row.get("name") or row.get("org_name") or ""
city = row.get("city") or row.get("locality") or ""
website = row.get("website") or row.get("url") or ""


user = _USER_TMPL.format(
name=name,
city=city,
website=website,
hints=json.dumps(hints, ensure_ascii=False),
return parsed, used_model
