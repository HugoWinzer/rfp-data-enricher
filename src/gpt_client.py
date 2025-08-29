import os
import json
import time
import typing as t
from dataclasses import dataclass

import requests

OPENAI_BASE = os.getenv("OPENAI_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Simple client using the Chat Completions API (widely compatible)
# No secrets printed; 429s are surfaced to the caller.
@dataclass
class GPTResult:
    text: str
    model: str
    usage: dict

def _headers():
    return {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

def ask_gpt(system: str, user: str, temperature: float = 0.2, max_tokens: int = 400) -> GPTResult:
    url = f"{OPENAI_BASE}/chat/completions"
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    resp = requests.post(url, headers=_headers(), json=payload, timeout=60)
    if resp.status_code == 429:
        # Bubble up quota behavior; the caller decides to stop.
        raise RuntimeError(f"OpenAI 429 rate limit: {resp.text}")
    resp.raise_for_status()
    data = resp.json()
    text = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})
    model = data.get("model", OPENAI_MODEL)
    return GPTResult(text=text, model=model, usage=usage)
