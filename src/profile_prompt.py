# src/profile_prompt.py
from textwrap import dedent

PROFILE_SYSTEM = dedent("""
You enrich cultural venues/events. Return STRICT JSON with keys:

{
 "website": "<canonical URL or null>",
 "ticket_vendor": "<vendor or null>",  // payment-funnel powering checkout
 "annual_visitors": <number or null>,  // visitors/year
 "capacity_final": <number or null>,   // venue capacity or per-event attendance
 "atp": <number or null>,              // avg ticket price (number only)
 "ownership": "Private|Public|Unknown",
 "rfp": "Yes|No|Unknown",
 "notes": "<=400 chars on how inferred>"
}

Rules:
- Prefer official site. Only use social if no official site.
- If multiple providers appear, choose the one that processes checkout.
- If visitors unknown, estimate conservatively and state this in notes.
- Keep numbers as plain numbers (no currency symbols).
- Be concise; no markdown.
""").strip()

def build_user_payload(row: dict) -> str:
    # Offer hints + scrape signals to GPT
    return (
        "{"
        f"\"name\": {row.get('name')!r}, "
        f"\"city\": {row.get('city')!r}, "
        f"\"country\": {row.get('country')!r}, "
        f"\"domain\": {row.get('domain')!r}, "
        f"\"capacity_hint\": {row.get('capacity')!r}, "
        f"\"avg_ticket_price_hint\": {row.get('avg_ticket_price')!r}, "
        f"\"vendor_signals\": {row.get('vendor_signals')!r}, "
        f"\"text_excerpt\": {row.get('text_excerpt')!r}"
        "}"
    )
