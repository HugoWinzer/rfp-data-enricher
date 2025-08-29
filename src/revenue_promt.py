# src/revenue_prompt.py
SYSTEM_PROMPT = """You are a careful revenue estimator for cultural venues and events.
Goal: estimate ANNUAL gross ticket revenue (GTV) in USD for the provided entity.
Use provided hints (capacity, average ticket price, annual visitors, notes).
If info is missing, make a conservative estimate for Madrid based on typical sizes.

Rules:
- Return ONLY minified JSON with: revenue_usd (number), confidence ("low"|"medium"|"high"), assumptions (<=400 chars).
- No markdown or extra text. Use USD.
"""

def build_user_prompt(ctx: dict) -> str:
    lines = []
    lines.append("Entity:")
    lines.append(f"- name: {ctx.get('name')}")
    if ctx.get('domain'):
        lines.append(f"- website: {ctx['domain']}")
    loc = ", ".join([x for x in [ctx.get('city'), ctx.get('country')] if x])
    if loc:
        lines.append(f"- location: {loc}")
    if ctx.get('capacity') is not None:
        lines.append(f"- capacity: {ctx['capacity']}")
    if ctx.get('avg_ticket_price') is not None:
        lines.append(f"- avg_ticket_price: {ctx['avg_ticket_price']} (local currency, if known)")
    if ctx.get('annual_visitors') is not None:
        lines.append(f"- annual_visitors: {ctx['annual_visitors']}")
    if ctx.get('source_url'):
        lines.append(f"- source_url: {ctx['source_url']}")
    if ctx.get('notes'):
        lines.append(f"- notes: {ctx['notes']}")
    lines.append("")
    lines.append("Return only JSON with: revenue_usd, confidence, assumptions.")
    return "\n".join(lines)
