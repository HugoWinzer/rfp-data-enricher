# src/segmenter.py
def size_segment(gtv: float | None) -> str | None:
    if gtv is None:
        return None
    if gtv >= 20_000_000:
        return "Diamond"
    if gtv >= 4_000_000:
        return "Gold"
    if gtv >= 2_000_000:
        return "Silver"
    return "Bronze"
