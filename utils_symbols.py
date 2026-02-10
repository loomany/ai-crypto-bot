from __future__ import annotations

import re

_USDT_PAIR_SUFFIX_RE = re.compile(r"\s*/\s*USDT\s*$", re.IGNORECASE)


def ui_symbol(symbol: str) -> str:
    text = str(symbol or "").strip()
    if not text:
        return text

    text = _USDT_PAIR_SUFFIX_RE.sub("", text)
    if text.upper().endswith("USDT"):
        return text[:-4]
    return text
