from __future__ import annotations

import time
from typing import Sequence

from ai_types import Candle


def normalize_klines(raw_klines: Sequence[list] | Sequence[Candle] | None) -> list[Candle]:
    if not raw_klines:
        return []

    first = raw_klines[0]
    if isinstance(first, Candle):
        return list(raw_klines)

    candles: list[Candle] = []
    for item in raw_klines:
        if isinstance(item, Candle):
            candles.append(item)
            continue
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        open_time = item[0]
        close_time = item[6] if len(item) > 6 else None
        quote_volume = item[7] if len(item) > 7 else None
        candles.append(
            Candle(
                ts=int(open_time),
                open=float(item[1]),
                high=float(item[2]),
                low=float(item[3]),
                close=float(item[4]),
                volume=float(item[5]),
                quote_volume=float(quote_volume) if quote_volume is not None else None,
                open_time=int(open_time),
                close_time=int(close_time) if close_time is not None else None,
            )
        )

    now_ms = int(time.time() * 1000)
    if candles and candles[-1].close_time is not None and candles[-1].close_time > now_ms:
        candles.pop()
    return candles
