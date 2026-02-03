from __future__ import annotations

import time
from typing import Dict, Any

from binance_client import get_klines
from trading_core import _compute_atr_series


_CACHE: Dict[str, Any] = {}
_CACHE_TTL_SEC = 20


def _pct_change(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b / a - 1.0) * 100.0


async def get_market_context(symbol: str = "BTCUSDT") -> Dict[str, Any]:
    """
    Returns dict with:
      mode: NORMAL | PANIC | TREND_UP | TREND_DOWN
      bias: NEUTRAL | LONG | SHORT
      allow_longs / allow_shorts
      min_score_long / min_score_short
      change_6h_pct
      atr_1h_pct
    Cached for _CACHE_TTL_SEC to avoid hammering Binance.
    """
    now = time.time()
    cached = _CACHE.get("ctx")
    if cached and (now - cached.get("ts", 0)) < _CACHE_TTL_SEC:
        return cached

    # 15m klines -> 6h change (24 candles)
    # 1h klines -> ATR% 1h
    kl_15m = await get_klines(symbol, "15m", limit=60)
    kl_1h = await get_klines(symbol, "1h", limit=120)

    closes_15m = [float(k[4]) for k in kl_15m]
    closes_1h = [float(k[4]) for k in kl_1h]
    highs_1h = [float(k[2]) for k in kl_1h]
    lows_1h = [float(k[3]) for k in kl_1h]

    last_close = closes_15m[-1] if closes_15m else 0.0
    close_6h_ago = closes_15m[-(24 + 1)] if len(closes_15m) >= 25 else (closes_15m[0] if closes_15m else 0.0)
    change_6h_pct = _pct_change(close_6h_ago, last_close)

    atr_series = _compute_atr_series(highs_1h, lows_1h, closes_1h, period=14)
    atr_last = float(atr_series[-1]) if atr_series else 0.0
    atr_1h_pct = (atr_last / closes_1h[-1] * 100.0) if (closes_1h and closes_1h[-1] != 0) else 0.0

    # --- MODE DETECTION ---
    if abs(change_6h_pct) >= 3.0 and atr_1h_pct >= 2.2:
        mode = "PANIC"
    elif change_6h_pct >= 1.5:
        mode = "TREND_UP"
    elif change_6h_pct <= -1.5:
        mode = "TREND_DOWN"
    else:
        mode = "NORMAL"

    # --- RULES ---
    if mode == "PANIC":
        ctx = {
            "mode": mode,
            "bias": "SHORT",
            "allow_longs": False,
            "allow_shorts": True,
            "min_score_long": 999.0,
            "min_score_short": 82.0,
            "change_6h_pct": change_6h_pct,
            "atr_1h_pct": atr_1h_pct,
            "ts": now,
        }
    elif mode == "TREND_UP":
        ctx = {
            "mode": mode,
            "bias": "LONG",
            "allow_longs": True,
            "allow_shorts": True,
            "min_score_long": 78.0,
            "min_score_short": 85.0,
            "change_6h_pct": change_6h_pct,
            "atr_1h_pct": atr_1h_pct,
            "ts": now,
        }
    elif mode == "TREND_DOWN":
        ctx = {
            "mode": mode,
            "bias": "SHORT",
            "allow_longs": True,
            "allow_shorts": True,
            "min_score_long": 85.0,
            "min_score_short": 78.0,
            "change_6h_pct": change_6h_pct,
            "atr_1h_pct": atr_1h_pct,
            "ts": now,
        }
    else:
        ctx = {
            "mode": mode,
            "bias": "NEUTRAL",
            "allow_longs": True,
            "allow_shorts": True,
            "min_score_long": 80.0,
            "min_score_short": 80.0,
            "change_6h_pct": change_6h_pct,
            "atr_1h_pct": atr_1h_pct,
            "ts": now,
        }

    _CACHE["ctx"] = ctx
    return ctx
