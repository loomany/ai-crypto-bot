from __future__ import annotations

from statistics import median
from typing import Any

from binance_rest import fetch_klines
from trading_core import compute_atr
from utils.safe_math import EPS, guarded_div
from utils_klines import normalize_klines


async def compute_alt_chop_state(
    symbols: list[str],
    *,
    tf_check: str,
    atr_tf: str,
    atr_period: int,
    ft_median_max: float,
    min_valid: int,
    entry_refs: dict[str, float] | None = None,
    sample_max: int = 12,
) -> dict[str, Any]:
    sample = [str(s).upper() for s in symbols if s][: max(0, int(sample_max))]
    values: list[float] = []
    samples: list[dict[str, float | str]] = []
    entry_refs = entry_refs or {}

    for symbol in sample:
        k_check_raw = await fetch_klines(symbol, tf_check, 2)
        k_atr_raw = await fetch_klines(symbol, atr_tf, max(atr_period + 3, 30))
        k_check = normalize_klines(k_check_raw)
        k_atr = normalize_klines(k_atr_raw)
        if not k_check or not k_atr:
            continue

        atr = compute_atr(k_atr, period=atr_period)
        if atr is None or float(atr) <= EPS:
            continue

        close_1h_last = float(k_check[-1].close)
        entry_ref = float(entry_refs.get(symbol, 0.0) or 0.0)
        if abs(entry_ref) <= EPS:
            entry_ref = close_1h_last

        ft = abs(close_1h_last - entry_ref) * guarded_div(1.0, float(atr), 0.0)
        values.append(float(ft))
        samples.append(
            {
                "symbol": symbol,
                "ft": round(float(ft), 4),
                "atr": round(float(atr), 6),
                "entry_ref": round(float(entry_ref), 8),
            }
        )

    ft_median = float(median(values)) if values else 0.0
    valid_count = len(values)
    is_chop = valid_count >= int(min_valid) and ft_median <= float(ft_median_max)
    top_samples = sorted(samples, key=lambda item: float(item.get("ft", 0.0)))[:5]
    return {
        "alt_chop": bool(is_chop),
        "ft_median": round(ft_median, 4),
        "ft_values_count": valid_count,
        "sample_symbols_count": len(sample),
        "min_valid": int(min_valid),
        "ft_median_max": float(ft_median_max),
        "samples": top_samples,
    }
