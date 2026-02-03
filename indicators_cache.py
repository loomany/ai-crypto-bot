from __future__ import annotations

from typing import Dict, Tuple, Optional

from binance_client import Candle
from trading_core import _compute_rsi_series, compute_atr, compute_ema

_INDICATOR_CACHE: Dict[Tuple[str, str, int, str, int], Optional[float]] = {}
_MAX_CACHE_SIZE = 10000


def _get_last_close_time(candles: list[Candle]) -> int | None:
    if not candles:
        return None
    return candles[-1].close_time


def _cache_get(
    key: Tuple[str, str, int, str, int],
    compute: callable,
) -> Optional[float]:
    if key in _INDICATOR_CACHE:
        return _INDICATOR_CACHE[key]
    value = compute()
    _INDICATOR_CACHE[key] = value
    if len(_INDICATOR_CACHE) > _MAX_CACHE_SIZE:
        for old_key in list(_INDICATOR_CACHE.keys())[: _MAX_CACHE_SIZE // 2]:
            _INDICATOR_CACHE.pop(old_key, None)
    return value


def get_cached_ema(
    symbol: str,
    tf: str,
    candles: list[Candle],
    period: int,
) -> Optional[float]:
    last_close_time = _get_last_close_time(candles)
    if last_close_time is None:
        return None
    closes = [c.close for c in candles]
    key = (symbol, tf, last_close_time, "ema", period)
    return _cache_get(key, lambda: compute_ema(closes, period))


def get_cached_atr(
    symbol: str,
    tf: str,
    candles: list[Candle],
    period: int,
) -> Optional[float]:
    last_close_time = _get_last_close_time(candles)
    if last_close_time is None:
        return None
    key = (symbol, tf, last_close_time, "atr", period)
    return _cache_get(key, lambda: compute_atr(candles, period))


def get_cached_rsi(
    symbol: str,
    tf: str,
    candles: list[Candle],
    period: int = 14,
) -> float:
    last_close_time = _get_last_close_time(candles)
    if last_close_time is None:
        return 50.0
    closes = [c.close for c in candles]
    key = (symbol, tf, last_close_time, "rsi", period)
    cached = _cache_get(
        key,
        lambda: (_compute_rsi_series(closes, period)[-1] if closes else 50.0),
    )
    return cached if cached is not None else 50.0
