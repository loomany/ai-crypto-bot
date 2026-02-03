import asyncio
import os
import time
from dataclasses import dataclass
from typing import List, Optional

from binance_rest import fetch_klines as fetch_klines_raw

KLINES_5M_LIMIT = int(os.environ.get("KLINES_5M_LIMIT", "120"))
KLINES_15M_LIMIT = int(os.environ.get("KLINES_15M_LIMIT", "160"))
KLINES_1H_LIMIT = int(os.environ.get("KLINES_1H_LIMIT", "120"))
KLINES_4H_LIMIT = int(os.environ.get("KLINES_4H_LIMIT", "120"))
KLINES_1D_LIMIT = int(os.environ.get("KLINES_1D_LIMIT", "60"))


@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    open_time: int
    close_time: int


async def fetch_klines(
    symbol: str,
    interval: str,
    limit: int,
    *,
    start_ms: int | None = None,
) -> List[Candle]:
    try:
        raw = await fetch_klines_raw(symbol, interval, limit, start_ms=start_ms)
    except Exception as exc:
        print(f"[BINANCE] ERROR {symbol}: {exc}")
        raise
    candles: List[Candle] = []
    if not raw:
        return candles

    for item in raw:
        candles.append(
            Candle(
                open=float(item[1]),
                high=float(item[2]),
                low=float(item[3]),
                close=float(item[4]),
                volume=float(item[5]),
                open_time=int(item[0]),
                close_time=int(item[6]),
            )
        )
    now_ms = int(time.time() * 1000)
    if candles and candles[-1].close_time > now_ms:
        candles.pop()
    return candles


async def get_required_candles(symbol: str):
    async def _safe(interval: str, limit: int) -> List[Candle]:
        try:
            return await fetch_klines(symbol, interval, limit)
        except Exception:
            return []

    tasks = {
        "1d": asyncio.create_task(_safe("1d", KLINES_1D_LIMIT)),
        "4h": asyncio.create_task(_safe("4h", KLINES_4H_LIMIT)),
        "1h": asyncio.create_task(_safe("1h", KLINES_1H_LIMIT)),
        "15m": asyncio.create_task(_safe("15m", KLINES_15M_LIMIT)),
        "5m": asyncio.create_task(_safe("5m", KLINES_5M_LIMIT)),
    }

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    out = {}
    for key, value in zip(tasks.keys(), results):
        if isinstance(value, Exception):
            out[key] = []
        else:
            out[key] = value
    return out


async def get_quick_candles(
    symbol: str,
    *,
    tfs: tuple[str, ...] = ("1h", "15m"),
    limit_1h: int = KLINES_1H_LIMIT,
    limit_15m: int = KLINES_15M_LIMIT,
    limit_overrides: dict[str, int] | None = None,
) -> dict[str, List[Candle]]:
    async def _safe(interval: str, limit: int) -> List[Candle]:
        try:
            return await fetch_klines(symbol, interval, limit)
        except Exception:
            return []

    limits = {
        "1h": limit_1h,
        "15m": limit_15m,
        "5m": KLINES_5M_LIMIT,
        "4h": KLINES_4H_LIMIT,
        "1d": KLINES_1D_LIMIT,
    }
    if limit_overrides:
        limits.update(limit_overrides)

    tasks = {tf: asyncio.create_task(_safe(tf, limits[tf])) for tf in tfs}

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    out: dict[str, List[Candle]] = {}
    for key, value in zip(tasks.keys(), results):
        if isinstance(value, Exception):
            out[key] = []
        else:
            out[key] = value
    return out
