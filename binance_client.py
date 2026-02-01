import asyncio
import os
from dataclasses import dataclass
from typing import List, Optional

from binance_rest import fetch_klines as fetch_klines_raw

_KLINES_SEM = asyncio.Semaphore(
    int(os.environ.get("BINANCE_KLINES_PER_SYMBOL_CONCURRENCY", "5"))
)
KLINES_5M_LIMIT = int(os.environ.get("KLINES_5M_LIMIT", "200"))
KLINES_15M_LIMIT = int(os.environ.get("KLINES_15M_LIMIT", "160"))
KLINES_1H_LIMIT = int(os.environ.get("KLINES_1H_LIMIT", "168"))
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


async def fetch_klines(symbol: str, interval: str, limit: int) -> List[Candle]:
    try:
        raw = await fetch_klines_raw(symbol, interval, limit)
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
    return candles


async def get_required_candles(symbol: str):
    async def _safe(interval: str, limit: int) -> List[Candle]:
        # per-symbol limiter: at most N parallel TF requests for this symbol
        async with _KLINES_SEM:
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
    limit_1h: int = 120,
    limit_15m: int = 120,
) -> dict[str, List[Candle]]:
    async def _safe(interval: str, limit: int) -> List[Candle]:
        async with _KLINES_SEM:
            try:
                return await fetch_klines(symbol, interval, limit)
            except Exception:
                return []

    tasks = {
        "1h": asyncio.create_task(_safe("1h", limit_1h)),
        "15m": asyncio.create_task(_safe("15m", limit_15m)),
    }

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    out: dict[str, List[Candle]] = {}
    for key, value in zip(tasks.keys(), results):
        if isinstance(value, Exception):
            out[key] = []
        else:
            out[key] = value
    return out
