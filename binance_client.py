import asyncio
import os
from dataclasses import dataclass
from typing import List, Optional

from binance_rest import fetch_json

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
_KLINES_SEM = asyncio.Semaphore(
    int(os.environ.get("BINANCE_KLINES_PER_SYMBOL_CONCURRENCY", "3"))
)


@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    open_time: int
    close_time: int


async def _fetch_json(url: str, params: dict | None = None) -> Optional[list]:
    return await fetch_json(url, params)


async def fetch_klines(symbol: str, interval: str, limit: int) -> List[Candle]:
    url = f"{BINANCE_BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    print(f"[BINANCE] request {symbol} klines")
    try:
        raw = await _fetch_json(url, params)
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
        "1d": asyncio.create_task(_safe("1d", 60)),
        "4h": asyncio.create_task(_safe("4h", 120)),
        "1h": asyncio.create_task(_safe("1h", 168)),
        "15m": asyncio.create_task(_safe("15m", 192)),
        "5m": asyncio.create_task(_safe("5m", 288)),
    }

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    out = {}
    for key, value in zip(tasks.keys(), results):
        if isinstance(value, Exception):
            out[key] = []
        else:
            out[key] = value
    return out
