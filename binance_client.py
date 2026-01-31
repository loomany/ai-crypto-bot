from dataclasses import dataclass
from typing import List, Optional

from binance_rest import fetch_json

BINANCE_BASE_URL = "https://api.binance.com/api/v3"


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
    raw = await _fetch_json(url, params)
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
    return {
        "1d": await fetch_klines(symbol, "1d", 60),
        "4h": await fetch_klines(symbol, "4h", 120),
        "1h": await fetch_klines(symbol, "1h", 168),
        "15m": await fetch_klines(symbol, "15m", 192),
        "5m": await fetch_klines(symbol, "5m", 288),
    }
