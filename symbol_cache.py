import time
from typing import Any, List

import aiohttp

from binance_rest import fetch_json
from market_cache import get_spot_24h

BINANCE_SPOT_BASE = "https://api.binance.com/api/v3"
BINANCE_FAPI_BASE = "https://fapi.binance.com"

SPOT_SYMBOLS_REFRESH_SEC = 60 * 30
FUTURES_SYMBOLS_REFRESH_SEC = 60 * 30

_spot_cache: dict[str, Any] = {"updated_at": 0.0, "symbols": []}
_futures_cache: dict[str, Any] = {"updated_at": 0.0, "symbols": []}


async def get_spot_usdt_symbols(session: aiohttp.ClientSession | None = None) -> List[str]:
    now = time.time()
    cached = _spot_cache.get("symbols", [])
    if cached and now - float(_spot_cache.get("updated_at", 0.0)) < SPOT_SYMBOLS_REFRESH_SEC:
        return cached

    data = await fetch_json(f"{BINANCE_SPOT_BASE}/exchangeInfo", session=session)
    if not data or "symbols" not in data:
        return cached

    symbols: List[str] = []
    for symbol_info in data["symbols"]:
        if (
            symbol_info.get("quoteAsset") == "USDT"
            and symbol_info.get("status") == "TRADING"
            and symbol_info.get("isSpotTradingAllowed", True)
        ):
            symbol = symbol_info.get("symbol")
            if not symbol:
                continue
            if any(token in symbol for token in ("UPUSDT", "DOWNUSDT", "3LUSDT", "3SUSDT")):
                continue
            symbols.append(symbol)

    _spot_cache["symbols"] = symbols
    _spot_cache["updated_at"] = now
    return symbols


async def get_all_usdt_symbols(session: aiohttp.ClientSession | None = None) -> List[str]:
    return await get_spot_usdt_symbols(session=session)


async def get_futures_usdt_symbols(session: aiohttp.ClientSession | None = None) -> List[str]:
    now = time.time()
    cached = _futures_cache.get("symbols", [])
    if cached and now - float(_futures_cache.get("updated_at", 0.0)) < FUTURES_SYMBOLS_REFRESH_SEC:
        return cached

    data = await fetch_json(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo", session=session)
    if not data:
        return cached

    symbols: List[str] = []
    for row in data.get("symbols", []):
        if row.get("contractType") != "PERPETUAL":
            continue
        if row.get("quoteAsset") != "USDT":
            continue
        symbol = row.get("symbol")
        if not symbol:
            continue
        symbols.append(symbol)

    _futures_cache["symbols"] = symbols
    _futures_cache["updated_at"] = now
    return symbols


async def get_top_usdt_symbols_by_volume(
    limit: int = 200,
    *,
    session: aiohttp.ClientSession | None = None,
) -> List[str]:
    data = await get_spot_24h()
    if not data:
        return []

    usdt_rows = []
    for row in data:
        symbol = row.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue
        try:
            quote_volume = float(row.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue
        usdt_rows.append((symbol, quote_volume))

    usdt_rows.sort(key=lambda item: item[1], reverse=True)
    return [symbol for symbol, _ in usdt_rows[:limit]]
