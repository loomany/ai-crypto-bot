import time
from typing import Any, List

import aiohttp

from binance_rest import fetch_json
from market_cache import get_spot_24h

BINANCE_SPOT_BASE = "https://api.binance.com/api/v3"
BINANCE_FAPI_BASE = "https://fapi.binance.com"

SPOT_SYMBOLS_REFRESH_SEC = 60 * 30
FUTURES_SYMBOLS_REFRESH_SEC = 60 * 30
EXCLUDED_BASE_ASSETS = {
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "BUSD",
    "PAXG",
    "EUR",
    "GBP",
    "TRY",
    "BRL",
    "AUD",
    "RUB",
    "UAH",
    "NGN",
    "IDR",
}
LEVERAGE_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S")

_spot_cache: dict[str, Any] = {"updated_at": 0.0, "symbols": []}
_futures_cache: dict[str, Any] = {"updated_at": 0.0, "symbols": []}


def _get_base_asset(symbol: str) -> str | None:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    return None


def is_tradeable_symbol(symbol: str, ticker_24h: dict | None = None) -> bool:
    if not symbol or not symbol.endswith("USDT"):
        return False
    base = _get_base_asset(symbol)
    if not base or base in EXCLUDED_BASE_ASSETS:
        return False
    if any(base.endswith(suffix) for suffix in LEVERAGE_SUFFIXES):
        return False
    if ticker_24h:
        try:
            last_price = float(ticker_24h.get("lastPrice", 0.0))
        except (TypeError, ValueError):
            last_price = 0.0
        if last_price <= 0:
            return False
    return True


def filter_tradeable_symbols(
    symbols: List[str],
    *,
    tickers_by_symbol: dict[str, dict] | None = None,
) -> tuple[List[str], int]:
    filtered: List[str] = []
    removed = 0
    for symbol in symbols:
        ticker = tickers_by_symbol.get(symbol) if tickers_by_symbol else None
        if is_tradeable_symbol(symbol, ticker):
            filtered.append(symbol)
        else:
            removed += 1
    return filtered, removed


async def get_spot_usdt_symbols(session: aiohttp.ClientSession | None = None) -> List[str]:
    now = time.time()
    cached = _spot_cache.get("symbols", [])
    if cached and now - float(_spot_cache.get("updated_at", 0.0)) < SPOT_SYMBOLS_REFRESH_SEC:
        return cached

    data = await fetch_json(
        f"{BINANCE_SPOT_BASE}/exchangeInfo",
        session=session,
        stage="exchangeInfo",
    )
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
            if not symbol or not is_tradeable_symbol(symbol):
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

    data = await fetch_json(
        f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo",
        session=session,
        stage="exchangeInfo",
    )
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
        if not is_tradeable_symbol(symbol, row):
            continue
        usdt_rows.append((symbol, quote_volume))

    usdt_rows.sort(key=lambda item: item[1], reverse=True)
    return [symbol for symbol, _ in usdt_rows[:limit]]


async def get_top_usdt_symbols_by_movers(
    gainers_limit: int = 0,
    losers_limit: int = 0,
    *,
    session: aiohttp.ClientSession | None = None,
) -> tuple[List[str], List[str]]:
    if gainers_limit <= 0 and losers_limit <= 0:
        return [], []

    data = await get_spot_24h()
    if not data:
        return [], []

    movers: list[tuple[str, float]] = []
    for row in data:
        symbol = row.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue
        if not is_tradeable_symbol(symbol, row):
            continue
        try:
            change_pct = float(row.get("priceChangePercent", 0.0))
        except (TypeError, ValueError):
            continue
        movers.append((symbol, change_pct))

    gainers = sorted(movers, key=lambda item: item[1], reverse=True)
    losers = sorted(movers, key=lambda item: item[1])

    return (
        [symbol for symbol, _ in gainers[:gainers_limit]] if gainers_limit > 0 else [],
        [symbol for symbol, _ in losers[:losers_limit]] if losers_limit > 0 else [],
    )
