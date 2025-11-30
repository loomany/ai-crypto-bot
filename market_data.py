import aiohttp

BINANCE_BASE_URL = "https://api.binance.com/api/v3"


async def _fetch_json(url: str, params: dict | None = None) -> dict | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                return await resp.json()
    except Exception:
        return None


async def get_ticker_price(symbol: str) -> dict | None:
    symbol = symbol.upper()
    url = f"{BINANCE_BASE_URL}/ticker/24hr"
    params = {"symbol": symbol}

    data = await _fetch_json(url, params)
    if not data:
        return None

    try:
        return {
            "symbol": data["symbol"],
            "price": float(data["lastPrice"]),
            "change_24h": float(data["priceChangePercent"]),
        }
    except (KeyError, TypeError, ValueError):
        return None
