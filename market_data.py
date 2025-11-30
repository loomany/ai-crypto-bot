import aiohttp

BINANCE_BASE_URL = "https://api.binance.com/api/v3"


async def _fetch_json(url: str, params: dict | None = None) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()


async def get_ticker_price(symbol: str) -> dict:
    """
    Возвращает данные по тикеру:
    {
        "symbol": "BTCUSDT",
        "last_price": 1000.0,
        "price_change_percent": -2.5
    }
    """
    symbol = symbol.upper()
    url = f"{BINANCE_BASE_URL}/ticker/24hr"
    params = {"symbol": symbol}
    data = await _fetch_json(url, params)

    return {
        "symbol": data["symbol"],
        "last_price": float(data["lastPrice"]),
        "price_change_percent": float(data["priceChangePercent"]),
    }
