import time
from typing import List, Dict, Any

import aiohttp

BINANCE_API = "https://api.binance.com"


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict | None = None):
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()


async def get_usdt_symbols(session: aiohttp.ClientSession) -> list[str]:
    """
    Получаем ВСЕ спотовые пары к USDT, которые сейчас торгуются.
    """
    data = await fetch_json(session, f"{BINANCE_API}/api/v3/exchangeInfo")
    symbols = []
    for s in data["symbols"]:
        if (
            s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and s.get("isSpotTradingAllowed", True)
        ):
            sym = s["symbol"]
            # отбрасываем всякие 3L/3S/UP/DOWN токены, если не нужны
            if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "3LUSDT", "3SUSDT")):
                continue
            symbols.append(sym)
    return symbols


async def get_klines_1m(session: aiohttp.ClientSession, symbol: str, limit: int = 25):
    params = {"symbol": symbol, "interval": "1m", "limit": limit}
    return await fetch_json(session, f"{BINANCE_API}/api/v3/klines", params=params)


def _calc_pump_from_klines(symbol: str, klines: list[list[str]]) -> Dict[str, Any] | None:
    """
    klines: список 1m свечей (последние N штук).
    Возвращает сигнал пампа или None.
    Средний фильтр:
      - рост цены >= 1.8% за 1 мин ИЛИ >= 3% за 5 мин
      - объём свечи >= 2.5x среднего
      - тело свечи >= 60% от диапазона (не просто фитиль)
    """
    if len(klines) < 6:
        return None

    # последние 21 свеча
    closes = [float(k[4]) for k in klines]
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    volumes = [float(k[5]) for k in klines]

    last = closes[-1]
    prev_1 = closes[-2]
    first_5 = closes[-6]

    change_1m = (last - prev_1) / prev_1 * 100
    change_5m = (last - first_5) / first_5 * 100

    vol_last = volumes[-1]
    if len(volumes) > 5:
        avg_vol = sum(volumes[-21:-1]) / max(1, len(volumes[-21:-1]))
    else:
        avg_vol = sum(volumes[:-1]) / max(1, len(volumes[:-1]))

    # объёмный фильтр
    if avg_vol <= 0:
        return None
    vol_ratio = vol_last / avg_vol

    # фильтр по цене
    price_pump = (change_1m >= 1.8) or (change_5m >= 3.0)
    if not price_pump:
        return None

    # объём должен сильно вырасти
    if vol_ratio < 2.5:
        return None

    high_last = highs[-1]
    low_last = lows[-1]

    rng = high_last - low_last
    body = abs(last - prev_1)
    if rng <= 0:
        return None

    body_ratio = body / rng

    # тело свечи хотя бы 60% диапазона, чтобы не было "иголки"
    if body_ratio < 0.6:
        return None

    # простой фильтр по неликвидным монетам
    # если общий объём сделки в долларах маленький — игнорим
    # vol_last — это base volume; оценим в USDT ~ last * vol_last
    notional = last * vol_last
    if notional < 30_000:
        return None

    return {
        "symbol": symbol,
        "price": last,
        "change_1m": round(change_1m, 2),
        "change_5m": round(change_5m, 2),
        "vol_ratio": round(vol_ratio, 2),
        "body_ratio": round(body_ratio, 2),
        "detected_at": int(time.time()),
    }


async def scan_pumps() -> List[Dict[str, Any]]:
    """
    Сканирует все USDT-пары и возвращает список обнаруженных пампов.
    Средний фильтр (для ловли движения 5–20%).
    """
    results: list[Dict[str, Any]] = []

    async with aiohttp.ClientSession() as session:
        symbols = await get_usdt_symbols(session)

        # Можно ограничить количество символов на один проход,
        # чтобы не убиться об лимиты Binance. Например, первые 200.
        # При желании убрать срез и сканировать вообще все.
        # symbols = symbols[:200]

        for symbol in symbols:
            try:
                klines = await get_klines_1m(session, symbol, limit=25)
            except Exception:
                continue

            sig = _calc_pump_from_klines(symbol, klines)
            if sig:
                results.append(sig)

    return results


def format_pump_message(signal: Dict[str, Any]) -> str:
    symbol = signal["symbol"]
    price = signal["price"]
    ch1 = signal["change_1m"]
    ch5 = signal["change_5m"]
    vol_ratio = signal["vol_ratio"]

    text = (
        "🚀 *PUMP DETECTED!*\n\n"
        f"Монета: *{symbol}*\n"
        f"Текущая цена: `{price}` USDT\n\n"
        f"Рост за 1 мин: `{ch1}%`\n"
        f"Рост за 5 мин: `{ch5}%`\n"
        f"Объём: `{vol_ratio}×` от среднего\n\n"
        "Возможные действия:\n"
        "— Вход возможен только для опытной торговли по откату.\n"
        "— Следи за откатом после импульса и ставь стоп под локальный минимум.\n\n"
        "⚠️ Очень высокий риск поймать вершину пампа.\n"
        "_Источник данных: Binance_"
    )
    return text
