from typing import Any, Dict, List, Optional

from binance_rest import fetch_json

BINANCE_BASE_URL = "https://api.binance.com/api/v3"


# ===== HTTP-ХЕЛПЕР =====

async def _fetch_json(url: str, params: dict | None = None) -> Optional[Any]:
    return await fetch_json(url, params)


# ===== ТЕХАНАЛИЗ НА СЫРЫХ ДАННЫХ =====

def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-diff)

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period if sum(losses) != 0 else 0.000001

    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = diff if diff > 0 else 0.0
        loss = -diff if diff < 0 else 0.0

        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period if avg_loss != 0 else 0.000001

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)


def _simple_trend(closes: List[float]) -> str:
    if len(closes) < 3:
        return "neutral"

    first = closes[0]
    last = closes[-1]
    change = (last - first) / first * 100

    if change > 1.5:
        return "bullish"
    elif change < -1.5:
        return "bearish"
    else:
        return "sideways"


def _support_resistance(closes: List[float]) -> tuple[float, float]:
    if not closes:
        return 0.0, 0.0
    support = min(closes)
    resistance = max(closes)
    return round(support, 8), round(resistance, 8)


def _volume_description(volumes: List[float]) -> str:
    if len(volumes) < 5:
        return "normal"

    avg = sum(volumes[:-1]) / (len(volumes) - 1)
    last = volumes[-1]

    if last > avg * 1.3:
        return "high"
    elif last < avg * 0.7:
        return "low"
    else:
        return "normal"


def _macd_signal(closes: List[float]) -> str:
    # Очень упрощённый MACD: сравнение короткой и длинной SMA
    if len(closes) < 26:
        return "neutral"

    short_window = closes[-12:]
    long_window = closes[-26:]

    short_sma = sum(short_window) / len(short_window)
    long_sma = sum(long_window) / len(long_window)

    if short_sma > long_sma * 1.003:
        return "bullish"
    elif short_sma < long_sma * 0.997:
        return "bearish"
    else:
        return "neutral"


# ===== ПОЛУЧЕНИЕ ДАННЫХ С BINANCE =====

async def get_ticker_basic(symbol: str) -> Optional[Dict[str, float]]:
    url = f"{BINANCE_BASE_URL}/ticker/24hr"
    params = {"symbol": symbol}
    data = await _fetch_json(url, params)
    if not data or "lastPrice" not in data:
        return None

    return {
        "symbol": data["symbol"],
        "price": float(data["lastPrice"]),
        "change_24h": float(data["priceChangePercent"]),
    }


async def _get_klines(symbol: str, interval: str, limit: int = 100) -> Optional[List[Dict[str, float]]]:
    url = f"{BINANCE_BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    data = await _fetch_json(url, params)
    if not data:
        return None

    klines: List[Dict[str, float]] = []
    for k in data:
        # kline schema: [open_time, open, high, low, close, volume, ...]
        open_price = float(k[1])
        high = float(k[2])
        low = float(k[3])
        close = float(k[4])
        volume = float(k[5])
        klines.append(
            {
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )

    return klines


# ===== ОСНОВНАЯ ФУНКЦИЯ АНАЛИЗА МОНЕТЫ =====

async def get_coin_analysis(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Основная функция, которой пользуется бот.
    Возвращает:
    {
      "symbol": "SOLUSDT",
      "price": 148.2,
      "change_24h": 3.2,
      "tf": {
          "4h": {...},
          "1h": {...},
          "15m": {...}
      },
      "levels": {...},
      "risk": "low/medium/high"
    }
    """
    ticker = await get_ticker_basic(symbol)
    if not ticker:
        return None

    price = ticker["price"]
    change_24h = ticker["change_24h"]

    timeframes = {
        "4h": "4h",
        "1h": "1h",
        "15m": "15m",
    }

    tf_data: Dict[str, Dict[str, Any]] = {}

    for name, interval in timeframes.items():
        klines = await _get_klines(symbol, interval, limit=80)
        if not klines:
            continue

        closes = [k["close"] for k in klines]
        volumes = [k["volume"] for k in klines]

        rsi_val = _rsi(closes)
        trend = _simple_trend(closes)
        support, resistance = _support_resistance(closes[-40:])
        vol_desc = _volume_description(volumes)
        macd = _macd_signal(closes)

        tf_data[name] = {
            "trend": trend,
            "rsi": rsi_val,
            "support": support,
            "resistance": resistance,
            "volume_desc": vol_desc,
            "macd": macd,
        }

    # Берём уровни из 4ч как более "глобальные"
    tf4h = tf_data.get("4h")
    if tf4h:
        support = tf4h["support"]
        resistance = tf4h["resistance"]
    else:
        support = round(price * 0.95, 8)
        resistance = round(price * 1.05, 8)

    # Простые уровни для сделки
    entry_low = round(price * 0.97, 8)
    entry_high = round(price * 0.98, 8)
    tp1 = round(price * 1.018, 8)
    tp2 = round(price * 1.035, 8)
    sl = round(price * 0.985, 8)

    # Оценка риска по RSI на 15m
    tf15 = tf_data.get("15m")
    risk = "medium"
    if tf15:
        rsi_15 = tf15["rsi"]
        if 40 <= rsi_15 <= 60:
            risk = "low"
        elif rsi_15 >= 70 or rsi_15 <= 30:
            risk = "high"

    return {
        "symbol": symbol,
        "price": round(price, 8),
        "change_24h": round(change_24h, 2),
        "tf": tf_data,
        "levels": {
            "support": support,
            "resistance": resistance,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
        },
        "risk": risk,
    }
