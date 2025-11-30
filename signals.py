import asyncio
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from market_data import _fetch_json, _get_klines, _rsi

BINANCE_BASE_URL = "https://api.binance.com/api/v3"

EXCHANGE_INFO_URL = f"{BINANCE_BASE_URL}/exchangeInfo"


async def _get_spot_usdt_symbols() -> List[str]:
    data = await _fetch_json(EXCHANGE_INFO_URL)
    if not data or "symbols" not in data:
        return []

    symbols: List[str] = []
    for symbol_info in data["symbols"]:
        if (
            symbol_info.get("quoteAsset") == "USDT"
            and symbol_info.get("status") == "TRADING"
            and symbol_info.get("isSpotTradingAllowed", True)
        ):
            symbols.append(symbol_info["symbol"])
    return symbols


def _percent_change(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    first, last = values[0], values[-1]
    if first == 0:
        return 0.0
    return (last - first) / first * 100


def _trend_from_change(change: float, strong: float = 3.0, weak: float = 1.0) -> str:
    if change >= strong:
        return "bullish"
    if change <= -strong:
        return "bearish"
    if change >= weak:
        return "slightly_bullish"
    if change <= -weak:
        return "slightly_bearish"
    return "sideways"


def _volume_spike(volumes: Sequence[float]) -> Tuple[float, float]:
    if not volumes:
        return 0.0, 0.0
    avg = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
    last = volumes[-1]
    ratio = last / avg if avg > 0 else 0.0
    return ratio, avg


def _atr(klines: Sequence[Dict[str, float]], period: int = 14) -> float:
    if len(klines) < 2:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(klines)):
        high = klines[i]["high"]
        low = klines[i]["low"]
        prev_close = klines[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    window = trs[-period:]
    return sum(window) / len(window)


def _analyze_monthly(klines_1d: Sequence[Dict[str, float]]) -> Dict[str, Any]:
    last_30 = list(klines_1d[-30:]) if klines_1d else []
    closes = [k["close"] for k in last_30]
    highs = [k["high"] for k in last_30]
    lows = [k["low"] for k in last_30]
    volumes = [k["volume"] for k in last_30]

    trend_change = _percent_change(closes)
    if trend_change >= 10:
        trend = "bullish"
    elif trend_change <= -10:
        trend = "bearish"
    else:
        trend = "sideways"

    rsi_30 = _rsi(list(closes)) if closes else 50.0
    avg_volume = sum(volumes) / len(volumes) if volumes else 0.0
    volume_today = volumes[-1] if volumes else 0.0
    volume_ratio_30 = volume_today / avg_volume if avg_volume else 0.0

    support = min(lows) if lows else 0.0
    resistance = max(highs) if highs else 0.0
    atr_30 = _atr(last_30, period=min(14, len(last_30) - 1)) if len(last_30) > 1 else 0.0

    return {
        "trend": trend,
        "trend_change": trend_change,
        "rsi": rsi_30,
        "volume_ratio": volume_ratio_30,
        "avg_volume": avg_volume,
        "support": support,
        "resistance": resistance,
        "atr": atr_30,
    }


def _entry_zone(lows: Sequence[float], buffer: float) -> Tuple[float, float]:
    if not lows:
        return 0.0, 0.0
    anchor_low = min(lows)
    low_zone = anchor_low
    high_zone = anchor_low + buffer
    return round(low_zone, 4), round(high_zone, 4)


def _risk_reward(entry: float, sl: float, tp1: float) -> float:
    risk = entry - sl
    reward = tp1 - entry
    if risk <= 0:
        return 0.0
    return reward / risk


def _prepare_signal(
    symbol: str,
    closes_1d: Sequence[float],
    closes_4h: Sequence[float],
    closes_1h: Sequence[float],
    closes_15m: Sequence[float],
    volumes_1h: Sequence[float],
    klines_1h: Sequence[Dict[str, float]],
    klines_1d: Sequence[Dict[str, float]],
) -> Optional[Dict[str, Any]]:
    # Trends
    change_1d = _percent_change(closes_1d)
    change_4h = _percent_change(closes_4h)
    change_1h = _percent_change(closes_1h)

    trend_1d = _trend_from_change(change_1d)
    trend_4h = _trend_from_change(change_4h, strong=2.0, weak=0.5)
    trend_1h = _trend_from_change(change_1h, strong=1.0, weak=0.3)

    rsi_1h = _rsi(list(closes_1h)) if closes_1h else 50.0
    rsi_15m = _rsi(list(closes_15m)) if closes_15m else 50.0

    volume_ratio, volume_avg = _volume_spike(volumes_1h)

    monthly = _analyze_monthly(klines_1d)
    monthly_trend = monthly["trend"]
    rsi_30 = monthly["rsi"]
    volume_ratio_30 = monthly["volume_ratio"]

    # Conditions for long
    if trend_1d in {"bearish"}:
        return None
    if trend_4h in {"bearish"}:
        return None
    if monthly_trend == "bearish":
        return None
    if rsi_30 > 70:
        return None
    if rsi_1h >= 70 or rsi_1h <= 30:
        return None
    if volume_ratio < 0.7:
        return None

    last_price = closes_15m[-1] if closes_15m else closes_1h[-1]
    recent_lows = list(closes_1h[-20:] + closes_15m[-20:]) if closes_15m else list(closes_1h[-20:])
    recent_lows = [v for v in recent_lows if v > 0]
    buffer = last_price * 0.003
    entry_low, entry_high = _entry_zone(recent_lows[-5:], buffer)

    atr = _atr(klines_1h)
    stop_distance = max(atr * 0.7, last_price * 0.01)
    sl = max(entry_low - stop_distance, entry_low * 0.96)
    if (entry_low - sl) / entry_low > 0.04:
        sl = entry_low * 0.96

    tp1 = entry_high + (entry_high - sl) * 2.0
    tp2 = entry_high + (entry_high - sl) * 3.0

    rr = _risk_reward(entry_high, sl, tp1)
    if rr < 2:
        return None

    score = 0
    score += 25 if monthly_trend == "bullish" else 10 if monthly_trend == "sideways" else 0
    score += 20 if trend_1d == "bullish" else 5 if trend_1d == "sideways" else 0
    score += 20 if trend_4h == "bullish" else 5 if trend_4h in {"sideways", "slightly_bullish"} else 0
    score += 10 if trend_4h == "bullish" and trend_1d == "bullish" else 0
    score += 10 if trend_1h not in {"bearish", "slightly_bearish"} else 0
    if 40 <= rsi_1h <= 65:
        score += 10
    if rsi_30 < 60:
        score += 5
    if volume_ratio >= 1.2:
        score += 10
    elif volume_ratio >= 0.7:
        score += 5
    if volume_ratio_30 > 1.5:
        score += 10
    elif volume_ratio_30 < 0.5:
        score -= 10
    score += 15 if rr >= 2 else 0
    if rr >= 3:
        score += 5
    if rsi_1h > 70:
        score -= 20
    if rsi_30 > 70:
        score -= 15
    if rsi_30 < 35:
        score -= 15
    if trend_4h == "bearish":
        score -= 20
    if trend_1d == "bearish":
        score -= 25
    if monthly_trend == "bearish":
        score -= 20
    if volume_ratio < 0.7:
        score -= 10

    if score < 90:
        return None

    reason_parts = [
        "Глобальный слой: 30d", 
        f"тренд {monthly_trend} ({monthly['trend_change']:.1f}%)",
        f"RSI30 {rsi_30:.1f}",
        f"объём 30d {volume_ratio_30:.2f}x",
        f"RSI 1H {rsi_1h:.1f} в комфортной зоне",
        f"объём 1H {volume_ratio:.2f}x от среднего {volume_avg:.2f}",
        f"R:R ~{rr:.2f}:1",
    ]

    return {
        "symbol": symbol,
        "direction": "long",
        "entry_zone": (round(entry_low, 4), round(entry_high, 4)),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "score": int(score),
        "reason": ", ".join(reason_parts),
        "levels": {
            "support": round(monthly["support"], 4),
            "resistance": round(monthly["resistance"], 4),
            "atr_30": round(monthly["atr"], 4),
        },
    }


async def _gather_klines(symbol: str) -> Optional[Dict[str, Any]]:
    klines_1d = await _get_klines(symbol, "1d", limit=60)
    klines_4h = await _get_klines(symbol, "4h", limit=120)
    klines_1h = await _get_klines(symbol, "1h", limit=120)
    klines_15m = await _get_klines(symbol, "15m", limit=96)

    if not (klines_1d and klines_4h and klines_1h and klines_15m):
        return None

    return {
        "1d": klines_1d,
        "4h": klines_4h,
        "1h": klines_1h,
        "15m": klines_15m,
    }


async def scan_market(batch_delay: float = 0.2, batch_size: int = 5) -> List[Dict[str, Any]]:
    """
    Сканирует весь рынок Binance по спотовым USDT-парам и возвращает сигналы.
    """
    symbols = await _get_spot_usdt_symbols()
    signals: List[Dict[str, Any]] = []

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        tasks = [asyncio.create_task(_gather_klines(symbol)) for symbol in batch]
        klines_list = await asyncio.gather(*tasks)

        for symbol, klines in zip(batch, klines_list):
            if not klines:
                continue
            closes_1d = [k["close"] for k in klines["1d"]]
            closes_4h = [k["close"] for k in klines["4h"]]
            closes_1h = [k["close"] for k in klines["1h"]]
            closes_15m = [k["close"] for k in klines["15m"]]
            volumes_1h = [k["volume"] for k in klines["1h"]]

            signal = _prepare_signal(
                symbol,
                closes_1d,
                closes_4h,
                closes_1h,
                closes_15m,
                volumes_1h,
                klines["1h"],
                klines["1d"],
            )
            if signal:
                signals.append(signal)

        await asyncio.sleep(batch_delay)

    return signals
