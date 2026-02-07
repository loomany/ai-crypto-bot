import asyncio
import time
from statistics import mean
from typing import Any, Dict, List, Optional

from ai_types import Candle
from binance_rest import get_klines, is_binance_degraded
from utils_klines import normalize_klines
from trading_core import _compute_rsi_series, compute_ema, detect_trend_and_structure


async def get_market_regime() -> Dict[str, Any]:
    """
    Определяет режим рынка по BTCUSDT.

    Codex:
      - можно сюда добавить DXY, VIX, Total Market Cap, Funding и т.п.
      - сейчас простая эвристика:
          • тренд BTC на 1d
          • среднедневная волатильность за 30 дней
    """
    symbol = "BTCUSDT"
    data = await _fetch_direct_bundle(symbol, ("1d", "4h", "1h", "15m"))
    candles_1d: List[Candle] = data.get("1d") or [] if data else []
    candles_4h: List[Candle] = data.get("4h") or [] if data else []
    candles_1h: List[Candle] = data.get("1h") or [] if data else []
    candles_15m: List[Candle] = data.get("15m") or [] if data else []
    if not candles_1d:
        return {
            "regime": "neutral",
            "description": "Нет данных по BTC.",
            "reason": "Нет данных по BTC.",
        }

    closes = [c.close for c in candles_1d[-30:]]
    if len(closes) < 5:
        return {
            "regime": "neutral",
            "description": "Слишком мало данных по BTC.",
            "reason": "Слишком мало данных по BTC.",
        }

    start = closes[0]
    end = closes[-1]
    change_pct = (end - start) / start * 100

    # волатильность (среднее абсолютное изменение дня к дню)
    day_moves = [abs(closes[i] - closes[i - 1]) / closes[i - 1] * 100 for i in range(1, len(closes))]
    vol = sum(day_moves) / len(day_moves)

    regime = "neutral"
    desc_parts = [f"Изменение BTC за 30д: {change_pct:+.2f}%;", f"средняя дневная волатильность: {vol:.2f}%"]

    if change_pct > 15 and vol <= 7:
        regime = "risk_on"
        desc_parts.append("рынок растёт стабильно — инвесторы готовы к риску.")
    elif change_pct < -10 and vol >= 7:
        regime = "risk_off"
        desc_parts.append("рынок под давлением, повышенная волатильность — осторожный режим.")
    else:
        regime = "neutral"
        desc_parts.append("режим ближе к нейтральному, без явного перекоса.")

    trend_1d = detect_trend_and_structure(candles_1d)["trend"] if candles_1d else "range"
    trend_4h = detect_trend_and_structure(candles_4h)["trend"] if candles_4h else "range"
    trend_1h = detect_trend_and_structure(candles_1h)["trend"] if candles_1h else "range"

    closes_15m = [c.close for c in candles_15m]
    rsi_15m_series = _compute_rsi_series(closes_15m) if closes_15m else []
    rsi_15m_value = rsi_15m_series[-1] if rsi_15m_series else 50.0

    closes_1h = [c.close for c in candles_1h]
    ema_fast = compute_ema(closes_1h, 50) if len(closes_1h) >= 50 else None
    ema_slow = compute_ema(closes_1h, 200) if len(closes_1h) >= 200 else None

    vols_15m = [c.volume for c in candles_15m[-21:-1]]
    last_vol = candles_15m[-1].volume if candles_15m else 0.0
    avg_vol = mean(vols_15m) if vols_15m else 0.0
    volume_ratio_15m = last_vol / avg_vol if avg_vol > 0 else 0.0

    reason = " ".join(desc_parts).strip()

    return {
        "regime": regime,
        "description": " ".join(desc_parts),
        "reason": reason,
        "trend_1d": trend_1d,
        "trend_4h": trend_4h,
        "trend_1h": trend_1h,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "rsi_15m": rsi_15m_value,
    }


async def _fetch_direct_bundle(
    symbol: str,
    tfs: tuple[str, ...],
) -> Optional[Dict[str, List[Candle]]]:
    if is_binance_degraded():
        return None
    limits = {
        "1d": 60,
        "4h": 120,
        "1h": 120,
        "15m": 160,
    }
    tasks = [asyncio.create_task(get_klines(symbol, tf, limits[tf])) for tf in tfs]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    bundle: Dict[str, List[Candle]] = {}
    for tf, result in zip(tfs, results):
        if isinstance(result, BaseException) or not isinstance(result, list):
            return None
        candles = normalize_klines(result)
        if not candles or len(candles) < 20:
            return None
        bundle[tf] = candles
    return bundle
