from typing import Any, Dict, List

from binance_client import Candle, get_required_candles


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
    data = await get_required_candles(symbol)
    candles_1d: List[Candle] = data.get("1d") or []
    if not candles_1d:
        return {"regime": "neutral", "description": "Нет данных по BTC."}

    closes = [c.close for c in candles_1d[-30:]]
    if len(closes) < 5:
        return {"regime": "neutral", "description": "Слишком мало данных по BTC."}

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

    return {
        "regime": regime,
        "description": " ".join(desc_parts),
    }
