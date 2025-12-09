from typing import Any, Dict, List

from binance_client import Candle


async def analyze_ai_patterns(
    symbol: str,
    candles_1h: List[Candle],
    candles_15m: List[Candle],
    candles_5m: List[Candle],
) -> Dict[str, Any]:
    """
    Анализ паттернов по графику.

    Сейчас: чисто технические эвристики (double top/bottom, локальный разворот и т.п.).
    Codex:
      - можно заменить/расширить вызовом OpenAI / ML модели, которая смотрит
        на свечи как на "фотографию" и классифицирует паттерн.

    Возвращает:
      {
        "pattern_trend": "bullish" | "bearish" | "neutral",
        "pattern_name": str | None,
        "pattern_strength": int (0-100),
      }
    """

    def _last_swings(closes: List[float]) -> float:
        if len(closes) < 4:
            return 0.0
        return (closes[-1] - closes[-4]) / closes[-4] * 100

    closes_1h = [c.close for c in candles_1h]
    closes_15 = [c.close for c in candles_15m]
    closes_5 = [c.close for c in candles_5m]

    move_1h = _last_swings(closes_1h)
    move_15 = _last_swings(closes_15)
    move_5 = _last_swings(closes_5)

    pattern_trend = "neutral"
    pattern_name = None
    strength = 0

    # Примитивная логика (Codex может заменить на AI):
    # 1) Быстрый откат после сильного движения вверх → потенциальная вершина.
    if move_1h > 5 and move_15 < -2 and move_5 < -1:
        pattern_trend = "bearish"
        pattern_name = "Проба вершины / локальный разворот вниз"
        strength = 70

    # 2) Сильный пролив + откуп на младших ТФ → потенциальное дно.
    elif move_1h < -5 and move_15 > 2 and move_5 > 1:
        pattern_trend = "bullish"
        pattern_name = "Проба дна / локальный разворот вверх"
        strength = 70

    return {
        "pattern_trend": pattern_trend,
        "pattern_name": pattern_name,
        "pattern_strength": strength,
    }
