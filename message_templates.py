from __future__ import annotations

from typing import Optional


def _trend_to_text(value: Optional[str]) -> str:
    if value in ("up", "bullish"):
        return "бычий"
    if value in ("down", "bearish"):
        return "медвежий"
    return "флет"


def _rsi_zone(value: float) -> str:
    if value >= 70:
        return "перекуплен"
    if value <= 30:
        return "перепродан"
    return "нейтр"


def _format_price(value: float, precision: int) -> str:
    return f"{value:.{precision}f}"


def _format_pct(value: float) -> str:
    return f"{value:+.1f}%"


def format_scenario_message(
    *,
    symbol_text: str,
    side: str,
    timeframe: str,
    entry_from: float,
    entry_to: float,
    sl: float,
    tp1: float,
    tp2: float,
    score: int,
    trend_1d: Optional[str],
    trend_4h: Optional[str],
    rsi_1h: float,
    volume_ratio: float,
    rr: float,
    price_precision: int,
    score_breakdown: Optional[list[dict]] = None,
) -> str:
    is_long = side == "LONG"
    emoji = "📈" if is_long else "📉"
    scenario_text = "LONG" if is_long else "SHORT"
    entry_mid = (entry_from + entry_to) / 2

    score = max(0, min(100, int(score)))

    holds_rule = "удерживается выше зоны" if is_long else "удерживается ниже зоны"
    stop_condition = "ниже" if is_long else "выше"
    invalid_level = _format_price(sl, price_precision)

    tp_candidates = [tp1, tp2]
    if is_long:
        tp_candidates = sorted(tp_candidates)
        targets_invalid = max(tp_candidates) <= entry_mid
    else:
        tp_candidates = sorted(tp_candidates, reverse=True)
        targets_invalid = min(tp_candidates) >= entry_mid

    if targets_invalid:
        tp_lines = [
            "🎯 TP1: требуют уточнения",
            "🎯 TP2: требуют уточнения",
        ]
    else:
        tp1_val, tp2_val = tp_candidates
        tp1_pct = (tp1_val / entry_mid - 1) * 100
        tp2_pct = (tp2_val / entry_mid - 1) * 100
        tp_lines = [
            f"🎯 TP1: {_format_price(tp1_val, price_precision)} ({_format_pct(tp1_pct)})",
            f"🎯 TP2: {_format_price(tp2_val, price_precision)} ({_format_pct(tp2_pct)})",
        ]

    breakdown_items = score_breakdown or []
    label_map = {
        "global_trend": "Глобальный тренд (1D)",
        "local_trend": "Локальный тренд (1H)",
        "near_key_level": "Реакция на ключевую зону (POI)",
        "liquidity_sweep": "Снос ликвидности",
        "volume_climax": "Объём относительно среднего",
        "rsi_divergence": "RSI-дивергенция",
        "atr_ok": "Волатильность (ATR)",
        "bb_extreme": "Экстремум Bollinger",
        "ma_trend_ok": "EMA-согласование",
        "orderflow": "Ордерфлоу",
        "whale_activity": "Китовая активность",
        "ai_pattern": "AI-паттерны",
        "market_regime": "Рыночный режим",
    }
    breakdown_lines = []
    for item in breakdown_items:
        key = item.get("key")
        label = item.get("label")
        if key in label_map:
            label = label_map[key]
        label = label or key or "Фактор"
        delta = item.get("points", item.get("delta", 0))
        try:
            delta_value = int(round(float(delta)))
        except (TypeError, ValueError):
            delta_value = 0
        sign = "−" if delta_value < 0 else "+"
        breakdown_lines.append(f"• {label}: {sign}{abs(delta_value)}")

    lines = [
        symbol_text,
        f"{emoji} Возможный {scenario_text}",
        f"⏱ Таймфрейм сценария: {timeframe} | Вход: 5–15m",
        "",
        "Зона интереса (POI):",
        f"• {_format_price(entry_from, price_precision)} – {_format_price(entry_to, price_precision)}",
        "",
        "Условия реализации:",
        f"• сценарий актуален, пока цена {holds_rule}",
        "• вход рассматривается только после подтверждения на 5–15m",
        "",
        "🔎 Подтверждение на 5–15m:",
        (
            "• закрытие свечи по направлению (выше зоны для LONG / ниже для SHORT)"
        ),
        "• цена удерживается вне зоны без быстрого возврата",
        "",
        "Отмена сценария:",
        f"• если 1H свеча закроется {stop_condition} {invalid_level}",
        "",
        "Потенциальные цели:",
        *tp_lines,
        "",
        "Краткий контекст:",
        f"• Тренд 1D / 4H: {_trend_to_text(trend_1d)} / {_trend_to_text(trend_4h)}",
        f"• RSI 1H: {rsi_1h:.1f} ({_rsi_zone(rsi_1h)})",
        f"• Объём: {volume_ratio:.2f}x к среднему",
        f"• RR ≈ 1 : {rr:.2f}",
        "",
        f"🧠 Score: {score} / 100",
        "",
        "🧩 Детали Score (сумма баллов):",
        *breakdown_lines,
        f"= Итоговая оценка: {score}",
        "",
        "ℹ️ Score — внутренняя оценка качества сценария, основанная на рыночных факторах и условиях модели.",
        "ℹ️ Бот ищет сетапы, не гарантирует прибыль.",
        "ℹ️ Сценарий требует подтверждения перед входом.",
    ]
    return "\n".join(lines)
